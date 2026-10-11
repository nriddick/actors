// The d/ benchmark on Ant Farm public actors instead of std.concurrency.
//
// spawn           -> runtime.createActor (InitMsg data is the initial state)
// send(Tid, msg)  -> handle.send(&node), a caller-owned inbox node
// StatusReply     -> the receiver fails the node; the sender reads the outcome
// receiveTimeout  -> the handler runs when a message or a reply wakes it
// StopActor       -> owner.requestRetire, then reclaim
//
// Actors run on a threadpool worker per logical processor. The main thread
// publishes ready actors into the Farm and waits for the coordinator's count.
import antfarm;
import actors;
import threadpool;
import core.atomic;
import core.thread;
import std.stdio;
import std.random;
import std.math;
import std.algorithm;
import std.range;
import std.datetime.stopwatch;

enum totalNodes = 800;
enum THRESHOLD = 3;
enum ubyte uncolored = 255;
enum uint alreadyColored = 1; // node failure code standing in for StatusReply

// ----------------------
// Messages
// ----------------------
struct ColorMsg
{
    uint sender;
    ubyte color;
}

struct ColoredMsg
{
    uint who;
    ubyte color;
}

// ----------------------
// Shared tables, filled before any actor runs
// ----------------------
// Each dot owns one outgoing ColorMsg node (one message in flight at a time)
// and one ColoredMsg node for its single report to the coordinator.
__gshared ActorHandle!Dot[] dots;
__gshared ActorHandle!Coordinator coordinator;
__gshared ActorInboxNode[] colorNodes;
__gshared ColorMsg[] colorMsgs;
__gshared ActorInboxNode[] coloredNodes;
__gshared ColoredMsg[] coloredMsgs;
shared uint coloredCount;

bool accepted(ActorSendResult result) nothrow @nogc @safe
{
    return result == ActorSendResult.queued
        || result == ActorSendResult.coalesced;
}

// ----------------------
// Dot Actor
// ----------------------
struct Dot
{
    uint id;
    ubyte color;
    ubyte threshold;
    bool announced;
    bool sending;
    ubyte[256] freq;
    size_t neighborIndex;
    immutable(uint)[] sortedNeighbors;
}

void dotActor(scope ref ActorBorrow!Dot actor, scope ref ActorContext context)
    nothrow @nogc @system
{
    ref Dot self = actor.value();

    for (auto node = context.popInbox(); node !is null; node = context.popInbox())
    {
        auto msg = cast(ColorMsg*) node.payload;
        immutable sender = msg.sender;
        if (self.color == uncolored)
        {
            if (++self.freq[msg.color] >= self.threshold)
                self.color = msg.color;
            node.complete();
        }
        else
        {
            // already colored: inform sender
            node.fail(alreadyColored);
        }
        dots[sender].wake();
    }

    if (self.color == uncolored)
        return;

    if (!self.announced)
    {
        coloredMsgs[self.id] = ColoredMsg(self.id, self.color);
        if (!accepted(coordinator.send(&coloredNodes[self.id])))
            fatal("coordinator rejected ColoredMsg");
        self.announced = true;
    }

    auto outgoing = &colorNodes[self.id];
    if (!outgoing.available)
        return; // the target still holds it and wakes us when done
    if (self.sending)
    {
        self.sending = false;
        if (outgoing.outcome == ActorNodeOutcome.failed)
            self.neighborIndex++;
    }

    if (!context.closing && self.neighborIndex < self.sortedNeighbors.length)
    {
        immutable target = self.sortedNeighbors[self.neighborIndex];
        colorMsgs[self.id] = ColorMsg(self.id, self.color);
        self.sending = accepted(dots[target].send(outgoing));
    }
}

// ----------------------
// Coordinator Actor
// ----------------------
struct Coordinator
{
    bool[totalNodes] coloredMap;
    ubyte[totalNodes] colorsMap;
    uint coloredCount;
}

void coordinatorActor(scope ref ActorBorrow!Coordinator actor,
        scope ref ActorContext context) nothrow @nogc @system
{
    ref Coordinator self = actor.value();
    for (auto node = context.popInbox(); node !is null; node = context.popInbox())
    {
        auto msg = cast(ColoredMsg*) node.payload;
        if (!self.coloredMap[msg.who])
        {
            self.coloredMap[msg.who] = true;
            self.colorsMap[msg.who] = msg.color;
            self.coloredCount++;
            atomicStore!(MemoryOrder.rel)(.coloredCount, self.coloredCount);
        }
        node.complete();
    }
}

// ----------------------
// Workers: one Farm consumer per pinned threadpool worker
// ----------------------
struct FarmBin
{
    AntFarm* farm;
}

ConsumerView workerView;
bool workerSubscribed;

bool pump(WorkerSelf* worker) nothrow @nogc @system
{
    auto bin = home!FarmBin();
    if (bin is null)
        return false;
    if (!workerSubscribed)
        workerSubscribed = workerView.subscribe(bin.farm) >= 0;
    if (!workerSubscribed)
        return false;
    return workerView.consumeNext();
}

void stopWorker(WorkerSelf* worker) nothrow @nogc @system
{
    if (workerSubscribed)
    {
        workerView.unsubscribe();
        workerSubscribed = false;
    }
}

// ----------------------
// Main
// ----------------------
void main()
{
    auto xs = totalNodes.iota.map!(e => uniform(50.0, 750.0)).array;
    auto ys = totalNodes.iota.map!(e => uniform(50.0, 550.0)).array;

    // Neighbor order per dot (d/ sorts this inside each spawned thread).
    auto neighbors = new immutable(uint)[][](totalNodes);
    foreach (uint id; 0 .. totalNodes)
    {
        auto order = totalNodes.iota.filter!(i => i != id).map!(i => cast(uint) i).array;
        immutable x = xs[id], y = ys[id];
        order.sort!((a, b) =>
                sqrt((x - xs[a]) ^^ 2 + (y - ys[a]) ^^ 2) <
                sqrt((x - xs[b]) ^^ 2 + (y - ys[b]) ^^ 2)
        );
        neighbors[id] = order.idup;
    }

    // Farm and threadpool: every logical processor consumes.
    auto topology = CacheAwarePool.topology();
    immutable workers = cast(uint) topology.logicalProcessorCount;
    auto farm = AntFarm.create(ringMiB: 2, expectedConsumers: workers,
        maxBulk: 0, maxSmall: 1, quotaSmall: 4096);
    if (farm is null)
        fatal("farm creation failed");
    auto token = farm.registerProducer(Tier.small);
    if (!token.valid)
        fatal("producer registration failed");
    auto runtime = ActorRuntime.create(farm, totalNodes + 1);
    if (runtime is null)
        fatal("runtime creation failed");

    auto bins = new FarmBin[](topology.llcCount);
    foreach (ref bin; bins)
        bin.farm = farm;
    install(bins);

    PoolOptions options;
    options.workerBody = &pump;
    options.workerStop = &stopWorker;
    auto pool = new CacheAwarePool(options);
    pool.start();
    pool.director().spin();

    // create actors with their InitMsg contents as initial state
    dots = new ActorHandle!Dot[](totalNodes);
    colorNodes = new ActorInboxNode[](totalNodes);
    colorMsgs = new ColorMsg[](totalNodes);
    coloredNodes = new ActorInboxNode[](totalNodes);
    coloredMsgs = new ColoredMsg[](totalNodes);

    auto coordinatorOwner = runtime.createActor!(Coordinator, coordinatorActor)(Coordinator.init);
    if (!coordinatorOwner.valid)
        fatal("coordinator creation failed");
    coordinator = coordinatorOwner.handle;

    auto owners = new ActorOwner!Dot[](totalNodes);
    foreach (uint i; 0 .. totalNodes)
    {
        colorNodes[i].initialize(&colorMsgs[i], i);
        coloredNodes[i].initialize(&coloredMsgs[i], i);

        Dot initial;
        initial.id = i;
        initial.color = i < 1 ? cast(ubyte) i : uncolored;
        initial.threshold = THRESHOLD;
        initial.sortedNeighbors = neighbors[i];
        owners[i] = runtime.createActor!(Dot, dotActor)(initial);
        if (!owners[i].valid)
            fatal("actor creation failed");
        dots[i] = owners[i].handle;
    }

    StopWatch sw;
    sw.start();

    // seeds announce themselves and start sending
    foreach (i; 0 .. totalNodes)
        if (i < 1)
            dots[i].wake();

    while (atomicLoad!(MemoryOrder.acq)(coloredCount) < totalNodes)
        runtime.flush(token);

    sw.stop();

    // stop actors: close admission, drain, reclaim
    foreach (ref owner; owners)
        owner.requestRetire();
    coordinatorOwner.requestRetire();
    // ActorOwner copies transfer ownership, so check retirement by ref.
    bool allRetired()
    {
        foreach (ref owner; owners)
            if (!owner.retired)
                return false;
        return coordinatorOwner.retired;
    }

    while (!allRetired())
        runtime.flush(token);
    foreach (ref owner; owners)
        while (owner.reclaim() == ActorReclaimResult.busy)
            runtime.flush(token);
    while (coordinatorOwner.reclaim() == ActorReclaimResult.busy)
        runtime.flush(token);

    pool.shutdown();
    uninstall!FarmBin();
    runtime.destroy();
    farm.unregisterProducer(token);
    farm.destroy();

    writeln("\nAll nodes colored!");
    writeln("Time: ", sw.peek.total!"msecs", " ms");
}
