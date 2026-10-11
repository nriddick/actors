# Ant Farm version

The `d/` benchmark ported to [Ant Farm](https://github.com/nriddick/antfarm)
public actors, kept as close to the `std.concurrency` code as the library
allows:

| `d/` (std.concurrency) | `antfarm/` |
| --- | --- |
| `spawn` per node (one OS thread each) | `runtime.createActor` per node, run by a threadpool worker on every logical processor |
| `InitMsg` | initial actor state |
| `send(tid, ColorMsg)` | `handle.send(&node)` with a node owned by the sender, one in flight at a time |
| `StatusReply` | the receiver fails the node; the sender reads the outcome |
| `receiveTimeout` loop | the handler runs when a message or a reply wakes it |
| coordinator in `main` | a coordinator actor; `main` publishes ready actors and waits for its count |
| `StopActor` | `requestRetire`, drain, `reclaim` (after the timer) |

Neighbour order is sorted in `main` before the timer starts; `d/` sorts it
inside each spawned thread.

## Building

Needs LDC 1.42+ or DMD 2.112+. Ant Farm is fetched from GitHub at a pinned
commit. Linux x86-64 is the tested target.

```
dub build -b release --compiler=ldc2
./actors
```

## Porting notes

The handler body maps almost line for line onto the `receiveTimeout`
callbacks. The friction was in the plumbing around it and in D idioms that do
not fit `nothrow @nogc` handlers over POD state.

**Plumbing**

- About 60 lines of setup and teardown that `std.concurrency` does not need:
  the Farm, a producer token, the runtime, threadpool `install`/`home`, a
  thread-local `ConsumerView` per worker, and an ordered shutdown. The package
  has no "runtime with N workers" helper.
- `send` and `wake` only mark an actor ready. A thread holding a producer token
  has to `runtime.flush` it into the Farm, so `main` loops on `flush` while it
  waits.
- Completing or failing an inbox node does not wake the sender, unlike a
  `StatusReply` arriving in a mailbox. The receiver calls `wake()` on the
  sender explicitly; leaving it out hangs quietly.
- A node is in flight at most once, so "send on every loop pass" became "one
  outstanding `ColorMsg` per sender". Nodes live in `__gshared` arrays rather
  than in actor state.

**D idioms**

- `ActorOwner`'s copy constructor transfers ownership, so Phobos algorithms
  that take elements by value move owners out of an array:
  `owners.all!(o => o.retired)` leaves `owners[0]` invalid. Retirement is
  checked with a `foreach (ref ...)` loop instead.
- `ref Dot self = actor.value;` does not compile; the property needs explicit
  parentheses, `actor.value()`.
- No associative arrays in actor state: `ubyte[ubyte] freq` is `ubyte[256]`,
  and the coordinator's `Tid`-keyed maps are fixed arrays indexed by node id.
- No GC closures in handlers, so the neighbour sort with its capturing lambda
  moved to `main`.
- A GC slice in POD actor state compiles, but actor state is malloc'd and not
  scanned. `sortedNeighbors` is valid only because `main` keeps the arrays
  alive.
- No type-based dispatch: the inbox payload is a `void*` cast to the message
  type. That is painless with one message type per actor; `d/`'s four message
  types each became a different mechanism.
