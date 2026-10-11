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
