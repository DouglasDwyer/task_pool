# task_pool

[![Crates.io](https://img.shields.io/crates/v/task_pool.svg)](https://crates.io/crates/task_pool)
[![Docs.rs](https://docs.rs/task_pool/badge.svg)](https://docs.rs/task_pool)

`task_pool` offers a flexible abstraction for composing and distributing work within a fixed hardware threadpool. To that end, it offers the following features:

- The ability to define and compose sources of work
- The ability to create hardware threadpool and consume those sources
- A variety of high-level abstractions for scheduling, such as awaitable tasks

### Usage

To use `task_pool`, there are three steps:

1. Creating and initializing `WorkProvider` instances (such as a queue or chain of multiple queues)
2. Creating a hardware `TaskPool` which consumes those instances
3. Spawning high-level tasks on the `WorkProvider`s which are handled by the threadpool

The following example shows these steps in action:

```rust
// 1. Create a queue from which we can spawn tasks
let queue = TaskQueue::<Fifo>::default();

// 2. Create a threadpool that draws from the provided queue. Forget the threadpool so that it runs indefinitely.
TaskPool::new(queue.clone(), 4).forget();

// 3. Spawn a task into the queue and synchronously await its completion.
assert_eq!(queue.spawn(once(|| { println!("This will execute on background thread."); 2 })).join(), 2);

// ...or, asynchronously await its completion.
assert_eq!(queue.spawn(once(|| { println!("This will execute on background thread."); 2 })).await, 2);
```