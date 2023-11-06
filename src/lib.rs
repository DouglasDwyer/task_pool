#![deny(warnings)]
#![warn(missing_docs)]
#![warn(clippy::missing_docs_in_private_items)]

//! `task_pool` offers a flexible abstraction for composing and distributing work within a fixed hardware threadpool. To that end, it offers the following features:
//!
//! - The ability to define and compose sources of work
//! - The ability to create hardware threadpool and consume those sources
//! - A variety of high-level abstractions for scheduling, such as awaitable tasks
//!
//! ### Usage
//!
//! To use `task_pool`, there are three steps:
//!
//! 1. Creating and initializing [`WorkProvider`] instances (such as a queue or chain of multiple queues)
//! 2. Creating a hardware [`TaskPool`] which consumes those instances
//! 3. Spawning high-level tasks on the [`WorkProvider`]s which are handled by the threadpool
//!
//! The following example shows these steps in action:
//!
//! ```rust
//! # use task_pool::*;
//! // 1. Create a queue from which we can spawn tasks
//! let queue = TaskQueue::<Fifo>::default();
//!
//! // 2. Create a threadpool that draws from the provided queue. Forget the threadpool so that it runs indefinitely.
//! TaskPool::new(queue.clone(), 4).forget();
//!
//! // 3. Spawn a task into the queue and synchronously await its completion.
//! assert_eq!(queue.spawn(once(|| { println!("This will execute on background thread."); 2 })).join(), 2);
//!
//! // ...or, asynchronously await its completion.
//! # async fn hide() {
//! # let queue = TaskQueue::<Fifo>::default();
//! assert_eq!(queue.spawn(once(|| { println!("This will execute on background thread."); 2 })).await, 2);
//! # }
//! ```

use arc_swap::*;
use fxhash::*;
use priority_queue::priority_queue::*;
use private::*;
use std::collections::*;
use std::future::*;
use std::hash::*;
use std::marker::*;
use std::mem::*;
use std::ops::*;
use std::sync::atomic::*;
use std::sync::*;
use std::task::*;
use takecell::*;

/// A persistent source of work for multiple threads.
pub trait WorkProvider: 'static + Send + Sync {
    /// Gets a reference to the notifier which raises an event when new work is available.
    fn change_notifier(&self) -> &ChangeNotifier;
    /// Obtains the next unit of queued work from the provider.
    fn next_task(&self) -> Option<Box<dyn '_ + WorkUnit>>;
}

/// A provider which multiplexes work units from other providers, in a fixed priority order.
#[derive(Default)]
pub struct ChainedWorkProvider {
    /// The notifier used to alert listeners when new work is available.
    notifier: Arc<ChangeNotifier>,
    /// The set of providers from which work will be drawn.
    providers: Vec<ChainedWorkProviderEntry>,
}

impl ChainedWorkProvider {
    /// Adds a provider to the chain. When the chain is queried for new work, it
    /// will attempt to use this provider when all previously-added providers are empty.
    pub fn with(mut self, provider: impl WorkProvider) -> Self {
        let notifier_cloned = self.notifier.clone();
        let listener = provider
            .change_notifier()
            .add_listener(move || notifier_cloned.notify());
        self.providers.push(ChainedWorkProviderEntry {
            listener,
            provider: Box::new(provider),
        });
        self
    }
}

impl WorkProvider for ChainedWorkProvider {
    fn change_notifier(&self) -> &ChangeNotifier {
        &self.notifier
    }

    fn next_task(&self) -> Option<Box<dyn '_ + WorkUnit>> {
        for entry in &self.providers {
            if let Some(task) = entry.provider.next_task() {
                return Some(task);
            }
        }

        None
    }
}

impl std::fmt::Debug for ChainedWorkProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChainedWorkProvider").finish()
    }
}

/// Stores one provider in a provider chain.
#[allow(dead_code)]
struct ChainedWorkProviderEntry {
    /// The listener handle which ensures that notifications from the provider are received.
    pub listener: ChangeNotificationListener,
    /// The work provider.
    pub provider: Box<dyn WorkProvider>,
}

/// A single, atomic unit of work that one thread should process.
pub trait WorkUnit {
    /// Executes this task on the current thread.
    fn execute(self: Box<Self>);
}

impl<F: FnOnce()> WorkUnit for F {
    fn execute(self: Box<Self>) {
        self();
    }
}

/// Offers a composable way to listen for the availability of new work from providers.
#[derive(Default)]
pub struct ChangeNotifier {
    /// The listeners that are registered to this change notifier.
    listeners: wasm_sync::RwLock<Vec<Weak<dyn Fn() + Send + Sync>>>,
}

impl ChangeNotifier {
    /// Informs all registered listeners that a change has occurred.
    pub fn notify(&self) {
        for listener in &*self.listeners.read().expect("Could not acquire read lock.") {
            if let Some(to_execute) = listener.upgrade() {
                to_execute();
            }
        }
    }

    /// Registers the specified callback to be invoked upon change. Returns a listener that
    /// must be kept alive to receive notifications.
    pub fn add_listener(
        &self,
        listener: impl 'static + Fn() + Send + Sync,
    ) -> ChangeNotificationListener {
        let mut listeners = self
            .listeners
            .write()
            .expect("Could not acquire write lock.");
        Self::clear_dead_listeners(&mut listeners);
        let result = Arc::new(listener) as Arc<dyn Fn() + Send + Sync>;
        listeners.push(Arc::downgrade(&result));
        ChangeNotificationListener(result)
    }

    /// Removes all dead listeners from the listeners list.
    fn clear_dead_listeners(listeners: &mut Vec<Weak<dyn Fn() + Send + Sync>>) {
        unsafe {
            let mut finish = 0;
            let len = listeners.len();
            listeners.set_len(0);
            let view = listeners.spare_capacity_mut();

            for i in 0..len {
                let item = view.get_unchecked_mut(i);
                if item.assume_init_ref().strong_count() == 0 {
                    item.assume_init_drop();
                } else {
                    view.swap(i, finish);
                    finish += 1;
                }
            }

            listeners.set_len(finish);
        }
    }
}

impl std::fmt::Debug for ChangeNotifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChangeNotifier").finish()
    }
}

/// Manages the lifetime of a registered change notification callback. Upon drop,
/// the associated callback will no longer be invoked.
pub struct ChangeNotificationListener(Arc<dyn Fn() + Send + Sync>);

impl std::fmt::Debug for ChangeNotificationListener {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("ChangeNotificationListener").finish()
    }
}

/// Controls a set of background threads that execute work from a provider.
#[allow(dead_code)]
pub struct TaskPool {
    /// The listener and control block.
    change_listener_inner: Option<(ChangeNotificationListener, Arc<TaskPoolInner>)>,
}

impl TaskPool {
    /// Creates a new pool that draws work from the given provider, with the specified number of background threads.
    pub fn new(provider: impl WorkProvider, threads: usize) -> Self {
        Self::with_spawner(provider, threads, |_, f| {
            std::thread::spawn(f);
        })
    }

    /// Creates a new pool that draws work from the given provider, with the specified number of background threads.
    /// The custom spawning function is invoked to create each thread.
    pub fn with_spawner(
        provider: impl WorkProvider,
        threads: usize,
        mut spawner: impl FnMut(usize, Box<dyn 'static + FnOnce() + Send>),
    ) -> Self {
        let inner = Arc::new(TaskPoolInner::new(provider));

        for id in 0..threads {
            let inner_clone = inner.clone();
            spawner(id, Box::new(move || inner_clone.run()));
        }

        let inner_clone = inner.clone();
        let change_listener = inner
            .provider()
            .change_notifier()
            .add_listener(move || inner_clone.notify_changed());

        Self {
            change_listener_inner: Some((change_listener, inner)),
        }
    }

    /// Drops this task pool without stopping the associated threads. The threads
    /// become leaked, and will run for the program's duration.
    pub fn forget(mut self) {
        unsafe {
            forget(
                replace(&mut self.change_listener_inner, None)
                    .unwrap_unchecked()
                    .0,
            );
        }
    }
}

impl Drop for TaskPool {
    fn drop(&mut self) {
        if let Some((_, inner)) = &self.change_listener_inner {
            inner.stop();
        }
    }
}

/// Coordinates work between threads for a task pool.
struct TaskPoolInner {
    /// The provider from which work can be drawn.
    work_provider: Box<dyn WorkProvider>,
    /// A counter which is used to determine when new tasks become available.
    task_counter: AtomicI32,
    /// A condition variable which is notified whenever the provider has new work.
    on_change: wasm_sync::Condvar,
    /// A lock utilized to ensure coherency between the task counter values that threads observe.
    lock: wasm_sync::Mutex<()>,
}

impl TaskPoolInner {
    /// Creates a new control block using the given provider.
    pub fn new(provider: impl WorkProvider) -> Self {
        Self {
            work_provider: Box::new(provider),
            task_counter: AtomicI32::new(1),
            on_change: wasm_sync::Condvar::default(),
            lock: wasm_sync::Mutex::default(),
        }
    }

    /// Gets the provider associated with this pool.
    pub fn provider(&self) -> &dyn WorkProvider {
        &*self.work_provider
    }

    /// Notifies the pool that new work is available from the provider.
    #[allow(unused_variables)]
    pub fn notify_changed(&self) {
        let guard = self.lock.lock().expect("Could not acquire mutex.");
        let old_value = self.task_counter.load(Ordering::Acquire);
        if old_value.is_negative() {
            let mut new_value = -old_value + 1;
            if new_value == i32::MAX - 1 {
                new_value = 1;
            }
            self.task_counter.store(new_value, Ordering::Release);
            self.on_change.notify_all();
        }
        else {
            let mut new_value = old_value + 1;
            if new_value == i32::MAX - 1 {
                new_value = 1;
            }
            self.task_counter.store(new_value, Ordering::Release);
        }
    }

    /// Executes the work in this pool as a background thread, repeatedly loading
    /// new work and sleeping when none is available.
    pub fn run(&self) {
        loop {
            let task_value = self.task_counter.load(Ordering::Acquire);
            match task_value.cmp(&0) {
                std::cmp::Ordering::Less => self.wait_for_change::<false>(task_value),
                std::cmp::Ordering::Equal => return,
                std::cmp::Ordering::Greater => {
                    if let Some(unit) = self.work_provider.next_task() {
                        unit.execute();
                    } else {
                        self.wait_for_change::<true>(task_value);
                    }
                }
            }
        }
    }

    /// Stops this pool and cancels all threads.
    #[allow(unused_variables)]
    pub fn stop(&self) {
        let guard = self.lock.lock().expect("Could not acquire mutex.");
        self.task_counter.store(0, Ordering::Release);
        self.on_change.notify_all();
    }

    /// Waits for new work to become available based upon the previous task value.
    fn wait_for_change<const FLIP_COUNTER: bool>(&self, task_value: i32) {
        let guard = self.lock.lock().expect("Could not acquire mutex.");
        let mut new_value = self.task_counter.load(Ordering::Acquire);

        if FLIP_COUNTER && new_value == task_value {
            new_value = -new_value;
            self.task_counter.store(new_value, Ordering::Release);
        }

        if new_value.is_negative() {
            drop(self.on_change.wait(guard));
        }
    }
}

/// Provides a group of work units that compose a task.
pub trait TaskProvider: 'static + Send + Sync {
    /// Gets the next task to execute.
    fn next_task(&self) -> Option<Box<dyn WorkUnit>>;
}

/// Represents a task that returns a result of the given type.
pub trait TaskCollection<T>: TaskProvider + Sized {
    /// Gets the result from this task.
    fn result(&self) -> T;
}

/// A handle to a queued group of work units, which output a single result.
#[derive(Debug)]
pub struct Task<T, B: QueueBacking> {
    /// The control block for this task.
    control: Arc<TaskControl>,
    /// A pointer to the function which extracts the result for the task.
    result: fn(*const ()) -> T,
    /// The task queue which owns the control block.
    backing: Arc<TaskQueueHolder<B>>,
}

impl<T, B: QueueBacking> Task<T, B> {
    /// Creates a new task for the given collection and backing.
    fn new<C: TaskCollection<T>>(provider: C, backing: Arc<TaskQueueHolder<B>>) -> Self {
        unsafe {
            let control = Arc::new(TaskControl::new(provider));
            let result = transmute(C::result as fn(&C) -> T);

            Self {
                control,
                result,
                backing,
            }
        }
    }

    /// Gets the control block for this task.
    fn control(&self) -> Arc<TaskControl> {
        self.control.clone()
    }

    /// Cancels this task, preventing any further threads from performing its work.
    pub fn cancel(self) {
        self.control.cancel();
    }

    /// Whether the task has been completed yet.
    pub fn complete(&self) -> bool {
        self.control.complete()
    }

    /// Attempts to get the result of this task if it has been completed. Otherwise, returns
    /// the original task.
    pub fn result(self) -> Result<T, Self> {
        if self.complete() {
            Ok(self.join())
        } else {
            Err(self)
        }
    }

    /// Joins the current thread with this task, completing all remaining work.
    /// After all work is complete, yields the result.
    pub fn join(self) -> T {
        unsafe {
            while let Some(work) = self.control.collection().next_task() {
                work.execute();
            }

            self.control.cancel();

            if self.complete() {
                self.get_result()
            } else {
                let waker = CondvarWaker::default();
                let guard = waker.lock.lock().unwrap_unchecked();
                self.control.set_result_waker(waker.as_waker());
                drop(waker.on_wake.wait_while(guard, |()| !self.complete()));
                self.get_result()
            }
        }
    }

    /// Joins with the remaining work on this task, completing all units while they are
    /// available. Returns immediately if there are outstanding units in progress on other threads.
    pub fn join_work(&self) {
        while let Some(work) = self.control.collection().next_task() {
            work.execute();
        }
    }

    /// Gets the result of this task.
    unsafe fn get_result(&self) -> T {
        (self.result)(transmute::<_, (*const (), *const ())>(self.control.collection()).0)
    }
}

impl<T, P: Ord + Send> Task<T, Priority<P>> {
    /// Updates the priority of this task to the specified value.
    pub fn set_priority(&mut self, priority: P) {
        unsafe {
            self.backing
                .inner
                .lock()
                .unwrap_unchecked()
                .queued
                .inner
                .change_priority(&PriorityHolder(self.control.clone()), priority);
        }
    }
}

impl<T, B: QueueBacking> Future for Task<T, B> {
    type Output = T;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        unsafe {
            if self.complete() {
                Poll::Ready(self.get_result())
            } else {
                self.control.set_result_waker(cx.waker().clone());
                Poll::Pending
            }
        }
    }
}

/// A structure which interally alerts a condvar upon wake.
#[derive(Clone, Default)]
struct CondvarWaker {
    /// The inner backing for the waker.
    inner: Arc<CondvarWakerInner>,
}

impl CondvarWaker {
    /// Converts this to a waker.
    pub fn as_waker(&self) -> Waker {
        unsafe {
            Waker::from_raw(Self::clone_waker(
                &self.inner as *const Arc<CondvarWakerInner> as *const (),
            ))
        }
    }

    /// Clones the waker.
    ///
    /// # Safety
    ///
    /// For this function to be sound, inner must be a valid pointer to an `Arc<CondvarWakerInner>`.
    unsafe fn clone_waker(inner: *const ()) -> RawWaker {
        unsafe {
            let value = &*(inner as *const Arc<CondvarWakerInner>);
            let data = Box::into_raw(Box::new(value.clone()));

            RawWaker::new(
                data as *const (),
                &RawWakerVTable::new(
                    Self::clone_waker,
                    Self::wake_waker,
                    Self::wake_by_ref_waker,
                    Self::drop_waker,
                ),
            )
        }
    }

    /// Wakes the waker, and consumes the pointer.
    ///
    /// # Safety
    ///
    /// For this function to be sound, inner must be a valid owned pointer to an `Arc<CondvarWakerInner>`.
    unsafe fn wake_waker(inner: *const ()) {
        Self::wake_by_ref_waker(inner);
        Self::drop_waker(inner);
    }

    /// Wakes the waker.
    ///
    /// # Safety
    ///
    /// For this function to be sound, inner must be a valid pointer to an `Arc<CondvarWakerInner>`.
    #[allow(unused_variables)]
    unsafe fn wake_by_ref_waker(inner: *const ()) {
        let inner = &*(inner as *const Arc<CondvarWakerInner>);
        let guard = inner.lock.lock().expect("Could not lock mutex");
        inner.on_wake.notify_all();
    }

    /// Drops the waker, consuming the given pointer.
    ///
    /// # Safety
    ///
    /// For this function to be sound, inner must be a valid owned pointer to an `Arc<CondvarWakerInner>`.
    unsafe fn drop_waker(inner: *const ()) {
        drop(Box::from_raw(inner as *mut Arc<CondvarWakerInner>));
    }
}

impl Deref for CondvarWaker {
    type Target = CondvarWakerInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

/// Stores the inner state for a condition variable waker.
#[derive(Default)]
struct CondvarWakerInner {
    /// The lock that should be used for waiting.
    lock: wasm_sync::Mutex<()>,
    /// The condition variable that is alerted on wake.
    on_wake: wasm_sync::Condvar,
}

/// Marks a task queue as executing events in a first-in-first-out order.
#[derive(Debug)]
pub struct Fifo {
    /// The inner storage for the queue.
    inner: VecDeque<Arc<TaskControl>>,
}

impl QueueBacking for Fifo {
    fn new() -> Self {
        Self {
            inner: VecDeque::new(),
        }
    }

    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    fn next(&mut self) -> Option<Arc<TaskControl>> {
        self.inner.pop_front()
    }
}

impl PushPopQueueBacking for Fifo {
    fn push(&mut self, task: Arc<TaskControl>) {
        self.inner.push_back(task);
    }
}

/// Marks a task queue as executing events in a last-in-first-out order.
#[derive(Debug)]
pub struct Lifo {
    /// The inner storage for the queue.
    inner: Vec<Arc<TaskControl>>,
}

impl QueueBacking for Lifo {
    fn new() -> Self {
        Self { inner: Vec::new() }
    }

    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    fn next(&mut self) -> Option<Arc<TaskControl>> {
        self.inner.pop()
    }
}

impl PushPopQueueBacking for Lifo {
    fn push(&mut self, task: Arc<TaskControl>) {
        self.inner.push(task);
    }
}

/// Implements hashing and reference-equality semantics for task pointers.
#[derive(Debug)]
struct PriorityHolder(pub Arc<TaskControl>);

impl Deref for PriorityHolder {
    type Target = Arc<TaskControl>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl PartialEq for PriorityHolder {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for PriorityHolder {}

impl Hash for PriorityHolder {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_usize(Arc::as_ptr(&self.0) as usize)
    }
}

/// Marks a task queue as executing events in a user-defined priority order.
#[derive(Debug)]
pub struct Priority<P: 'static + Ord + Send> {
    /// The backing queue for events.
    inner: PriorityQueue<PriorityHolder, P, FxBuildHasher>,
}

impl<P: Ord + Send> QueueBacking for Priority<P> {
    fn new() -> Self {
        Self {
            inner: PriorityQueue::default(),
        }
    }

    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    fn next(&mut self) -> Option<Arc<TaskControl>> {
        self.inner.pop().map(|x| x.0 .0)
    }
}

/// Sends ordered tasks to a pool for background processing.
#[derive(Debug)]
pub struct TaskQueue<B: QueueBacking> {
    /// The implementation holder for this queue.
    inner: Arc<TaskQueueHolder<B>>,
}

impl<B: PushPopQueueBacking> TaskQueue<B> {
    /// Joins the pool in executing the given task. Both the current thread and the background
    /// threads complete the task's work. Semantically, this is equivalent to calling `spawn(task).join()`,
    /// but is more efficient by ensuring that the present thread always receives at least one work item.
    pub fn join<T>(&self, task: impl TaskCollection<T>) -> T {
        let work = Task::new(task, self.inner.clone());
        let next_job = work.control.collection().next_task();

        self.push_control(work.control());

        if let Some(job) = next_job {
            job.execute();
        }

        work.join()
    }

    /// Spawns a new task into the queue.
    pub fn spawn<T>(&self, task: impl TaskCollection<T>) -> Task<T, B> {
        let work = Task::new(task, self.inner.clone());
        self.push_control(work.control());
        work
    }

    /// Pushes the given task control into the work queue.
    fn push_control(&self, control: Arc<TaskControl>) {
        unsafe {
            let mut queue = self.inner.inner.lock().unwrap_unchecked();

            if queue.queued.is_empty() {
                self.inner.notifier.notify();
            }

            queue.queued.push(control);
        }
    }
}

impl<P: Ord + Send + Sync> TaskQueue<Priority<P>> {
    /// Joins the pool in executing the given task. Both the current thread and the background
    /// threads complete the task's work. Semantically, this is equivalent to calling `spawn(task).join()`,
    /// but is more efficient by ensuring that the present thread always receives at least one work item.
    pub fn join<T>(&self, priority: P, task: impl TaskCollection<T>) -> T {
        let work = Task::new(task, self.inner.clone());
        let next_job = work.control.collection().next_task();

        self.push_control(priority, work.control());

        if let Some(job) = next_job {
            job.execute();
        }

        work.join()
    }

    /// Spawns a new task into the queue, with the given priority.
    pub fn spawn<T>(&self, priority: P, task: impl TaskCollection<T>) -> Task<T, Priority<P>> {
        let work = Task::new(task, self.inner.clone());
        self.push_control(priority, work.control());
        work
    }

    /// Pushes the given task control into the work queue.
    fn push_control(&self, priority: P, control: Arc<TaskControl>) {
        unsafe {
            let mut queue = self.inner.inner.lock().unwrap_unchecked();

            if queue.queued.is_empty() {
                self.inner.notifier.notify();
            }

            queue.queued.inner.push(PriorityHolder(control), priority);
        }
    }
}

impl<B: QueueBacking> Default for TaskQueue<B> {
    fn default() -> Self {
        Self {
            inner: Arc::new(TaskQueueHolder {
                notifier: ChangeNotifier::default(),
                inner: wasm_sync::Mutex::new(TaskQueueInner {
                    current: None,
                    queued: B::new(),
                }),
            }),
        }
    }
}

impl<B: QueueBacking> Clone for TaskQueue<B> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<B: QueueBacking> WorkProvider for TaskQueue<B> {
    fn change_notifier(&self) -> &ChangeNotifier {
        &self.inner.notifier
    }

    fn next_task(&self) -> Option<Box<dyn WorkUnit>> {
        unsafe {
            let mut inner = self.inner.inner.lock().unwrap_unchecked();

            loop {
                if let Some(current) = &inner.current {
                    if current.increment_in_progress() {
                        if let Some(unit) = current.collection().next_task() {
                            let control = current.clone();
                            return Some(Box::new(move || {
                                unit.execute();
                                control.decrement_in_progress()
                            }));
                        } else {
                            current.cancel();
                            current.decrement_in_progress();
                        }
                    }
                }

                inner.current = inner.queued.next();
                inner.current.as_ref()?;
            }
        }
    }
}

/// Holds the backing implementation for a task queue.
#[derive(Debug)]
struct TaskQueueHolder<B: QueueBacking> {
    /// A notifier that may be used to alert other threads to newly-available work.
    notifier: ChangeNotifier,
    /// The inner queue state.
    inner: wasm_sync::Mutex<TaskQueueInner<B>>,
}

/// Maintains the current state of a task queue.
#[derive(Debug)]
struct TaskQueueInner<B: QueueBacking> {
    /// The piece of in-progress work, if any.
    current: Option<Arc<TaskControl>>,
    /// The queue that holds upcoming work.
    queued: B,
}

/// Returns a queueable task that executes a single closure one time.
pub fn once<T: 'static + Send>(f: impl 'static + FnOnce() -> T + Send) -> impl TaskCollection<T> {
    /// Ensures that the task's output is always treated as sync, because it won't be accessed on multiple threads.
    struct SyncWrapper<T: Send>(T);
    unsafe impl<T: Send> Sync for SyncWrapper<T> {}

    /// Represents a task that executes a single closure once, returning the result.
    struct OnceTask<T: 'static + Send> {
        /// The closure to execute.
        f: TakeOwnCell<Box<dyn WorkUnit>>,
        /// The result of the closure.
        result: Arc<ArcSwapOption<SyncWrapper<T>>>,
    }

    impl<T: Send> TaskProvider for OnceTask<T> {
        fn next_task(&self) -> Option<Box<dyn WorkUnit>> {
            self.f.take()
        }
    }

    impl<T: Send> TaskCollection<T> for OnceTask<T> {
        fn result(&self) -> T {
            unsafe {
                Arc::into_inner(
                    self.result
                        .swap(None)
                        .expect("Task was not yet complete or already taken."),
                )
                .unwrap_unchecked()
                .0
            }
        }
    }

    unsafe impl<T: Send> Send for OnceTask<T> {}
    unsafe impl<T: Send> Sync for OnceTask<T> {}

    let result = Arc::new(ArcSwapOption::const_empty());
    let result_cloned = result.clone();
    OnceTask {
        f: TakeOwnCell::new(Box::new(move || {
            result_cloned.store(Some(Arc::new(SyncWrapper(f()))));
        })),
        result,
    }
}

/// Hides implementation details from external crates.
mod private {
    use super::*;

    /// Provides the backing for a task queue.
    pub trait QueueBacking: 'static + Send {
        /// Creates a new instance of the backing queue.
        fn new() -> Self;
        /// Whether this queue is empty.
        fn is_empty(&self) -> bool;
        /// Gets the next item in the queue, if any.
        fn next(&mut self) -> Option<Arc<TaskControl>>;
    }

    /// Denotes a task queue with a simple push-pop ordering scheme for tasks.
    pub trait PushPopQueueBacking: QueueBacking {
        /// Pushes the given control block onto the task queue.
        fn push(&mut self, task: Arc<TaskControl>);
    }

    /// Manages the execution of work for a task.
    pub struct TaskControl {
        /// The collection of work associated with the task.
        collection: Box<dyn TaskProvider>,
        /// The number of work units in-progress.
        pub in_progress: AtomicUsize,
        /// A waker that should by used to notify other threads when this task has completed.
        result_waker: ArcSwapOption<Waker>,
    }

    impl TaskControl {
        /// The magic number used to atomically signal a task being cancelled.
        const CANCEL_FLAG: usize = 1 << (usize::BITS - 1);

        /// Creates a new task controller.
        pub fn new(provider: impl TaskProvider) -> Self {
            let collection = Box::new(provider);
            let in_progress = AtomicUsize::default();
            let result_waker = ArcSwapOption::const_empty();

            Self {
                collection,
                in_progress,
                result_waker,
            }
        }

        /// Cancels this task.
        pub fn cancel(&self) {
            self.in_progress
                .fetch_or(Self::CANCEL_FLAG, Ordering::Release);
        }

        /// Gets the collection of work associated with this task.
        pub fn collection(&self) -> &dyn TaskProvider {
            &*self.collection
        }

        /// Determines whether this task has completed all processing.
        pub fn complete(&self) -> bool {
            self.in_progress.load(Ordering::Acquire) == Self::CANCEL_FLAG
        }

        /// Increments the number of in-progress tasks by one, returning whether this task has been cancelled.
        pub fn increment_in_progress(&self) -> bool {
            self.in_progress
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |x| {
                    (x < Self::CANCEL_FLAG).then_some(x + 1)
                })
                .is_ok()
        }

        /// Decrements the number of in-progress tasks by the specified amount.
        pub fn decrement_in_progress(&self) {
            if self.in_progress.fetch_sub(1, Ordering::AcqRel) == Self::CANCEL_FLAG + 1 {
                if let Some(value) = &*self.result_waker.load() {
                    value.wake_by_ref();
                }
            }
        }

        /// Sets a waker that will be notified when the result of this computation is available.
        pub fn set_result_waker(&self, waker: Waker) {
            self.result_waker.store(Some(Arc::new(waker)));
        }
    }

    impl std::fmt::Debug for TaskControl {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("TaskControl")
                .field("in_progress", &self.in_progress)
                .field("result_waker", &self.result_waker)
                .finish()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_executor::*;

    async fn execute_background() {
        let queue = TaskQueue::<Fifo>::default();
        TaskPool::new(queue.clone(), 4).forget();

        assert_eq!(
            queue
                .spawn(once(|| {
                    println!("This will execute on background thread.");
                    2
                }))
                .await,
            2
        );
    }

    #[test]
    fn execute_double() {
        let queue_a = TaskQueue::<Fifo>::default();
        let queue_b = TaskQueue::<Lifo>::default();

        let first_task = queue_a.spawn(once(|| 2));
        let second_task = queue_b.spawn(once(|| 2));

        TaskPool::new(
            ChainedWorkProvider::default()
                .with(queue_a.clone())
                .with(queue_b.clone()),
            4,
        )
        .forget();

        assert_eq!(first_task.join(), second_task.join());
    }

    #[test]
    fn execute_double_twice() {
        let queue_a = TaskQueue::<Fifo>::default();
        let queue_b = TaskQueue::<Lifo>::default();

        let first_task = queue_a.spawn(once(|| 2));
        let second_task = queue_b.spawn(once(|| 2));

        TaskPool::new(
            ChainedWorkProvider::default()
                .with(queue_a.clone())
                .with(queue_b.clone()),
            1,
        )
        .forget();

        assert_eq!(first_task.join(), second_task.join());

        for _i in 0..1000 {
            let third_task =
                queue_a.spawn(once(|| std::thread::sleep(std::time::Duration::new(0, 10))));
            let fourth_task = queue_b.spawn(once(|| {
                std::thread::sleep(std::time::Duration::new(0, 200))
            }));
            assert_eq!(third_task.join(), fourth_task.join());
        }
    }

    #[test]
    fn execute_background_blocking() {
        block_on(execute_background());
    }
}
