use std::marker::PhantomData;
use std::sync::*;
use std::ops::*;

pub trait MutexLockStrategy: 'static + Default + Clone + Send + Sync {
    fn lock<T>(mutex: &Mutex<T>) -> LockResult<MutexGuard<'_, T>> {
        mutex.lock()
    }

    fn read<T>(rw: &RwLock<T>) -> LockResult<RwLockReadGuard<'_, T>> {
        rw.read()
    }

    fn write<T>(rw: &RwLock<T>) -> LockResult<RwLockWriteGuard<'_, T>> {
        rw.write()
    }

    fn wait<'a, T>(condvar: &Condvar, guard: MutexGuard<'a, T>) -> LockResult<MutexGuard<'a, T>> {
        condvar.wait(guard)
    }
}

#[derive(Default, Clone)]
pub struct DefaultMutexLockStrategy {}
impl MutexLockStrategy for DefaultMutexLockStrategy {}

pub trait WorkUnit: Send {
    fn execute(self: Box<Self>);
}

impl WorkUnit for dyn FnOnce() + Send {
    fn execute(self: Box<Self>) {
        self()
    }
}

pub trait WorkProvider: 'static + Send + Sync {
    fn next_unit(&self) -> Option<Box<dyn WorkUnit>>;
    fn wake_listeners(&self);
}

#[derive(Clone)]
pub struct TaskPool<M: MutexLockStrategy = DefaultMutexLockStrategy> {
    provider: Arc<RwLock<Option<Arc<dyn WorkProvider>>>>,
    data: PhantomData<M>
}

impl<M: MutexLockStrategy> TaskPool<M> {
    pub fn new(threads: usize) -> Self {
        Self::new_with_spawner(threads, |_, f| { std::thread::spawn(f); })
    }

    pub fn new_with_spawner(threads: usize, mut spawner: impl FnMut(usize, Box<dyn FnOnce() + Send + 'static>)) -> Self {
        let provider: Arc<RwLock<Option<Arc<dyn WorkProvider>>>> = Arc::new(RwLock::new(Some(Arc::new(EmptyWorkProvider::<M>::default()))));
        
        for id in 0..threads {
            Self::start_thread(id, provider.clone(), &mut spawner);
        }

        Self { data: PhantomData::default(), provider }
    }

    pub fn work_provider(&self) -> Arc<dyn WorkProvider> {
        M::read(&self.provider).unwrap().clone().unwrap()
    }

    pub fn set_work_provider(&self, provider: impl WorkProvider) {
        self.set_provider_option(Some(Arc::new(provider)));
    }

    pub fn set_work_provider_boxed(&self, provider: Arc<dyn WorkProvider>) {
        self.set_provider_option(Some(provider));
    }

    fn set_provider_option(&self, provider: Option<Arc<dyn WorkProvider>>) {
        let old = std::mem::replace(&mut *M::write(&self.provider).unwrap(), provider);
        old.unwrap().wake_listeners();
    }

    fn start_thread(id: usize, provider: Arc<RwLock<Option<Arc<dyn WorkProvider>>>>, spawner: &mut impl FnMut(usize, Box<dyn FnOnce() + Send + 'static>)) {
        spawner(id, Box::new(move || {
            loop {
                let op = M::read(&provider).unwrap().clone();
                if let Some(p) = op {
                    if let Some(unit) = p.next_unit() {
                        unit.execute();
                    }
                }
                else {
                    break;
                }
            }
        }));
    }
}

impl<M: MutexLockStrategy> Drop for TaskPool<M> {
    fn drop(&mut self) {
        self.set_provider_option(None);
    }
}

#[derive(Clone)]
pub struct TaskPoolStack<M: MutexLockStrategy = DefaultMutexLockStrategy> {
    pool: TaskPool<M>,
    providers: Arc<Mutex<Vec<Arc<dyn WorkProvider>>>>
}

impl<M: MutexLockStrategy> TaskPoolStack<M> {
    pub fn new(pool: TaskPool<M>) -> Self {
        let providers = Arc::new(Mutex::new(vec!(pool.work_provider())));

        Self { pool, providers }
    }

    pub fn push(&self, wp: impl WorkProvider) -> TaskPoolStackGuard<M> {
        let provider = Arc::new(wp);
        let mut provs = M::lock(&self.providers).unwrap();
        provs.push(provider.clone());
        self.pool.set_work_provider_boxed(provider);
        TaskPoolStackGuard { stack: self }
    }

    pub fn pop(&self) {
        let mut provs = M::lock(&self.providers).unwrap();
        provs.pop();
        self.pool.set_work_provider_boxed(provs[provs.len() - 1].clone());
    }
}

pub struct TaskPoolStackGuard<'a, M: MutexLockStrategy> {
    stack: &'a TaskPoolStack<M>
}

impl<'a, M: MutexLockStrategy> TaskPoolStackGuard<'a, M> {
    pub fn forget(self) {
        let _ = std::mem::ManuallyDrop::new(self);
    }
}

impl<'a, M: MutexLockStrategy> Drop for TaskPoolStackGuard<'a, M> {
    fn drop(&mut self) {
        self.stack.pop();
    }
}

#[derive(Default)]
pub struct EmptyWorkProvider<M: MutexLockStrategy = DefaultMutexLockStrategy> {
    state: CondMutex<()>,
    data: PhantomData<M>
}

impl<M: MutexLockStrategy> WorkProvider for EmptyWorkProvider<M> {
    fn next_unit(&self) -> Option<Box<dyn WorkUnit>> {
        let _ = M::wait(&self.state.condvar, M::lock(&self.state.mutex).unwrap()).unwrap();
        None
    }

    fn wake_listeners(&self) {
        self.state.condvar.notify_all();
    }
}

pub struct SingletonTask<I: 'static + Send = (), R: 'static + Send = ()>(FoldTask<I, R, Vec<R>>);

impl<I: 'static + Send, R: Send> SingletonTask<I, R> {
    pub fn new(func: impl Fn(I) -> R + Send + Sync + 'static, input: I) -> Self {
        Self(FoldTask::new(func, |v, r| v.push(r), Vec::new(), vec!(input)))
    }
}

impl<I: 'static + Send, R: 'static + Send> WorkCollection for SingletonTask<I, R> {
    fn next_unit(&self) -> Option<Box<dyn WorkUnit>> {
        self.0.next_unit()
    }
}

impl<I: 'static + Send, R: 'static + Send> Task for SingletonTask<I, R> {
    type Output = R;

    fn complete(&self) -> bool {
        self.0.complete()
    }

    fn wait(&self) -> Self::Output {
        self.0.wait().pop().unwrap()
    }
}

pub struct MapTask<I: 'static + Send, R: 'static + Send>(FoldTask<I, R, Vec<R>>);

impl<I: 'static + Send, R: Send> MapTask<I, R> {
    pub fn new(func: impl Fn(I) -> R + Send + Sync + 'static, input: Vec<I>) -> Self {
        Self(FoldTask::new(func, |v, r| v.push(r), Vec::new(), input))
    }
}

impl<I: 'static + Send, R: 'static + Send> WorkCollection for MapTask<I, R> {
    fn next_unit(&self) -> Option<Box<dyn WorkUnit>> {
        self.0.next_unit()
    }
}

impl<I: 'static + Send, R: 'static + Send> Task for MapTask<I, R> {
    type Output = Vec<R>;

    fn complete(&self) -> bool {
        self.0.complete()
    }

    fn wait(&self) -> Self::Output {
        self.0.wait()
    }
}

pub struct FoldTask<I: 'static + Send, R, O: 'static + Send, M: MutexLockStrategy = DefaultMutexLockStrategy> {
    func: Arc<dyn Fn(I) -> R + Send + Sync>,
    fold: Arc<dyn Fn(&mut O, R) + Send + Sync>,
    state: Arc<CondMutex<FlatmapTaskState<I, O>>>,
    data: PhantomData<M>
}

impl<I: Send, R, O: Send, M: MutexLockStrategy> FoldTask<I, R, O, M> {
    pub fn new(func: impl Fn(I) -> R + Send + Sync + 'static, fold: impl Fn(&mut O, R) + Send + Sync + 'static, default: O, input: Vec<I>) -> Self {
        let func = Arc::new(func);
        let fold = Arc::new(fold);

        let left = input.len();
        let input = input.into_iter();
        let st = FlatmapTaskState { input, output: Some(default), left };
        let state = Arc::new(CondMutex::new(st));
        Self { func, fold, state, data: PhantomData::default() }
    }
}

impl<I: 'static + Send, R: 'static, O: 'static + Send, M: MutexLockStrategy> WorkCollection for FoldTask<I, R, O, M> {
    fn next_unit(&self) -> Option<Box<dyn WorkUnit>> {
        let ms = self.state.clone();
        let func = self.func.clone();
        let fold = self.fold.clone();

        let mut fts = M::lock(&self.state.mutex).unwrap();
        fts.input.next().map(|x| Box::new(ClosureWorkUnit { func: move || {
            let res = func(x);
            let mut g = M::lock(&ms.mutex).unwrap();
            fold(g.output.as_mut().unwrap(), res);
            g.left -= 1;
            drop(g);
            ms.condvar.notify_all();
        } }) as Box<dyn WorkUnit>)
    }
}

impl<I: 'static + Send, R: 'static, O: 'static + Send, M: MutexLockStrategy> Task for FoldTask<I, R, O, M> {
    type Output = O;

    fn complete(&self) -> bool {
        M::lock(&self.state.mutex).unwrap().left == 0
    }

    fn wait(&self) -> Self::Output {
        let mut fts = M::lock(&self.state.mutex).unwrap();
        while fts.left != 0 {
            fts = M::wait(&self.state.condvar, fts).unwrap();
        }

        std::mem::replace(&mut fts.output, None).unwrap()
    }
}

struct FlatmapTaskState<I: Send, O: Send> {
    input: std::vec::IntoIter<I>,
    left: usize,
    output: Option<O>
}

struct ClosureWorkUnit<F: FnOnce() + Send> {
    func: F
}

impl<F: FnOnce() + Send> WorkUnit for ClosureWorkUnit<F> {
    fn execute(self: Box<Self>) {
        (self.func)()
    }
}

pub trait Task: WorkCollection {
    type Output: 'static;

    fn complete(&self) -> bool;
    fn wait(&self) -> Self::Output;
}

pub trait WorkCollection: 'static + Send + Sync {
    fn next_unit(&self) -> Option<Box<dyn WorkUnit>>;
}

#[derive(Clone)]
pub struct TaskQueue<P: Send + Clone + Ord = i32, M: MutexLockStrategy = DefaultMutexLockStrategy> {
    state: Arc<CondMutex<TaskQueueState<P, M>>>
}

impl<P: 'static + Send + Clone + Ord, M: MutexLockStrategy> TaskQueue<P, M> {
    pub fn spawn<T: Task>(&self, task: T, priority: P) -> TaskHandle<T::Output, P, M> {
        let mut tqs = M::lock(&self.state.mutex).unwrap();
        
        let ta = Arc::new(task);
        let tta: Arc<dyn Task<Output = T::Output>> = Arc::<T>::clone(&ta);
        let twa: Arc<dyn WorkCollection> = Arc::<T>::clone(&ta);

        tqs.push_task(twa.clone().into(), priority);
        self.wake_listeners();
        TaskHandle { state: self.state.clone(), task: tta.into(), work: twa.into() }
    }
}

impl<P: 'static + Send + Clone + Ord, M: MutexLockStrategy> Default for TaskQueue<P, M> {
    fn default() -> Self {
        let state = Arc::new(CondMutex::new(TaskQueueState::default()));
        Self { state }
    }
}

impl<P: 'static + Send + Clone + Ord, M: MutexLockStrategy> WorkProvider for TaskQueue<P, M> {
    fn next_unit(&self) -> Option<Box<dyn WorkUnit>> {
        let mut tqs = self.state.mutex.lock().unwrap();
        let res = tqs.next_unit();

        if res.is_none() {
            let _ = M::wait(&self.state.condvar, tqs).unwrap();
        }

        res
    }

    fn wake_listeners(&self) {
        self.state.condvar.notify_all();
    }
}

struct TaskQueueState<P: Ord + Clone, M: MutexLockStrategy> {
    current: Option<WorkReference<dyn WorkCollection>>,
    queue: priority_queue::PriorityQueue<WorkReference<dyn WorkCollection>, P>,
    data: PhantomData<M>
}

impl<P: Ord + Clone, M: MutexLockStrategy> TaskQueueState<P, M> {
    pub fn priority(&self, task: &WorkReference<dyn WorkCollection>) -> Option<P> {
        self.queue.get_priority(&task).map(|x| x.clone())
    }

    pub fn set_priority(&mut self, task: &WorkReference<dyn WorkCollection>, priority: P) {
        let _ = self.queue.change_priority(&task, priority);
    }

    pub fn cancel_task(&mut self, task: &WorkReference<dyn WorkCollection>) {
        if self.queue.remove(task).is_none() && self.current.as_ref().map_or(false, |x| x == task) {
            self.current = None;
        }
    }

    pub fn next_unit(&mut self) -> Option<Box<dyn WorkUnit>> {
        if self.current.is_none() {
            self.current = self.queue.pop().map(|(x, _)| x);
        }

        while let Some(wf) = &self.current {
            if let Some(unit) = wf.next_unit() {
                return Some(unit);
            }
            else {
                drop(wf);
                self.current = self.queue.pop().map(|(x, _)| x);
            }
        }

        return None;
    }

    pub fn push_task(&mut self, task: WorkReference<dyn WorkCollection>, priority: P) {
        let _ = self.queue.push(task, priority);
    }
}

impl<P: Ord + Clone, M: MutexLockStrategy> Default for TaskQueueState<P, M> {
    fn default() -> Self {
        let current = None;
        let queue = priority_queue::PriorityQueue::new();
        let data = PhantomData::default();
        Self { current, queue, data }
    }
}

struct WorkReference<T: ?Sized> {
    work: Arc<T>
}

impl<T: ?Sized> From<Arc<T>> for WorkReference<T> {
    fn from(work: Arc<T>) -> Self {
        Self { work }
    }
}

impl<T: ?Sized> Clone for WorkReference<T> {
    fn clone(&self) -> Self {
        Self { work: Arc::clone(&self.work) }
    }
}

impl<T: ?Sized> Deref for WorkReference<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &*self.work
    }
}

impl<T: ?Sized> std::hash::Hash for WorkReference<T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        Arc::as_ptr(&self.work).hash(state)
    }
}

impl<T: ?Sized> PartialEq for WorkReference<T> {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.work, &other.work)
    }
}

impl<T: ?Sized> Eq for WorkReference<T> {}


pub struct TaskHandle<O, P: Ord + Clone, M: MutexLockStrategy> {
    state: Arc<CondMutex<TaskQueueState<P, M>>>,
    task: WorkReference<dyn Task<Output = O>>,
    work: WorkReference<dyn WorkCollection>
}

impl<O: 'static, P: Ord + Clone, M: MutexLockStrategy> TaskHandle<O, P, M> {
    pub fn priority(&self) -> Option<P> {
        self.state.mutex.lock().unwrap().priority(&self.work)
    }

    pub fn set_priority(&self, priority: P) {
        self.state.mutex.lock().unwrap().set_priority(&self.work, priority);
    }

    pub fn complete(&self) -> bool {
        self.task.complete()
    }
    
    pub fn result(self) -> Result<O, Self> {
        if self.task.complete() {
            Ok(self.join())
        }
        else {
            Err(self)
        }
    }

    pub fn join(self) -> O {
        while let Some(unit) = self.task.next_unit() {
            unit.execute();
        }

        self.task.work.wait()
    }

    pub fn cancel(self) {
        self.state.mutex.lock().unwrap().cancel_task(&self.work);
    }
}

#[derive(Default)]
struct CondMutex<T> {
    pub condvar: Condvar,
    pub mutex: Mutex<T>
}

impl<T> CondMutex<T> {
    pub fn new(data: T) -> Self {
        Self { mutex: Mutex::new(data), condvar: Condvar::default() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parallel_multiply() {
        let pool: TaskPool = TaskPool::new(8);
        let queue: TaskQueue = TaskQueue::default();
        pool.set_work_provider(queue.clone());

        let mul = 2;
        let input = vec!(2, 3, 4);
        let task = MapTask::new(move |x| mul * x, input.clone());
        let handle = queue.spawn(task, 0);

        let mut res = handle.join();
        res.sort();

        assert!(input.into_iter().map(|x| mul * x).eq(res.clone().into_iter()))
    }

    #[test]
    fn test_join() {
        let pool: TaskPool = TaskPool::new(0);
        let queue: TaskQueue = TaskQueue::default();
        pool.set_work_provider(queue.clone());

        let mul = 2;
        let input = vec!(2, 3, 4);
        let task = MapTask::new(move |x| mul * x, input.clone());
        let handle = queue.spawn(task, 0);

        let res = handle.join();

        assert!(input.into_iter().map(|x| mul * x).eq(res.clone().into_iter()))
    }

    #[test]
    fn test_threads() {
        let pool: TaskPool = TaskPool::new(8);
        let queue: TaskQueue = TaskQueue::default();
        pool.set_work_provider(queue.clone());

        let mul = 2;
        let input = vec!(2, 3, 4);
        let task = MapTask::new(move |x| mul * x, input.clone());
        let mut handle = Err(queue.spawn(task, 0));

        while handle.is_err() {
            handle = handle.unwrap_err().result();
        }

        let mut res = handle.map_err(|_| unreachable!()).unwrap();
        res.sort();

        assert!(input.into_iter().map(|x| mul * x).eq(res.clone().into_iter()))
    }
}