#include <dmlc/json.h>
#include <dmlc/io.h>
#include <dmlc/memory_io.h>
#include <dmlc/concurrentqueue.h>
#include <dmlc/blockingconcurrentqueue.h>
#include <gtest/gtest.h>
#include <mutex>
#if __cplusplus > 201103L  /* C++14 */
#include <shared_mutex>
#endif
#include <condition_variable>
#ifdef __linux__
#include <sys/syscall.h>
#endif

/*!
 * \brief Simple manually-signalled event gate which remains open
 */
class SimpleEvent {
 public:
  SimpleEvent()
    : signaled_(false) {}
  void wait() {
    std::unique_lock<std::mutex> lock(mutex_);
    if (!signaled_) {
      condition_variable_.wait(lock);
    }
  }
  void signal() {
    signaled_ = true;
    std::unique_lock<std::mutex> lk(mutex_);
    condition_variable_.notify_all();
  }
  void reset() {
    std::unique_lock<std::mutex> lk(mutex_);
    signaled_ = false;
  }
  /*! \brief Signal event upon destruction, even for exceptions (RAII) */
  struct SetReadyOnDestroy {
    explicit inline SetReadyOnDestroy(std::shared_ptr<SimpleEvent> *event)
      : event_(*event) {
    }
    inline ~SetReadyOnDestroy() {
      if (event_) {
        event_->signal();
      }
    }
    std::shared_ptr<SimpleEvent>  event_;
  };

 private:
  std::mutex              mutex_;
  std::condition_variable condition_variable_;
  std::atomic<bool>       signaled_;
};

#if 1

#if 1 && defined(__linux__)
/**
 *   __         _
 *  / _|       | |
 * | |_  _   _ | |_  ___ __  __
 * |  _|| | | || __|/ _ \\ \/ /
 * | |  | |_| || |_|  __/ >  <
 * |_|   \__,_| \__|\___|/_/\_\
 *
 *
 */
// Using a futex shaves nearly a second off of dpmf netflix calculation
// (10.9s -> 10.1s) per iteration
class futex {
 public:
  futex() : lock_val_(0) {}

  ~futex() {}

  void lock() {
    int s = 0;
    if(!lock_val_.compare_exchange_strong(s, 1)) {
      if(s != 2)
        s = lock_val_.exchange(2);
      while(s != 0) {
        fwait(&lock_val_, 2);
        s = lock_val_.exchange(2);
      }
    }
  }

  void unlock() {
    DCHECK_GE(lock_val_.load(), 1) << "multiple unlock() calls in a row";
    if(lock_val_-- != 1)  {
      //if old value was 2
      lock_val_ = 0;
      fwakeup_one(&lock_val_);
    }
  }

 protected:
  static constexpr int WAIT = 0, WAKE = 1;

  inline static long fwait( void *futex, int comparand ) {
    const long r = syscall(SYS_futex, futex, WAIT, comparand, NULL, NULL, 0);
    int e;
    DCHECK_EQ(r == 0 || r == EWOULDBLOCK || (r == -1 && ((e = errno) == EAGAIN || e == EINTR)), true) << "fwait: lock failed";
    return r;
  }

  inline static long fwakeup_one( void *futex ) {
    const long r = ::syscall(SYS_futex, futex, WAKE, 1, NULL, NULL, 0);
    DCHECK_EQ(r == 0 || r == 1, true) << "fwakeup_one: more than one thread woken up?"; // also 1 ok
    return r;
  }

  inline static long fwakeup_all( void *futex ) {
    const long r = ::syscall(SYS_futex, futex, WAKE, INT_MAX, NULL, NULL, 0);
    DCHECK_GE(r, 0) << "fwakeup_all: error in waking up threads";
    return r;
  }
 private:
  std::atomic<int> lock_val_;
};

using fast_mutex = futex;

#else

using fast_mutex = std::mutex;

#endif
#if __cplusplus > 201103L  /* C++14 */
typedef std::shared_timed_mutex       SharedMutex;
typedef std::unique_lock<SharedMutex> WriteLock;
struct ReadLock
{
  SharedMutex *m_;

  ReadLock(SharedMutex &m)
    : m_(&m) { if (m_) m_->lock_shared(); }

  ~ReadLock() { if (m_) m_->unlock_shared(); }
};
#else
typedef std::mutex       SharedMutex;
typedef std::unique_lock<SharedMutex> WriteLock;
struct ReadLock {
  SharedMutex *m_;
  ReadLock(SharedMutex &m)
    : m_(&m) { if (m_) m_->lock(); }

  ~ReadLock() { if (m_) m_->unlock(); }
};
#endif

class ThreadGroup {
 public:
  /**
   *  __  __                                        _  _______  _                            _
   * |  \/  |                                      | ||__   __|| |                          | |
   * | \  / |  __ _  _ __    __ _   __ _   ___   __| |   | |   | |__   _ __  ___   __ _   __| |
   * | |\/| | / _` || '_ \  / _` | / _` | / _ \ / _` |   | |   | '_ \ | '__|/ _ \ / _` | / _` |
   * | |  | || (_| || | | || (_| || (_| ||  __/| (_| |   | |   | | | || |  |  __/| (_| || (_| |
   * |_|  |_| \__,_||_| |_| \__,_| \__, | \___| \__,_|   |_|   |_| |_||_|   \___| \__,_| \__,_|
   *                                __/ |
   *                               |___/
   */

  class ManagedThread
  {
   public:
    typedef std::shared_ptr<ManagedThread> SharedPtr;
    typedef std::weak_ptr<ManagedThread>   WeakPtr;

   private:
    typedef std::thread                    Thread;

    std::string name_;
    mutable SharedMutex csThread_;
    std::atomic<Thread *> thread_;
    std::shared_ptr<SimpleEvent> evReady_;
    std::shared_ptr<SimpleEvent> evStart_;
    ThreadGroup *owner_;
    std::atomic<bool> shutdown_requested_;
    bool autoRemove_;

   protected:
    static int startHere(ManagedThread::SharedPtr pThis) {
      int rc;
      if(pThis) {
        pThis->evReady_->signal();
        rc = pThis->run_thread();
#if defined(AWSDL_DEBUG_LOG_THREADS) && !defined(NDEBUG)
        LOG(INFO) << "Thread " << pThis->name_ << " exiting";
#endif
        if (pThis->autoRemove_) {
          pThis->owner_->remove_thread(pThis);
        }
      } else {
        LOG(ERROR) << "Null pThis thread pointer";
        rc = EINVAL;
      }
      return rc;
    }

   protected:
    virtual int run_thread() { return 0; };

   public:
    ManagedThread(const std::string& threadName, ThreadGroup *owner, std::thread *thrd = NULL)
      : name_(threadName)
        , thread_(thrd)
        , evReady_(std::make_shared<SimpleEvent>())
        , evStart_(std::make_shared<SimpleEvent>())
        , owner_(owner)
        , shutdown_requested_(false)
        , autoRemove_(false) {
      CHECK_NOTNULL(owner);
    }

    virtual ~ManagedThread() {
#if defined(AWSDL_DEBUG_LOG_THREADS) && !defined(NDEBUG)
      const std::string name = getName();
            LOG(INFO) << "ManagedThread::~ManagedThread( " << name << " )";
#endif
      if (!is_current_thread()) {
        request_shutdown();
        join();
      }
      WriteLock guard(csThread_);
      if (thread_) {
        Thread *thrd = thread_;
        thread_ = NULL;
        delete thrd;
      }
    }

    const char *getName() const {
      return name_.c_str();
    }

    static bool launch(ManagedThread::SharedPtr pThis, bool autoRemove = false) {
      WriteLock guard(pThis->csThread_);
      CHECK_EQ(!pThis->thread_, true);
      CHECK_NOTNULL(pThis->owner_);
      pThis->autoRemove_ = autoRemove;
      pThis->thread_ = new std::thread(startHere, pThis);
      pThis->owner_->add_thread(pThis);
      pThis->evReady_->wait();
      pThis->evStart_->signal();
      return pThis->thread_ != NULL;
    }

    bool is_current_thread() {
      ReadLock guard(csThread_);
      return thread_.load() ? (thread_.load()->get_id() == std::this_thread::get_id()) : false;
    }

    virtual void request_shutdown() {
      shutdown_requested_ = true;
    }

    virtual bool is_shutdown_requested() const {
      return shutdown_requested_.load();
    }

    bool joinable() const {
      ReadLock guard(csThread_);
      if (thread_) {
        //CHECK_EQ(autoRemove_, false);  // TODO: If we need this, join needs to be checked by searching the group or exit event.
        return thread_.load()->joinable();
      }
      return false;
    }

    void join() {
#if defined(AWSDL_DEBUG_LOG_THREADS) && !defined(NDEBUG)
      const std::string name = getName();
      LOG(INFO) << "join() on " << name << " ( " << thread_.load()->get_id() << " )";
#endif
      ReadLock guard(csThread_);
      // should be careful calling (or any function externally) this when in
      // auto-remove mode
      if (thread_ && thread_.load()->get_id() != std::thread::id()) {
        std::thread::id someId;

        CHECK_EQ(!autoRemove_,
                 true);  // TODO: If we need this, join needs to be checked by searching the group or exit event.
        CHECK_NOTNULL(thread_.load());
        if (thread_.load()->joinable()) {
          thread_.load()->join();
        } else {
          LOG(WARNING) << "Thread " << name_ << " ( " << thread_.load()->get_id() << " ) not joinable";
        }
      }
    }

    void setAutoRemove(bool ar) {
      autoRemove_ = ar;
    }

    std::thread::id get_id() const {
      ReadLock guard(csThread_);
      return thread_.load()->get_id();
    }
  };

  mutable SharedMutex                m_;
  std::set<ManagedThread::SharedPtr> threads_;
  std::shared_ptr<SimpleEvent>       evEmpty_;

 public:
  inline ThreadGroup()
    : evEmpty_(std::make_shared<SimpleEvent>()) {

  }

  virtual ~ThreadGroup() {
    request_shutdown_all();
    join_all();
  }


  inline bool is_this_thread_in() const {
    std::thread::id id = std::this_thread::get_id();
    ReadLock guard(m_);
    for (auto it = threads_.begin(), end = threads_.end(); it != end; ++it) {
      ManagedThread::SharedPtr thrd = *it;
      if (thrd->get_id() == id)
        return true;
    }
    return false;
  }

  inline bool is_thread_in(ManagedThread::SharedPtr thrd) const {
    if (thrd) {
      std::thread::id id = thrd->get_id();
      ReadLock guard(m_);
      for (auto it = threads_.begin(), end = threads_.end(); it != end; ++it) {
        ManagedThread::SharedPtr thrd = *it;
        if (thrd->get_id() == id)
          return true;
      }
      return false;
    } else {
      return false;
    }
  }

  inline void add_thread(ManagedThread::SharedPtr thrd) {
    if (thrd) {
      WriteLock guard(m_);
      threads_.insert(thrd);
      evEmpty_->reset();
    }
  }

  inline void remove_thread(ManagedThread::SharedPtr thrd) {
    WriteLock guard(m_);
    threads_.erase(thrd);
    if(threads_.empty()) {
      evEmpty_->signal();
    }
  }

  inline void join_all() {
    CHECK_EQ(!is_this_thread_in(), true);

    ManagedThread::SharedPtr thrd;
    do {
      {
        ReadLock guard(m_);
        if (!threads_.empty()) {
          thrd = *threads_.begin();
        } else {
          thrd = ManagedThread::SharedPtr(NULL);
        }
      }
      if (thrd) {
        if (thrd->joinable()) {
          thrd->join();
        }
        remove_thread(thrd);
      }
    } while (thrd);
  }

  inline void request_shutdown_all() {
    ReadLock guard(m_);
    for (auto it = threads_.begin(), end = threads_.end(); it != end; ++it) {
      ManagedThread::SharedPtr thrd = *it;
      thrd->request_shutdown();
    }
  }

  inline size_t size() const {
    ReadLock guard(m_);
    return threads_.size();
  }

  inline bool empty() const {
    ReadLock guard(m_);
    return threads_.size() == 0;
  }

  template<typename F, typename T, typename ThreadType = ManagedThread>
  inline typename ThreadType::SharedPtr create_thread(const std::string& threadName, F threadfunc, T data) {
    std::thread *thrd = new std::thread(threadfunc, data);
    typename ThreadType::SharedPtr newThread(new ThreadType(threadName, this, thrd));
    add_thread(newThread);
    return newThread;
  }

  template<typename F, typename ThreadType = ManagedThread>
  inline typename ThreadType::SharedPtr create_thread(const std::string& threadName, F threadfunc) {
    std::thread *thrd = new std::thread(threadfunc);
    typename ThreadType::SharedPtr newThread(new ThreadType(threadName, this, thrd));
    add_thread(newThread);
    return newThread;
  }

};
#endif

/*!
 * \brief Simple thread lifecycle management
 */
class SimpleThreadGroup {
 public:
  ~SimpleThreadGroup() {
    WaitForAll();
  }
  size_t Count() const {
    std::unique_lock<std::mutex> lk(cs_threads_);
    return threads_.size();
  }
  void WaitForAll() {
    while(Count()) {
      std::shared_ptr<std::thread> thrd(nullptr);
      do {
        std::unique_lock<std::mutex> lk(cs_threads_);
        if (!threads_.empty()) {
          thrd = *threads_.begin();
          threads_.erase(thrd);
        }
      } while (false);
      if(thrd) {
        if(thrd->joinable()) {
          thrd->join();
        }
      }
    }
  }
  template<typename StartFunction, typename ...Args>
  void Start(StartFunction start_function, Args ...args) {
    std::unique_lock<std::mutex> lk(cs_threads_);
    std::shared_ptr<std::thread> thrd = std::make_shared<std::thread>(start_function, args...);
    threads_.insert(thrd);
  }
 private:
  mutable std::mutex                               cs_threads_;
  std::unordered_set<std::shared_ptr<std::thread>> threads_;
};

template<typename TQueue>
struct LFQThreadData {
  LFQThreadData() : count_(0) {}
  std::atomic<int> count_;
  std::shared_ptr<TQueue> q_ = std::make_shared<TQueue>();
  std::shared_ptr<SimpleEvent> ready_ = std::make_shared<SimpleEvent>();
  std::mutex cs_map_;
  std::set<int> thread_map_;
};

template<typename TQueue>
static void PushThread(const int id, std::shared_ptr<LFQThreadData<TQueue>> data) {
  ++data->count_;
  data->ready_->wait();
  data->q_->enqueue(id);
  std::unique_lock<std::mutex> lk(data->cs_map_);
  data->thread_map_.erase(id);
}

template<typename TQueue>
static void PullThread(const int id, std::shared_ptr<LFQThreadData<TQueue>> data) {
  ++data->count_;
  data->ready_->wait();
  int val;
  GTEST_ASSERT_EQ(data->q_->try_dequeue(val), true);
  std::unique_lock<std::mutex> lk(data->cs_map_);
  data->thread_map_.erase(id);
}

template<typename TQueue>
static void BlockingPullThread(const int id, std::shared_ptr<LFQThreadData<TQueue>> data) {
  ++data->count_;
  data->ready_->wait();
  int val;
  data->q_->wait_dequeue(val);
  std::unique_lock<std::mutex> lk(data->cs_map_);
  data->thread_map_.erase(id);
}

TEST(Lockfree, ConcurrentQueue) {
  SimpleThreadGroup threads;
  const int ITEM_COUNT = 100;
  auto data = std::make_shared<LFQThreadData<moodycamel::ConcurrentQueue<int>>>();
  for(size_t x = 0; x < ITEM_COUNT; ++x) {
    std::unique_lock<std::mutex> lk(data->cs_map_);
    data->thread_map_.insert(x);
    threads.Start(PushThread<moodycamel::ConcurrentQueue<int>>, x, data);
  }
  while(data->count_ < ITEM_COUNT) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  data->ready_->signal();
  size_t remaining = ITEM_COUNT;
  do {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    std::unique_lock<std::mutex> lk(data->cs_map_);
    remaining = data->thread_map_.size();
  } while (remaining);

  size_t count = data->q_->size_approx();
  GTEST_ASSERT_EQ(count, ITEM_COUNT);

  threads.WaitForAll();
  GTEST_ASSERT_EQ(threads.Count(), 0);

  for(size_t x = 0; x < ITEM_COUNT; ++x) {
    std::unique_lock<std::mutex> lk(data->cs_map_);
    data->thread_map_.insert(x);
    threads.Start(PullThread<moodycamel::ConcurrentQueue<int>>, x, data);
  }
  data->ready_->signal();
  threads.WaitForAll();
  GTEST_ASSERT_EQ(threads.Count(), 0);

  count = data->q_->size_approx();
  GTEST_ASSERT_EQ(count, 0);
}

TEST(Lockfree, BlockingConcurrentQueue) {

  using BlockingQueue = moodycamel::BlockingConcurrentQueue<
    int, moodycamel::ConcurrentQueueDefaultTraits>;

  SimpleThreadGroup threads;
  const int ITEM_COUNT = 100;
  auto data = std::make_shared<LFQThreadData<BlockingQueue>>();
  for(size_t x = 0; x < ITEM_COUNT; ++x) {
    std::unique_lock<std::mutex> lk(data->cs_map_);
    data->thread_map_.insert(x);
    threads.Start(PushThread<BlockingQueue>, x, data);
  }
  while(data->count_ < ITEM_COUNT) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  data->ready_->signal();
  size_t remaining = ITEM_COUNT;
  do {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    std::unique_lock<std::mutex> lk(data->cs_map_);
    remaining = data->thread_map_.size();
  } while (remaining);

  size_t count = data->q_->size_approx();
  GTEST_ASSERT_EQ(count, ITEM_COUNT);

  threads.WaitForAll();
  GTEST_ASSERT_EQ(threads.Count(), 0);

  for(size_t x = 0; x < ITEM_COUNT; ++x) {
    std::unique_lock<std::mutex> lk(data->cs_map_);
    data->thread_map_.insert(x);
    threads.Start(BlockingPullThread<BlockingQueue>, x, data);
  }
  data->ready_->signal();
  threads.WaitForAll();
  GTEST_ASSERT_EQ(threads.Count(), 0);

  count = data->q_->size_approx();
  GTEST_ASSERT_EQ(count, 0);
}

