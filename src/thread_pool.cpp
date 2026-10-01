#include "scheduler/thread_pool.hpp"

namespace scheduler {
thread_local const ThreadPool* ThreadPool::current_pool_ = nullptr;

ThreadPool::ThreadPool(std::size_t workers, std::size_t capacity) : capacity_(capacity) {
    if (workers == 0 || capacity == 0) {
        throw std::invalid_argument("workers and queue capacity must be positive");
    }
    workers_.reserve(workers);
    try {
        for (std::size_t i = 0; i < workers; ++i) {
            workers_.emplace_back([this] { worker_loop(); });
        }
    } catch (...) {
        // If thread creation fails partway, wake and join the workers already created.
        shutdown();
        throw;
    }
}
ThreadPool::~ThreadPool() { shutdown(); }

void ThreadPool::enqueue(std::function<void()> task) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!accepting_) { throw PoolStopped(); }
        if (queue_.size() >= capacity_) { throw QueueFull(); }
        queue_.push(std::move(task));
        ++submitted_;
    }
    work_available_.notify_one();
}
void ThreadPool::worker_loop() {
    current_pool_ = this;
    for (;;) {
        std::function<void()> task;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            work_available_.wait(lock, [this] { return !queue_.empty() || !accepting_; });
            if (queue_.empty()) { break; } // Only possible here when shutdown has begun.
            task = std::move(queue_.front());
            queue_.pop();
            ++active_;
        }
        // User code runs without holding the queue lock. packaged_task captures exceptions.
        task();
        {
            std::lock_guard<std::mutex> lock(mutex_);
            --active_;
            ++completed_;
            if (queue_.empty() && active_ == 0) { idle_.notify_all(); }
        }
    }
    current_pool_ = nullptr;
}
void ThreadPool::require_owner_thread() const {
    if (current_pool_ == this) {
        throw std::logic_error("wait_idle/shutdown cannot be called by this pool's worker");
    }
}
void ThreadPool::wait_idle() {
    require_owner_thread();
    std::unique_lock<std::mutex> lock(mutex_);
    idle_.wait(lock, [this] { return queue_.empty() && active_ == 0; });
}
void ThreadPool::shutdown() {
    // Check BEFORE locking shutdown_mutex_: an owner may hold it while joining this worker.
    require_owner_thread();
    std::lock_guard<std::mutex> join_lock(shutdown_mutex_);
    {
        std::lock_guard<std::mutex> lock(mutex_);
        accepting_ = false;
    }
    work_available_.notify_all();
    for (auto& worker : workers_) {
        if (worker.joinable()) { worker.join(); }
    }
}
Stats ThreadPool::stats() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return {queue_.size(), active_, submitted_, completed_, accepting_};
}
} // namespace scheduler
