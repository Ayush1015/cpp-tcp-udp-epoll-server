#pragma once
#include <condition_variable>
#include <cstddef>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

namespace scheduler {
class QueueFull : public std::runtime_error {
public:
    QueueFull() : std::runtime_error("queue is full; retry later") {}
};
class PoolStopped : public std::runtime_error {
public:
    PoolStopped() : std::runtime_error("pool is shutting down") {}
};
struct Stats {
    std::size_t queued, active, submitted, completed;
    bool accepting;
};

// FIFO, nonblocking submission. Capacity limits queued jobs, not running jobs.
// The owner must keep the pool alive until all jobs have finished.
class ThreadPool {
public:
    explicit ThreadPool(std::size_t workers, std::size_t capacity);
    ~ThreadPool();
    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;
    ThreadPool(ThreadPool&&) = delete;
    ThreadPool& operator=(ThreadPool&&) = delete;

    template <class F>
    auto submit(F&& function) -> std::future<std::invoke_result_t<std::decay_t<F>&>> {
        using Result = std::invoke_result_t<std::decay_t<F>&>;
        auto task = std::make_shared<std::packaged_task<Result()>>(std::forward<F>(function));
        auto result = task->get_future();
        enqueue([task] { (*task)(); });
        return result;
    }
    void wait_idle(); // A snapshot barrier: concurrent producers may submit afterwards.
    void shutdown(); // Reject new jobs, drain accepted jobs, join. Idempotent.
    Stats stats() const;

private:
    void enqueue(std::function<void()> task);
    void worker_loop();
    void require_owner_thread() const;
    const std::size_t capacity_;
    mutable std::mutex mutex_;
    std::mutex shutdown_mutex_; // Serializes callers joining the same threads.
    std::condition_variable work_available_, idle_;
    std::queue<std::function<void()>> queue_;
    std::vector<std::thread> workers_;
    bool accepting_ = true;
    std::size_t active_ = 0, submitted_ = 0, completed_ = 0;
    static thread_local const ThreadPool* current_pool_;
};
} // namespace scheduler
