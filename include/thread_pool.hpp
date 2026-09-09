#pragma once

#include "pipeline_types.hpp"
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <vector>
#include <atomic>
#include <functional>
#include <memory>

namespace io_pipeline {

using WorkFunction = std::function<void()>;

// Work-stealing thread pool for pipeline processing
class ThreadPool {
public:
    explicit ThreadPool(std::size_t num_threads = std::thread::hardware_concurrency());
    
    ~ThreadPool();

    // Non-copyable
    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;

    // Submit work to pool
    void submit(WorkFunction work);

    // Submit work to specific thread queue (for work distribution)
    void submit_to_queue(std::size_t queue_index, WorkFunction work);

    // Wait for all work to complete
    void wait_for_completion();

    // Shutdown thread pool gracefully
    void shutdown();

    // Get number of threads
    [[nodiscard]] std::size_t num_threads() const { return num_threads_; }

    // Get total work items queued
    [[nodiscard]] std::size_t total_queued_work() const;

    // Check if pool is running
    [[nodiscard]] bool is_running() const { return running_.load(); }

private:
    struct WorkerQueue {
        std::queue<WorkFunction> queue;
        mutable std::mutex mutex;
        std::condition_variable cv;
    };

    std::size_t num_threads_;
    std::vector<std::unique_ptr<WorkerQueue>> worker_queues_;
    std::vector<std::thread> workers_;
    std::atomic<bool> running_{true};
    std::atomic<std::size_t> active_work_{0};
    std::condition_variable completion_cv_;
    mutable std::mutex completion_mutex_;

    void worker_thread(std::size_t queue_index);
    [[nodiscard]] WorkFunction steal_work(std::size_t my_queue_index);
};

} // namespace io_pipeline