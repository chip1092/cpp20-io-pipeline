#include "thread_pool.hpp"
#include <iostream>

namespace io_pipeline {

ThreadPool::ThreadPool(std::size_t num_threads)
    : num_threads_(num_threads > 0 ? num_threads : std::thread::hardware_concurrency()) {
    
    worker_queues_.reserve(num_threads_);
    workers_.reserve(num_threads_);

    for (std::size_t i = 0; i < num_threads_; ++i) {
        worker_queues_.push_back(std::make_unique<WorkerQueue>());
        workers_.emplace_back(&ThreadPool::worker_thread, this, i);
    }
}

ThreadPool::~ThreadPool() {
    shutdown();
}

void ThreadPool::submit(WorkFunction work) {
    if (!running_.load(std::memory_order_acquire)) {
        return;
    }

    static thread_local std::size_t last_queue = 0;
    std::size_t queue_index = (last_queue++) % num_threads_;
    submit_to_queue(queue_index, std::move(work));
}

void ThreadPool::submit_to_queue(std::size_t queue_index, WorkFunction work) {
    if (queue_index >= num_threads_) {
        return;
    }

    auto& queue = *worker_queues_[queue_index];
    {
        std::lock_guard<std::mutex> lock(queue.mutex);
        queue.queue.push(std::move(work));
        active_work_.fetch_add(1, std::memory_order_release);
    }
    queue.cv.notify_one();
}

void ThreadPool::wait_for_completion() {
    std::unique_lock<std::mutex> lock(completion_mutex_);
    completion_cv_.wait(lock, [this] { 
        return active_work_.load(std::memory_order_acquire) == 0; 
    });
}

void ThreadPool::shutdown() {
    running_.store(false, std::memory_order_release);
    
    // Notify all worker threads
    for (auto& queue : worker_queues_) {
        queue->cv.notify_all();
    }

    // Wait for all threads to finish
    for (auto& thread : workers_) {
        if (thread.joinable()) {
            thread.join();
        }
    }
}

std::size_t ThreadPool::total_queued_work() const {
    std::size_t total = 0;
    for (const auto& queue : worker_queues_) {
        std::lock_guard<std::mutex> lock(queue->mutex);
        total += queue->queue.size();
    }
    return total;
}

void ThreadPool::worker_thread(std::size_t queue_index) {
    while (running_.load(std::memory_order_acquire)) {
        auto& my_queue = *worker_queues_[queue_index];
        WorkFunction work;

        {
            std::unique_lock<std::mutex> lock(my_queue.mutex);
            my_queue.cv.wait(lock, [this, &my_queue] {
                return !my_queue.queue.empty() || 
                       !running_.load(std::memory_order_acquire);
            });

            if (my_queue.queue.empty()) {
                continue;
            }

            work = std::move(my_queue.queue.front());
            my_queue.queue.pop();
        }

        if (work) {
            try {
                work();
            } catch (const std::exception& e) {
                std::cerr << "Exception in thread pool: " << e.what() << "\n";
            }
            
            auto remaining = active_work_.fetch_sub(1, std::memory_order_release) - 1;
            if (remaining == 0) {
                completion_cv_.notify_all();
            }
        }
    }
}

WorkFunction ThreadPool::steal_work(std::size_t my_queue_index) {
    // Try to steal work from other queues
    for (std::size_t i = 1; i < num_threads_; ++i) {
        std::size_t other_index = (my_queue_index + i) % num_threads_;
        auto& other_queue = *worker_queues_[other_index];
        
        std::lock_guard<std::mutex> lock(other_queue.mutex);
        if (!other_queue.queue.empty()) {
            auto work = std::move(other_queue.queue.front());
            other_queue.queue.pop();
            return work;
        }
    }
    return nullptr;
}

} // namespace io_pipeline