#pragma once

#include "pipeline_types.hpp"
#include <atomic>
#include <stdexcept>
#include <array>
#include <memory>
#include <vector>

namespace io_pipeline {

template <typename T, std::size_t MaxCapacity>
class LockFreeCircularBuffer {
public:
    static_assert(MaxCapacity > 0 && (MaxCapacity & (MaxCapacity - 1)) == 0,
                  "Capacity must be power of 2 for efficient masking");

    LockFreeCircularBuffer() 
        : buffer_(std::make_unique<std::array<T, MaxCapacity>>()),
          head_(0), 
          tail_(0),
          size_(0) {}

    // Non-copyable, non-movable for lock-free safety
    LockFreeCircularBuffer(const LockFreeCircularBuffer&) = delete;
    LockFreeCircularBuffer& operator=(const LockFreeCircularBuffer&) = delete;

    // Push element to buffer - returns true if successful
    [[nodiscard]] bool push(const T& value) {
        std::size_t current_size = size_.load(std::memory_order_acquire);
        
        if (current_size >= MaxCapacity) {
            return false; // Buffer full
        }

        std::size_t tail = tail_.load(std::memory_order_relaxed);
        (*buffer_)[tail & (MaxCapacity - 1)] = value;
        tail_.store((tail + 1) & MASK, std::memory_order_release);
        size_.fetch_add(1, std::memory_order_release);
        
        return true;
    }

    // Pop element from buffer - returns true if successful
    [[nodiscard]] bool pop(T& value) {
        std::size_t current_size = size_.load(std::memory_order_acquire);
        
        if (current_size == 0) {
            return false; // Buffer empty
        }

        std::size_t head = head_.load(std::memory_order_relaxed);
        value = (*buffer_)[head & (MaxCapacity - 1)];
        head_.store((head + 1) & MASK, std::memory_order_release);
        size_.fetch_sub(1, std::memory_order_release);
        
        return true;
    }

    // Try push with busy-wait retry
    [[nodiscard]] bool push_retry(const T& value, int max_retries = 100) {
        for (int i = 0; i < max_retries; ++i) {
            if (push(value)) return true;
            // Exponential backoff
            for (volatile int j = 0; j < (1 << i); ++j) {}
        }
        return false;
    }

    // Peek at front without removing
    [[nodiscard]] bool peek(T& value) const {
        std::size_t current_size = size_.load(std::memory_order_acquire);
        if (current_size == 0) return false;
        
        std::size_t head = head_.load(std::memory_order_relaxed);
        value = (*buffer_)[head & (MaxCapacity - 1)];
        return true;
    }

    // Get current size
    [[nodiscard]] std::size_t size() const {
        return size_.load(std::memory_order_acquire);
    }

    // Check if empty
    [[nodiscard]] bool empty() const {
        return size() == 0;
    }

    // Check if full
    [[nodiscard]] bool full() const {
        return size() >= MaxCapacity;
    }

    // Get capacity
    [[nodiscard]] constexpr std::size_t capacity() const {
        return MaxCapacity;
    }

    // Clear buffer
    void clear() {
        head_.store(0, std::memory_order_release);
        tail_.store(0, std::memory_order_release);
        size_.store(0, std::memory_order_release);
    }

private:
    static constexpr std::size_t MASK = MaxCapacity - 1;
    
    std::unique_ptr<std::array<T, MaxCapacity>> buffer_;
    std::atomic<std::size_t> head_;
    std::atomic<std::size_t> tail_;
    std::atomic<std::size_t> size_;
};

// Dynamic circular buffer with automatic growth capability
class DynamicCircularBuffer {
public:
    explicit DynamicCircularBuffer(std::size_t initial_capacity = 1024,
                                    std::size_t max_capacity = 1024 * 1024,
                                    float growth_factor = 2.0f);

    ~DynamicCircularBuffer();

    // Non-copyable
    DynamicCircularBuffer(const DynamicCircularBuffer&) = delete;
    DynamicCircularBuffer& operator=(const DynamicCircularBuffer&) = delete;

    // Push message pointer
    [[nodiscard]] ErrorCode push(std::shared_ptr<Message> msg);

    // Pop message pointer
    [[nodiscard]] bool pop(std::shared_ptr<Message>& msg);

    // Peek without removing
    [[nodiscard]] bool peek(std::shared_ptr<Message>& msg) const;

    // Current size
    [[nodiscard]] std::size_t size() const;

    // Current capacity
    [[nodiscard]] std::size_t capacity() const;

    // Utilization percentage
    [[nodiscard]] float utilization() const;

    // Attempt to grow buffer (may be called from backpressure handling)
    [[nodiscard]] bool try_grow();

    // Get statistics
    struct Stats {
        std::size_t current_size;
        std::size_t current_capacity;
        std::size_t growth_count;
        std::size_t failed_pushes;
        std::size_t total_messages_processed;
    };

    [[nodiscard]] Stats get_stats() const;

private:
    struct BufferImpl;
    std::unique_ptr<BufferImpl> impl_;
    
    std::atomic<std::size_t> size_;
    std::atomic<std::size_t> growth_count_;
    std::atomic<std::size_t> failed_pushes_;
    std::atomic<std::size_t> total_processed_;
    
    const std::size_t max_capacity_;
    const float growth_factor_;
};

} // namespace io_pipeline