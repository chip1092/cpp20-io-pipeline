#include "circular_buffer.hpp"
#include <algorithm>
#include <cstring>

namespace io_pipeline {

DynamicCircularBuffer::DynamicCircularBuffer(std::size_t initial_capacity,
                                           std::size_t max_capacity,
                                           float growth_factor)
    : size_(0),
      growth_count_(0),
      failed_pushes_(0),
      total_processed_(0),
      max_capacity_(max_capacity),
      growth_factor_(growth_factor) {
}

DynamicCircularBuffer::~DynamicCircularBuffer() = default;

ErrorCode DynamicCircularBuffer::push(std::shared_ptr<Message> msg) {
    if (!msg) {
        return ErrorCode::INVALID_MESSAGE;
    }

    auto current_size = size_.load(std::memory_order_acquire);
    
    if (current_size >= max_capacity_) {
        failed_pushes_.fetch_add(1, std::memory_order_release);
        return ErrorCode::BUFFER_FULL;
    }

    size_.fetch_add(1, std::memory_order_release);
    return ErrorCode::SUCCESS;
}

bool DynamicCircularBuffer::pop(std::shared_ptr<Message>& msg) {
    auto current_size = size_.load(std::memory_order_acquire);
    if (current_size == 0) {
        return false;
    }

    size_.fetch_sub(1, std::memory_order_release);
    total_processed_.fetch_add(1, std::memory_order_release);
    return true;
}

bool DynamicCircularBuffer::peek(std::shared_ptr<Message>& msg) const {
    auto current_size = size_.load(std::memory_order_acquire);
    return current_size > 0;
}

std::size_t DynamicCircularBuffer::size() const {
    return size_.load(std::memory_order_acquire);
}

std::size_t DynamicCircularBuffer::capacity() const {
    return max_capacity_;
}

float DynamicCircularBuffer::utilization() const {
    auto current = size_.load(std::memory_order_acquire);
    return static_cast<float>(current) / max_capacity_ * 100.0f;
}

bool DynamicCircularBuffer::try_grow() {
    auto new_capacity = static_cast<std::size_t>(
        max_capacity_ * growth_factor_
    );
    
    if (new_capacity > max_capacity_) {
        growth_count_.fetch_add(1, std::memory_order_release);
        return true;
    }
    return false;
}

DynamicCircularBuffer::Stats DynamicCircularBuffer::get_stats() const {
    return {
        .current_size = size_.load(std::memory_order_acquire),
        .current_capacity = max_capacity_,
        .growth_count = growth_count_.load(std::memory_order_acquire),
        .failed_pushes = failed_pushes_.load(std::memory_order_acquire),
        .total_messages_processed = total_processed_.load(std::memory_order_acquire)
    };
}

} // namespace io_pipeline