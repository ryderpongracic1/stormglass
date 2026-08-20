#pragma once

#include <condition_variable>
#include <cstddef>
#include <mutex>
#include <optional>
#include <queue>
#include <utility>

namespace stormglass {

// A bounded, blocking, thread-safe queue.
//
// Used as the single-producer (Router) / single-consumer (one Worker) hand-off
// channel in PartitionedPipeline. Correctness over cleverness: a plain
// std::mutex + two condition_variables, NOT a hand-rolled lock-free ring. The
// bound provides backpressure so the Router cannot outrun a slow Worker and
// balloon memory.
//
// Although current use is strictly SPSC, the implementation is safe for
// multiple producers/consumers because every wait/notify goes through the
// mutex. `capacity` is the maximum number of buffered items; Push blocks while
// full and Pop blocks while empty. Close() unblocks any waiters and makes
// subsequent Pop return std::nullopt once the queue has drained — the clean
// shutdown signal.
template <typename T>
class BoundedQueue {
public:
    explicit BoundedQueue(std::size_t capacity) : capacity_(capacity) {}

    // Blocks while the queue is full. Returns false if the queue was closed.
    bool Push(T value) {
        std::unique_lock<std::mutex> lock(mutex_);
        not_full_.wait(lock, [&] { return items_.size() < capacity_ || closed_; });
        if (closed_) return false;
        items_.push(std::move(value));
        lock.unlock();
        not_empty_.notify_one();
        return true;
    }

    // Blocks while empty. Returns std::nullopt only once the queue is closed
    // AND fully drained, so a consumer can loop until it gets nullopt.
    std::optional<T> Pop() {
        std::unique_lock<std::mutex> lock(mutex_);
        not_empty_.wait(lock, [&] { return !items_.empty() || closed_; });
        if (items_.empty()) {
            return std::nullopt;  // closed and drained
        }
        T value = std::move(items_.front());
        items_.pop();
        lock.unlock();
        not_full_.notify_one();
        return value;
    }

    // Marks the queue closed and wakes all waiters. Buffered items remain
    // available to Pop until drained.
    void Close() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            closed_ = true;
        }
        not_empty_.notify_all();
        not_full_.notify_all();
    }

private:
    const std::size_t capacity_;
    std::mutex mutex_;
    std::condition_variable not_full_;
    std::condition_variable not_empty_;
    std::queue<T> items_;
    bool closed_ = false;
};

} // namespace stormglass
