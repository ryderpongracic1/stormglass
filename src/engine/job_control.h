#pragma once

#include "stream/record.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <optional>
#include <thread>

namespace stormglass {

// How a stop request ends a running job.
enum class StopMode : uint8_t {
    // Treat the stop as end of input: fire every open window (the same final
    // flush a bounded source gets) and return. Use when no restart will follow.
    kFinal,
    // Stop without firing open windows. With checkpointing enabled the job first
    // checkpoints at the source's current offset, so a restart resumes exactly
    // where it stopped and emits each window once, with its complete value.
    // Without checkpointing, open window state is discarded.
    kSuspend,
};

// Stop request shared by a running job and the threads controlling it. The
// first request wins; later requests cannot change its mode.
class StopSignal {
public:
    // Returns true if this call made the request.
    bool Request(StopMode mode) {
        uint8_t expected = kNone;
        return state_.compare_exchange_strong(expected, static_cast<uint8_t>(mode));
    }
    [[nodiscard]] bool requested() const { return state_.load() != kNone; }
    [[nodiscard]] StopMode mode() const { return static_cast<StopMode>(state_.load()); }

private:
    static constexpr uint8_t kNone = 0xff;
    std::atomic<uint8_t> state_{kNone};
};

// Sleep schedule used when a source reports "no data yet" with an empty batch:
// yield first, then back off exponentially to a 1 ms ceiling so a quiet live
// source costs neither a spinning core nor much added latency.
class IdleBackoff {
public:
    void Wait() {
        if (step_ == 0) {
            std::this_thread::yield();
        } else {
            std::this_thread::sleep_for(std::chrono::microseconds(
                std::min<int64_t>(int64_t{50} << std::min(step_ - 1, 5), 1000)));
        }
        step_ = std::min(step_ + 1, 6);
    }
    void Reset() { step_ = 0; }

private:
    int step_ = 0;
};

// Point-in-time job counters, readable from any thread while Run() executes.
// Each worker publishes after every input batch, so values trail processing
// by at most one batch and are not an atomic cut across workers.
struct JobProgress {
    bool running = false;
    uint64_t records_read = 0;          // data records pulled from the source
    uint64_t records_processed = 0;     // data records applied to window state
    uint64_t windows_fired = 0;         // per worker owning a key of the window
    uint64_t windows_refired = 0;
    uint64_t late_records_accepted = 0;
    uint64_t late_records_dropped = 0;
    uint64_t checkpoints_written = 0;   // per-partition files for partitioned jobs
    // Newest offset whose checkpoint is complete (every partition written).
    std::optional<uint64_t> last_complete_checkpoint;
    // Minimum watermark across partitions; Timestamp::min() before the first.
    Timestamp output_watermark{Timestamp::min()};
};

} // namespace stormglass
