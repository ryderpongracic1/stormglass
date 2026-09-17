#pragma once

#include "sink/sink.h"

#include <string>
#include <vector>

namespace stormglass {

// A sink that durably records every emitted window result to a file so that
// output committed before a process crash (SIGKILL) survives the crash.
//
// Restart safety: the default kAppend mode keeps existing output, so a job that
// restores from a checkpoint can reopen the same path. Opening first cuts off a
// torn trailing record left by a killed writer, so new records never frame
// against partial bytes. Replay after restore is at-least-once: a result may be
// written again, and allowed-lateness re-fires legitimately write revisions of
// a window. ReadLatest() gives the idempotent view (last write per key and
// window); ReadAll() gives the raw log.
//
// Each Emit() appends one length-framed record and fsyncs, so what the sink
// "committed" before a crash is a real on-disk fact, not something reconstructed
// by an end-of-stream final flush. A writer killed mid-append can leave a torn
// trailing record; ReadAll() tolerates that by stopping at the last complete
// record — the same partial-write discipline the checkpoint writer relies on.
//
// Record framing (little-endian):
//   key_len:      u32
//   key:          key_len bytes
//   window_start: i64 (ms since epoch)
//   window_end:   i64 (ms since epoch)
//   sum:          i64
//   count:        u64
class DurableFileSink : public Sink {
public:
    enum class OpenMode {
        kAppend,    // keep complete records, drop a torn tail, append after them
        kTruncate,  // discard any existing file contents
    };

    explicit DurableFileSink(const std::string& path, OpenMode mode = OpenMode::kAppend);
    ~DurableFileSink() override;

    DurableFileSink(const DurableFileSink&) = delete;
    DurableFileSink& operator=(const DurableFileSink&) = delete;

    void Emit(const WindowResult& result) override;
    void Flush() override;

    [[nodiscard]] bool ok() const { return fd_ >= 0; }

    // Read every fully-written record from a durable sink file. A torn trailing
    // record (writer SIGKILLed mid-append) is dropped rather than treated as an
    // error. Returns an empty vector if the file is absent or unreadable.
    static std::vector<WindowResult> ReadAll(const std::string& path);

    // The last record written for each (key, window), ordered by (window.start,
    // key). Collapses replay duplicates and keeps the newest late re-fire, which
    // is the result an uninterrupted run would end with.
    static std::vector<WindowResult> ReadLatest(const std::string& path);

private:
    int fd_ = -1;
};

} // namespace stormglass
