#pragma once

#include "source/source.h"

#include <cstdint>
#include <random>
#include <vector>

namespace stormglass {

/// A deterministic idle span, expressed in the SOURCE's OWN data-record index
/// (NOT wall-clock — see SourceMergeConfig::idle_timeout). It means: once this
/// source has produced exactly `start_offset` data records, it goes quiet for
/// `length` consecutive round-robin turns before producing its next data record.
/// The delayed record keeps its ORIGINAL event-time, so when it finally appears
/// the merged watermark may already have advanced past it (making it late).
///
/// This is consumed ONLY by SourceMerge (which pauses the wrapped generator for
/// the span, freezing its watermark). The standalone DeterministicGenerator
/// IGNORES it, so a bare generator is byte-for-byte unchanged. Spans must be
/// sorted by start_offset and non-overlapping. Default (empty) == no gaps ==
/// Phase-1 behavior.
struct IdleSpan {
    uint64_t start_offset = 0;  // this source's data-record index where the gap begins
    uint64_t length = 0;        // consecutive empty pulls (idle ticks) before resuming
};

/// Controls how the generator distributes event-time disorder.
///   kBounded     — jitter is uniform in [-max_disorder, 0]. Combined with the
///                  wm = max_seen - max_disorder rule, no record is ever emitted
///                  below the watermark, so nothing is ever genuinely late.
///   kHeavyTailed — a `late_fraction` of records are pushed strictly beyond the
///                  disorder bound (by max_disorder + [1, late_tail]), landing
///                  below the emitted watermark and exercising the late-data
///                  drop / re-fire paths. The remaining records use bounded
///                  jitter, and the watermark rule is unchanged.
enum class DisorderMode : uint8_t { kBounded, kHeavyTailed };

struct GeneratorConfig {
    uint64_t seed = 42;
    uint32_t num_keys = 10;
    uint64_t num_records = 100000;

    // Event-time base advances by this many ms per record (base_ms = offset *
    // event_time_step). Default 1 reproduces the original 1ms-per-record stream
    // bit-for-bit. SourceMerge sets DIVERGENT steps across its wrapped sources so
    // their watermarks (wm = max_seen - max_disorder) advance at different rates,
    // making the MIN-combine non-trivial: a slow (small-step) source holds the
    // merged watermark back and gates downstream firing.
    int64_t event_time_step = 1;

    Duration max_disorder{5000};
    uint32_t batch_size = 1024;
    uint32_t watermark_interval = 100;

    // Records between emitted checkpoint barriers (0 = no barriers).
    // The source stamps each barrier with the absolute source offset, and the
    // pipeline snapshots when it dequeues one — see ControlType::kCheckpointBarrier.
    uint64_t checkpoint_interval = 0;

    // Heavy-tailed disorder controls (ignored when disorder_mode == kBounded).
    DisorderMode disorder_mode = DisorderMode::kBounded;
    double late_fraction = 0.0;   // P(record is a heavy-tail late record), [0, 1]
    Duration late_tail{0};        // max extra lateness beyond max_disorder

    // Idle spans (v3 Phase 2). Consumed ONLY by SourceMerge to MODEL a source
    // going quiet; the standalone DeterministicGenerator IGNORES this field, so
    // a bare generator remains byte-for-byte unchanged. Empty (default) == no
    // gaps == Phase-1 behavior. See IdleSpan.
    std::vector<IdleSpan> idle_spans;
};

class DeterministicGenerator : public Source {
public:
    explicit DeterministicGenerator(GeneratorConfig config);

    std::optional<Batch> Next() override;
    void Seek(uint64_t offset) override;
    [[nodiscard]] uint64_t CurrentOffset() const override;

private:
    Record GenerateRecord();

    GeneratorConfig config_;
    std::mt19937_64 rng_;
    uint64_t offset_ = 0;
    uint64_t records_since_watermark_ = 0;
    uint64_t records_since_checkpoint_ = 0;
    Timestamp max_event_time_seen_{Timestamp::min()};
};

} // namespace stormglass
