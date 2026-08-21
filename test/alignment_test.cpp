#include <gtest/gtest.h>

#include "source/generator.h"
#include "source/source_merge.h"
#include "stream/record.h"

#include <algorithm>
#include <cstdint>
#include <optional>
#include <variant>
#include <vector>

// v3 Phase 3 — K-way Chandy-Lamport barrier ALIGNMENT, tested DIRECTLY.
//
// Each wrapped source emits its OWN kCheckpointBarrier at its own record offsets.
// SourceMerge holds an early channel at its barrier N (buffering its subsequent
// records) until barrier N has arrived on EVERY active channel, then emits ONE
// merged barrier stamped with the aligned cut's merged offset. These tests prove:
//   (a) an early channel is BLOCKED until all K deliver barrier N;
//   (b) the CONSISTENCY property: the merged barrier N sits at the exact cut of
//       "records preceding barrier N on every channel" — no leak, none missing;
//   (c) K == 1 reduces EXACTLY to the single-source (pre-Phase-3) behavior;
//   (d) Seek replays the aligned merged sequence bit-for-bit (determinism);
//   (e) an IDLE channel is excluded from alignment and cannot deadlock it.
//
// oracle.{h,cpp} are BYTE-UNMODIFIED — alignment gets its own direct coverage
// here rather than through the oracle (barriers do not affect aggregation).

namespace stormglass {
namespace {

int64_t Ms(Timestamp t) { return t.time_since_epoch().count(); }

GeneratorConfig Gen(uint64_t seed, uint64_t records, int64_t step,
                    uint64_t ckpt_interval) {
    GeneratorConfig g{};
    g.seed = seed;
    g.num_keys = 8;
    g.num_records = records;
    g.event_time_step = step;
    g.max_disorder = Duration{200};
    g.batch_size = 512;
    g.watermark_interval = 50;
    g.checkpoint_interval = ckpt_interval;
    return g;
}

struct DrainResult {
    std::vector<Record> records;
    std::vector<uint64_t> barrier_offsets;  // merged barrier checkpoint_offsets, in order
    std::vector<int64_t> watermarks;        // emitted merged watermarks, in order
};

DrainResult Drain(SourceMerge& s) {
    DrainResult out;
    while (auto batch = s.Next()) {
        for (const auto& item : batch->items) {
            if (std::holds_alternative<Record>(item)) {
                out.records.push_back(std::get<Record>(item));
            } else {
                const auto& c = std::get<ControlRecord>(item);
                if (c.type == ControlType::kCheckpointBarrier) {
                    out.barrier_offsets.push_back(c.checkpoint_offset);
                } else {
                    out.watermarks.push_back(Ms(c.watermark));
                }
            }
        }
    }
    return out;
}

// ===========================================================================
// (a) EARLY CHANNEL BLOCKED until all K deliver barrier N.
// ===========================================================================
//
// Source 0 emits a barrier every 50 records; source 1 every 5000. Source 0
// reaches its barrier 1 early and MUST be blocked (records buffered, count
// frozen) until source 1 finally delivers ITS barrier 1 — only then is the
// merged barrier emitted, at the aligned cut offset 50 + 5000 = 5050, and both
// channels unblock.
TEST(Alignment, EarlyChannelIsBlockedUntilAllChannelsDeliverBarrier) {
    SourceMergeConfig mc;
    mc.merged_batch_size = 1;  // one merged data record per Next(): fine-grained
    mc.sources = {Gen(1, 6000, 1, /*I0=*/50), Gen(2, 6000, 1, /*I1=*/5000)};
    SourceMerge m(mc);

    // Drive until source 0 has delivered its first barrier.
    while (m.BarriersSeen(0) < 1) {
        auto b = m.Next();
        ASSERT_TRUE(b.has_value());
    }
    // The instant source 0 delivered barrier 1, the epoch is NOT closed (source 1
    // has not delivered), so source 0 is BLOCKED and source 1 has 0 barriers.
    EXPECT_EQ(m.EpochsClosed(), 0u);
    EXPECT_TRUE(m.IsChannelBlocked(0));
    EXPECT_EQ(m.BarriersSeen(1), 0u);
    const uint64_t offset_when_blocked = m.CurrentOffset();

    // While source 0 stays blocked, its barrier count is frozen at 1 and the
    // merged offset keeps climbing — driven ONLY by source 1's records. Capture
    // the merged barrier's stamped offset the moment the epoch closes.
    uint64_t merged_barrier_offset = 0;
    while (m.BarriersSeen(1) < 1) {
        ASSERT_TRUE(m.IsChannelBlocked(0)) << "source 0 must remain blocked";
        EXPECT_EQ(m.BarriersSeen(0), 1u) << "blocked channel must not advance past its barrier";
        auto b = m.Next();
        ASSERT_TRUE(b.has_value());
        for (const auto& item : b->items) {
            if (std::holds_alternative<ControlRecord>(item)) {
                const auto& c = std::get<ControlRecord>(item);
                if (c.type == ControlType::kCheckpointBarrier) merged_barrier_offset = c.checkpoint_offset;
            }
        }
    }
    // Source 1 delivered barrier 1 -> epoch closes -> ONE merged barrier stamped
    // at the aligned cut 50 + 5000 = 5050, and source 0 unblocks.
    EXPECT_EQ(m.EpochsClosed(), 1u);
    EXPECT_FALSE(m.IsChannelBlocked(0)) << "epoch closed: source 0 unblocked";
    EXPECT_EQ(merged_barrier_offset, 5050u)
        << "aligned cut = 50 (source0 up to its barrier) + 5000 (source1)";

    // Non-vacuity: source 0 was held far below where free round-robin would have
    // carried it (~2500 of the first 5050 merged records).
    EXPECT_LT(offset_when_blocked, 5050u);
}

// ===========================================================================
// (b) CONSISTENCY: the merged barrier N is exactly the aligned cut.
// ===========================================================================
//
// With per-source intervals I_i, epoch N closes only when EVERY channel has
// emitted exactly N * I_i records (each blocked at its Nth barrier, 0 past), so
// the Nth merged barrier's offset is N * (Sum of I_i). Any record leaking past a
// channel's barrier N would push the offset ABOVE N*Sum; a premature close would
// leave it BELOW. Exact equality is a direct proof of "records preceding barrier
// N on EVERY channel — no leak, none missing".
//
// Sources are sized so BOTH exhaust at epoch 20 exactly (2000/100 == 6000/300 ==
// 20), keeping the arithmetic clean (no post-exhaustion single-channel epochs).
TEST(Alignment, MergedBarrierSitsExactlyAtAlignedCut) {
    const uint64_t I0 = 100, I1 = 300;
    const uint64_t S = I0 + I1;  // 400 merged records per fully-aligned epoch
    SourceMergeConfig mc;
    mc.merged_batch_size = 512;
    mc.sources = {Gen(11, /*R0=*/2000, 2, I0), Gen(22, /*R1=*/6000, 1, I1)};
    SourceMerge m(mc);

    auto d = Drain(m);

    ASSERT_EQ(d.barrier_offsets.size(), 20u);
    for (uint64_t n = 1; n <= 20; ++n) {
        EXPECT_EQ(d.barrier_offsets[n - 1], n * S)
            << "merged barrier " << n << " must sit at the aligned cut n*(I0+I1)";
    }
    EXPECT_EQ(d.records.size(), 8000u);
    EXPECT_EQ(m.EpochsClosed(), 20u);
}

// ===========================================================================
// (c) K == 1 reduces EXACTLY to the single-source (pre-Phase-3) behavior.
// ===========================================================================
//
// A single wrapped source with interval N: alignment over one channel is
// trivially satisfied, so the merged barrier lands at offsets N, 2N, … — exactly
// the bare generator's own barrier offsets — and the data records are identical.
// Watermarks match the bare generator's strictly-advancing subsequence, since
// SourceMerge suppresses non-advancing merged watermarks (as in every phase).
TEST(Alignment, KOneReducesToSingleSourceBarriers) {
    GeneratorConfig g = Gen(7, 5000, 1, /*N=*/1000);

    SourceMergeConfig mc;
    mc.merged_batch_size = 512;
    mc.sources = {g};
    SourceMerge merge(mc);
    DeterministicGenerator bare(g);

    auto md = Drain(merge);

    std::vector<Record> bare_records;
    std::vector<uint64_t> bare_barriers;
    std::vector<int64_t> bare_wms;
    while (auto batch = bare.Next()) {
        for (const auto& item : batch->items) {
            if (std::holds_alternative<Record>(item)) {
                bare_records.push_back(std::get<Record>(item));
            } else {
                const auto& c = std::get<ControlRecord>(item);
                if (c.type == ControlType::kCheckpointBarrier) bare_barriers.push_back(c.checkpoint_offset);
                else bare_wms.push_back(Ms(c.watermark));
            }
        }
    }

    ASSERT_EQ(md.records.size(), bare_records.size());
    for (size_t i = 0; i < md.records.size(); ++i) {
        EXPECT_EQ(md.records[i].key, bare_records[i].key) << "record " << i;
        EXPECT_EQ(md.records[i].value, bare_records[i].value) << "record " << i;
        EXPECT_EQ(md.records[i].event_time, bare_records[i].event_time) << "record " << i;
    }
    // Barriers identical: {1000, 2000, 3000, 4000, 5000}.
    ASSERT_EQ(bare_barriers.size(), 5u);
    EXPECT_EQ(md.barrier_offsets, bare_barriers);
    // Watermarks: merged == strictly-advancing subsequence of bare's.
    std::vector<int64_t> bare_adv;
    int64_t last = Ms(Timestamp::min());
    for (int64_t w : bare_wms) if (w > last) { bare_adv.push_back(w); last = w; }
    EXPECT_EQ(md.watermarks, bare_adv);
    EXPECT_EQ(merge.EpochsClosed(), 5u);
}

// ===========================================================================
// (d) Seek determinism WITH alignment: replaying to an aligned cut reproduces
//     the identical merged sequence (so crash-restore replays exactly).
// ===========================================================================
TEST(Alignment, SeekReproducesIdenticalAlignedSequence) {
    SourceMergeConfig mc;
    mc.merged_batch_size = 256;
    mc.sources = {Gen(3, 8000, 2, 120), Gen(4, 8000, 1, 250), Gen(5, 8000, 3, 175)};

    SourceMerge a(mc);
    a.Next();
    a.Next();
    a.Next();
    const uint64_t offset = a.CurrentOffset();
    ASSERT_GT(offset, 0u);

    SourceMerge b(mc);
    b.Seek(offset);
    ASSERT_EQ(b.CurrentOffset(), offset);
    EXPECT_EQ(a.EpochsClosed(), b.EpochsClosed())
        << "aligned-epoch state must be reconstructed by replay";

    for (int batch = 0; batch < 4; ++batch) {
        auto ba = a.Next();
        auto bb = b.Next();
        ASSERT_EQ(ba.has_value(), bb.has_value()) << "batch " << batch;
        if (!ba.has_value()) break;
        ASSERT_EQ(ba->items.size(), bb->items.size()) << "batch " << batch;
        for (size_t i = 0; i < ba->items.size(); ++i) {
            ASSERT_EQ(ba->items[i].index(), bb->items[i].index());
            if (std::holds_alternative<Record>(ba->items[i])) {
                const auto& ra = std::get<Record>(ba->items[i]);
                const auto& rb = std::get<Record>(bb->items[i]);
                EXPECT_EQ(ra.key, rb.key);
                EXPECT_EQ(ra.value, rb.value);
                EXPECT_EQ(ra.event_time, rb.event_time);
            } else {
                const auto& ca = std::get<ControlRecord>(ba->items[i]);
                const auto& cb = std::get<ControlRecord>(bb->items[i]);
                EXPECT_EQ(ca.type, cb.type);
                EXPECT_EQ(ca.watermark, cb.watermark);
                EXPECT_EQ(ca.checkpoint_offset, cb.checkpoint_offset);
            }
        }
    }
}

// ===========================================================================
// (e) IDLE x ALIGNMENT: an idle channel is EXCLUDED from the alignment set and
//     cannot deadlock it.
// ===========================================================================
//
// RULE (documented in source_merge.h): a channel marked idle (excluded from the
// watermark MIN by idle_timeout) is simultaneously excluded from the barrier
// alignment set. The open epoch closes once every channel ACTIVE at that moment
// has delivered its barrier; the idle channel does not hold it up. On resume the
// channel rejoins and catches up (its barriers for epochs already closed simply
// increment its count).
//
// Setup: source 0 stays active and keeps delivering barriers; source 1 goes quiet
// mid-stream long enough to be excluded. WITHOUT the exclusion rule, source 0
// would block at its next barrier FOREVER waiting for the quiet source 1 (a
// deadlock). WITH the rule, source 0 keeps closing epochs ALONE while source 1 is
// idle — so EpochsClosed advances during the idle window — and the whole stream
// still drains to completion (both sources fully emitted).
TEST(Alignment, IdleChannelIsExcludedAndDoesNotDeadlockAlignment) {
    GeneratorConfig s0 = Gen(1, 6000, 4, /*I0=*/100);
    GeneratorConfig s1 = Gen(2, 6000, 1, /*I1=*/100);
    // Source 1 goes quiet at its 150th record for 2000 turns, then resumes.
    s1.idle_spans.push_back(IdleSpan{/*start_offset=*/150, /*length=*/2000});

    SourceMergeConfig mc;
    mc.merged_batch_size = 1;
    mc.idle_timeout = 30;  // exclude source 1 after 30 empty pulls
    mc.sources = {s0, s1};
    SourceMerge m(mc);

    bool saw_idle = false;
    uint64_t epochs_when_idle_began = 0;
    uint64_t epochs_during_idle = 0;
    uint64_t data_records = 0;

    while (auto batch = m.Next()) {
        for (const auto& item : batch->items) {
            if (std::holds_alternative<Record>(item)) ++data_records;
        }
        if (m.IsSourceIdle(1)) {
            if (!saw_idle) {
                saw_idle = true;
                epochs_when_idle_began = m.EpochsClosed();
            }
            epochs_during_idle = std::max(epochs_during_idle, m.EpochsClosed());
        }
    }

    ASSERT_TRUE(saw_idle) << "source 1 must have been excluded (idle) during the gap";
    // The heart of the no-deadlock proof: epochs closed WHILE source 1 was idle,
    // driven by source 0 alone — alignment did NOT wait for the excluded channel.
    EXPECT_GT(epochs_during_idle, epochs_when_idle_began)
        << "merged barriers must keep closing while a channel is idle-excluded";
    // The stream drained fully (terminated, no deadlock) and source 1 resumed.
    EXPECT_EQ(data_records, 12000u) << "both sources fully drained";
    EXPECT_FALSE(m.IsSourceIdle(1)) << "source 1 resumed by end of stream";
}

}  // namespace
}  // namespace stormglass
