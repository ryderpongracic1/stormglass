#include "nemesis/nemesis.h"

#include <cinttypes>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>

using namespace stormglass;

void PrintUsage() {
    std::fprintf(stderr,
        "Usage: stormglass_nemesis [options]\n"
        "  --seeds N              Number of nemesis runs (default: 20)\n"
        "  --records N            Records per run (default: 50000)\n"
        "  --checkpoint-interval N  (default: 5000)\n"
        "  --phase <mid-checkpoint|mid-emission|between>  in-process phase (default: between)\n"
        "  --real-kill            Headline mode: genuine fork() + SIGKILL crash\n"
        "  --partitioned          Partitioned engine: fork+SIGKILL at a TORN global checkpoint\n"
        "  --workers N            Worker count for --partitioned (default: 4)\n"
        "  --real-phase <between|mid-checkpoint>  real-kill point (default: between)\n"
        "  --keys N               Keys per run, real-kill only (default: 50)\n"
        "  --disorder-profile <bounded|heavy-tailed>  real-kill (default: bounded)\n"
        "  --late-fraction F      heavy-tail late-record fraction (default: 0)\n"
        "  --late-tail-ms N       heavy-tail extra lateness bound (default: 0)\n"
        "  --lateness-ms N        allowed lateness for real-kill (default: 0)\n"
        "                         (heavy-tailed + lateness re-fires a lot and the\n"
        "                          durable sink fsyncs each emit — use small\n"
        "                          --records, e.g. 6000)\n"
        "  --verbose              Print per-run details\n"
        "  --help                 Show this help\n");
}

namespace {

int RunInProcess(uint64_t num_seeds, uint64_t num_records,
                 uint64_t checkpoint_interval, NemesisPhase phase, bool verbose) {
    const char* phase_name = "between checkpoints";
    if (phase == NemesisPhase::kMidCheckpoint) phase_name = "mid-checkpoint";
    else if (phase == NemesisPhase::kMidEmission) phase_name = "mid-emission";

    std::printf("Nemesis test (in-process stop-restart): %" PRIu64 " runs, kill %s\n",
                num_seeds, phase_name);

    uint64_t passed = 0, total_duplicates = 0, total_missing = 0;
    for (uint64_t i = 0; i < num_seeds; ++i) {
        NemesisConfig config{};
        config.seed = 42 + i;
        config.num_records = num_records;
        config.checkpoint_interval = checkpoint_interval;
        config.kill_phase = phase;
        config.kill_position = 0.3 + 0.4 * (static_cast<double>(i) / static_cast<double>(num_seeds));

        auto result = RunNemesis(config);
        if (verbose) {
            std::printf("  [run %" PRIu64 "/%" PRIu64 "] %s (killed at %" PRIu64 ", restored, %" PRIu64 " duplicates, %" PRIu64 " missing)",
                        i + 1, num_seeds, result.passed ? "PASS" : "FAIL",
                        result.records_before_kill, result.duplicates_at_sink,
                        result.missing_results);
            if (!result.passed && !result.failure_detail.empty()) {
                std::printf(" — %s", result.failure_detail.c_str());
            }
            std::printf("\n");
        }
        if (result.passed) ++passed;
        total_duplicates += result.duplicates_at_sink;
        total_missing += result.missing_results;
    }

    std::printf("\nResult: %" PRIu64 "/%" PRIu64 " passed, %" PRIu64 " missing results across all runs\n",
                passed, num_seeds, total_missing);
    std::printf("Total duplicates: %" PRIu64 " (at-least-once working as designed)\n",
                total_duplicates);
    return (passed == num_seeds) ? 0 : 1;
}

int RunRealKill(uint64_t num_seeds, uint64_t num_records, uint64_t checkpoint_interval,
                uint32_t num_keys, RealKillPoint point, bool verbose,
                DisorderMode disorder_mode, double late_fraction,
                uint64_t late_tail_ms, uint64_t lateness_ms) {
    const char* point_name =
        (point == RealKillPoint::kMidCheckpoint) ? "mid-checkpoint" : "between-checkpoints";

    std::printf("Nemesis test (REAL fork+SIGKILL): %" PRIu64 " runs, kill %s\n",
                num_seeds, point_name);

    uint64_t passed = 0, total_duplicates = 0, total_missing = 0;
    uint64_t kills_confirmed = 0, stale_tmps = 0, clean_flushes = 0;

    for (uint64_t i = 0; i < num_seeds; ++i) {
        RealKillConfig config{};
        config.seed = 42 + i;
        config.num_records = num_records;
        config.num_keys = num_keys;
        config.disorder_mode = disorder_mode;
        config.late_fraction = late_fraction;
        config.late_tail = Duration{static_cast<int64_t>(late_tail_ms)};
        config.allowed_lateness = Duration{static_cast<int64_t>(lateness_ms)};
        config.checkpoint_interval = checkpoint_interval;
        config.kill_point = point;

        auto result = RunRealKillNemesis(config);

        if (result.passed) ++passed;
        if (result.killed_by_sigkill) ++kills_confirmed;
        if (result.stale_tmp_after_kill) ++stale_tmps;
        if (result.final_flush_completed) ++clean_flushes;
        total_duplicates += result.duplicates;
        total_missing += result.missing_results;

        if (verbose) {
            std::printf("  [run %" PRIu64 "/%" PRIu64 "] %s killed=%s flush=%s stale_tmp=%s "
                        "restored@%" PRIu64 " pre=%" PRIu64 " post=%" PRIu64 " union=%" PRIu64 " oracle=%" PRIu64 " "
                        "missing=%" PRIu64 " dup=%" PRIu64 " attempts=%u",
                        i + 1, num_seeds, result.passed ? "PASS" : "FAIL",
                        result.killed_by_sigkill ? "SIGKILL" : "no",
                        result.final_flush_completed ? "yes" : "no",
                        result.stale_tmp_after_kill ? "yes" : "no",
                        result.restored_offset, result.pre_crash_emits,
                        result.post_restore_emits, result.union_results,
                        result.oracle_results, result.missing_results,
                        result.duplicates, result.attempts);
            if (!result.passed && !result.failure_detail.empty()) {
                std::printf(" — %s", result.failure_detail.c_str());
            }
            std::printf("\n");
        }
    }

    std::printf("\nResult: %" PRIu64 "/%" PRIu64 " passed, %" PRIu64 " missing results across all runs\n",
                passed, num_seeds, total_missing);
    std::printf("Kill evidence: %" PRIu64 "/%" PRIu64 " terminated by SIGKILL, %" PRIu64 " clean-flush escapes, "
                "%" PRIu64 " interrupted checkpoint writes (stale .tmp recovered)\n",
                kills_confirmed, num_seeds, clean_flushes, stale_tmps);
    std::printf("Total duplicates: %" PRIu64 " (at-least-once; idempotent sink upgrades to effectively-once)\n",
                total_duplicates);
    return (passed == num_seeds && total_missing == 0) ? 0 : 1;
}

int RunPartitionedRealKill(uint64_t num_seeds, uint64_t num_records,
                           uint64_t checkpoint_interval, uint32_t num_keys,
                           uint32_t num_workers, bool verbose) {
    std::printf("Nemesis test (PARTITIONED fork+SIGKILL at torn global checkpoint): "
                "%" PRIu64 " runs, %u workers\n", num_seeds, num_workers);

    uint64_t passed = 0, total_missing = 0, total_duplicates = 0;
    uint64_t kills = 0, torns = 0, clean_flushes = 0;

    for (uint64_t i = 0; i < num_seeds; ++i) {
        PartitionedRealKillConfig config{};
        config.seed = 42 + i;
        config.num_records = num_records;
        config.num_keys = num_keys;
        config.num_workers = num_workers;
        config.checkpoint_interval = checkpoint_interval;
        config.target_checkpoint = 2;

        auto result = RunPartitionedRealKillNemesis(config);

        if (result.passed) ++passed;
        if (result.killed_by_sigkill) ++kills;
        if (result.torn_checkpoint_observed) ++torns;
        if (result.final_flush_completed) ++clean_flushes;
        total_missing += result.missing_results;
        total_duplicates += result.duplicates;

        if (verbose) {
            std::printf("  [run %" PRIu64 "/%" PRIu64 "] %s killed=%s flush=%s torn=%s "
                        "restored@%" PRIu64 " torn@%" PRIu64 " pre=%" PRIu64 " post=%" PRIu64 " "
                        "union=%" PRIu64 " oracle=%" PRIu64 " missing=%" PRIu64 " dup=%" PRIu64 " attempts=%u",
                        i + 1, num_seeds, result.passed ? "PASS" : "FAIL",
                        result.killed_by_sigkill ? "SIGKILL" : "no",
                        result.final_flush_completed ? "yes" : "no",
                        result.torn_checkpoint_observed ? "yes" : "no",
                        result.restored_offset, result.torn_offset,
                        result.pre_crash_emits, result.post_restore_emits,
                        result.union_results, result.oracle_results,
                        result.missing_results, result.duplicates, result.attempts);
            if (!result.passed && !result.failure_detail.empty()) {
                std::printf(" — %s", result.failure_detail.c_str());
            }
            std::printf("\n");
        }
    }

    std::printf("\nResult: %" PRIu64 "/%" PRIu64 " passed, %" PRIu64 " missing across all runs\n",
                passed, num_seeds, total_missing);
    std::printf("Kill evidence: %" PRIu64 "/%" PRIu64 " SIGKILL, %" PRIu64 " torn global checkpoints "
                "captured, %" PRIu64 " clean-flush escapes\n",
                kills, num_seeds, torns, clean_flushes);
    std::printf("Total duplicates: %" PRIu64 " (at-least-once across all workers)\n",
                total_duplicates);
    return (passed == num_seeds && total_missing == 0) ? 0 : 1;
}

} // namespace

int main(int argc, char* argv[]) {
    uint64_t num_seeds = 20;
    uint64_t num_records = 50000;
    uint64_t checkpoint_interval = 5000;
    uint32_t num_keys = 50;
    NemesisPhase phase = NemesisPhase::kBetweenCheckpoints;
    bool real_kill = false;
    RealKillPoint real_point = RealKillPoint::kBetweenCheckpoints;
    bool verbose = false;
    bool records_set = false;
    bool interval_set = false;
    bool partitioned = false;
    uint32_t num_workers = 4;
    DisorderMode disorder_mode = DisorderMode::kBounded;
    double late_fraction = 0.0;
    uint64_t late_tail_ms = 0;
    uint64_t lateness_ms = 0;

    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], "--seeds") == 0 && i + 1 < argc) {
            num_seeds = std::strtoull(argv[++i], nullptr, 10);
        } else if (std::strcmp(argv[i], "--records") == 0 && i + 1 < argc) {
            num_records = std::strtoull(argv[++i], nullptr, 10);
            records_set = true;
        } else if (std::strcmp(argv[i], "--checkpoint-interval") == 0 && i + 1 < argc) {
            checkpoint_interval = std::strtoull(argv[++i], nullptr, 10);
            interval_set = true;
        } else if (std::strcmp(argv[i], "--keys") == 0 && i + 1 < argc) {
            num_keys = static_cast<uint32_t>(std::strtoul(argv[++i], nullptr, 10));
        } else if (std::strcmp(argv[i], "--phase") == 0 && i + 1 < argc) {
            ++i;
            if (std::strcmp(argv[i], "mid-checkpoint") == 0) {
                phase = NemesisPhase::kMidCheckpoint;
            } else if (std::strcmp(argv[i], "mid-emission") == 0) {
                phase = NemesisPhase::kMidEmission;
            } else if (std::strcmp(argv[i], "between") == 0) {
                phase = NemesisPhase::kBetweenCheckpoints;
            } else {
                std::fprintf(stderr, "Unknown phase: %s\n", argv[i]);
                return 1;
            }
        } else if (std::strcmp(argv[i], "--real-kill") == 0) {
            real_kill = true;
        } else if (std::strcmp(argv[i], "--partitioned") == 0) {
            partitioned = true;
        } else if (std::strcmp(argv[i], "--workers") == 0 && i + 1 < argc) {
            num_workers = static_cast<uint32_t>(std::strtoul(argv[++i], nullptr, 10));
        } else if (std::strcmp(argv[i], "--real-phase") == 0 && i + 1 < argc) {
            ++i;
            if (std::strcmp(argv[i], "between") == 0) {
                real_point = RealKillPoint::kBetweenCheckpoints;
            } else if (std::strcmp(argv[i], "mid-checkpoint") == 0) {
                real_point = RealKillPoint::kMidCheckpoint;
            } else {
                std::fprintf(stderr, "Unknown real-phase: %s\n", argv[i]);
                return 1;
            }
        } else if (std::strcmp(argv[i], "--disorder-profile") == 0 && i + 1 < argc) {
            ++i;
            if (std::strcmp(argv[i], "heavy-tailed") == 0) {
                disorder_mode = DisorderMode::kHeavyTailed;
            } else if (std::strcmp(argv[i], "bounded") == 0) {
                disorder_mode = DisorderMode::kBounded;
            } else {
                std::fprintf(stderr, "Unknown disorder-profile: %s\n", argv[i]);
                return 1;
            }
        } else if (std::strcmp(argv[i], "--late-fraction") == 0 && i + 1 < argc) {
            late_fraction = std::strtod(argv[++i], nullptr);
        } else if (std::strcmp(argv[i], "--late-tail-ms") == 0 && i + 1 < argc) {
            late_tail_ms = std::strtoull(argv[++i], nullptr, 10);
        } else if (std::strcmp(argv[i], "--lateness-ms") == 0 && i + 1 < argc) {
            lateness_ms = std::strtoull(argv[++i], nullptr, 10);
        } else if (std::strcmp(argv[i], "--verbose") == 0) {
            verbose = true;
        } else if (std::strcmp(argv[i], "--help") == 0) {
            PrintUsage();
            return 0;
        } else {
            std::fprintf(stderr, "Unknown option: %s\n", argv[i]);
            PrintUsage();
            return 1;
        }
    }

    if (partitioned) {
        // Partitioned real-kill defaults: small workload (per-emit fsync),
        // frequent catchable checkpoints.
        if (!records_set) num_records = 6000;
        if (!interval_set) checkpoint_interval = 500;
        return RunPartitionedRealKill(num_seeds, num_records, checkpoint_interval,
                                      num_keys, num_workers, verbose);
    }

    if (real_kill) {
        // Real-kill defaults favor frequent, catchable checkpoints. Only override
        // when the user set them explicitly.
        if (!num_records || (!records_set && num_records == 50000)) num_records = 40000;
        if (!interval_set && checkpoint_interval == 5000) checkpoint_interval = 1000;
        return RunRealKill(num_seeds, num_records, checkpoint_interval, num_keys,
                           real_point, verbose, disorder_mode, late_fraction,
                           late_tail_ms, lateness_ms);
    }

    return RunInProcess(num_seeds, num_records, checkpoint_interval, phase, verbose);
}
