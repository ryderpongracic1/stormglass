#pragma once
#include "stream/batch.h"
#include <optional>
#include <string>

namespace stormglass {

// A pull-based input. Next() returns:
//   * a batch with items: records and in-band control (watermarks, barriers);
//   * an EMPTY batch: no data is available yet, but the input is still live.
//     Engines back off briefly and call Next() again, so a quiet live source
//     neither ends the job nor spins a core;
//   * std::nullopt: end of input (or the source stopped after Cancel()).
class Source {
public:
    virtual ~Source() = default;
    virtual std::optional<Batch> Next() = 0;
    virtual void Seek(uint64_t offset) = 0;
    [[nodiscard]] virtual uint64_t CurrentOffset() const = 0;

    // Stable identity of the replayed record/control trajectory, recorded in a
    // checkpoint directory's job manifest so restore can reject a different
    // source. Include everything that changes what Seek(O) followed by Next()
    // yields (identity, ordering, watermark and barrier cadence); exclude what
    // only extends or regroups it (stream length, batch size). Empty means the
    // source is not validated on restore.
    [[nodiscard]] virtual std::string Descriptor() const { return ""; }

    // Whether Seek(O) followed by Next() reproduces the trajectory an
    // uninterrupted run produced after O. Checkpointed jobs refuse to start on a
    // source that returns false, since restoring from it would silently corrupt
    // state. Defaults to true, the historical contract.
    [[nodiscard]] virtual bool Replayable() const { return true; }

    // Called from another thread when the job is asked to stop. Must be
    // thread-safe, must not throw, and should make a Next() blocked waiting for
    // input return promptly (an empty batch, a partial batch, or std::nullopt).
    // Sources that never block need not override it.
    virtual void Cancel() {}
};

} // namespace stormglass
