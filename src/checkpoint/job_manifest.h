#pragma once

#include <map>
#include <stdexcept>
#include <string>

namespace stormglass {

// Job-configuration fingerprint for a native checkpoint directory.
//
// Operator checkpoints hold keyed panes and a watermark, but not the job shape
// that gives them meaning: a pane keyed by a 1000 ms window is silently wrong
// under a 2000 ms assigner, and N partition files are meaningless to M workers.
// The manifest records that shape once, as ordered key=value fields in
// <checkpoint_dir>/job.manifest, and every later run must present identical
// fields before any checkpoint is restored.
//
// Fields are opaque strings chosen by the engine (worker count, lateness,
// WindowAssigner::Descriptor(), Source::Descriptor()). An empty descriptor is
// still compared, so it only matches another empty descriptor.
using JobManifest = std::map<std::string, std::string>;

// Thrown when a checkpoint directory belongs to a different job configuration,
// or holds checkpoints with no manifest to verify them against.
class JobManifestMismatch : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

// Validate `expected` against <checkpoint_dir>/job.manifest.
//   * Manifest present: every field must match exactly, else JobManifestMismatch
//     naming each differing field.
//   * Manifest absent, no *.ckpt anywhere under the directory: a fresh job. The
//     manifest is written atomically (tmp + fsync + rename + directory fsync)
//     before the caller writes its first checkpoint.
//   * Manifest absent but checkpoints present: unverifiable, JobManifestMismatch.
// The directory is created if missing. I/O failures throw std::system_error.
void ValidateOrCreateJobManifest(const std::string& checkpoint_dir,
                                 const JobManifest& expected);

} // namespace stormglass
