#include "checkpoint/job_manifest.h"

#include <cerrno>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <optional>
#include <sstream>
#include <system_error>
#include <unistd.h>

namespace stormglass {

namespace {

constexpr const char* kManifestName = "job.manifest";
constexpr const char* kHeader = "stormglass-job-manifest v1";

std::string Serialize(const JobManifest& manifest) {
    std::string out = std::string(kHeader) + "\n";
    for (const auto& [key, value] : manifest) {
        if (key.empty() || key.find_first_of("=\n") != std::string::npos ||
            value.find('\n') != std::string::npos) {
            throw std::invalid_argument("job manifest field cannot contain '=' in its key or a newline: " + key);
        }
        out += key + "=" + value + "\n";
    }
    return out;
}

std::optional<JobManifest> Load(const std::string& path) {
    std::ifstream in(path);
    if (!in) return std::nullopt;
    std::string line;
    if (!std::getline(in, line) || line != kHeader) {
        throw JobManifestMismatch("unrecognized job manifest format: " + path);
    }
    JobManifest manifest;
    while (std::getline(in, line)) {
        auto eq = line.find('=');
        if (eq == std::string::npos || eq == 0) {
            throw JobManifestMismatch("malformed job manifest line in " + path + ": " + line);
        }
        manifest[line.substr(0, eq)] = line.substr(eq + 1);
    }
    return manifest;
}

bool HasCheckpointFiles(const std::string& dir) {
    for (const auto& entry : std::filesystem::recursive_directory_iterator(dir)) {
        const auto name = entry.path().filename().string();
        if (name.size() > 5 && name.compare(name.size() - 5, 5, ".ckpt") == 0) return true;
    }
    return false;
}

void SyncOrThrow(int fd, const char* what) {
    if (::fsync(fd) != 0) {
        const int err = errno;
        ::close(fd);
        throw std::system_error(err, std::generic_category(), what);
    }
}

void WriteAtomically(const std::string& dir, const std::string& contents) {
    const std::string final_path = dir + "/" + kManifestName;
    const std::string tmp_path = final_path + ".tmp";

    int fd = ::open(tmp_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) throw std::system_error(errno, std::generic_category(), "open job manifest");
    std::size_t written = 0;
    while (written < contents.size()) {
        auto n = ::write(fd, contents.data() + written, contents.size() - written);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) {
            const int err = errno;
            ::close(fd);
            ::unlink(tmp_path.c_str());
            throw std::system_error(err, std::generic_category(), "write job manifest");
        }
        written += static_cast<std::size_t>(n);
    }
    SyncOrThrow(fd, "fsync job manifest");
    ::close(fd);

    if (::rename(tmp_path.c_str(), final_path.c_str()) != 0) {
        const int err = errno;
        ::unlink(tmp_path.c_str());
        throw std::system_error(err, std::generic_category(), "rename job manifest");
    }
    int dir_fd = ::open(dir.c_str(), O_RDONLY | O_DIRECTORY);
    if (dir_fd < 0) throw std::system_error(errno, std::generic_category(), "open checkpoint dir");
    SyncOrThrow(dir_fd, "fsync checkpoint dir");
    ::close(dir_fd);
}

} // namespace

void ValidateOrCreateJobManifest(const std::string& checkpoint_dir,
                                 const JobManifest& expected) {
    std::filesystem::create_directories(checkpoint_dir);
    const std::string contents = Serialize(expected);
    const std::string path = checkpoint_dir + "/" + kManifestName;

    if (auto stored = Load(path)) {
        if (*stored == expected) return;
        std::ostringstream diff;
        diff << "checkpoint directory " << checkpoint_dir
             << " was written by a different job configuration:";
        auto describe = [](const JobManifest& m, const std::string& key) {
            auto it = m.find(key);
            return it == m.end() ? std::string("<absent>") : "'" + it->second + "'";
        };
        JobManifest keys = *stored;
        keys.insert(expected.begin(), expected.end());
        for (const auto& [key, unused] : keys) {
            if (describe(*stored, key) != describe(expected, key)) {
                diff << " " << key << " checkpointed=" << describe(*stored, key)
                     << " configured=" << describe(expected, key) << ";";
            }
        }
        throw JobManifestMismatch(diff.str());
    }

    if (HasCheckpointFiles(checkpoint_dir)) {
        throw JobManifestMismatch("checkpoint directory " + checkpoint_dir +
                                  " holds checkpoints but no job manifest; refusing an unverifiable restore");
    }
    WriteAtomically(checkpoint_dir, contents);
}

} // namespace stormglass
