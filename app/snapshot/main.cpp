#include "snapshot/chandy_lamport.h"
#include "snapshot/codec.h"
#include "snapshot/store.h"
#include "snapshot/tcp_transport.h"
#include "snapshot/window_operator.h"
#include "sink/memory_sink.h"
#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <filesystem>
#include <iostream>
#include <poll.h>
#include <sys/wait.h>
#include <unistd.h>

using namespace stormglass;
using namespace stormglass::snapshot;
namespace {
const Topology topology{{0, {2}}, {1, {2}}, {2, {0, 1}}};
constexpr uint64_t kRecords = 100;
const std::string config = "stormglass-tcp-demo-v1:tumbling=1000:keys=4:records=100:peers=0,1,2";
using Clock = std::chrono::steady_clock;
void Require(bool ok, const char *message) {
    if (!ok)
        throw std::runtime_error(message);
}
Record Event(NodeId id, uint64_t i) {
    return {"key-" + std::to_string(i % 4), static_cast<int64_t>(i + 1 + id * 100),
            Timestamp{Duration{static_cast<int64_t>(i)}},
            Timestamp{Duration{static_cast<int64_t>(i)}}};
}
std::vector<PeerSocket> Connections(NodeId id, const std::array<std::array<int, 2>, 2> &sockets) {
    if (id == 2)
        return {{0, sockets[0][1]}, {1, sockets[1][1]}};
    return {{2, sockets[id][0]}};
}
int Child(NodeId id, std::vector<PeerSocket> sockets, int permission, const std::string &directory,
          uint64_t epoch, const std::vector<LocalSnapshot> &restored, bool stall) {
    try {
        TcpTransport transport(std::move(sockets));
        Store store(directory);
        MemorySink sink;
        std::unique_ptr<WindowOperator> window;
        uint64_t cursor = 0;
        bool ack = false, saved = false, allowed = false;
        if (id == 2)
            window = std::make_unique<WindowOperator>(std::vector<NodeId>{0, 1}, sink);
        auto capture = [&]() {
            if (window)
                return window->Capture();
            Encoder e;
            e.U64(cursor);
            return e.bytes;
        };
        auto deliver = [&](NodeId peer, const Bytes &b) {
            if (window)
                window->Apply(peer, b);
            else {
                Require(b == Bytes{99}, "invalid producer acknowledgement");
                ack = true;
            }
        };
        Participant participant(id, topology.at(id), capture, deliver,
                                [&](NodeId peer, const Message &m) { transport.Queue(peer, m); });
        if (!restored.empty()) {
            const auto &s = *std::find_if(restored.begin(), restored.end(),
                                          [&](const auto &s) { return s.node == id; });
            if (window)
                window->Restore(s.state);
            else {
                Decoder d(s.state);
                cursor = d.U64();
                d.End();
                Require(cursor <= kRecords, "invalid saved source cursor");
            }
            participant.Restore(s);
        }
        auto receive = [&](NodeId peer, const Message &m) {
            participant.Receive(peer, m);
            if (participant.HasCompleted()) {
                store.SaveLocal(participant.TakeCompleted());
                saved = true;
            }
        };
        if (window)
            participant.Initiate(epoch);
        else {
            auto cut = std::min(kRecords, cursor + 25);
            while (cursor < cut) {
                participant.Send(2, WindowOperator::Data(Event(id, cursor)));
                ++cursor;
            }
        }
        const auto deadline = Clock::now() + std::chrono::seconds(15);
        bool tail_sent = false, ack_sent = false;
        while (Clock::now() < deadline) {
            if (stall && id == 1) {
                ::usleep(1000);
                continue;
            } // deliberate missing marker in crash case
            transport.PollOnce(receive, 1);
            if (!window && saved && !tail_sent) {
                while (cursor < kRecords) {
                    participant.Send(2, WindowOperator::Data(Event(id, cursor)));
                    ++cursor;
                }
                participant.Send(2, WindowOperator::Watermark(Timestamp{Duration{1000}}));
                tail_sent = true;
            }
            if (!window && ack)
                return 0;
            if (window) {
                pollfd p{permission, POLLIN, 0};
                if (!allowed && ::poll(&p, 1, 0) > 0) {
                    char c = 0;
                    Require(::read(permission, &c, 1) == 1 && c == 'C',
                            "invalid commit permission");
                    allowed = true;
                }
                if (allowed && !ack_sent && window->records() == 2 * kRecords &&
                    window->watermark() == Timestamp{Duration{1000}}) {
                    window->FinalFlush();
                    std::map<std::string, std::pair<int64_t, uint64_t>> expected, actual;
                    for (NodeId source : {0u, 1u})
                        for (uint64_t i = 0; i < kRecords; ++i) {
                            auto r = Event(source, i);
                            expected[r.key].first += r.value;
                            ++expected[r.key].second;
                        }
                    for (const auto &r : sink.Results()) {
                        Require(r.window.start == Timestamp{Duration{0}} &&
                                    r.window.end == Timestamp{Duration{1000}},
                                "incorrect recovered window");
                        Require(
                            actual.emplace(r.key, std::make_pair(r.result.value, r.result.count))
                                .second,
                            "duplicate recovered window");
                    }
                    Require(actual == expected,
                            "recovered output disagrees with independent aggregate");
                    participant.Send(0, Bytes{99});
                    participant.Send(1, Bytes{99});
                    ack_sent = true;
                }
                if (ack_sent && transport.Drained())
                    return 0;
            }
        }
        throw std::runtime_error("TCP snapshot demo deadline exceeded");
    } catch (const std::exception &e) {
        std::cerr << "node " << id << ": " << e.what() << '\n';
        return 1;
    }
}
struct Children {
    std::array<pid_t, 3> pids{};
    ~Children() {
        for (auto pid : pids)
            if (pid > 0) {
                ::kill(pid, SIGKILL);
                while (::waitpid(pid, nullptr, 0) < 0 && errno == EINTR) {
                }
            }
    }
};
void Run(const std::string &dir, uint64_t epoch, const std::vector<LocalSnapshot> &restored,
         bool crash) {
    std::array<std::array<int, 2>, 2> sockets{};
    for (auto &pair : sockets) {
        uint16_t port = 0;
        int listener = ListenTcp("127.0.0.1", 0, port);
        pair[0] = ConnectTcp("127.0.0.1", port);
        pair[1] = AcceptTcp(listener);
        ::close(listener);
    }
    int permission[2];
    Require(::pipe(permission) == 0, "pipe failed");
    Children children;
    for (NodeId id = 0; id < 3; ++id) {
        auto pid = ::fork();
        Require(pid >= 0, "fork failed");
        if (pid == 0) {
            ::close(permission[1]);
            if (id != 2)
                ::close(permission[0]);
            auto own = Connections(id, sockets);
            for (auto pair : sockets)
                for (int fd : pair)
                    if (std::none_of(own.begin(), own.end(), [&](auto s) { return s.fd == fd; }))
                        ::close(fd);
            int code = Child(id, std::move(own), permission[0], dir, epoch, restored, crash);
            if (id == 2)
                ::close(permission[0]);
            ::_exit(code);
        }
        children.pids[id] = pid;
    }
    ::close(permission[0]);
    for (auto pair : sockets)
        for (int fd : pair)
            ::close(fd);
    auto deadline = Clock::now() + std::chrono::seconds(12);
    bool ready = false;
    while (Clock::now() < deadline) {
        ready = true;
        for (NodeId id = 0; id < (crash ? 1u : 3u); ++id)
            ready &= std::filesystem::exists(std::filesystem::path(dir) /
                                             ("epoch-" + std::to_string(epoch)) /
                                             ("node-" + std::to_string(id) + ".snapshot"));
        if (ready)
            break;
        ::usleep(1000);
    }
    Require(ready, "participants did not finish snapshots");
    Store store(dir);
    if (crash) {
        Require(::kill(children.pids[2], SIGKILL) == 0, "SIGKILL failed");
        int status = 0;
        Require(::waitpid(children.pids[2], &status, 0) == children.pids[2] &&
                    WIFSIGNALED(status) && WTERMSIG(status) == SIGKILL,
                "crash not confirmed");
        children.pids[2] = 0;
        bool rejected = false;
        try {
            store.Commit(epoch, topology, config);
        } catch (...) {
            rejected = true;
        }
        Require(rejected, "partial snapshot committed");
        rejected = false;
        try {
            store.LoadCommitted(epoch, topology, config);
        } catch (...) {
            rejected = true;
        }
        Require(rejected, "partial snapshot restored");
        ::close(permission[1]);
        return;
    }
    store.Commit(epoch, topology, config);
    Require(::write(permission[1], "C", 1) == 1, "commit permission failed");
    ::close(permission[1]);
    for (auto &pid : children.pids) {
        int status = 0;
        pid_t result = 0;
        while (Clock::now() < deadline && (result = ::waitpid(pid, &status, WNOHANG)) == 0)
            ::usleep(1000);
        Require(result == pid, "child did not terminate");
        pid = 0;
        Require(WIFEXITED(status) && WEXITSTATUS(status) == 0, "snapshot child failed");
    }
}
} // namespace
int main(int argc, char **argv) try {
    std::string directory;
    bool crash = false;
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--directory" && i + 1 < argc)
            directory = argv[++i];
        else if (arg == "--crash")
            crash = true;
        else
            throw std::invalid_argument("usage: --directory PATH [--crash]");
    }
    std::vector<char> temporary;
    bool cleanup = directory.empty();
    if (cleanup) {
        std::string pattern = "/tmp/stormglass-tcp-snapshot-XXXXXX";
        temporary.assign(pattern.begin(), pattern.end());
        temporary.push_back(0);
        Require(::mkdtemp(temporary.data()) != nullptr, "mkdtemp failed");
        directory = temporary.data();
    }
    // Initial run, optional death with a marker still missing, then a fresh
    // three-process recovery from the last globally committed consistent cut.
    Run(directory, 1, {}, false);
    Store store(directory);
    auto snapshot = store.LoadCommitted(1, topology, config);
    std::size_t in_flight = 0;
    for (auto &s : snapshot)
        in_flight += s.channels.size();
    Require(in_flight > 0, "test did not capture in-flight messages");
    if (crash) {
        Run(directory, 2, snapshot, true);
        snapshot = store.LoadLatestCommitted(topology, config);
        Require(snapshot.front().epoch == 1, "incomplete epoch replaced prior commit");
    }
    Run(directory, crash ? 3 : 2, snapshot, false);
    std::cout << "PASS: 3 TCP processes; epoch=1 in_flight=" << in_flight
              << "; recovered_records=200; independent_window_aggregate=match; mid_marker_sigkill="
              << (crash ? 1 : 0) << '\n';
    if (cleanup)
        std::filesystem::remove_all(directory);
    else
        std::cout << "Snapshots: " << directory << '\n';
    return 0;
} catch (const std::exception &e) {
    std::cerr << "snapshot demo: " << e.what() << '\n';
    return 1;
}
