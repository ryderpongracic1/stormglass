#include "snapshot/chandy_lamport.h"
#include "snapshot/codec.h"
#include "snapshot/store.h"
#include "snapshot/tcp_transport.h"
#include "snapshot/window_operator.h"
#include "sink/memory_sink.h"
#include "checkpoint/crc32c.h"
#include <gtest/gtest.h>
#include <array>
#include <deque>
#include <filesystem>
#include <fstream>
#include <random>
#include <thread>
#include <unistd.h>

using namespace stormglass;
using namespace stormglass::snapshot;
namespace {
Bytes Number(int64_t n) {
    Encoder e;
    e.I64(n);
    return e.bytes;
}
int64_t Number(const Bytes &b) {
    Decoder d(b);
    auto n = d.I64();
    d.End();
    return n;
}
std::vector<LocalSnapshot> Cut() {
    LocalSnapshot a{0, 1, Number(9), {{1, 0}}, {{1, 1}}, {{1, 0}}, {}};
    LocalSnapshot b{1, 1, Number(10), {{0, 0}}, {{0, 0}}, {{0, 1}}, {{0, 1, Number(1)}}};
    return {a, b};
}
const Topology pair_topology{{0, {1}}, {1, {0}}};
struct Temp {
    std::string path;
    Temp() {
        char pattern[] = "/tmp/stormglass-cl-test-XXXXXX";
        auto p = ::mkdtemp(pattern);
        if (!p)
            throw std::runtime_error("mkdtemp");
        path = p;
    }
    ~Temp() { std::filesystem::remove_all(path); }
};
Record R(int64_t value, int64_t event = 1) {
    return {"k", value, Timestamp{Duration{event}}, Timestamp{Duration{0}}};
}
} // namespace
TEST(ChandyLamport, RandomFifoSchedulesPreserveIndependentTokenConservation) {
    const Topology topology{{0, {1, 2}}, {1, {0, 2}}, {2, {0, 1}}};
    for (uint64_t seed = 0; seed < 100; ++seed) {
        SCOPED_TRACE(seed);
        std::mt19937_64 rng(seed);
        std::array<int64_t, 3> balances{10000, 10000, 10000};
        std::map<std::pair<NodeId, NodeId>, std::deque<Message>> queues;
        std::array<std::unique_ptr<Participant>, 3> nodes;
        std::map<NodeId, LocalSnapshot> snapshots;
        for (NodeId id = 0; id < 3; ++id)
            nodes[id] = std::make_unique<Participant>(
                id, topology.at(id), [&, id] { return Number(balances[id]); },
                [&, id](NodeId, const Bytes &b) { balances[id] += Number(b); },
                [&, id](NodeId peer, const Message &m) { queues[{id, peer}].push_back(m); });
        auto send = [&] {
            NodeId from = rng() % 3, to = (from + 1 + rng() % 2) % 3;
            int64_t amount = 1 + rng() % 5;
            balances[from] -= amount;
            nodes[from]->Send(to, Number(amount));
        };
        for (int i = 0; i < 30; ++i)
            send();
        nodes[0]->Initiate(1);
        if (seed % 2 == 0)
            nodes[1]->Initiate(1); // concurrent initiators, same epoch
        auto receive = [&] {
            std::vector<std::pair<NodeId, NodeId>> ready;
            for (auto &[edge, q] : queues)
                if (!q.empty())
                    ready.push_back(edge);
            if (ready.empty())
                return false;
            auto edge = ready[rng() % ready.size()];
            auto m = std::move(queues[edge].front());
            queues[edge].pop_front();
            nodes[edge.second]->Receive(edge.first, m);
            if (nodes[edge.second]->HasCompleted())
                snapshots.emplace(edge.second, nodes[edge.second]->TakeCompleted());
            return true;
        };
        for (int step = 0; step < 300; ++step) {
            if (rng() % 2)
                send();
            receive();
        }
        while (receive()) {
        }
        ASSERT_EQ(snapshots.size(), 3u);
        std::vector<LocalSnapshot> all;
        int64_t conserved = 0;
        for (const auto &[id, s] : snapshots) {
            (void)id;
            all.push_back(s);
            conserved += Number(s.state);
            for (auto &r : s.channels)
                conserved += Number(r.payload);
        }
        EXPECT_NO_THROW(ValidateGlobal(all, topology));
        EXPECT_EQ(conserved, 30000);
        EXPECT_EQ(balances[0] + balances[1] + balances[2], 30000);
    }
}
TEST(ChandyLamport, FirstMarkerSavesEmptyArrivingChannelAndRecordsOtherChannel) {
    int64_t balance = 0;
    std::vector<std::pair<NodeId, Message>> outgoing;
    Participant p(
        2, {0, 1}, [&] { return Number(balance); },
        [&](NodeId, const Bytes &b) { balance += Number(b); },
        [&](NodeId peer, const Message &m) { outgoing.emplace_back(peer, m); });
    p.Receive(0, {Message::Kind::Data, 1, 0, Number(5)});
    p.Receive(0, {Message::Kind::Marker, 1, 7, {}});
    p.Receive(0, {Message::Kind::Data, 2, 0, Number(100)}); // post-cut on marked input
    p.Receive(1, {Message::Kind::Data, 1, 0, Number(3)});
    EXPECT_FALSE(p.HasCompleted());
    p.Receive(1, {Message::Kind::Marker, 1, 7, {}});
    auto s = p.TakeCompleted();
    EXPECT_EQ(Number(s.state), 5);
    ASSERT_EQ(s.channels.size(), 1u);
    EXPECT_EQ(s.channels[0].peer, 1u);
    EXPECT_EQ(Number(s.channels[0].payload), 3);
    EXPECT_EQ(balance, 108);
    ASSERT_EQ(outgoing.size(), 2u);
    for (auto &[peer, m] : outgoing) {
        (void)peer;
        EXPECT_EQ(m.kind, Message::Kind::Marker);
        EXPECT_EQ(m.epoch, 7u);
    }
}
TEST(ChandyLamport, MarkersPrecedePostCutApplicationSends) {
    std::vector<Message> sent;
    Participant p(
        0, {1}, [] { return Bytes{}; }, [](NodeId, const Bytes &) {},
        [&](NodeId, const Message &m) { sent.push_back(m); });
    p.Send(1, Number(1));
    p.Initiate(1);
    p.Send(1, Number(2));
    ASSERT_EQ(sent.size(), 3u);
    EXPECT_EQ(sent[1].kind, Message::Kind::Marker);
    EXPECT_EQ(sent[1].sequence, 1u);
    EXPECT_EQ(sent[2].sequence, 2u);
}
TEST(ChandyLamport, QuietChannelCannotBeExcluded) {
    Participant p(
        0, {1, 2}, [] { return Bytes{}; }, [](NodeId, const Bytes &) {},
        [](NodeId, const Message &) {});
    p.Initiate(1);
    p.Receive(1, {Message::Kind::Marker, 0, 1, {}});
    EXPECT_FALSE(p.HasCompleted());
    EXPECT_THROW(p.TakeCompleted(), std::runtime_error);
}
TEST(ChandyLamport, DuplicateOutOfOrderAndMixedEpochFailClosed) {
    for (int scenario = 0; scenario < 3; ++scenario) {
        Participant p(
            0, {1, 2}, [] { return Bytes{}; }, [](NodeId, const Bytes &) {},
            [](NodeId, const Message &) {});
        p.Initiate(1);
        if (scenario == 0)
            EXPECT_THROW(p.Receive(1, {Message::Kind::Data, 2, 0, {}}), std::runtime_error);
        if (scenario == 1)
            EXPECT_THROW(p.Receive(1, {Message::Kind::Marker, 0, 2, {}}), std::runtime_error);
        if (scenario == 2) {
            p.Receive(1, {Message::Kind::Marker, 0, 1, {}});
            EXPECT_THROW(p.Receive(1, {Message::Kind::Marker, 0, 1, {}}), std::runtime_error);
        }
        EXPECT_TRUE(p.failed());
        EXPECT_THROW(p.Send(1, {}), std::runtime_error);
    }
}
TEST(ChandyLamport, ChannelRecordingBudgetFailurePoisonsEpoch) {
    Participant p(
        0, {1}, [] { return Bytes{}; }, [](NodeId, const Bytes &) {},
        [](NodeId, const Message &) {}, 16);
    p.Initiate(1);
    EXPECT_THROW(p.Receive(1, {Message::Kind::Data, 1, 0, Bytes(17)}), std::runtime_error);
    EXPECT_TRUE(p.failed());
    EXPECT_FALSE(p.HasCompleted());
}
TEST(ChandyLamport, RestoreReplaysChannelBeforeFreshSequence) {
    auto s = Cut()[1];
    int64_t balance = Number(s.state);
    Participant p(
        1, {0}, [&] { return Number(balance); },
        [&](NodeId, const Bytes &b) { balance += Number(b); }, [](NodeId, const Message &) {});
    p.Restore(s);
    EXPECT_EQ(balance, 11);
    p.Receive(0, {Message::Kind::Data, 2, 0, Number(4)});
    EXPECT_EQ(balance, 15);
    EXPECT_THROW(p.Receive(0, {Message::Kind::Data, 1, 0, Number(1)}), std::runtime_error);
}
TEST(ChandyLamport, MultipleEpochsCaptureNewCuts) {
    Participant p(
        0, {1}, [] { return Bytes{}; }, [](NodeId, const Bytes &) {},
        [](NodeId, const Message &) {});
    for (uint64_t epoch = 1; epoch <= 3; ++epoch) {
        p.Initiate(epoch);
        p.Receive(1, {Message::Kind::Marker, 0, epoch, {}});
        EXPECT_EQ(p.TakeCompleted().epoch, epoch);
    }
    EXPECT_THROW(p.Initiate(3), std::runtime_error);
}
TEST(ChandyLamport, GlobalCutRejectsMissingOrOrphanMessages) {
    EXPECT_NO_THROW(ValidateGlobal(Cut(), pair_topology));
    auto s = Cut();
    s[1].channels.clear();
    EXPECT_THROW(ValidateGlobal(s, pair_topology), std::runtime_error);
    s = Cut();
    s[0].sent[1] = 0;
    EXPECT_THROW(ValidateGlobal(s, pair_topology), std::runtime_error);
    s = Cut();
    s[1].channels[0].sequence = 2;
    EXPECT_THROW(ValidateGlobal(s, pair_topology), std::runtime_error);
    s = Cut();
    s[1].epoch = 2;
    EXPECT_THROW(ValidateGlobal(s, pair_topology), std::runtime_error);
}
TEST(ChandyLamportStore, LocalFilesCannotRestoreBeforeCommit) {
    Temp temp;
    Store store(temp.path);
    auto s = Cut();
    store.SaveLocal(s[0]);
    EXPECT_THROW(store.Commit(1, pair_topology, "job"), std::runtime_error);
    store.SaveLocal(s[1]);
    EXPECT_THROW(store.LoadCommitted(1, pair_topology, "job"), std::runtime_error);
    store.Commit(1, pair_topology, "job");
    auto restored = store.LoadCommitted(1, pair_topology, "job");
    ASSERT_EQ(restored.size(), 2u);
    EXPECT_NO_THROW(ValidateGlobal(restored, pair_topology));
}
TEST(ChandyLamportStore, CommittedConfigurationCannotChange) {
    Temp temp;
    Store store(temp.path);
    for (auto s : Cut())
        store.SaveLocal(s);
    store.Commit(1, pair_topology, "job");
    EXPECT_THROW(store.LoadCommitted(1, pair_topology, "other-job"), std::runtime_error);
    EXPECT_THROW(store.LoadCommitted(1, Topology{{0, {1}}}, "job"), std::runtime_error);
}
TEST(ChandyLamportStore, CorruptionAndTruncationRejected) {
    auto bytes = Store::EncodeLocal(Cut()[1]);
    auto broken = bytes;
    broken[20] ^= 1;
    EXPECT_THROW(Store::DecodeLocal(broken), std::runtime_error);
    bytes.pop_back();
    EXPECT_THROW(Store::DecodeLocal(bytes), std::runtime_error);
}
TEST(ChandyLamportStore, ChangedPayloadWithValidCrcStillBreaksManifest) {
    Temp temp;
    Store store(temp.path);
    auto s = Cut();
    for (auto local : s)
        store.SaveLocal(local);
    store.Commit(1, pair_topology, "job");
    s[0].state = Number(99);
    auto bytes = Store::EncodeLocal(s[0]);
    auto path = std::filesystem::path(temp.path) / "epoch-1/node-0.snapshot";
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    out.write(reinterpret_cast<const char *>(bytes.data()), bytes.size());
    out.close();
    EXPECT_THROW(store.LoadCommitted(1, pair_topology, "job"), std::runtime_error);
}
TEST(ChandyLamportStore, SnapshotSlotsAreImmutableAndInterruptedEpochCannotReplaceOld) {
    Temp temp;
    Store store(temp.path);
    auto s = Cut();
    for (auto local : s)
        store.SaveLocal(local);
    store.Commit(1, pair_topology, "job");
    EXPECT_NO_THROW(store.SaveLocal(s[0]));
    s[0].state = Number(999);
    EXPECT_THROW(store.SaveLocal(s[0]), std::runtime_error);
    s = Cut();
    s[0].epoch = 2;
    store.SaveLocal(s[0]);
    EXPECT_THROW(store.Commit(2, pair_topology, "job"), std::runtime_error);
    EXPECT_NO_THROW(store.LoadCommitted(1, pair_topology, "job"));
}
TEST(NetworkWindowSnapshot, RestoresWatermarkMinAndPendingLateRefire) {
    MemorySink sink;
    WindowOperator original({0, 1}, sink, 1000, 0, 500);
    original.Apply(0, WindowOperator::Data(R(5)));
    original.Apply(1, WindowOperator::Data(R(7)));
    original.Apply(0, WindowOperator::Watermark(Timestamp{Duration{1000}}));
    EXPECT_TRUE(sink.Results().empty());
    original.Apply(1, WindowOperator::Watermark(Timestamp{Duration{1000}}));
    ASSERT_EQ(sink.Results().size(), 1u);
    original.Apply(1, WindowOperator::Data(R(3)));
    auto state = original.Capture();
    MemorySink restored_sink;
    WindowOperator restored({0, 1}, restored_sink, 1000, 0, 500);
    restored.Restore(state);
    EXPECT_EQ(restored.records(), 3u);
    restored.FinalFlush();
    ASSERT_EQ(restored_sink.Results().size(), 1u);
    EXPECT_EQ(restored_sink.Results()[0].result.value, 15);
    EXPECT_EQ(restored_sink.Results()[0].result.count, 3u);
}
TEST(NetworkWindowSnapshot, SlidingStateRoundTripAndConfigurationRejection) {
    MemorySink sink;
    WindowOperator original({0, 1}, sink, 1000, 500, 100);
    original.Apply(0, WindowOperator::Data(R(7, 750)));
    auto bytes = original.Capture();
    MemorySink restored_sink;
    WindowOperator restored({0, 1}, restored_sink, 1000, 500, 100);
    restored.Restore(bytes);
    restored.FinalFlush();
    ASSERT_EQ(restored_sink.Results().size(), 2u);
    for (auto &r : restored_sink.Results())
        EXPECT_EQ(r.result.value, 7);
    WindowOperator wrong({0, 1}, sink, 1000, 0, 100);
    EXPECT_THROW(wrong.Restore(bytes), std::runtime_error);
    WindowOperator missing({0}, sink, 1000, 500, 100);
    EXPECT_THROW(missing.Restore(bytes), std::runtime_error);
}
TEST(NetworkWindowSnapshot, UnknownInputAndRegressingWatermarkFail) {
    MemorySink sink;
    WindowOperator op({0, 1}, sink);
    EXPECT_THROW(op.Apply(3, WindowOperator::Data(R(1))), std::runtime_error);
    op.Apply(0, WindowOperator::Watermark(Timestamp{Duration{100}}));
    EXPECT_THROW(op.Apply(0, WindowOperator::Watermark(Timestamp{Duration{99}})),
                 std::runtime_error);
}
TEST(TcpSnapshotTransport, RealTcpFramingAndBidirectionalMarkers) {
    uint16_t port = 0;
    int listener = ListenTcp("127.0.0.1", 0, port);
    int a = ConnectTcp("127.0.0.1", port);
    int b = AcceptTcp(listener);
    ::close(listener);
    TcpTransport left({{1, a}}), right({{0, b}});
    left.Queue(1, {Message::Kind::Data, 1, 0, Bytes(40000, 7)});
    left.Queue(1, {Message::Kind::Marker, 1, 5, {}});
    std::vector<Message> messages;
    right.Queue(0, {Message::Kind::Marker, 0, 5, {}});
    int reverse = 0;
    for (int i = 0; i < 1000 && (messages.size() < 2 || !reverse); ++i) {
        left.PollOnce(
            [&](NodeId, const Message &m) {
                EXPECT_EQ(m.epoch, 5u);
                ++reverse;
            },
            1);
        right.PollOnce([&](NodeId, const Message &m) { messages.push_back(m); }, 1);
    }
    ASSERT_EQ(messages.size(), 2u);
    EXPECT_EQ(messages[0].payload, Bytes(40000, 7));
    EXPECT_EQ(messages[1].kind, Message::Kind::Marker);
    EXPECT_EQ(reverse, 1);
}
TEST(TcpSnapshotTransport, OutputBudgetAndDisconnectFailClosed) {
    uint16_t port = 0;
    int listener = ListenTcp("127.0.0.1", 0, port);
    int a = ConnectTcp("127.0.0.1", port);
    int b = AcceptTcp(listener);
    ::close(listener);
    TcpTransport transport({{1, a}}, 32);
    EXPECT_THROW(transport.Queue(1, {Message::Kind::Data, 1, 0, Bytes(100)}), std::runtime_error);
    ::close(b);
    EXPECT_THROW(transport.PollOnce([](NodeId, const Message &) {}, 0), std::runtime_error);
}
TEST(ChandyLamportStore, LatestCommittedFallsBackAfterTornOrCorruptEpoch) {
    Temp temp;
    Store store(temp.path);
    auto snapshots = Cut();
    for (auto s : snapshots)
        store.SaveLocal(s);
    store.Commit(1, pair_topology, "job");
    snapshots[0].epoch = 2;
    store.SaveLocal(snapshots[0]);
    EXPECT_EQ(store.LoadLatestCommitted(pair_topology, "job").front().epoch, 1u);
    snapshots[1].epoch = 2;
    store.SaveLocal(snapshots[1]);
    store.Commit(2, pair_topology, "job");
    EXPECT_EQ(store.LoadLatestCommitted(pair_topology, "job").front().epoch, 2u);
    std::ofstream out(std::filesystem::path(temp.path) / "epoch-2/COMMITTED", std::ios::trunc);
    out << "torn";
    out.close();
    EXPECT_EQ(store.LoadLatestCommitted(pair_topology, "job").front().epoch, 1u);
}
TEST(NetworkWindowSnapshot, ChannelReplayRestoresDataAndWatermarkProgress) {
    MemorySink sink;
    WindowOperator op({0, 1}, sink);
    Participant p(
        2, {0, 1}, [&] { return op.Capture(); },
        [&](NodeId peer, const Bytes &b) { op.Apply(peer, b); }, [](NodeId, const Message &) {});
    p.Receive(0, {Message::Kind::Data, 1, 0, WindowOperator::Data(R(5))});
    p.Receive(0, {Message::Kind::Data, 2, 0, WindowOperator::Watermark(Timestamp{Duration{1000}})});
    p.Receive(0, {Message::Kind::Marker, 2, 1, {}});
    p.Receive(1, {Message::Kind::Data, 1, 0, WindowOperator::Data(R(7))});
    p.Receive(1, {Message::Kind::Data, 2, 0, WindowOperator::Watermark(Timestamp{Duration{1000}})});
    p.Receive(1, {Message::Kind::Marker, 2, 1, {}});
    auto snapshot = p.TakeCompleted();
    ASSERT_EQ(snapshot.channels.size(), 2u);
    MemorySink result;
    WindowOperator recovered({0, 1}, result);
    recovered.Restore(snapshot.state);
    Participant restored(
        2, {0, 1}, [&] { return recovered.Capture(); },
        [&](NodeId peer, const Bytes &b) { recovered.Apply(peer, b); },
        [](NodeId, const Message &) {});
    restored.Restore(snapshot);
    ASSERT_EQ(result.Results().size(), 1u);
    EXPECT_EQ(result.Results()[0].result.value, 12);
    EXPECT_EQ(recovered.watermark(), Timestamp{Duration{1000}});
    EXPECT_EQ(recovered.records(), 2u);
}
TEST(TcpSnapshotTransport, ClosedQuietChannelIsAnError) {
    uint16_t port = 0;
    int listener = ListenTcp("127.0.0.1", 0, port);
    int a = ConnectTcp("127.0.0.1", port);
    int b = AcceptTcp(listener);
    ::close(listener);
    TcpTransport transport({{1, a}});
    ::close(b);
    EXPECT_THROW(transport.PollOnce([](NodeId, const Message &) {}, 1000), std::runtime_error);
}
