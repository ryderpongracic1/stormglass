#include <gtest/gtest.h>

#include "sink/durable_file_sink.h"

#include <cstdint>
#include <cstdio>
#include <fcntl.h>
#include <filesystem>
#include <string>
#include <unistd.h>

using namespace stormglass;

namespace {

WindowResult MakeResult(const std::string& key, int64_t start, int64_t end,
                        int64_t sum, uint64_t count) {
    return WindowResult{
        .key = key,
        .window = Window{Timestamp{Duration{start}}, Timestamp{Duration{end}}},
        .result = AggregateResult{sum, count},
    };
}

std::string TempPath() {
    char tmpl[] = "/tmp/stormglass_durable_XXXXXX";
    int fd = ::mkstemp(tmpl);
    if (fd >= 0) ::close(fd);
    return std::string(tmpl);
}

} // namespace

TEST(DurableFileSinkTest, RoundTrip) {
    std::string path = TempPath();
    {
        DurableFileSink sink(path);
        ASSERT_TRUE(sink.ok());
        sink.Emit(MakeResult("key-0001", 0, 1000, 123, 3));
        sink.Emit(MakeResult("key-0002", 1000, 2000, -45, 7));
        sink.Emit(MakeResult("key-0001", 1000, 2000, 999, 10));
        sink.Flush();
    }

    auto records = DurableFileSink::ReadAll(path);
    ASSERT_EQ(records.size(), 3u);

    EXPECT_EQ(records[0].key, "key-0001");
    EXPECT_EQ(records[0].window.start.time_since_epoch().count(), 0);
    EXPECT_EQ(records[0].window.end.time_since_epoch().count(), 1000);
    EXPECT_EQ(records[0].result.value, 123);
    EXPECT_EQ(records[0].result.count, 3u);

    EXPECT_EQ(records[1].key, "key-0002");
    EXPECT_EQ(records[1].result.value, -45);
    EXPECT_EQ(records[1].result.count, 7u);

    EXPECT_EQ(records[2].key, "key-0001");
    EXPECT_EQ(records[2].window.end.time_since_epoch().count(), 2000);
    EXPECT_EQ(records[2].result.value, 999);

    std::filesystem::remove(path);
}

TEST(DurableFileSinkTest, EmptyAndMissingFile) {
    // Missing file → empty.
    EXPECT_TRUE(DurableFileSink::ReadAll("/tmp/stormglass_no_such_file_xyz").empty());

    // Freshly created empty file → empty.
    std::string path = TempPath();
    { DurableFileSink sink(path); ASSERT_TRUE(sink.ok()); }
    EXPECT_TRUE(DurableFileSink::ReadAll(path).empty());
    std::filesystem::remove(path);
}

TEST(DurableFileSinkTest, TornTrailingRecordTolerated) {
    std::string path = TempPath();
    {
        DurableFileSink sink(path);
        sink.Emit(MakeResult("key-0001", 0, 1000, 10, 1));
        sink.Emit(MakeResult("key-0002", 1000, 2000, 20, 2));
        sink.Flush();
    }

    // Simulate a writer SIGKILLed mid-append: a record header claiming a key of
    // 5 bytes but only 2 bytes actually written, then nothing else.
    int fd = ::open(path.c_str(), O_WRONLY | O_APPEND);
    ASSERT_GE(fd, 0);
    uint8_t torn[] = {0x05, 0x00, 0x00, 0x00, 'a', 'b'};  // key_len=5, only "ab"
    ASSERT_EQ(::write(fd, torn, sizeof(torn)), static_cast<ssize_t>(sizeof(torn)));
    ::close(fd);

    // Only the two complete records are returned; the torn tail is dropped.
    auto records = DurableFileSink::ReadAll(path);
    ASSERT_EQ(records.size(), 2u);
    EXPECT_EQ(records[0].key, "key-0001");
    EXPECT_EQ(records[1].key, "key-0002");

    std::filesystem::remove(path);
}

TEST(DurableFileSinkTest, TruncatedHeaderTolerated) {
    std::string path = TempPath();
    {
        DurableFileSink sink(path);
        sink.Emit(MakeResult("key-0001", 0, 1000, 10, 1));
        sink.Flush();
    }

    // Append only 2 bytes — not even a full 4-byte length header.
    int fd = ::open(path.c_str(), O_WRONLY | O_APPEND);
    ASSERT_GE(fd, 0);
    uint8_t partial[] = {0xFF, 0xFF};
    ASSERT_EQ(::write(fd, partial, sizeof(partial)), static_cast<ssize_t>(sizeof(partial)));
    ::close(fd);

    auto records = DurableFileSink::ReadAll(path);
    ASSERT_EQ(records.size(), 1u);
    EXPECT_EQ(records[0].key, "key-0001");

    std::filesystem::remove(path);
}
