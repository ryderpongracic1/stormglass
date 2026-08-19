#include <gtest/gtest.h>
#include "stream/record.h"
#include "stream/batch.h"
#include "stream/watermark.h"
#include "aggregate/sum.h"
#include "window/tumbling.h"
#include "sink/memory_sink.h"
#include "engine/pipeline.h"
namespace stormglass { namespace {
TEST(Smoke, Compiles) { EXPECT_TRUE(true); }
TEST(Smoke, RecordConstruction) { Record r{.key="k1",.value=42,.event_time=Timestamp{Duration{1000}},.processing_time=Timestamp{Duration{1001}}}; EXPECT_EQ(r.key,"k1"); EXPECT_EQ(r.value,42); }
TEST(Smoke, WatermarkAdvances) { WatermarkTracker wm; EXPECT_TRUE(wm.Advance(Timestamp{Duration{100}})); EXPECT_FALSE(wm.Advance(Timestamp{Duration{50}})); EXPECT_TRUE(wm.Advance(Timestamp{Duration{200}})); EXPECT_EQ(wm.Current(),Timestamp{Duration{200}}); }
TEST(Smoke, SumKernel) { SumInt64Kernel sum; sum.Add(10); sum.Add(20); sum.Add(30); auto r=sum.Result(); EXPECT_EQ(r.value,60); EXPECT_EQ(r.count,3u); sum.Reset(); EXPECT_EQ(sum.Result().value,0); }
TEST(Smoke, TumblingAssigner) { TumblingAssigner a{Duration{10000}}; auto w=a.AssignWindows(Timestamp{Duration{15000}}); ASSERT_EQ(w.size(),1u); EXPECT_EQ(w[0].start,Timestamp{Duration{10000}}); EXPECT_EQ(w[0].end,Timestamp{Duration{20000}}); }
TEST(Smoke, MemorySink) { MemorySink s; WindowResult wr{.key="k1",.window={Timestamp{Duration{0}},Timestamp{Duration{10000}}},.result={100,5}}; s.Emit(wr); ASSERT_EQ(s.Results().size(),1u); EXPECT_EQ(s.Results()[0].result.value,100); s.Clear(); EXPECT_TRUE(s.Results().empty()); }
} }
