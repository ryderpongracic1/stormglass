package io.stormglass.bench;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Locale;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.legacy.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.legacy.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public final class FlinkComparison {
    private static final byte[] MAGIC = "SGFXv001".getBytes(StandardCharsets.US_ASCII);
    private static final int HEADER_BYTES = 48;
    private static final int ENTRY_BYTES = 32;

    public static final class Event {
        public String key;
        public long value;
        public long eventTimeMs;
        public long keyHash;

        public Event() {}
        Event(String key, long value, long eventTimeMs, long keyHash) {
            this.key = key;
            this.value = value;
            this.eventTimeMs = eventTimeMs;
            this.keyHash = keyHash;
        }
    }

    public static final class SumCount {
        public long sum;
        public long count;
        public long keyHash;
        public SumCount() {}
    }

    public static final class WindowResult {
        public String key;
        public long startMs;
        public long endMs;
        public long sum;
        public long count;
        public long keyHash;
        public WindowResult() {}
        WindowResult(String key, long startMs, long endMs, long sum, long count, long keyHash) {
            this.key = key;
            this.startMs = startMs;
            this.endMs = endMs;
            this.sum = sum;
            this.count = count;
            this.keyHash = keyHash;
        }
    }

    private static final class FixtureSource extends RichParallelSourceFunction<Event> {
        private final String fixture;
        private final long cycles;
        private volatile boolean running = true;

        FixtureSource(String fixture, long cycles) {
            this.fixture = fixture;
            this.cycles = cycles;
        }

        @Override
        public void run(SourceContext<Event> context) throws Exception {
            if (getRuntimeContext().getTaskInfo().getIndexOfThisSubtask() != 0) {
                throw new IllegalStateException("fixture source must have parallelism 1");
            }
            try (RandomAccessFile file = new RandomAccessFile(fixture, "r");
                 FileChannel channel = file.getChannel()) {
                if (channel.size() > Integer.MAX_VALUE) {
                    throw new IllegalArgumentException("fixture must be smaller than 2 GiB");
                }
                MappedByteBuffer mapped = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
                mapped.order(ByteOrder.LITTLE_ENDIAN);
                Header header = readHeader(mapped);
                String[] keys = new String[Math.toIntExact(header.keys)];
                long[] keyHashes = new long[keys.length];
                for (int key = 0; key < keys.length; ++key) {
                    keys[key] = String.format(Locale.ROOT, "key-%04d", key);
                    keyHashes[key] = fnv(0xcbf29ce484222325L,
                                         keys[key].getBytes(StandardCharsets.UTF_8));
                }
                if (channel.size() != HEADER_BYTES + header.entries * ENTRY_BYTES) {
                    throw new IOException("fixture size does not match its header");
                }
                for (long cycle = 0; cycle < cycles && running; ++cycle) {
                    ByteBuffer data = mapped.duplicate().order(ByteOrder.LITTLE_ENDIAN);
                    data.position(HEADER_BYTES);
                    long shift = Math.multiplyExact(cycle, header.cycleSpanMs);
                    for (long entry = 0; entry < header.entries && running; ++entry) {
                        int type = Byte.toUnsignedInt(data.get());
                        data.position(data.position() + 3);
                        int keyId = data.getInt();
                        long value = data.getLong();
                        long eventOrWatermark = data.getLong();
                        data.getLong(); // sequence: useful to other readers, not needed here
                        synchronized (context.getCheckpointLock()) {
                            if (type == 0) {
                                if (keyId < 0 || keyId >= keys.length) {
                                    throw new IOException("fixture key id out of range");
                                }
                                Event event = new Event(keys[keyId], value,
                                    Math.addExact(eventOrWatermark, shift), keyHashes[keyId]);
                                context.collectWithTimestamp(event, event.eventTimeMs);
                            } else if (type == 1) {
                                // stormglass stores an exclusive frontier: W closes
                                // [start, end) when W >= end. Flink watermarks are
                                // inclusive timestamps and fire at end - 1, so shift
                                // the representation by one millisecond.
                                long frontier = Math.addExact(eventOrWatermark, shift);
                                context.emitWatermark(new Watermark(Math.subtractExact(frontier, 1)));
                            } else {
                                throw new IOException("invalid fixture entry type " + type);
                            }
                        }
                    }
                }
                synchronized (context.getCheckpointLock()) {
                    context.emitWatermark(new Watermark(Long.MAX_VALUE));
                }
            }
        }

        @Override
        public void cancel() { running = false; }
    }

    private record Header(long records, long entries, long cycleSpanMs, long keys) {}

    private static Header readHeader(ByteBuffer data) throws IOException {
        for (byte expected : MAGIC) {
            if (!data.hasRemaining() || data.get() != expected) throw new IOException("invalid fixture magic");
        }
        int version = data.getInt();
        int headerBytes = data.getInt();
        long records = data.getLong();
        long entries = data.getLong();
        long cycleSpanMs = data.getLong();
        long keys = data.getLong();
        if (version != 1 || headerBytes != HEADER_BYTES || records <= 0 || entries <= 0 ||
            cycleSpanMs <= 0 || keys <= 0 || keys > Integer.MAX_VALUE) {
            throw new IOException("unsupported fixture header");
        }
        return new Header(records, entries, cycleSpanMs, keys);
    }

    private static final class SumAggregate implements AggregateFunction<Event, SumCount, SumCount> {
        @Override public SumCount createAccumulator() { return new SumCount(); }
        @Override public SumCount add(Event event, SumCount acc) {
            if (acc.count == 0) acc.keyHash = event.keyHash;
            acc.sum = Math.addExact(acc.sum, event.value);
            acc.count = Math.addExact(acc.count, 1);
            return acc;
        }
        @Override public SumCount getResult(SumCount acc) { return acc; }
        @Override public SumCount merge(SumCount a, SumCount b) {
            a.sum = Math.addExact(a.sum, b.sum);
            a.count = Math.addExact(a.count, b.count);
            if (a.keyHash == 0) a.keyHash = b.keyHash;
            return a;
        }
    }

    private static final class AddWindowIdentity
            extends ProcessWindowFunction<SumCount, WindowResult, String, TimeWindow> {
        @Override
        public void process(String key, Context context, Iterable<SumCount> values,
                            Collector<WindowResult> out) {
            SumCount value = values.iterator().next();
            out.collect(new WindowResult(key, context.window().getStart(), context.window().getEnd(),
                                         value.sum, value.count, value.keyHash));
        }
    }

    public static final class XorAccumulator implements Accumulator<Long, Long> {
        private long value;
        @Override public void add(Long item) { value ^= item; }
        @Override public Long getLocalValue() { return value; }
        @Override public void resetLocal() { value = 0; }
        @Override public void merge(Accumulator<Long, Long> other) { value ^= other.getLocalValue(); }
        @Override public Accumulator<Long, Long> clone() {
            XorAccumulator copy = new XorAccumulator();
            copy.value = value;
            return copy;
        }
    }

    private static final class DigestSink extends RichSinkFunction<WindowResult> {
        private transient LongCounter outputCount;
        private transient LongCounter digestSum;
        private transient XorAccumulator digestXor;
        private long localOutputs;
        private long localDigestSum;
        private long localDigestXor;

        @Override
        public void open(OpenContext openContext) {
            outputCount = new LongCounter();
            digestSum = new LongCounter();
            digestXor = new XorAccumulator();
            getRuntimeContext().addAccumulator("outputs", outputCount);
            getRuntimeContext().addAccumulator("digest_sum", digestSum);
            getRuntimeContext().addAccumulator("digest_xor", digestXor);
        }

        @Override
        public void invoke(WindowResult value, Context context) {
            long hash = resultHash(value);
            ++localOutputs;
            localDigestSum += hash;
            localDigestXor ^= hash;
        }

        @Override
        public void close() {
            outputCount.add(localOutputs);
            digestSum.add(localDigestSum);
            digestXor.add(localDigestXor);
        }
    }

    private static final class CountLateSink extends RichSinkFunction<Event> {
        private transient LongCounter count;
        private long localCount;
        @Override public void open(OpenContext openContext) {
            count = new LongCounter();
            getRuntimeContext().addAccumulator("late_dropped", count);
        }
        @Override public void invoke(Event value, Context context) { ++localCount; }
        @Override public void close() { count.add(localCount); }
    }

    private static long mix(long z) {
        z = (z ^ (z >>> 30)) * 0xbf58476d1ce4e5b9L;
        z = (z ^ (z >>> 27)) * 0x94d049bb133111ebL;
        return z ^ (z >>> 31);
    }

    private static long fnv(long hash, byte[] bytes) {
        for (byte b : bytes) { hash ^= Byte.toUnsignedInt(b); hash *= 1099511628211L; }
        return hash;
    }

    private static long fnvLong(long hash, long value) {
        for (int i = 0; i < 8; ++i) { hash ^= (value >>> (8 * i)) & 0xffL; hash *= 1099511628211L; }
        return hash;
    }

    private static long resultHash(WindowResult value) {
        long hash = value.keyHash;
        hash = fnvLong(hash, value.startMs);
        hash = fnvLong(hash, value.endMs);
        hash = fnvLong(hash, value.sum);
        hash = fnvLong(hash, value.count);
        return mix(hash);
    }

    private record Args(String fixture, long cycles, int parallelism, long latenessMs) {
        static Args parse(String[] argv) {
            String fixture = null;
            long cycles = 10;
            int parallelism = 1;
            long latenessMs = 0;
            for (int i = 0; i < argv.length; ++i) {
                String arg = argv[i];
                if (i + 1 >= argv.length) throw new IllegalArgumentException("missing value for " + arg);
                String value = argv[++i];
                switch (arg) {
                    case "--fixture" -> fixture = Path.of(value).toAbsolutePath().toString();
                    case "--cycles" -> cycles = Long.parseLong(value);
                    case "--parallelism" -> parallelism = Integer.parseInt(value);
                    case "--lateness-ms" -> latenessMs = Long.parseLong(value);
                    default -> throw new IllegalArgumentException("unknown argument: " + arg);
                }
            }
            if (fixture == null || cycles <= 0 || parallelism <= 0 || latenessMs < 0) {
                throw new IllegalArgumentException(
                    "usage: --fixture PATH [--cycles N] [--parallelism N] [--lateness-ms N]");
            }
            return new Args(fixture, cycles, parallelism, latenessMs);
        }
    }

    public static void main(String[] argv) throws Exception {
        Args args = Args.parse(argv);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(args.parallelism);
        Configuration config = new Configuration();
        config.setString("pipeline.object-reuse", "true");
        env.configure(config);

        DataStream<Event> events = env.addSource(new FixtureSource(args.fixture, args.cycles))
            .name("binary-fixture-source").setParallelism(1);
        OutputTag<Event> late = new OutputTag<Event>("late-data") {};
        SingleOutputStreamOperator<WindowResult> results = events
            .keyBy((KeySelector<Event, String>) event -> event.key)
            .window(TumblingEventTimeWindows.of(Duration.ofMillis(1_000)))
            .allowedLateness(Duration.ofMillis(args.latenessMs))
            .sideOutputLateData(late)
            .aggregate(new SumAggregate(), new AddWindowIdentity())
            .name("keyed-tumbling-window").setParallelism(args.parallelism);
        results.addSink(new DigestSink()).name("digest-sink").setParallelism(args.parallelism);
        results.getSideOutput(late).addSink(new CountLateSink())
            .name("late-data-counter").setParallelism(args.parallelism);

        JobExecutionResult execution = env.execute("stormglass-flink-comparison");
        Header header;
        try (RandomAccessFile file = new RandomAccessFile(args.fixture, "r");
             FileChannel channel = file.getChannel()) {
            ByteBuffer bytes = ByteBuffer.allocate(HEADER_BYTES).order(ByteOrder.LITTLE_ENDIAN);
            channel.read(bytes);
            bytes.flip();
            header = readHeader(bytes);
        }
        long records = Math.multiplyExact(header.records, args.cycles);
        long runtimeMs = execution.getNetRuntime();
        long outputs = execution.getAccumulatorResult("outputs");
        long digestXor = execution.getAccumulatorResult("digest_xor");
        long digestSum = execution.getAccumulatorResult("digest_sum");
        Long lateDroppedResult = execution.getAccumulatorResult("late_dropped");
        long lateDropped = lateDroppedResult == null ? 0 : lateDroppedResult;
        double rate = records / (runtimeMs / 1_000.0) / 1_000_000.0;
        System.out.printf(Locale.ROOT,
            "engine=flink version=2.3.0 records=%d parallelism=%d seconds=%.6f " +
            "m_records_per_second=%.3f outputs=%d late_dropped=%d digest_xor=%016x digest_sum=%016x%n",
            records, args.parallelism, runtimeMs / 1_000.0, rate, outputs, lateDropped,
            digestXor, digestSum);
    }
}
