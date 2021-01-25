package org.uwh.sparta;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.uwh.*;
import org.uwh.flink.util.Sources;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SpartaDataStreamTest {
    private static Logger logger = LoggerFactory.getLogger(SpartaDataStreamTest.class);
    private static String COB = "20210101";

    static class AggSink implements SinkFunction<RowData> {
        public static Map<String,Double> issuerJTD = new HashMap<>();
        public static List<RowData> events = new ArrayList<>(1000);

        @Override
        public void invoke(RowData value) {
            events.add(value);

            boolean addition = value.getRowKind() == RowKind.INSERT || value.getRowKind() == RowKind.UPDATE_AFTER;
            String issuer = value.getString(TwoStageDataStreamFlowBuilder.RESULT_FIELD_SMCI).toString();
            double jtd = value.getDouble(TwoStageDataStreamFlowBuilder.RESULT_FIELD_JTD);

            logger.info("Received row: " + value + ", issuer=" + issuer + ", jtd=" + jtd);

            if (addition) {
                issuerJTD.put(issuer, jtd);
            } else {
                issuerJTD.remove(issuer);
            }
        }
    }

    static class CaptureSink implements SinkFunction<RowData> {
        public static List<RowData> events = new ArrayList<>(1000);

        @Override
        public void invoke(RowData value) throws Exception {
            events.add(value);
        }
    }

    private StreamExecutionEnvironment env;
    private TwoStageDataStreamFlowBuilder builder;

    private static class TwoStageDataStreamFlowBuilder {
        public static final int RESULT_FIELD_SMCI = 0;
        public static final int RESULT_FIELD_JTD = 1;
        
        public TwoStageDataStreamFlowBuilder(StreamExecutionEnvironment env) {}

        public void setThrottleMs(long ms) {}
        public void setRiskSource(DataStream<IssuerRisk> stream) {}
        public void setRiskPositionSource(DataStream<RiskPosition> stream) {}
        public void setIssuerSource(DataStream<Issuer> stream) {}
        public void setThresholdSource(DataStream<RiskThreshold> stream) {}

        public void build() {}

        public DataStream<RowData> getResultStream() { return null; }
        public DataStream<RowData> getJoinedStream() { return null; }
    }

    @BeforeEach
    public void setup() {
        AggSink.issuerJTD.clear();
        AggSink.events.clear();
        CaptureSink.events.clear();

        env = StreamExecutionEnvironment.createLocalEnvironment(4);
        builder = new TwoStageDataStreamFlowBuilder(env);
        builder.setThrottleMs(50);
        builder.setIssuerSource(Sources.start(env, TypeInformation.of(Issuer.class)).toStream());
        builder.setThresholdSource(Sources.start(env, TypeInformation.of(RiskThreshold.class)).toStream());
    }

    @Test
    public void testAggregation() throws Exception {
        withRiskRecords(
                Tuple4.of("IBM", "T1", 100.0, 1L),
                Tuple4.of("IBM", "T2", -50.0, 2L)
        );

        run();

        assertEquals(50, AggSink.issuerJTD.get("IBM"), 0.1);
    }

    @Disabled
    @Test
    public void testThroughput() throws Exception {
        int numRisk = 10_000_000;
        int numPositions = 100_000;
        List<RiskPosition> pos = Generators.positionList(numPositions, 1000);
        List<IssuerRisk> risks = Generators.issuerRiskList(numRisk, numPositions, 1000);

        builder.setRiskPositionSource(env.fromCollection(pos));
        builder.setRiskSource(env.fromCollection(risks));

        builder.build();

        long start = System.currentTimeMillis();
        env.execute();
        long end = System.currentTimeMillis();

        double execTime = (end - start) / 1000.0;
        double throughput = numRisk / execTime;

        System.out.println("Execution time: "+ execTime + ", throughput: " + throughput);
        assertTrue(throughput >= 100_000);
    }

    @Test
    public void testAvoidDupeIdenticalEvents() throws Exception {
        withRiskRecords(
                Tuple4.of("IBM", "T1", 100.0, 1L),
                Tuple4.of("IBM", "T1", 100.0, 2L)
        );

        run();

        System.out.println(CaptureSink.events);
        assertEquals(1, CaptureSink.events.size());
    }

    @Test
    public void testResultThrottling() throws Exception {
        withRiskRecords(
                Tuple4.of("IBM", "T1", 200.0, 1L),
                Tuple4.of("IBM", "T2", 100.0, 1L),
                Tuple4.of("IBM", "T3", 50.0, 1L)
        );

        run();

        assertEquals(1, AggSink.events.size());
        assertEquals(350.0, AggSink.issuerJTD.get("IBM"), 0.01);
    }

    @Test
    public void testThrottledRiskRetriggers() throws Exception {
        builder.setRiskSource(Sources.start(env, TypeInformation.of(IssuerRisk.class))
                .then(Collections.singleton(new IssuerRisk(UIDType.UITID, "T1", "IBM", COB, 10.0, 50.0, 1L)))
                .delay(500)
                .then(Collections.singleton(new IssuerRisk(UIDType.UITID, "T1", "IBM", COB, 10.0, 100.0, 2L)))
                .toStream());
        builder.setRiskPositionSource(Sources.start(env, TypeInformation.of(RiskPosition.class))
                .then(Collections.singleton(new RiskPosition(UIDType.UITID, "T1", "BOOK", ProductType.CDS, 1L)))
                .delay(1000)
                .then(Collections.singleton(new RiskPosition(UIDType.UITID, "T1", "BOOK", ProductType.CDS, 1L)))
                .toStream());

        run();

        // insert, upsert-before, upsert-after
        assertEquals(3, AggSink.events.size());
        assertEquals(List.of(RowKind.INSERT, RowKind.UPDATE_BEFORE, RowKind.UPDATE_AFTER), AggSink.events.stream().map(e -> e.getRowKind()).collect(Collectors.toList()));
        assertEquals(100.0, AggSink.issuerJTD.get("IBM"), 0.1);
    }

    @Test
    public void testMultiIssuer() throws Exception {
        withRiskRecords(
                Tuple4.of("IBM", "T1", 100.0, 1L),
                Tuple4.of("IBM", "T2", -50.0, 2L),
                Tuple4.of("MSFT", "T3", 1000.0, 3L)
        );

        run();

        assertEquals(50, AggSink.issuerJTD.get("IBM"), 0.1);
        assertEquals(1000.0, AggSink.issuerJTD.get("MSFT"), 0.1);
    }

    @Test
    public void testTradeUpdateAmount() throws Exception {
        withRiskRecords(
                Tuple4.of("IBM", "T1", 100.0, 1L),
                Tuple4.of("IBM", "T2", -50.0, 2L),
                Tuple4.of("IBM", "T2", -100.0, 3L)
        );

        run();

        assertEquals(0.0, AggSink.issuerJTD.get("IBM"), 0.1);
    }

    @Test
    public void testOutOfOrderMessage() throws Exception {
        withRiskRecords(
                Tuple4.of("IBM", "T1", 100.0, 1L),
                Tuple4.of("IBM", "T2", -50.0, 3L),
                Tuple4.of("IBM", "T2", -100.0, 2L)
        );

        run();

        assertEquals(50.0, AggSink.issuerJTD.get("IBM"), 0.1);
    }

    @Test
    public void testTradeUpdateIssuer() throws Exception {
        withRiskRecords(
                Tuple4.of("IBM", "T1", 100.0, 1L),
                Tuple4.of("IBM", "T2", -50.0, 2L),
                Tuple4.of("MSFT", "T3", 1000.0, 3L),
                Tuple4.of("MSFT", "T2", -50.0, 4L)
        );

        run();

        assertEquals(100.0, AggSink.issuerJTD.get("IBM"), 0.1);
        assertEquals(950.0, AggSink.issuerJTD.get("MSFT"), 0.1);
    }

    @Test
    public void testCanHandleSameTradeIdWithDifferentType() throws Exception {
        withRiskRecords(
                new IssuerRisk(UIDType.UITID, "T1", "IBM", COB, 0.0, 50.0, 1L),
                new IssuerRisk(UIDType.UIPID, "T1", "IBM", COB, 0.0, 50.0, 2L)
        );

        run();

        assertEquals(100.0, AggSink.issuerJTD.get("IBM"), 0.1);
    }

    private void withRiskRecords(Tuple4<String, String, Double, Long>... recs) throws Exception {
        List<IssuerRisk> risk = Arrays.stream(recs).map(t -> new IssuerRisk(UIDType.UITID, t.f1, t.f0, COB, 0.0, t.f2, t.f3)).collect(Collectors.toList());
        List<RiskPosition> pos = Arrays.stream(recs).map(t -> new RiskPosition(UIDType.UITID, t.f1, "BOOK", ProductType.CDS, 1L)).collect(Collectors.toList());

        builder.setRiskSource(env.fromCollection(risk));
        DataStream<RiskPosition> posStream = Sources
                .start(env, TypeInformation.of(RiskPosition.class))
                .then(pos)
                .delay(500)
                .then(Collections.singleton(new RiskPosition(UIDType.UITID, "T1", "BOOK", ProductType.CDS, 2L)))
                .toStream();
        builder.setRiskPositionSource(posStream);
    }

    private void withRiskRecords(IssuerRisk... recs) throws Exception {
        builder.setRiskSource(env.fromElements(recs));

        DataStream<RiskPosition> posStream = Sources
                .start(env, TypeInformation.of(RiskPosition.class))
                .then(Arrays.stream(recs).map(r -> new RiskPosition(r.getUIDType(), r.getUID(), "BOOK", ProductType.CDS, 1L)).collect(Collectors.toList()))
                .delay(500)
                .then(Collections.singleton(new RiskPosition(UIDType.UITID, "T1", "BOOK", ProductType.CDS, 2L)))
                .toStream();
        builder.setRiskPositionSource(posStream);
    }

    private void run() throws Exception {
        builder.build();
        builder.getResultStream().addSink(new AggSink());
        builder.getJoinedStream().addSink(new CaptureSink());

        env.execute();
        logger.info("Final state: " + AggSink.issuerJTD);
    }

}
