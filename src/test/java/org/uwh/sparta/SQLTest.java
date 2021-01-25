package org.uwh.sparta;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class SQLTest {
    private static Logger logger = LoggerFactory.getLogger(SQLTest.class);

    static class AggSink implements SinkFunction<Tuple2<Boolean, Row>> {
        public static Map<String,Double> issuerJTD = new HashMap<>();

        @Override
        public void invoke(Tuple2<Boolean, Row> value) {
            boolean addition = value.f0;
            String issuer = (String) value.f1.getField(0);
            double jtd = (Double) value.f1.getField(1);

            logger.info("Received row: " + value);

            if (addition) {
                issuerJTD.put(issuer, jtd);
            } else {
                issuerJTD.remove(issuer);
            }
        }
    }

    private StreamExecutionEnvironment env;
    private SpartaFlowBuilder builder;

    @BeforeEach
    public void setup() {
        AggSink.issuerJTD.clear();

        env = StreamExecutionEnvironment.createLocalEnvironment(4);
        builder = new SpartaFlowBuilder(env);
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

    private void withRiskRecords(Tuple4<String, String, Double, Long>... recs) {
        builder.setRiskSource(env.fromCollection(List.of(recs)));
    }

    private void run() throws Exception {
        builder.build();
        builder.getAggregateIssuerStream().addSink(new AggSink());

        env.execute();
        logger.info("Final state: " + AggSink.issuerJTD);
    }
}
