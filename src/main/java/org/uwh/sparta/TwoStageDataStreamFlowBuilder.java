package org.uwh.sparta;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.uwh.*;

import static org.apache.flink.table.api.Expressions.$;

public class TwoStageDataStreamFlowBuilder {
    private final StreamExecutionEnvironment env;
    private DataStream<IssuerRisk> riskSource;
    private DataStream<RiskPosition> positionSource;
    private DataStream<Issuer> issuerSource;
    private DataStream<RiskThreshold> thresholdSource;
    private DataStream<RowData> resultStream;
    private DataStream<RowData> joinedStream;
    private long throttleMs = 1000;

    public static final int RESULT_FIELD_FIRMACCOUNT = 0;
    public static final int RESULT_FIELD_SMCI = 1;
    public static final int RESULT_FIELD_CR01 = 2;
    public static final int RESULT_FIELD_JTD = 3;

    public static final int RISK_AND_POS_FIELD_UIDTYPE = 0;
    public static final int RISK_AND_POS_FIELD_UID = 1;
    public static final int RISK_AND_POS_FIELD_FIRMACCOUNT = 2;
    public static final int RISK_AND_POS_FIELD_SMCI = 3;
    public static final int RISK_AND_POS_FIELD_CR01 = 4;
    public static final int RISK_AND_POS_FIELD_JTD = 5;


    public TwoStageDataStreamFlowBuilder(StreamExecutionEnvironment env) {
        this.env = env;
    }

    public void setRiskSource(DataStream<IssuerRisk> riskSource) {
        this.riskSource = riskSource;
    }

    public void setThrottleMs(long throttleMs) {
        this.throttleMs = throttleMs;
    }

    public void setRiskPositionSource(DataStream<RiskPosition> positionSource) {
        this.positionSource = positionSource;
    }

    public void setIssuerSource(DataStream<Issuer> issuerSource) {
        this.issuerSource = issuerSource;
    }

    public void setThresholdSource(DataStream<RiskThreshold> thresholdSource) {
        this.thresholdSource = thresholdSource;
    }

    public DataStream<RowData> getResultStream() {
        return resultStream;
    }

    public DataStream<RowData> getJoinedStream() {
        return joinedStream;
    }

    public void build() {
        joinedStream = riskSource.keyBy(risk -> risk.getUIDType() + ":" + risk.getUID())
                .connect(positionSource.keyBy(pos -> pos.getUIDType() + ":" + pos.getUID()))
                .process(new RiskPositionJoin());

        DataStream<RowData> aggStream = joinedStream
                .keyBy(row -> Tuple2.of(row.getString(RISK_AND_POS_FIELD_FIRMACCOUNT).toString(), row.getString(RISK_AND_POS_FIELD_SMCI).toString()),
                        new TupleTypeInfo<>(Types.STRING, Types.STRING))
                .process(new AggFunction(throttleMs));

        resultStream = aggStream;

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        Table issuerRisk = tenv.fromDataStream(aggStream
                        .filter(row -> row.getRowKind() == RowKind.INSERT || row.getRowKind() == RowKind.UPDATE_AFTER)
                        .map(row -> Tuple4.of(row.getString(RESULT_FIELD_FIRMACCOUNT).toString(), row.getString(RESULT_FIELD_SMCI).toString(), row.getDouble(RESULT_FIELD_CR01), row.getDouble(RESULT_FIELD_JTD)),
                                new TupleTypeInfo<>(Types.STRING, Types.STRING, Types.DOUBLE, Types.DOUBLE)),
                // TODO what is the way to directly convert from RowData into a native row?
                $("f0").as("firmaccount"),
                $("f1").as("smci"),
                $("f2").as("cr01"),
                $("f3").as("jtd"));

        Table stage1 = tenv.sqlQuery("select firmaccount, smci, last_value(cr01) as cr01, last_value(jtd) as jtd from " + issuerRisk + " group by firmaccount, smci");

        Table issuersRaw = tenv.fromDataStream(issuerSource, $("SMCI").as("smci"), $("Name").as("name"), $("UltimateParentSMCI").as("ultimate_smci"));

        Table issuers = tenv.sqlQuery("select smci, last_value(name) as name, last_value(ultimate_smci) as ultimate_smci from " + issuersRaw + " group by smci");

        Table stage2 = tenv.sqlQuery("select firmaccount, risk.smci, name, ultimate_smci, cr01, jtd from " + stage1 + " risk inner join " + issuers + " issuer on risk.smci = issuer.smci");

        // TODO how to captule drilldown?
        Table stage3 = tenv.sqlQuery("select ultimate_smci, sum(cr01) as cr01, sum(jtd) as jtd from " + stage2 + " group by ultimate_smci");

        Table thresholdRaw = tenv.fromDataStream(
                thresholdSource.map(t -> Tuple3.of(t.getRiskFactor(), t.getThresholds().get(RiskMeasure.CR01.name()), t.getThresholds().get(RiskMeasure.JTD.name())),
                        new TupleTypeInfo<>(Types.STRING, Types.DOUBLE, Types.DOUBLE)),
                $("f0").as("smci"), $("f1").as("cr01_threshold"), $("f2").as("jtd_threshold"));

        Table thresholds = tenv.sqlQuery("select smci, last_value(cr01_threshold) as cr01_threshold, last_value(jtd_threshold) as jtd_threshold from " + thresholdRaw + " group by smci");

        Table stage4 = tenv.sqlQuery("select ultimate_smci, " +
                "cr01, cr01_threshold, 100*abs(cr01/cr01_threshold) as cr01_utilization, " +
                "jtd, jtd_threshold, 100*abs(jtd/jtd_threshold) as jtd_utilization " +
                "from " + stage3 + " risk " +
                "join " + thresholds + " threshold on risk.ultimate_smci = threshold.smci");

        tenv.toRetractStream(stage4, RowData.class).print();
    }

    public static class AggState {
        private double cr01;
        private double jtd;
        private boolean timerSet;

        public AggState(double cr01, double jtd, boolean timerSet) {
            this.cr01 = cr01;
            this.jtd = jtd;
            this.timerSet = timerSet;
        }

        public double getCr01() {
            return cr01;
        }

        public void setCr01(double cr01) {
            this.cr01 = cr01;
        }

        public double getJtd() {
            return jtd;
        }

        public void setJtd(double jtd) {
            this.jtd = jtd;
        }

        public boolean isTimerSet() {
            return timerSet;
        }

        public void setTimerSet(boolean timerSet) {
            this.timerSet = timerSet;
        }
    }

    private static class AggFunction extends KeyedProcessFunction<Tuple2<String,String>, RowData, RowData> {
        private transient ValueState<AggState> latestState;
        private transient ValueState<AggState> lastPublished;
        private final long throttleMs;

        public AggFunction(long throttleMs) {
            this.throttleMs = throttleMs;
        }


        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            latestState = getRuntimeContext().getState(new ValueStateDescriptor<>("sum", TypeInformation.of(AggState.class)));
            lastPublished = getRuntimeContext().getState(new ValueStateDescriptor<>("lastPublished", TypeInformation.of(AggState.class)));
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext context, Collector<RowData> collector) throws Exception {
            AggState published = lastPublished.value();

            if (published != null) {
                collector.collect(makeEvent(RowKind.UPDATE_BEFORE, context.getCurrentKey(), published));
            }

            AggState current = latestState.value();
            lastPublished.update(current);

            // unset the timer
            latestState.update(new AggState(current.getCr01(), current.getJtd(), false));

            RowKind kind = (published != null) ? RowKind.UPDATE_AFTER : RowKind.INSERT;
            collector.collect(makeEvent(kind, context.getCurrentKey(), current));
        }

        @Override
        public void processElement(RowData rowData, Context context, Collector<RowData> collector) throws Exception {
            AggState currentState = latestState.value();
            AggState newState = updateState(currentState, rowData);

            if (currentState == null || !currentState.isTimerSet()) {
                newState.setTimerSet(true);
                context.timerService().registerProcessingTimeTimer(context.timerService().currentProcessingTime()+throttleMs);
            }

            latestState.update(newState);
        }

        private AggState updateState(AggState current, RowData rowData) {
            if (rowData.getRowKind() == RowKind.DELETE || rowData.getRowKind() == RowKind.UPDATE_BEFORE) {
                return new AggState(
                        current.getCr01() - rowData.getDouble(RISK_AND_POS_FIELD_CR01),
                        current.getJtd() - rowData.getDouble(RISK_AND_POS_FIELD_JTD),
                        current.isTimerSet()
                );
            } else {
                return new AggState(
                        (current != null ? current.getCr01() : 0.0) + rowData.getDouble(RISK_AND_POS_FIELD_CR01),
                        (current != null ? current.getJtd() : 0.0) + rowData.getDouble(RISK_AND_POS_FIELD_JTD),
                        current != null && current.isTimerSet()
                );
            }
        }

        private RowData makeEvent(RowKind kind, Tuple2<String,String> currentKey, AggState state) {
            GenericRowData row = new GenericRowData(4);
            row.setRowKind(kind);
            row.setField(RESULT_FIELD_FIRMACCOUNT, StringData.fromString(currentKey.f0));
            row.setField(RESULT_FIELD_SMCI, StringData.fromString(currentKey.f1));
            row.setField(RESULT_FIELD_CR01, state.getCr01());
            row.setField(RESULT_FIELD_JTD, state.getJtd());

            return row;
        }
    }
}
