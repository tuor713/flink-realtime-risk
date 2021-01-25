package org.uwh.sparta;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.RowKind;
import org.uwh.*;
import org.uwh.flink.data.generic.Field;
import org.uwh.flink.data.generic.Record;
import org.uwh.flink.data.generic.RecordType;
import org.uwh.flink.data.generic.Stream;

import java.util.*;

public class SQLJob {
    private static final Field<String> F_ISSUER_ID = new Field<>("issuer","id", String.class, Types.STRING);
    private static final Field<String> F_ISSUER_NAME = new Field<>("issuer", "name", String.class, Types.STRING);
    private static final Field<String> F_ISSUER_ULTIMATE_PARENT_ID = new Field<>("issuer", "ultimate-parent-id", F_ISSUER_ID);
    private static final Field<String> F_ISSUER_ULTIMATE_PARENT_NAME = new Field<>("issuer", "ultimate-parent-name", F_ISSUER_NAME);

    private static final Field<String> F_ACCOUNT_MNEMONIC = new Field<>("account", "mnemonic", String.class, Types.STRING);
    private static final Field<String> F_ACCOUNT_STRATEGY_CODE = new Field<>("account", "strategy-code", String.class, Types.STRING);

    private static final Field<String> F_POS_UID = new Field<>("position", "uid", String.class, Types.STRING);
    private static final Field<String> F_POS_UID_TYPE = new Field<>("position", "uid-type", String.class, Types.STRING);
    private static final Field<String> F_POS_PRODUCT_TYPE = new Field<>("position", "product-type", String.class, Types.STRING);

    private static final Field<Double> F_RISK_ISSUER_CR01 = new Field<>("issuer-risk","cr01", Double.class, Types.DOUBLE);
    private static final Field<Double> F_RISK_ISSUER_JTD = new Field<>("issuer-risk","jtd", Double.class, Types.DOUBLE);

    private static final Field<List<IssuerRiskLine>> F_RISK_ISSUER_RISKS = new Field<List<IssuerRiskLine>>("issuer-risk", "risks", (Class) List.class, new ListTypeInfo<>(IssuerRiskLine.class));

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment.setDefaultLocalParallelism(4);
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(4);
        env.getConfig().disableGenericTypes();

        int numPositions = 500_000;
        DataStream<RiskPosition> posStream = Generators.positions(env, numPositions, Generators.NO_USED_ACCOUNT);
        DataStream<Issuer> issuerStream = Generators.issuers(env, Generators.NO_ISSUER, Generators.NO_ULTIMATE);
        DataStream<FirmAccount> accountStream = Generators.accounts(env, Generators.NO_ACCOUNT);
        DataStream<IssuerRiskBatch> batchStream = Generators.batchRisk(env, numPositions, Generators.NO_USED_ISSUER);


        RecordType issuerType = new RecordType(env.getConfig(), F_ISSUER_ID, F_ISSUER_NAME, F_ISSUER_ULTIMATE_PARENT_ID);
        Stream issuers = Stream.fromDataStream(
                issuerStream,
                issuer -> new Record(issuerType).with(F_ISSUER_ID, issuer.getSMCI()).with(F_ISSUER_NAME, issuer.getName()).with(F_ISSUER_ULTIMATE_PARENT_ID, issuer.getUltimateParentSMCI()),
                issuerType
        ).map(record -> {
            if (record.get(F_ISSUER_ULTIMATE_PARENT_ID) == null) {
                Record output = new Record(record);
                output.set(F_ISSUER_ULTIMATE_PARENT_ID, output.get(F_ISSUER_ID));
                return output;
            } else {
                return record;
            }
        }, new RecordType(env.getConfig(), F_ISSUER_ID, F_ISSUER_NAME, F_ISSUER_ULTIMATE_PARENT_ID));

        Stream parentIssuer = issuers.select(Map.of(
                F_ISSUER_ID, F_ISSUER_ULTIMATE_PARENT_ID,
                F_ISSUER_NAME, F_ISSUER_ULTIMATE_PARENT_NAME
        ));

        Stream issuersWithParent = issuers.joinManyToOne(parentIssuer, F_ISSUER_ULTIMATE_PARENT_ID, F_ISSUER_ID);

        RecordType accountType = new RecordType(env.getConfig(), F_ACCOUNT_MNEMONIC, F_ACCOUNT_STRATEGY_CODE);
        Stream accounts = Stream.fromDataStream(
                accountStream,
                account -> new Record(accountType).with(F_ACCOUNT_MNEMONIC, account.getMnemonic()).with(F_ACCOUNT_STRATEGY_CODE, account.getStrategyCode()),
                accountType
        );

        RecordType posType = new RecordType(env.getConfig(), F_POS_UID_TYPE, F_POS_UID, F_ACCOUNT_MNEMONIC, F_POS_PRODUCT_TYPE);
        Stream positions = Stream.fromDataStream(
                posStream,
                pos -> new Record(posType).with(F_POS_UID_TYPE, pos.getUIDType().name()).with(F_POS_UID, pos.getUID()).with(F_ACCOUNT_MNEMONIC, pos.getFirmAccountMnemonic()).with(F_POS_PRODUCT_TYPE, pos.getProductType().name()),
                posType
        );

        Stream posWithAccount = positions.joinManyToOne(
                accounts,
                F_ACCOUNT_MNEMONIC,
                rec -> Tuple2.of(rec.get(F_POS_UID_TYPE), rec.get(F_POS_UID)),
                new TupleTypeInfo<>(Types.STRING, Types.STRING));

        RecordType batchType = new RecordType(env.getConfig(), F_POS_UID_TYPE, F_POS_UID, F_RISK_ISSUER_RISKS);
        Stream batchRisk = Stream.fromDataStream(
                batchStream,
                risk -> new Record(batchType).with(F_POS_UID_TYPE, risk.getUIDType().name()).with(F_POS_UID, risk.getUID()).with(F_RISK_ISSUER_RISKS, risk.getRisk()),
                batchType
        );

        RecordType issuerRiskType = new RecordType(env.getConfig(), F_POS_UID_TYPE, F_POS_UID, F_ISSUER_ID, F_RISK_ISSUER_CR01, F_RISK_ISSUER_JTD);
        RecordType joinType = issuerRiskType.join(posWithAccount.getRecordType());
        Stream riskWithPosition = batchRisk.joinOneToOne(
                posWithAccount,
                rec -> Tuple2.of(rec.get(F_POS_UID_TYPE), rec.get(F_POS_UID)),
                new TupleTypeInfo<>(Types.STRING, Types.STRING),
                (curL, curR, newL, newR) -> joinRiskAndPosition(joinType, curL, curR, newL, newR),
                joinType);

        Stream finalStream = riskWithPosition.joinManyToOne(
                issuersWithParent,
                F_ISSUER_ID,
                rec -> Tuple2.of(rec.get(F_POS_UID_TYPE), rec.get(F_POS_UID)),
                new TupleTypeInfo<>(Types.STRING, Types.STRING));

        Stream aggStream = finalStream.aggregate(List.of(F_ISSUER_ULTIMATE_PARENT_ID, F_POS_PRODUCT_TYPE), List.of(F_RISK_ISSUER_CR01, F_RISK_ISSUER_JTD), 10_000);

        aggStream.log("AGG", 1_000);

        env.execute();
    }

    private static Collection<Record> joinRiskAndPosition(RecordType joinType, Record curRisk, Record curPosition, Record newRisk, Record newPosition) {
        List<Record> res = new ArrayList<>();
        if (newPosition != null) {
            // Position update
            for (IssuerRiskLine risk : curRisk.get(F_RISK_ISSUER_RISKS)) {
                if (curPosition != null) {
                    res.add(joinRecord(RowKind.UPDATE_BEFORE, joinType, risk, curPosition));
                    res.add(joinRecord(RowKind.UPDATE_AFTER, joinType, risk, newPosition));
                } else {
                    res.add(joinRecord(RowKind.INSERT, joinType, risk, newPosition));
                }
            }
        } else {
            // Risk update
            Set<String> newIssuers = new HashSet<>();
            newRisk.get(F_RISK_ISSUER_RISKS).forEach(risk -> newIssuers.add(risk.getSMCI()));

            Set<String> oldIssuers = new HashSet<>();
            if (curRisk != null) {
                for (IssuerRiskLine risk : curRisk.get(F_RISK_ISSUER_RISKS)) {
                    oldIssuers.add(risk.getSMCI());
                    res.add(joinRecord(
                            newIssuers.contains(risk.getSMCI()) ? RowKind.UPDATE_BEFORE : RowKind.DELETE,
                            joinType,
                            risk,
                            curPosition
                    ));
                }
            }

            for (IssuerRiskLine risk : newRisk.get(F_RISK_ISSUER_RISKS)) {
                res.add(joinRecord(
                        oldIssuers.contains(risk.getSMCI()) ? RowKind.UPDATE_AFTER : RowKind.INSERT,
                        joinType,
                        risk,
                        curPosition
                ));
            }
        }

        return res;
    }

    private static Record joinRecord(RowKind kind, RecordType joinType, IssuerRiskLine risk, Record position) {
        Record res = new Record(kind, joinType);

        for (Field f : position.getType().getFields()) {
            res.set(f, position.get(f));
        }

        return res.with(F_ISSUER_ID, risk.getSMCI())
                .with(F_RISK_ISSUER_CR01, risk.getCR01())
                .with(F_RISK_ISSUER_JTD, risk.getJTD());
    }

    /*

    public static void tupleJoin() throws Exception {
        StreamExecutionEnvironment.setDefaultLocalParallelism(4);
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(4);

        // General purpose logic
        // - risk * position join
        // - account join
        // - issuer join
        // - parent join

        DataStream<IssuerRisk> riskStream = Generators.issuerRisk(env, 100000, 500);
        DataStream<RiskPosition> posStream = Generators.positions(env, 100000, 200);
        DataStream<Issuer> issuerStream = Generators.issuers(env, 500, 100);
        DataStream<FirmAccount> accountStream = Generators.accounts(env, 200);

        // 0 -> UIDtype
        // 1 -> UID
        // 2 -> book
        // 3 -> issuer
        // 4 -> cr01
        // 5 -> jtd
        DataStream<Tuple3<RowKind, IssuerRisk, RiskPosition>> joinedStream = riskStream.keyBy(risk -> risk.getUIDType() + ":" + risk.getUID())
                .connect(posStream.keyBy(pos -> pos.getUIDType() + ":" + pos.getUID()))
                .process(new RiskPositionJoin2(false));

        // 6 -> strategy
        DataStream<Tuple4<RowKind, IssuerRisk, RiskPosition, FirmAccount>> stage2 = joinedStream.keyBy(t -> t.f2.getFirmAccountMnemonic())
                .connect(accountStream.keyBy(FirmAccount::getMnemonic))
                .process(new ManyToOneJoin<>(
                        (t, acc) -> Tuple4.of(t.f0, t.f1, t.f2, acc),
                        new TupleTypeInfo<>(TypeInformation.of(RowKind.class), TypeInformation.of(IssuerRisk.class), TypeInformation.of(RiskPosition.class)),
                        Types.STRING,
                        t -> t.f1.getUIDType() + ":" + t.f1.getUID(),
                        TypeInformation.of(FirmAccount.class)
                ),
                        new TupleTypeInfo<>(TypeInformation.of(RowKind.class), TypeInformation.of(IssuerRisk.class), TypeInformation.of(RiskPosition.class), TypeInformation.of(FirmAccount.class)));

        KeyedStream<Issuer, String> keyedIssuers = issuerStream.keyBy(Issuer::getSMCI);

        // 7 -> issuer name
        // 8 -> ultimate parent
        DataStream<Tuple5<RowKind, IssuerRisk, RiskPosition, FirmAccount, Issuer>> stage3 = stage2.keyBy(t -> t.f1.getSMCI())
                .connect(keyedIssuers)
                .process(new ManyToOneJoin<>(
                        (t, issuer) -> Tuple5.of(t.f0, t.f1, t.f2, t.f3, issuer),
                        new TupleTypeInfo<>(TypeInformation.of(RowKind.class), TypeInformation.of(IssuerRisk.class), TypeInformation.of(RiskPosition.class), TypeInformation.of(FirmAccount.class)),
                        Types.STRING,
                        t -> t.f1.getUIDType() + ":" + t.f1.getUID(),
                        TypeInformation.of(Issuer.class)
                ),
                        new TupleTypeInfo<>(TypeInformation.of(RowKind.class), TypeInformation.of(IssuerRisk.class), TypeInformation.of(RiskPosition.class), TypeInformation.of(FirmAccount.class), TypeInformation.of(Issuer.class)));

        // 9 -> ultimate parent name
        DataStream<Tuple6<RowKind, IssuerRisk, RiskPosition, FirmAccount, Issuer, Issuer>> stage4 = stage3.keyBy(t -> {
            return (t.f4.getUltimateParentSMCI() == null) ? t.f4.getSMCI() : t.f4.getUltimateParentSMCI();
        })
                .connect(keyedIssuers)
                .process(new ManyToOneJoin<>(
                        (t, issuer) -> Tuple6.of(t.f0, t.f1, t.f2, t.f3, t.f4, issuer),
                        new TupleTypeInfo<>(TypeInformation.of(RowKind.class), TypeInformation.of(IssuerRisk.class), TypeInformation.of(RiskPosition.class), TypeInformation.of(FirmAccount.class), TypeInformation.of(Issuer.class)),
                        Types.STRING,
                        t -> t.f1.getUIDType() + ":" + t.f1.getUID(),
                        TypeInformation.of(Issuer.class)
                        ),
                        new TupleTypeInfo<>(TypeInformation.of(RowKind.class), TypeInformation.of(IssuerRisk.class), TypeInformation.of(RiskPosition.class), TypeInformation.of(FirmAccount.class), TypeInformation.of(Issuer.class), TypeInformation.of(Issuer.class)));

        stage4.addSink(new LogSink<>("Source 4", 100000)).disableChaining();

        // Tier 2 logic
        // - aggregation
        // - threshold join
        // - exposure calculation

        env.execute();
    }

    private static RowData join(GenericRowData left, GenericRowData right) {
        GenericRowData result = new GenericRowData(left.getArity() + right.getArity());
        int leftArity = left.getArity();
        for (int i=0; i<leftArity; i++) {
            result.setField(i, left.getField(i));
        }
        for (int i=0; i<right.getArity(); i++) {
            result.setField(leftArity+i, right.getField(i));
        }
        result.setRowKind(left.getRowKind());
        return result;
    }

    public static void twoStageSQLJob() throws Exception
    {
        StreamExecutionEnvironment.setDefaultLocalParallelism(4);
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(4);

        TwoStageDataStreamFlowBuilder builder = new TwoStageDataStreamFlowBuilder(env);

        builder.setRiskPositionSource(Generators.positions(env, 100000, 200));
        builder.setRiskSource(Generators.issuerRisk(env, 100000, 500));
        builder.setIssuerSource(Generators.issuers(env, 500, 100));
        builder.setThresholdSource(Generators.thresholds(env, 100));

        builder.build();

        env.execute();
    }


    public static void allInOneSQL() throws Exception {
        StreamExecutionEnvironment.setDefaultLocalParallelism(4);
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(4);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        tenv.executeSql("CREATE TABLE RiskPosition (uidtype STRING, uid STRING, book STRING, product STRING) " +
                "WITH ('connector' = 'sample', 'dataset' = 'real-position')");

        tenv.executeSql("CREATE TABLE Account (mnemonic STRING, strategycode STRING) " +
                "WITH ('connector' = 'sample', 'dataset' = 'real-account')");

        tenv.executeSql("CREATE TABLE Risk (uidtype STRING, uid STRING, smci STRING, cr01 DOUBLE, jtd DOUBLE) " +
                "WITH ('connector' = 'sample', 'dataset' = 'real-risk')");



        Table agg = tenv.sqlQuery(
                "select smci, book, strategycode, sum(jtd) as jtd, sum(cr01) as cr01 from (" +
                "  select p.uidtype, p.uid, r.smci, p.book, a.strategycode, r.cr01, r.jtd from RiskPosition p " +
                "  left outer join Risk r on p.uidtype = r.uidtype and p.uid = r.uid " +
                "  left outer join Account a on p.book = a.mnemonic " +
                "  where a.strategycode is not null AND r.smci is not null" +
                ") group by smci, book, strategycode");

        tenv.toRetractStream(agg, RowData.class).addSink(new LogSink<>("Agg", 10000));

        env.execute();
    }

    public static void spartaSQLCustomWithJoin() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(4);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        tenv.executeSql("CREATE TABLE LiveRisk (issuer STRING, uid STRING, jtd DOUBLE, eventtime BIGINT) " +
                "WITH (" +
                "'connector' = 'sample', 'dataset' = 'risk'" +
                ")");

        tenv.executeSql("CREATE TABLE RiskPosition (uid STRING, book STRING) " +
                "WITH (" +
                "'connector' = 'sample', 'dataset' = 'position'" +
                ")");

        tenv.executeSql("CREATE TABLE FirmAccount (book STRING, strategy STRING, desk STRING) " +
                "WITH (" +
                "'connector' = 'sample', 'dataset' = 'account'" +
                ")");

        Table join = tenv.sqlQuery("SELECT p.uid, p.book, r.issuer, fa.strategy, fa.desk, r.jtd FROM RiskPosition p LEFT OUTER JOIN LiveRisk r on r.uid = p.uid LEFT OUTER JOIN FirmAccount fa on p.book = fa.book ");
        tenv.createTemporaryView("PositionAndRisk", join);

        Table agg = tenv.sqlQuery("SELECT desk, issuer, sum(jtd) from PositionAndRisk where issuer is not null group by desk, issuer");
        tenv.toRetractStream(agg, RowData.class).print("FINAL AGG");

        env.execute();
    }

    public static void spartaSQLCustomTableSource() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(4);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        tenv.executeSql("CREATE TABLE LiveRisk (issuer STRING, uid STRING, jtd DOUBLE, eventtime BIGINT) " +
                "WITH (" +
                "'connector' = 'sample', 'dataset' = 'risk'" +
                ")");

        Table issuerRisk = tenv.sqlQuery(
                "select issuer, sum(jtd) as jtd from LiveRisk group by issuer"
        );

        tenv.toRetractStream(issuerRisk, new RowTypeInfo(
                Types.STRING,
                Types.DOUBLE
        )).print("XXX");

        env.execute();
    }

    public static void spartaSample() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(4);

        SpartaFlowBuilder builder = new SpartaFlowBuilder(env);
        builder.setRiskSource(env.fromElements(
                Tuple4.of("IBM", "T1", 100.0, 1L),
                Tuple4.of("IBM", "T2", -50.0, 2L),
                Tuple4.of("MSFT", "T3", 1000.0, 3L),
                Tuple4.of("MSFT", "T2", -50.0, 4L)
        ));

        builder.build();
        builder.getAggregateIssuerStream().print("XXX");

        env.execute();
    }

    public static void spartaLoad() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(4);

        SpartaFlowBuilder builder = new SpartaFlowBuilder(env);

        DataStream<IssuerRisk> issuerRisk = Generators.issuerRisk(env, Generators.NO_POSITION, Generators.NO_USED_ISSUER);
        DataStream<Tuple4<String,String,Double,Long>> risk = issuerRisk
                .map(
                        new RichMapFunction<IssuerRisk, Tuple4<String, String, Double, Long>>() {
                            private transient Meter meter;
                            private long n = 0;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                meter = getRuntimeContext().getMetricGroup().meter("myMeter", new MeterView(10));
                            }

                            @Override
                            public Tuple4<String, String, Double, Long> map(IssuerRisk r) throws Exception {
                                meter.markEvent();
                                n++;
                                if (n%1000000 == 0) {
                                    System.out.println(getRuntimeContext().getIndexOfThisSubtask() + "> " + meter.getRate());
                                }

                                return Tuple4.of(r.getSMCI(), r.getUIDType().toString() + ":" + r.getUID(), r.getJTD(), r.getAuditDateTimeUTC());
                            }
                        }
                );

        builder.setRiskSource(risk);
        builder.build();

        builder.getAggregateIssuerStream().addSink(new SinkFunction<>() {
            private long n=0;

            @Override
            public void invoke(Tuple2<Boolean, Row> value) {
                n++;
                if (n%10000000 == 0) {
                    System.out.println(value);
                }
            }
        });

        env.execute();
    }

    public static class LatestDoubleValue extends LatestValue<Double> {}
    public static class LatestStringValue extends LatestValue<String> {}
    public static class LatestLongValue extends LatestValue<Long> {}

    public static class LatestValue<T> extends AggregateFunction<T, Tuple2<Long, T>> {
        @Override
        public T getValue(Tuple2<Long, T> state) {
            return state.f1;
        }

        @Override
        public Tuple2<Long, T> createAccumulator() {
            return Tuple2.of(0L, (T) null);
        }

        public void accumulate(Tuple2<Long, T> state, T val, long time) {
            if (state.f0 <= time) {
                state.f0 = time;
                state.f1 = val;
            }
        }
    }

    */
}
