package org.uwh.risk;

import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.EitherTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.types.Either;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;
import org.uwh.*;
import org.uwh.flink.data.generic.Field;
import org.uwh.flink.data.generic.Record;
import org.uwh.flink.data.generic.RecordType;
import org.uwh.flink.data.generic.Stream;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.uwh.flink.data.generic.Expressions.$;

public class Experiments {
    private static final Field<String> F_ACCOUNT_MNEMONIC = new Field<>("account", "mnemonic", Types.STRING);
    private static final Field<String> F_ACCOUNT_STRATEGY_CODE = new Field<>("account", "strategy-code", Types.STRING);

    private static final Field<String> F_POS_UID = new Field<>("position", "uid", Types.STRING);
    private static final Field<UIDType> F_POS_UID_TYPE = new Field<>("position", "uid-type", TypeInformation.of(UIDType.class));

    private static final Field<Double> F_RISK_ISSUER_JTD = new Field<>("issuer-risk","jtd", Double.class, Types.DOUBLE);
    private static final Field<String> F_ISSUER_NAME = new Field<>("issuer", "name", String.class, Types.STRING);

    private static AtomicInteger count = new AtomicInteger(0);


    @Test
    public void testRiskJoin() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(4);
        env.getConfig().disableGenericTypes();

        int numPos = 1000;
        DataStream<Issuer> issuers = Generators.issuers(env, 1000, 100);
        DataStream<IssuerRiskBatch> risk = Generators.oneTimeBatchRisk(env, numPos, 1000);
        DataStream<RiskPosition> positions = Generators.positions(env, numPos, 100);

        Stream issuerStream = Stream.fromDataStream(issuers,
                $(issuer -> issuer.getSMCI(), Fields.F_ISSUER_ID),
                $(issuer -> issuer.getName(), Fields.F_ISSUER_NAME));
        Stream riskStream = Stream.fromDataStream(risk,
                $(r -> r.getUIDType().toString(), Fields.F_POS_UID_TYPE),
                $(r -> r.getUID(), Fields.F_POS_UID),
                $(r -> r.getRisk(), Fields.F_RISK_ISSUER_RISKS));
        Stream posStream = Stream.fromDataStream(positions,
                $(pos -> pos.getUIDType().toString(), Fields.F_POS_UID_TYPE),
                $(pos -> pos.getUID(), Fields.F_POS_UID),
                $(pos -> pos.getFirmAccountMnemonic(), Fields.F_ACCOUNT_MNEMONIC));

        RiskJoin join = new RiskJoin(riskStream.getRecordType(), posStream.getRecordType(), issuerStream.getRecordType());

        DataStream<Record> joined = riskStream
                .getDataStream()
                .map(rec -> Either.Left(rec), new EitherTypeInfo<>(riskStream.getRecordType(), posStream.getRecordType()))
                .union(posStream.getDataStream().map(rec -> Either.Right(rec), new EitherTypeInfo<>(riskStream.getRecordType(), posStream.getRecordType())))
                .keyBy(t -> {
                    Record rec = t.isLeft() ? t.left() : t.right();
                    return rec.get(Fields.F_POS_UID_TYPE)+":"+rec.get(Fields.F_POS_UID);
                })
                .connect(issuerStream.getDataStream().broadcast(join.getMapStateDescriptor()))
                .process(join);

        joined.addSink(new SinkFunction<>() {
            @Override
            public void invoke(Record value) throws Exception {
                count.incrementAndGet();
            }
        });

        env.execute();

        // Expected
        // - 0..199 => bond => 200
        // - 200..999 => deriv
        //   - 200 index * 125 => 25000
        //   - 600 non-index => 600
        assertEquals(25800, count.get());
    }

    @Test
    public void testBroadcastJoin() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(4);
        DataStream<FirmAccount> accounts = Generators.accounts(env, 1000);
        DataStream<Issuer> issuers = Generators.issuers(env, 1000, 100);

        DataStream<IssuerRisk> risk = Generators.oneTimeIssuerRisk(env, 1000, 1000);
        DataStream<RiskPosition> positions = Generators.positions(env, 1000, 100);

        DataStream<Either<RiskPosition, IssuerRisk>> riskOrPos =
                positions.map(pos -> Either.Left(pos), new EitherTypeInfo<>(TypeInformation.of(RiskPosition.class), TypeInformation.of(IssuerRisk.class)))
                    .union(risk.map(r -> Either.Right(r), new EitherTypeInfo<>(TypeInformation.of(RiskPosition.class), TypeInformation.of(IssuerRisk.class))));


        MapStateDescriptor<String, Either<FirmAccount,Issuer>> descriptor = new MapStateDescriptor<>(
                "accounts",
                Types.STRING,
                new EitherTypeInfo<>(TypeInformation.of(FirmAccount.class), TypeInformation.of(Issuer.class)));

        ValueStateDescriptor<Tuple2<RiskPosition,IssuerRisk>> keyDescriptor = new ValueStateDescriptor<>("pending", new TupleTypeInfo<>(TypeInformation.of(RiskPosition.class), TypeInformation.of(IssuerRisk.class)));

        DataStream<Either<FirmAccount,Issuer>> refDataStream =
                accounts.map(acc -> Either.Left(acc), new EitherTypeInfo<>(TypeInformation.of(FirmAccount.class), TypeInformation.of(Issuer.class)))
                        .union(issuers.map(issuer -> Either.Right(issuer), new EitherTypeInfo<>(TypeInformation.of(FirmAccount.class), TypeInformation.of(Issuer.class))));

        RecordType resType = new RecordType(env.getConfig(), F_POS_UID_TYPE, F_POS_UID, F_ACCOUNT_MNEMONIC, F_ACCOUNT_STRATEGY_CODE, F_ISSUER_NAME, F_RISK_ISSUER_JTD);


        riskOrPos
                .keyBy(v -> {
                    if (v.isLeft()) {
                        return v.left().getUIDType() + ":" + v.left().getUID();
                    } else {
                        return v.right().getUIDType() + ":" + v.right().getUID();
                    }
                })
                .connect(refDataStream.broadcast(descriptor))
                .process(new KeyedBroadcastProcessFunction<String, Either<RiskPosition,IssuerRisk>, Either<FirmAccount,Issuer>, Record>() {
                    private transient ValueState<Tuple2<RiskPosition,IssuerRisk>> pendingRec;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        pendingRec = getRuntimeContext().getState(keyDescriptor);
                    }

                    @Override
                    public void processElement(Either<RiskPosition,IssuerRisk> in, ReadOnlyContext readOnlyContext, Collector<Record> collector) throws Exception {
                        Tuple2<RiskPosition,IssuerRisk> cur = pendingRec.value();
                        if (in.isLeft()) {
                            RiskPosition pos = in.left();
                            IssuerRisk risk = (cur != null) ? cur.f1 : null;

                            ReadOnlyBroadcastState<String, Either<FirmAccount, Issuer>> state = readOnlyContext.getBroadcastState(descriptor);
                            if (risk != null && state.contains("F:" + pos.getFirmAccountMnemonic()) && state.contains("I:" + risk.getSMCI())) {
                                pendingRec.clear();
                                collector.collect(createResult(pos, risk, state.get("F:" + pos.getFirmAccountMnemonic()).left(), state.get("I:"+risk.getSMCI()).right()));
                            } else {
                                pendingRec.update(Tuple2.of(pos, risk));
                            }
                        } else {
                            RiskPosition pos = (cur != null) ? cur.f0 : null;
                            IssuerRisk risk = in.right();

                            ReadOnlyBroadcastState<String, Either<FirmAccount, Issuer>> state = readOnlyContext.getBroadcastState(descriptor);
                            if (pos != null && state.contains("F:" + pos.getFirmAccountMnemonic()) && state.contains("I:" + risk.getSMCI())) {
                                pendingRec.clear();
                                collector.collect(createResult(pos, risk, state.get("F:" + pos.getFirmAccountMnemonic()).left(), state.get("I:"+risk.getSMCI()).right()));
                            } else {
                                pendingRec.update(Tuple2.of(pos, risk));
                            }
                        }
                    }

                    @Override
                    public void processBroadcastElement(Either<FirmAccount,Issuer> value, Context context, Collector<Record> collector) throws Exception {
                        BroadcastState<String,Either<FirmAccount,Issuer>> state = context.getBroadcastState(descriptor);

                        if (value.isLeft()) {
                            FirmAccount fa = value.left();
                            state.put("F:"+fa.getMnemonic(), value);

                            context.applyToKeyedState(keyDescriptor, (key, riskPositionValueState) -> {
                                Tuple2<RiskPosition, IssuerRisk> cur = riskPositionValueState.value();

                                if (cur.f0 != null && cur.f1 != null && fa.getMnemonic().equals(cur.f0.getFirmAccountMnemonic()) && state.contains("I:" + cur.f1.getSMCI())) {
                                    collector.collect(createResult(cur.f0, cur.f1, fa, state.get("I:" + cur.f1.getSMCI()).right()));
                                    riskPositionValueState.clear();
                                }
                            });
                        } else {
                            Issuer issuer = value.right();
                            state.put("I:"+value.right().getSMCI(), value);

                            context.applyToKeyedState(keyDescriptor, (key, riskPositionValueState) -> {
                                Tuple2<RiskPosition, IssuerRisk> cur = riskPositionValueState.value();

                                if (cur.f0 != null && cur.f1 != null && cur.f1.getSMCI().equals(issuer.getSMCI()) && state.contains("F:"+cur.f0.getFirmAccountMnemonic())) {
                                    collector.collect(createResult(cur.f0, cur.f1, state.get("F:"+ cur.f0.getFirmAccountMnemonic()).left(), issuer));
                                    riskPositionValueState.clear();
                                }
                            });

                        }
                    }

                    private Record createResult(RiskPosition pos, IssuerRisk risk, FirmAccount fa, Issuer issuer) {
                        Record res = new Record(resType);
                        res.set(F_POS_UID_TYPE, pos.getUIDType());
                        res.set(F_POS_UID, pos.getUID());
                        res.set(F_ACCOUNT_MNEMONIC, pos.getFirmAccountMnemonic());
                        res.set(F_ACCOUNT_STRATEGY_CODE, fa.getStrategyCode());
                        res.set(F_ISSUER_NAME, issuer.getName());
                        res.set(F_RISK_ISSUER_JTD, risk.getJTD());
                        return res;
                    }
                }, resType).print("OUT");

        env.execute();
    }
}
