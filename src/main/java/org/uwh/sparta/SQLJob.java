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
}
