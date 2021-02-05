package org.uwh.sparta;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
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

import static org.uwh.flink.data.generic.Expressions.$;
import static org.uwh.flink.data.generic.Expressions.as;
import static org.uwh.sparta.Fields.*;

public class SQLJob {

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

        Stream issuers = Stream.fromDataStream(
                issuerStream,
                $(Issuer::getSMCI, F_ISSUER_ID),
                $(Issuer::getName, F_ISSUER_NAME),
                $(it -> it.getUltimateParentSMCI()!= null ? it.getUltimateParentSMCI() : it.getSMCI(),  F_ISSUER_ULTIMATE_PARENT_ID)
        );

        Stream parentIssuer = issuers.select(
                as(F_ISSUER_ID, F_ISSUER_ULTIMATE_PARENT_ID),
                as(F_ISSUER_NAME, F_ISSUER_ULTIMATE_PARENT_NAME));

        Stream issuersWithParent = issuers.joinManyToOne(parentIssuer, F_ISSUER_ULTIMATE_PARENT_ID, F_ISSUER_ID);

        Stream accounts = Stream.fromDataStream(
                accountStream,
                $(FirmAccount::getMnemonic, F_ACCOUNT_MNEMONIC),
                $(FirmAccount::getStrategyCode, F_ACCOUNT_STRATEGY_CODE)
        );

        Stream positions = Stream.fromDataStream(
                posStream,
                $(pos -> pos.getUIDType().name(), F_POS_UID_TYPE),
                $(RiskPosition::getUID, F_POS_UID),
                $(RiskPosition::getFirmAccountMnemonic, F_ACCOUNT_MNEMONIC),
                $(pos -> pos.getProductType().name(), F_POS_PRODUCT_TYPE)
        );

        Stream posWithAccount = positions.joinManyToOne(
                accounts,
                F_ACCOUNT_MNEMONIC,
                rec -> Tuple2.of(rec.get(F_POS_UID_TYPE), rec.get(F_POS_UID)),
                new TupleTypeInfo<>(Types.STRING, Types.STRING));

        Stream batchRisk = Stream.fromDataStream(
                batchStream,
                $(r -> r.getUIDType().name(), F_POS_UID_TYPE),
                $(IssuerRiskBatch::getUID, F_POS_UID),
                $(IssuerRiskBatch::getRisk, F_RISK_ISSUER_RISKS)
        );

        RecordType issuerRiskType = new RecordType(env.getConfig(), F_POS_UID_TYPE, F_POS_UID, F_ISSUER_ID, F_RISK_ISSUER_CR01, F_RISK_ISSUER_JTD);
        RecordType joinType = issuerRiskType.join(posWithAccount.getRecordType());
        Stream riskWithPosition = batchRisk.joinOneToOne(
                posWithAccount,
                rec -> Tuple2.of(rec.get(F_POS_UID_TYPE), rec.get(F_POS_UID)),
                new TupleTypeInfo<>(Types.STRING, Types.STRING),
                (curL, curR, newL, newR) -> joinRiskAndPosition(joinType, curL, curR, newL, newR),
                joinType,
                Stream.ChangeLogMode.CHANGE_LOG);

        Stream finalStream = riskWithPosition.joinManyToOne(
                issuersWithParent,
                F_ISSUER_ID,
                rec -> Tuple2.of(rec.get(F_POS_UID_TYPE), rec.get(F_POS_UID)),
                new TupleTypeInfo<>(Types.STRING, Types.STRING));

        finalStream.log("STAGE1", 500_000);

        // === Tier 2 logic ===

        Stream aggStream = finalStream.aggregate(List.of(F_ISSUER_ULTIMATE_PARENT_ID), List.of(F_RISK_ISSUER_CR01, F_RISK_ISSUER_JTD), 10_000);

        DataStream<RiskThreshold> thresholdStream = Generators.thresholds(env, Generators.NO_ULTIMATE);

        Stream thresholds = Stream.fromDataStream(
                thresholdStream.filter(thres -> thres.getRiskFactorType() == RiskFactorType.Issuer),
                $(RiskThreshold::getRiskFactor, F_ISSUER_ULTIMATE_PARENT_ID),
                $(it -> it.getThresholds().get(RiskMeasure.CR01.name()), F_RISK_LIMIT_CR01_THRESHOLD),
                $(it -> it.getThresholds().get(RiskMeasure.JTD.name()), F_RISK_LIMIT_JTD_THRESHOLD)
        );

        Stream aggWithThresholds = aggStream.joinOneToOne(thresholds, F_ISSUER_ULTIMATE_PARENT_ID);

        aggWithThresholds.select(
                F_ISSUER_ULTIMATE_PARENT_ID,
                F_RISK_ISSUER_CR01,
                F_RISK_LIMIT_CR01_THRESHOLD,
                as(rec -> 100 * Math.abs(rec.get(F_RISK_ISSUER_CR01) / rec.get(F_RISK_LIMIT_CR01_THRESHOLD)), F_RISK_LIMIT_CR01_UTILIZATION),
                F_RISK_ISSUER_JTD,
                F_RISK_LIMIT_JTD_THRESHOLD,
                as(rec -> 100 * Math.abs(rec.get(F_RISK_ISSUER_JTD) / rec.get(F_RISK_LIMIT_JTD_THRESHOLD)), F_RISK_LIMIT_JTD_UTILIZATION)
        ).log("LIMIT", 1_000);

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
