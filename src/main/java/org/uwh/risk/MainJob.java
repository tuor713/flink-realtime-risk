package org.uwh.risk;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.EitherTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FromElementsFunction;
import org.apache.flink.types.Either;
import org.uwh.*;
import org.uwh.flink.data.generic.Record;
import org.uwh.flink.data.generic.Stream;
import org.uwh.flink.util.DelayedParallelSource;
import org.uwh.flink.util.DelayedSource;

import java.util.List;

import static org.uwh.flink.data.generic.Expressions.$;
import static org.uwh.flink.data.generic.Expressions.as;
import static org.uwh.risk.Fields.*;

public class MainJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment.setDefaultLocalParallelism(4);
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(4);
        env.getConfig().disableGenericTypes();

        int numPositions = 500_000;

        /*
        Need to delay the main firehoses so that ref data, particularly issuers, can be loaded first. Otherwise the broadcast side of the join is very slow (involving iteration).
         */
        int delayMs = 10_000;
        List<RiskPosition> posList = Generators.positionList(numPositions, Generators.NO_USED_ACCOUNT);
        DataStream<RiskPosition> posStream = env.addSource(new DelayedSource<>(
                new FromElementsFunction<>(TypeInformation.of(RiskPosition.class).createSerializer(env.getConfig()), posList),
                TypeInformation.of(RiskPosition.class), delayMs));
        DataStream<IssuerRiskBatch> batchStream = env.addSource(new DelayedParallelSource<>(
                Generators.batchRisk(numPositions, Generators.NO_USED_ISSUER),
                TypeInformation.of(IssuerRiskBatch.class),
                delayMs));

        DataStream<Issuer> issuerStream = Generators.issuers(env, Generators.NO_ISSUER, Generators.NO_ULTIMATE);
        DataStream<FirmAccount> accountStream = Generators.accounts(env, Generators.NO_ACCOUNT);

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

        RiskJoin join = new RiskJoin(batchRisk.getRecordType(), positions.getRecordType(), issuersWithParent.getRecordType());

        DataStream<Record> finalDataStream = batchRisk
                .getDataStream()
                .map(rec -> Either.Left(rec), new EitherTypeInfo<>(batchRisk.getRecordType(), positions.getRecordType()))
                .union(positions.getDataStream().map(rec -> Either.Right(rec), new EitherTypeInfo<>(batchRisk.getRecordType(), positions.getRecordType())))
                .keyBy(t -> {
                    Record rec = t.isLeft() ? t.left() : t.right();
                    return rec.get(Fields.F_POS_UID_TYPE)+":"+rec.get(Fields.F_POS_UID);
                })
                .connect(issuersWithParent.getDataStream().broadcast(join.getMapStateDescriptor()))
                .process(join);

        Stream finalStream = new Stream(finalDataStream, join.getProducedType(), Stream.ChangeLogMode.CHANGE_LOG);

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
}
