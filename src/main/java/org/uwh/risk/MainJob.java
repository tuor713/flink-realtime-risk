package org.uwh.risk;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.EitherTypeInfo;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FromElementsFunction;
import org.apache.flink.types.Either;
import org.uwh.*;
import org.uwh.flink.data.generic.Field;
import org.uwh.flink.data.generic.Record;
import org.uwh.flink.data.generic.RecordType;
import org.uwh.flink.data.generic.Stream;
import org.uwh.flink.util.DelayedSourceFunction;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.uwh.flink.data.generic.Expressions.*;
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
        int delayMs = 120_000;
        List<RiskPosition> posList = Generators.positionList(numPositions, Generators.NO_USED_ACCOUNT);
        DataStream<RiskPosition> posStream = DelayedSourceFunction.delay(
                env,
                new FromElementsFunction<>(TypeInformation.of(RiskPosition.class).createSerializer(env.getConfig()), posList),
                TypeInformation.of(RiskPosition.class),
                delayMs).name("Risk Position");
        DataStream<IssuerRiskBatch> batchStream = DelayedSourceFunction.delay(
                env,
                Generators.batchRisk(numPositions, Generators.NO_USED_ISSUER),
                TypeInformation.of(IssuerRiskBatch.class),
                delayMs).name("Issuer Risk Batch");

        DataStream<Issuer> issuerStream = Generators.issuers(env, Generators.NO_ISSUER, Generators.NO_ULTIMATE);
        DataStream<FirmAccount> accountStream = Generators.accounts(env, Generators.NO_ACCOUNT);

        Stream issuers = Stream.fromDataStream(
                issuerStream,
                $(Issuer::getSMCI, F_ISSUER_ID),
                $(Issuer::getName, F_ISSUER_NAME),
                $(it -> it.getUltimateParentSMCI()!= null ? it.getUltimateParentSMCI() : it.getSMCI(),  F_ISSUER_ULTIMATE_PARENT_ID)
        );

        Stream parentIssuer = issuers
                .where(rec -> rec.get(F_ISSUER_ID).equals(rec.get(F_ISSUER_ULTIMATE_PARENT_ID)))
                .select(
                        as(F_ISSUER_ID, F_ISSUER_ULTIMATE_PARENT_ID),
                        as(F_ISSUER_NAME, F_ISSUER_ULTIMATE_PARENT_NAME));

        DataStream<String> ids = Generators.batchRisk(env, numPositions, Generators.NO_USED_ISSUER)
                .flatMap((batch, collector) -> {
                    for (IssuerRiskLine risk : batch.getRisk()) {
                        collector.collect(risk.getSMCI());
                    }
                }, Types.STRING);

        Stream issuersWithParent = issuers.filter(ids, F_ISSUER_ID).joinManyToOne(parentIssuer, F_ISSUER_ULTIMATE_PARENT_ID, F_ISSUER_ID);

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

        RecordType riskLineType = new RecordType(env.getConfig(), F_ISSUER_ID, F_RISK_ISSUER_CR01, F_RISK_ISSUER_JTD, F_RISK_ISSUER_JTD_ROLLDOWN);
        TypeInformation<List<Record>> riskLineTypeInfo = new ListTypeInfo<>(riskLineType);
        Field<List<Record>> fieldRiskLines = new Field<>("issuer-risk", "risks", riskLineTypeInfo);

        Stream batchRisk = Stream.fromDataStream(
                batchStream,
                $(r -> r.getUIDType().name(), F_POS_UID_TYPE),
                $(IssuerRiskBatch::getUID, F_POS_UID),
                $(r -> {
                    List<Record> risks = new ArrayList<>();
                    for (IssuerRiskLine line : r.getRisk()) {
                        risks.add(new Record(riskLineType)
                                .with(F_ISSUER_ID, line.getSMCI())
                                .with(F_RISK_ISSUER_CR01, line.getCR01())
                                .with(F_RISK_ISSUER_JTD, line.getJTD())
                                .with(F_RISK_ISSUER_JTD_ROLLDOWN, line.getJTDRolldown()));
                    }

                    return risks;
                }, fieldRiskLines)
        );

        RiskJoin join = new RiskJoin(batchRisk.getRecordType(), fieldRiskLines, riskLineType, posWithAccount.getRecordType(), issuersWithParent.getRecordType());

        DataStream<Record> finalDataStream = batchRisk
                .getDataStream()
                .map(rec -> Either.Left(rec), new EitherTypeInfo<>(batchRisk.getRecordType(), posWithAccount.getRecordType()))
                .union(posWithAccount.getDataStream().map(rec -> Either.Right(rec), new EitherTypeInfo<>(batchRisk.getRecordType(), posWithAccount.getRecordType())))
                .keyBy(t -> {
                    Record rec = t.isLeft() ? t.left() : t.right();
                    return rec.get(Fields.F_POS_UID_TYPE)+":"+rec.get(Fields.F_POS_UID);
                })
                .connect(issuersWithParent.getDataStream().broadcast(join.getMapStateDescriptor()))
                .process(join).name("Risk - Position - Issuer Join");

        Stream finalStream = new Stream(finalDataStream, join.getProducedType(), Stream.ChangeLogMode.CHANGE_LOG);

        finalStream.log("STAGE1", 500_000);

        // === Tier 2 logic ===

        Stream firstAggStream = finalStream.aggregate(
                List.of(F_ISSUER_ULTIMATE_PARENT_ID, F_ISSUER_ID, F_POS_PRODUCT_TYPE),
                List.of(
                        sum(F_RISK_ISSUER_CR01),
                        sum(F_RISK_ISSUER_JTD),
                        new RolldownAggegration(),
                        last(F_ISSUER_ULTIMATE_PARENT_NAME),
                        last(F_ISSUER_NAME)
                ),
                10_000);

        firstAggStream.log("STAGE2", 1_000);

        firstAggStream.rollup(
                List.of(
                        Set.of(F_ISSUER_ULTIMATE_PARENT_ID, F_ISSUER_ID),
                        Set.of(F_ISSUER_ULTIMATE_PARENT_ID, F_POS_PRODUCT_TYPE)
                ),
                List.of(
                        sum(F_RISK_ISSUER_CR01),
                        sum(F_RISK_ISSUER_JTD)
                )
        ).log("ROLLUP", 1_000);

        Stream aggStream = firstAggStream.aggregate(
                List.of(F_ISSUER_ULTIMATE_PARENT_ID),
                List.of(
                        sum(F_RISK_ISSUER_CR01),
                        sum(F_RISK_ISSUER_JTD),
                        new RolldownAggegration(),
                        last(F_ISSUER_ULTIMATE_PARENT_NAME)
                ),
                -1);

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

    private static class RolldownAggegration implements Aggregation<List<RolldownItem>,List<RolldownItem>> {
        @Override
        public Field<List<RolldownItem>> getInputField() {
            return F_RISK_ISSUER_JTD_ROLLDOWN;
        }

        @Override
        public Field<List<RolldownItem>> getOutputField() {
            return F_RISK_ISSUER_JTD_ROLLDOWN;
        }

        @Override
        public List<RolldownItem> init(List<RolldownItem> value, boolean retract) {
            if (retract) {
                return value.stream().map(it -> new RolldownItem(it.getDate(), -it.getJTD())).collect(Collectors.toList());
            } else {
                return value;
            }
        }

        @Override
        public List<RolldownItem> update(List<RolldownItem> value, List<RolldownItem> accumulator, boolean retract) {
            double factor = retract ? -1 : 1;
            List<RolldownItem> result = new ArrayList<>();

            int i = 0;
            int j = 0;
            while (i < value.size() || j < accumulator.size()) {
                if (i == value.size()) {
                    for (int k = j; k<accumulator.size(); k++) {
                        result.add(accumulator.get(k));
                    }
                    break;
                }
                if (j == accumulator.size()) {
                    for (int k = i; k<value.size(); k++) {
                        result.add(new RolldownItem(value.get(k).getDate(), value.get(k).getJTD()));
                    }
                    break;
                }

                LocalDate iDate = value.get(i).getDate();
                LocalDate jDate = accumulator.get(j).getDate();
                if (iDate.equals(jDate)) {
                    result.add(new RolldownItem(iDate, accumulator.get(j).getJTD() + factor * value.get(i).getJTD()));
                    i++;
                    j++;
                } else if (iDate.isBefore(jDate)) {
                    result.add(new RolldownItem(iDate, factor * value.get(i).getJTD()));
                    i++;
                } else {
                    result.add(accumulator.get(j));
                    j++;
                }
            }

            return result;
        }
    }
}
