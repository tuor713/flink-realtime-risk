package org.uwh.sparta;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.uwh.*;
import org.uwh.flink.util.RightFilter;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/*
Initial explorations with Flink DataStreams to implement Sparta

Including having enough local state to execute SQL queries in Flink operators
 */
public class StreamingJob {
    public static final int NO_ISSUER = 10000;
    public static final int NO_ACCOUNT = 10000;
    public static final int NO_USED_ACCOUNT = 250;
    public static final int NO_USED_ISSUER = 1000;
    public static final int NO_ULTIMATE = 100;
    public static final int NO_POSITION = 100000;

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStream<Long> tickStream = Generators.ticks(env, 2, TimeUnit.SECONDS);
        DataStream<Command> commandStream = tickStream.map(tick -> new Command(CommandType.TICK, tick)).flatMap(new FlatMapFunction<Command, Command>() {
            @Override
            public void flatMap(Command command, Collector<Command> collector) throws Exception {
                collector.collect(command);
                if (((Long) command.getPayload()) == 5) {
                    Map<String, List<String>> filters = new HashMap<>();
                    filters.put("FirmAccountMnemonic", Arrays.asList("ACC000001"));
                    collector.collect(
                            new Command(
                                    CommandType.QUERY,
                                    new Query(
                                            Arrays.asList("UIDType", "UID"),
                                            Arrays.asList(),
                                            filters)));
                }
            }
        });

        DataStream<IssuerRisk> issuerRiskStream = Generators.issuerRisk(env, NO_POSITION, NO_USED_ISSUER);
        DataStream<RiskPosition> positionStream = Generators.positions(env, NO_POSITION, NO_USED_ACCOUNT);
        DataStream<RiskThreshold> thresholdStream = Generators.thresholds(env, NO_ISSUER);
        DataStream<Issuer> issuerStream = Generators.issuers(env, NO_ISSUER, NO_ULTIMATE);
        KeyedStream<FirmAccount, String> accountStream = Generators.accounts(env, NO_ACCOUNT).keyBy(FirmAccount::getMnemonic);

        DataStream<FirmAccount> filteredAccounts = accountStream.connect(positionStream.map(RiskPosition::getFirmAccountMnemonic).keyBy(s -> s))
                .process(new RightFilter<>(FirmAccount.class));
        /*
        DataStream<Issuer> filteringIssuers = issuerStream
                .keyBy(Issuer::getSMCI)
                .connect(issuerRiskStream.map(IssuerRisk::getSMCI).keyBy(s -> s))
                .process(new RightFilter<>(Issuer.class));
         */

        KeyedStream<RiskPosition, String> keyedPositions = positionStream.keyBy((pos) -> pos.getUIDType() + ":" + pos.getUID());
        KeyedStream<IssuerRisk, String> riskByPositionId = issuerRiskStream.keyBy(risk -> risk.getUIDType() + ":" + risk.getUID());

        DataStream<Tuple2<IssuerRisk, RiskPosition>> riskPositionStream = riskByPositionId
                .connect(keyedPositions)
                .process(new RefJoin<>(TypeInformation.of(IssuerRisk.class), TypeInformation.of(RiskPosition.class)));

        DataStream<RiskAggregate<Tuple2<String, String>>> riskAggregateStream =
            riskPositionStream
                    .keyBy(t -> Tuple2.of(t.f0.getSMCI(), t.f1.getFirmAccountMnemonic()), new TupleTypeInfo<>(TypeInformation.of(String.class), TypeInformation.of(String.class)))
                    .process(new Aggregator<>(
                                t -> t.f0.getUIDType() + ":" + t.f1.getUID(),
                                new TupleTypeInfo<>(TypeInformation.of(IssuerRisk.class), TypeInformation.of(RiskPosition.class)),
                                t -> t.f0.getCR01(),
                                t -> t.f0.getJTD()));

        /*
        calculateUtilizations(riskAggregateStream, thresholdStream)
                .filter(util -> Math.abs(util.getUtilization().get(RiskMeasure.JTD.name()).getUtilization()) > 250)
                .print("Utilization");
         */

        riskCache(riskPositionStream, commandStream);

        env.execute();
    }

    private static void riskCache(DataStream<Tuple2<IssuerRisk, RiskPosition>> riskPositionStream, DataStream<Command> commandStream) {
        riskPositionStream.keyBy(t -> t.f0.getUIDType() + ":" + t.f0.getUID())
                .connect(commandStream.broadcast(new MapStateDescriptor[0]))
                .process(new KeyedBroadcastProcessFunction<String, Tuple2<IssuerRisk, RiskPosition>, Command, String>() {
                    private transient ValueState<Tuple2<IssuerRisk, RiskPosition>> risk;

                    @Override
                    public void open(Configuration parameters) {
                        risk = getRuntimeContext().getState(new ValueStateDescriptor<>(
                                "risk",
                                new TupleTypeInfo<>(TypeInformation.of(IssuerRisk.class), TypeInformation.of(RiskPosition.class))));
                    }

                    @Override
                    public void processElement(Tuple2<IssuerRisk, RiskPosition> t, ReadOnlyContext readOnlyContext, Collector<String> collector) throws Exception {
                        risk.update(t);
                    }

                    @Override
                    public void processBroadcastElement(Command cmd, Context context, Collector<String> collector) throws Exception {
                        if (cmd.getType() == CommandType.TICK) {
                            final AtomicInteger count = new AtomicInteger(0);
                            long start = System.nanoTime();
                            context.applyToKeyedState(
                                    new ValueStateDescriptor<>("risk", new TupleTypeInfo<>(TypeInformation.of(IssuerRisk.class), TypeInformation.of(RiskPosition.class))),
                                    (key, state) -> {
                                        count.incrementAndGet();
                                    });
                            long end = System.nanoTime();
                            long duration = (end - start) / 1000000;

                            collector.collect(getRuntimeContext().getTaskName() + ":" + getRuntimeContext().getIndexOfThisSubtask() + " -> " + count.get() + ", took " + duration + "ms");
                        } else if (cmd.getType() == CommandType.QUERY) {
                            System.out.println("Executing query -> " + cmd.getPayload());
                            QueryExecutor qe = new QueryExecutor((Query) cmd.getPayload());
                            context.applyToKeyedState(
                                    new ValueStateDescriptor<>("risk", new TupleTypeInfo<>(TypeInformation.of(IssuerRisk.class), TypeInformation.of(RiskPosition.class))),
                                    (key, state) -> qe.accept(key, (Tuple2<IssuerRisk,RiskPosition>) state.value())
                            );
                        }
                    }
                }).print("Command Stream");
    }

    private static DataStream<RiskThresholdUtilization> calculateUtilizations(DataStream<RiskAggregate<Tuple2<String,String>>> riskAggStream, DataStream<RiskThreshold> thresholds) {
        DataStream<RiskAggregate<String>> issuerRisk =
                riskAggStream.keyBy(agg -> agg.getDimensions().f0)
                    .process(new Aggregator<>(
                            agg -> agg.getDimensions().f0 + ":" + agg.getDimensions().f1,
                            TypeInformation.of(new TypeHint<RiskAggregate<Tuple2<String, String>>>() {}),
                            agg -> agg.getCr01(),
                            agg -> agg.getJtd()
                    ));

        return issuerRisk.keyBy(agg -> agg.getDimensions())
                .connect(thresholds.keyBy(t -> t.getRiskFactor()))
                .process(new RefJoin<RiskAggregate<String>, RiskThreshold, Tuple2<RiskAggregate<String>, RiskThreshold>>(TypeInformation.of(new TypeHint<RiskAggregate<String>>() {}), TypeInformation.of(RiskThreshold.class)))
                .map(new IssuerRiskUtilization());
    }
}
