package org.uwh.sparta;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class Aggregator<K, IN> extends KeyedProcessFunction<K, IN, RiskAggregate<K>> {
    private transient MapState<String, IN> riskMap;
    private transient ValueState<RiskAggregate> result;
    private final KeySelector<IN, String> keySelector;
    private final TypeInformation<IN> inTypeInfo;
    private final KeySelector<IN, Double> getCR01;
    private final KeySelector<IN, Double> getJTD;

    public Aggregator(KeySelector<IN, String> keySelector, TypeInformation<IN> inTypeInfo, KeySelector<IN, Double> getCR01, KeySelector<IN, Double> getJTD) {
        this.keySelector = keySelector;
        this.inTypeInfo = inTypeInfo;
        this.getCR01 = getCR01;
        this.getJTD = getJTD;
    }

    @Override
    public void open(Configuration parameters) {
        riskMap = getRuntimeContext().getMapState(new MapStateDescriptor<>("contributions", TypeInformation.of(String.class), inTypeInfo));
        result = getRuntimeContext().getState(new ValueStateDescriptor<>("result", RiskAggregate.class));
    }

    @Override
    public void processElement(IN input, Context context, Collector<RiskAggregate<K>> collector) throws Exception {
        RiskAggregate<K> prevResult = (RiskAggregate<K>) result.value();
        String riskKey = keySelector.getKey(input);
        IN prev = riskMap.get(riskKey);
        riskMap.put(riskKey, input);

        double prevCr01 = (prev != null) ? getCR01.getKey(prev) : 0.0;
        double prevJtd = (prev != null) ? getJTD.getKey(prev) : 0.0;

        RiskAggregate<K> newResult;
        if (prevResult != null) {
            newResult = new RiskAggregate<>(
                    prevResult.getDimensions(),
                    prevResult.getCr01() - prevCr01 + getCR01.getKey(input),
                    prevResult.getJtd() - prevJtd + getJTD.getKey(input),
                    (prev == null) ? prevResult.getCount()+1 : prevResult.getCount());
        } else {
            newResult = new RiskAggregate<>(context.getCurrentKey(), getCR01.getKey(input), getJTD.getKey(input), 1);
        }

        result.update(newResult);
        collector.collect(newResult);
    }
}
