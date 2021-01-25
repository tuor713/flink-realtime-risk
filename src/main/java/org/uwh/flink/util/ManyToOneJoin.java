package org.uwh.flink.util;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Collections;

/**
 * Utility function for live-live join between
 */
public class ManyToOneJoin<KX, X, Y, Z> extends KeyedCoProcessFunction<String, X, Y, Z> {
    private transient MapState<KX, X> leftCache;
    private transient ValueState<Y> refCache;
    private final TypeInformation<KX> leftKey;
    private final KeySelector<X, KX> leftKeyFunction;
    private final TypeInformation<X> leftInfo;
    private final TypeInformation<Y> refInfo;
    private final DeltaJoinFunction<X,Y,Z> joinFunction;

    public ManyToOneJoin(DeltaJoinFunction<X,Y,Z> joinFunction, TypeInformation<X> leftInfo, TypeInformation<KX> leftKey, KeySelector<X, KX> leftKeyFunction, TypeInformation<Y> refInfo) {
        this.leftInfo = leftInfo;
        this.refInfo = refInfo;
        this.leftKey = leftKey;
        this.leftKeyFunction = leftKeyFunction;
        this.joinFunction = joinFunction;
    }

    public ManyToOneJoin(JoinFunction<X,Y,Z> joinFunction, TypeInformation<X> leftInfo, TypeInformation<KX> leftKey, KeySelector<X, KX> leftKeyFunction, TypeInformation<Y> refInfo) {
        this.leftInfo = leftInfo;
        this.refInfo = refInfo;
        this.leftKey = leftKey;
        this.leftKeyFunction = leftKeyFunction;
        this.joinFunction = (currentX, currentY, newX, newY) -> Collections.singleton(joinFunction.join(newX != null ? newX : currentX, newY != null ? newY : currentY));
    }

    @Override
    public void open(Configuration parameters) {
        leftCache = getRuntimeContext().getMapState(new MapStateDescriptor<>("left", leftKey, leftInfo));
        refCache = getRuntimeContext().getState(new ValueStateDescriptor<>("right", refInfo));
    }

    @Override
    public void processElement1(X left, Context context, Collector<Z> collector) throws Exception {
        Y ref = refCache.value();
        if (ref != null) {
            KX key = leftKeyFunction.getKey(left);
            X prevLeft = leftCache.get(key);
            leftCache.put(key, left);
            for (Z z : joinFunction.join(prevLeft, ref, left, null)) {
                collector.collect(z);
            }
        } else {
            leftCache.put(leftKeyFunction.getKey(left), left);
        }
    }

    @Override
    public void processElement2(Y ref, Context context, Collector<Z> collector) throws Exception {
        Y prevRef = refCache.value();
        refCache.update(ref);
        for (X left : leftCache.values()) {
            for (Z z : joinFunction.join(left, prevRef, null, ref)) {
                collector.collect(z);
            }
        }
    }
}
