package org.uwh.flink.util;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Collections;

public class OneToOneJoin<X, Y, Z> extends KeyedCoProcessFunction<String, X, Y, Z> {
    private transient ValueState<X> leftCache;
    private transient ValueState<Y> refCache;
    private final TypeInformation<X> leftInfo;
    private final TypeInformation<Y> refInfo;
    private final DeltaJoinFunction<X,Y,Z> joinFunction;

    public OneToOneJoin(DeltaJoinFunction<X,Y,Z> joinFunction, TypeInformation<X> leftInfo, TypeInformation<Y> refInfo) {
        this.leftInfo = leftInfo;
        this.refInfo = refInfo;
        this.joinFunction = joinFunction;
    }

    public OneToOneJoin(JoinFunction<X,Y,Z> joinFunction, TypeInformation<X> leftInfo, TypeInformation<Y> refInfo) {
        this.leftInfo = leftInfo;
        this.refInfo = refInfo;
        this.joinFunction = (currentX, currentY, newX, newY) -> Collections.singleton(joinFunction.join(newX != null ? newX : currentX, newY != null ? newY : currentY));
    }

    @Override
    public void open(Configuration parameters) {
        leftCache = getRuntimeContext().getState(new ValueStateDescriptor<>("left", leftInfo));
        refCache = getRuntimeContext().getState(new ValueStateDescriptor<>("right", refInfo));
    }

    @Override
    public void processElement1(X left, Context context, Collector<Z> collector) throws Exception {
        Y ref = refCache.value();
        if (ref != null) {
            X prevLeft = leftCache.value();
            leftCache.update(left);
            for (Z z : joinFunction.join(prevLeft, ref, left, null)) {
                collector.collect(z);
            }
        } else {
            leftCache.update(left);
        }
    }

    @Override
    public void processElement2(Y ref, Context context, Collector<Z> collector) throws Exception {
        X left = leftCache.value();
        if (left != null) {
            Y prevRef = refCache.value();
            refCache.update(ref);
            for (Z z : joinFunction.join(left, prevRef, null, ref)) {
                collector.collect(z);
            }
        } else {
            refCache.update(ref);
        }
    }
}