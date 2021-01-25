package org.uwh.flink.util;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Filter a data stream to only a whitelisted set of entries, by key. Other data items are held in state, at least the latest version
 * so as to be allow the item to be emitted immediately if it is included in the whitelist
 * @param <K> The type of keys
 * @param <X> The type of data item being filtered
 */
public class RightFilter<K, X> extends KeyedCoProcessFunction<K, X, K, X> implements ResultTypeQueryable<X> {
    private transient ValueState<Boolean> allow;
    private transient ValueState<X> item;
    private Class<X> itemClass;
    private JoinFunction<X,X,X> latestValueSelector;

    // returns the later value
    private static class DefaultJoinFunction<X> implements JoinFunction<X,X,X> {
        @Override
        public X join(X x, X x2) {
            return x2;
        }
    }

    public RightFilter(Class<X> itemClass) {
        this(itemClass, new DefaultJoinFunction<X>());
    }

    public RightFilter(Class<X> itemClass, JoinFunction latestValueSelector) {
        this.itemClass = itemClass;
        this.latestValueSelector = latestValueSelector;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        allow = getRuntimeContext().getState(new ValueStateDescriptor<>("allow", Boolean.class));
        item = getRuntimeContext().getState(new ValueStateDescriptor<X>("item", itemClass));
    }

    @Override
    public TypeInformation<X> getProducedType() {
        return TypeInformation.of(itemClass);
    }

    @Override
    public void processElement1(X x, Context context, Collector<X> collector) throws Exception {
        X current = item.value();
        if (current != null) {
            item.update(latestValueSelector.join(current, x));
        } else {
            item.update(x);
        }
        Boolean b = allow.value();
        if (b != null) {
            // TODO should we emit x, even if it is not the latest value
            // in general probably yes, but it could be surprising behavior
            collector.collect(x);
        }
    }

    @Override
    public void processElement2(K s, Context context, Collector<X> collector) throws Exception {
        Boolean prev = allow.value();
        if (prev == null) {
            allow.update(true);
            X x = item.value();
            if (x != null) {
                collector.collect(x);
            }
        }
    }
}
