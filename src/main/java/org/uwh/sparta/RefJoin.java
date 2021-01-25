package org.uwh.sparta;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

public class RefJoin<X,Y,Z extends Tuple> extends KeyedCoProcessFunction<String, X, Y, Z> implements ResultTypeQueryable<Z> {
    private transient ValueState<X> leftCache;
    private transient ValueState<Y> refCache;
    private final TypeInformation<Y> refInfo;
    private final TypeInformation<X> leftInfo;

    public RefJoin(TypeInformation<X> leftInfo, TypeInformation<Y> refInfo) {
        this.leftInfo = leftInfo;
        this.refInfo = refInfo;
    }

    @Override
    public void open(Configuration parameters) {
        leftCache = getRuntimeContext().getState(new ValueStateDescriptor<>("left", leftInfo));
        refCache = getRuntimeContext().getState(new ValueStateDescriptor<>("right", refInfo));
    }

    @Override
    public TypeInformation<Z> getProducedType() {
        if (refInfo.isTupleType()) {
            TupleTypeInfo tinfo = (TupleTypeInfo) refInfo;
            TypeInformation[] infos = new TypeInformation[refInfo.getArity()+1];
            infos[0] = leftInfo;
            for (int i=1; i<=refInfo.getArity(); i++) {
                infos[i] = tinfo.getTypeAt(i-1);
            }
            return new TupleTypeInfo<>(tinfo);
        } else {
            return new TupleTypeInfo<>(leftInfo, refInfo);
        }
    }

    @Override
    public void processElement1(X left, Context context, Collector<Z> collector) throws Exception {
        Y ref = refCache.value();
        if (ref != null) {
            collector.collect(createResult(left, ref));
        } else {
            leftCache.update(left);
        }
    }

    @Override
    public void processElement2(Y ref, Context context, Collector<Z> collector) throws Exception {
        refCache.update(ref);
        X left = leftCache.value();
        if (left != null) {
            leftCache.update(null);
            collector.collect(createResult(left, ref));
        }
    }

    private Z createResult(X left, Y ref) {
        if (refInfo.isTupleType()) {
            Tuple tref = (Tuple) ref;
            Z res = (Z) Tuple.newInstance(tref.getArity() + 1);
            res.setField(left, 0);
            for (int i = 1; i <= tref.getArity(); i++) {
                res.setField(tref.getField(i - 1), i);
            }
            return res;
        } else {
            return (Z) new Tuple2<>(left, ref);
        }
    }
}
