package org.uwh.flink.util;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.functions.AggregateFunction;

public class LatestSum extends AggregateFunction<Double, LatestSumState> {

    @Override
    public Double getValue(LatestSumState latestSumState) {
        return latestSumState.sum;
    }

    @Override
    public LatestSumState createAccumulator() {
        return new LatestSumState();
    }

    public void accumulate(LatestSumState state, double jtd, String uid, long time) {
        // System.out.println("XXX - Accumulate - " + uid + ", " + jtd + ", " + time);
        Tuple2<Long, Double> prev = state.trades.get(uid);

        if (prev != null) {
            if (prev.f0 <= time) {
                state.trades.put(uid, Tuple2.of(time, jtd));
                state.sum += jtd - prev.f1;
            } else {
                // do nothing the update is stale
            }
        } else {
            state.trades.put(uid, Tuple2.of(time, jtd));
            state.sum += jtd;
        }
    }

    public void retract(LatestSumState state, double jtd, String uid, long time) {
        // System.out.println("XXX - Retract - " + uid + ", " + jtd + ", " + time);

        Tuple2<Long, Double> prev = state.trades.remove(uid);
        if (prev != null) {
            state.sum -= prev.f1;
        } else {
            System.out.println("XXX ERROR condition - retract without add");
        }
    }
}