package org.uwh.flink.util;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.HashMap;
import java.util.Map;

public class LatestSumState {
    public double sum = 0;
    public Map<String, Tuple2<Long, Double>> trades = new HashMap<>();
}