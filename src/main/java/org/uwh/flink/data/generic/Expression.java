package org.uwh.flink.data.generic;

import org.apache.flink.api.common.functions.MapFunction;

public interface Expression<T> extends MapFunction<Record,T> {
    Field<T> getResultField();
    default boolean isStar() { return false; }
}
