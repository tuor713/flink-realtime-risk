package org.uwh.flink.data.generic;

import org.apache.flink.api.common.functions.MapFunction;

public class Expressions {
    public static<T>  Expression<T> map(MapFunction<Record,T> mapper, Field<T> field) {
        return new Expression<T>() {
            @Override
            public Field<T> getResultField() {
                return field;
            }

            @Override
            public T map(Record rec) throws Exception {
                return mapper.map(rec);
            }
        };
    }
}
