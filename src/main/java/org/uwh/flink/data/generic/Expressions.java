package org.uwh.flink.data.generic;

import org.apache.flink.api.common.functions.MapFunction;

public class Expressions {
    public interface FieldConverter<T,F> extends MapFunction<T,F> {
        Field<F> getField();
    }

    // Special case placeholder
    public static final Expression<Object> STAR = new Expression<>() {
        @Override
        public Field<Object> getResultField() {
            return null;
        }

        @Override
        public Object map(Record record) throws Exception {
            return null;
        }

        public boolean isStar() { return true; }

        @Override
        public String toString() {
            return "*";
        }
    };

    public static<T,F> FieldConverter<T,F> $(MapFunction<T,F> selector, Field<F> field) {
        return new FieldConverter<T, F>() {
            @Override
            public Field<F> getField() {
                return field;
            }

            @Override
            public F map(T t) throws Exception {
                return selector.map(t);
            }
        };
    }

    public static<T>  Expression<T> as(MapFunction<Record,T> mapper, Field<T> field) {
        return new Expression<>() {
            @Override
            public Field<T> getResultField() {
                return field;
            }

            @Override
            public T map(Record rec) throws Exception {
                return mapper.map(rec);
            }

            @Override
            public String toString() {
                return "<exp> AS "+field.toString();
            }
        };
    }

    public static<T> Expression<T> as(Field<T> oldField, Field<T> newField) {
        return new Expression<>() {
            @Override
            public Field<T> getResultField() {
                return newField;
            }

            @Override
            public T map(Record record) throws Exception {
                return record.get(oldField);
            }

            @Override
            public String toString() {
                return oldField.toString() + " AS " + newField.toString();
            }

        };
    }
}
