package org.uwh.flink.data.generic;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.types.logical.*;

import java.io.Serializable;
import java.util.Objects;

public class Field<T> implements Serializable, Comparable<Field<T>>, Expression<T> {
    private final String namespace;
    private final String name;
    private final Class<T> clazz;
    private final TypeInformation<T> type;

    public Field(String namespace, String name, Class<T> clazz, TypeInformation<T> type) {
        this.namespace = namespace;
        this.name = name;
        this.clazz = clazz;
        this.type = type;
    }

    public Field(String namespace, String name, Field<T> parent) {
        this(namespace, name, parent.getTypeClass(), parent.getType());
    }

    public String getNamespace() {
        return namespace;
    }

    public String getName() {
        return name;
    }

    public TypeInformation<T> getType() {
        return type;
    }

    public LogicalType getLogicalType(ExecutionConfig config) {
        // TODO support more types
        if (getType().equals(Types.STRING)) {
            return new VarCharType();
        } else if (getType().equals(Types.DOUBLE)) {
            return new DoubleType();
        } else if (getType().equals(Types.LONG)) {
            return new BigIntType();
        } else if (clazz.isEnum()) {
            return new TinyIntType();
        } else {
            return new RawType<>(clazz, type.createSerializer(config));
        }
    }

    public Class<T> getTypeClass() {
        return clazz;
    }

    @Override
    public Field<T> getResultField() {
        return this;
    }

    @Override
    public T map(Record rec) {
        return rec.get(this);
    }

    @Override
    public int compareTo(Field<T> o) {
        return (namespace + "/" + name).compareTo(o.getNamespace() + "/" + o.getName());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Field<?> field = (Field<?>) o;
        return namespace.equals(field.namespace) && name.equals(field.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(namespace, name);
    }
}
