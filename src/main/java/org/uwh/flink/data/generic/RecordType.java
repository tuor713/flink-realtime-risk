package org.uwh.flink.data.generic;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryRawValueData;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RawType;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

public class RecordType implements Serializable, ResultTypeQueryable<RowData> {
    // TODO Add distinction between mandatory and optional fields
    @SuppressWarnings("rawtypes")
    private final TreeSet<Field> fields = new TreeSet<>();
    private final Map<Field,Integer> indices = new HashMap<>();
    private final ExecutionConfig config;
    private final FieldGetter[] getters;
    private final FieldSetter[] setters;

    @FunctionalInterface
    public interface FieldGetter<T> extends Serializable {
        T get(RowData data);
    }

    public interface FieldSetter<T> extends Serializable {
        void set(GenericRowData data, T value);
    }

    @SuppressWarnings("rawtypes")
    public RecordType(ExecutionConfig config, Field... fields) {
        this(config, Arrays.asList(fields));
    }

    @SuppressWarnings("rawtypes")
    public RecordType(ExecutionConfig config, Collection<Field> fields) {
        this.config = config;
        this.fields.addAll(fields);
        int idx = 0;

        getters = new FieldGetter[fields.size()];
        setters = new FieldSetter[fields.size()];

        for (Field f : this.fields) {
            indices.put(f, idx);

            getters[idx] = createGetter(f, idx);
            setters[idx] = createSetter(f, idx);

            idx++;
        }
    }

    private FieldGetter createGetter(Field f, int index) {
        LogicalType type = f.getLogicalType(config);
        switch (type.getTypeRoot()) {
            case DOUBLE:
                return data -> data.getDouble(index);
            case BIGINT:
                return data -> data.getLong(index);
            case VARCHAR:
                return data -> {
                    StringData res = data.getString(index);
                    return (res != null) ? res.toString() : null;
                };
            case RAW:
                TypeSerializer serializer = ((RawType) type).getTypeSerializer();
                return data -> {
                    RawValueData res = data.getRawValue(index);
                    return (res != null) ? res.toObject(serializer) : null;
                };
            default:
                throw new IllegalStateException("No support for field type for "+f);
        }
    }

    private FieldSetter createSetter(Field f, int index) {
        LogicalType type = f.getLogicalType(config);
        switch (type.getTypeRoot()) {
            case DOUBLE:
            case BIGINT:
                return (data, value) -> data.setField(index, value);
            case VARCHAR:
                return (data, value) -> data.setField(index, StringData.fromString((String) value));
            case RAW:
                return (data, value) -> data.setField(index, BinaryRawValueData.fromObject(value));
            default:
                throw new IllegalStateException("No support for field type for "+f);
        }
    }

    public<T> T get(RowData data, Field<T> field) {
        return (T) getters[indexOf(field)].get(data);
    }

    public<T> void set(GenericRowData data, Field<T> field, T value) {
        setters[indexOf(field)].set(data, value);
    }

    @SuppressWarnings("rawtypes")
    public NavigableSet<Field> getFields() {
        return Collections.unmodifiableNavigableSet(fields);
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return new RowDataTypeInfo(
            fields.stream().map(f -> f.getLogicalType(config)).collect(Collectors.toList()).toArray(new LogicalType[0])
        );
    }

    public int indexOf(Field f) {
        return indices.get(f);
    }

    public RecordType join(RecordType other) {
        TreeSet<Field> fields = new TreeSet<>();
        fields.addAll(this.fields);
        fields.addAll(other.fields);
        return new RecordType(config, fields);
    }

    public RecordType mapped(Map<Field,Field> mapping) {
        for (Field key : mapping.keySet()) {
            if (!fields.contains(key)) {
                throw new IllegalArgumentException("Mapping maps a field that does not exist in the source " + key);
            }
        }

        return new RecordType(config, mapping.values());
    }

    public ExecutionConfig getConfig() {
        return config;
    }
}
