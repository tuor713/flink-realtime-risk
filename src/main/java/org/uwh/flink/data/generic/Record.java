package org.uwh.flink.data.generic;

import org.apache.avro.generic.GenericData;
import org.apache.flink.types.RowKind;

import java.util.Collection;
import java.util.List;
import java.util.Objects;

public class Record {
    private final GenericData.Record data;
    private final RecordType type;

    public Record(GenericData.Record record, RecordType type) {
        this.data = new GenericData.Record(record, false);
        this.type = type;
    }

    public Record(RecordType type) {
        this(RowKind.INSERT, type);
    }

    public Record(RowKind kind, RecordType type) {
        this.data = new GenericData.Record(type.getSchema());
        this.type = type;
        set(type.getRowTypeFieldRef(), kind);
    }

    public Record(Record rec) {
        this.data = new GenericData.Record(rec.data, false);
        this.type = rec.type;
        set(type.getRowTypeFieldRef(), rec.getKind());
    }

    public Record(RowKind kind, Record rec) {
        this(kind, rec.getType(), rec);
    }

    // Copy constructor but potentially narrower type
    public Record(RowKind kind, RecordType type, Record rec) {
        this.type = type;
        this.data = new GenericData.Record(type.getSchema());

        set(type.getRowTypeFieldRef(), kind);
        copyAll(rec, type.getFields());
    }

    public<T> T get(Field<T> field) {
        return type.get(data, field);
    }

    public<T> T get(FieldRef<T> field) {
        return type.get(data, field);
    }

    public<T> void set(Field<T> field, T value) {
        type.set(data, field, value);
    }

    public<T> void set(FieldRef<T> field, T value) {
        type.set(data, field, value);
    }

    public Object getRaw(Field field) {
        return data.get(type.indexOf(field));
    }

    public Object getRaw(FieldRef field) {
        return data.get(field.getPosition());
    }

    public void setRaw(Field field, Object value) {
        data.put(type.indexOf(field), value);
    }

    public void setRaw(FieldRef field, Object value) {
        data.put(field.getPosition(), value);
    }

    public<T> Record with(Field<T> field, T value) {
        set(field, value);
        return this;
    }

    public<T> void copy(Record other, Field<T> field) {
        setRaw(field, other.getRaw(field));
    }

    public<T> void copy(Record other, FieldRef<T> inField, FieldRef<T> outField) {
        setRaw(outField, other.getRaw(inField));
    }

    public void copyAll(Record other, Collection<Field> fields) {
        for (Field f: fields) {
            setRaw(f, other.getRaw(f));
        }
    }

    public void copyAll(Record other, List<FieldRef> inFields, List<FieldRef> outFields) {
        for (int i=0; i<inFields.size(); i++) {
            copy(other, inFields.get(i), outFields.get(i));
        }
    }

    public void copyInto(Record rec) {
        copyAll(rec, rec.getType().getFields());
    }

    public RowKind getKind() {
        return get(type.getRowTypeFieldRef());
    }

    public GenericData.Record getGenericRecord() {
        return data;
    }

    public RecordType getType() {
        return type;
    }

    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("Record[")
                .append(getKind().shortString())
                .append("]{");
        boolean first = true;
        for (Field f : type.getFields()) {
            if (first) {
                first = false;
            } else {
                builder.append(", ");
            }
            builder.append(f.getNamespace()).append('/').append(f.getName()).append('=').append(get(f));
        }
        builder.append("}");
        return builder.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Record record = (Record) o;

        if (!type.equals(record.type)) return false;

        for (Field f : type.getFields()) {
            if (!Objects.equals(get(f), record.get(f))) return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return data.hashCode();
    }
}
