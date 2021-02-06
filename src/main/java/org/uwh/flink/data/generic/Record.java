package org.uwh.flink.data.generic;

import org.apache.avro.generic.GenericData;
import org.apache.flink.types.RowKind;

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
        set(RecordType.F_ROW_KIND, kind);
    }

    public Record(Record rec) {
        this.data = new GenericData.Record(rec.data, false);
        this.type = rec.type;
        set(RecordType.F_ROW_KIND, rec.getKind());
    }

    public Record(RowKind kind, Record rec) {
        this(kind, rec.getType(), rec);
    }

    // Copy constructor but potentially narrower type
    public Record(RowKind kind, RecordType type, Record rec) {
        this.type = type;
        this.data = new GenericData.Record(type.getSchema());
        set(RecordType.F_ROW_KIND, kind);

        for (Field f : type.getFields()) {
            this.set(f, rec.get(f));
        }
    }

    public<T> T get(Field<T> field) {
        return type.get(data, field);
    }

    public<T> void set(Field<T> field, T value) {
        type.set(data, field, value);
    }

    public<T> Record with(Field<T> field, T value) {
        set(field, value);
        return this;
    }

    public void copyInto(Record rec) {
        for (Field f : rec.getType().getFields()) {
            set(f, rec.get(f));
        }
    }

    public RowKind getKind() {
        return get(RecordType.F_ROW_KIND);
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
