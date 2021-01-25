package org.uwh.flink.data.generic;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

public class Record {
    private final RowData data;
    private final RecordType type;

    public Record(RowData data, RecordType type) {
        this.data = data;
        this.type = type;
    }

    public Record(RecordType type) {
        this.data = new GenericRowData(type.getFields().size());
        this.type = type;
    }

    public Record(RowKind kind, RecordType type) {
        this.data = new GenericRowData(kind, type.getFields().size());
        this.type = type;
    }

    public Record(Record rec) {
        this.data = new GenericRowData(rec.data.getArity());
        this.type = rec.type;

        for (Field f : type.getFields()) {
            this.set(f, rec.get(f));
        }
    }

    // Copy constructor but potentially narrower type
    public Record(RowKind kind, RecordType type, Record rec) {
        this.data = new GenericRowData(kind, type.getFields().size());
        this.type = type;

        for (Field f : type.getFields()) {
            this.set(f, rec.get(f));
        }
    }

    public<T> T get(Field<T> field) {
        return type.get(data, field);
    }

    public<T> void set(Field<T> field, T value) {
        type.set((GenericRowData) data, field, value);
    }

    public<T> Record with(Field<T> field, T value) {
        set(field, value);
        return this;
    }

    public RowKind getKind() {
        return data.getRowKind();
    }

    public RowData getRow() {
        return data;
    }

    public RecordType getType() {
        return type;
    }

    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("Record[")
                .append(data.getRowKind().shortString())
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
}
