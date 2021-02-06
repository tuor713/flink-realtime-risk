package org.uwh.flink.data.generic;

import org.apache.avro.generic.GenericData;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.NestedSerializersSnapshotDelegate;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.java.typeutils.runtime.DataInputViewStream;
import org.apache.flink.api.java.typeutils.runtime.DataOutputViewStream;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.formats.avro.typeutils.AvroSerializer;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class RecordSerializer extends TypeSerializer<Record> {
    private final RecordType type;
    private final TypeSerializer<GenericData.Record> delegate;

    public RecordSerializer(ExecutionConfig config, RecordType type) {
        this.type = type;
        delegate = new AvroSerializer<>(GenericData.Record.class, type.getSchema());
    }

    private RecordSerializer(RecordType type, TypeSerializer<GenericData.Record> delegate) {
        this.type = type;
        this.delegate = delegate;
    }

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public TypeSerializer<Record> duplicate() {
        return new RecordSerializer(type, delegate);
    }

    @Override
    public Record createInstance() {
        return new Record(type);
    }

    @Override
    public Record copy(Record record) {
        return new Record(record);
    }

    @Override
    public Record copy(Record input, Record output) {
        return null;
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(Record record, DataOutputView dataOutputView) throws IOException {
        delegate.serialize(record.getGenericRecord(), dataOutputView);
    }

    @Override
    public Record deserialize(DataInputView dataInputView) throws IOException {
        GenericData.Record row = delegate.deserialize(dataInputView);
        return new Record(row, type);
    }

    @Override
    public Record deserialize(Record reuse, DataInputView dataInputView) throws IOException {
        // TODO do we have to do this?
        return deserialize(dataInputView);
    }

    @Override
    public void copy(DataInputView dataInputView, DataOutputView dataOutputView) throws IOException {
        delegate.copy(dataInputView, dataOutputView);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RecordSerializer that = (RecordSerializer) o;
        return Objects.equals(type, that.type) && Objects.equals(delegate, that.delegate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, delegate);
    }

    @Override
    public TypeSerializerSnapshot<Record> snapshotConfiguration() {
        return new RecordSerializerSnapshot(this.type.getFields(), new NestedSerializersSnapshotDelegate(this.delegate));
    }

    private static class RecordSerializerSnapshot implements TypeSerializerSnapshot<Record> {
        private Set<Field> fields;
        private NestedSerializersSnapshotDelegate delegate;

        public RecordSerializerSnapshot() {}

        public RecordSerializerSnapshot(Set<Field> fields, NestedSerializersSnapshotDelegate delegate) {
            this.fields = fields;
            this.delegate = delegate;
        }

        @Override
        public int getCurrentVersion() {
            return 1;
        }

        @Override
        public void writeSnapshot(DataOutputView dataOutputView) throws IOException {
            dataOutputView.writeInt(fields.size());
            DataOutputViewStream out = new DataOutputViewStream(dataOutputView);

            for (Field f : fields) {
                InstantiationUtil.serializeObject(out);
            }

            delegate.writeNestedSerializerSnapshots(dataOutputView);
        }

        @Override
        public void readSnapshot(int version, DataInputView dataInputView, ClassLoader classLoader) throws IOException {
            int noFields = dataInputView.readInt();
            fields = new HashSet<>();
            DataInputViewStream in = new DataInputViewStream(dataInputView);

            for (int i=0; i<noFields; i++) {
                try {
                    fields.add(InstantiationUtil.deserializeObject(in, classLoader));
                } catch (ClassNotFoundException e) {
                    throw new IOException(e);
                }
            }


            delegate = NestedSerializersSnapshotDelegate.readNestedSerializerSnapshots(dataInputView, classLoader);
        }

        @Override
        public TypeSerializer<Record> restoreSerializer() {
            return new RecordSerializer(new RecordType(new ExecutionConfig(), fields), delegate.getRestoredNestedSerializer(0));
        }

        @Override
        public TypeSerializerSchemaCompatibility<Record> resolveSchemaCompatibility(TypeSerializer<Record> typeSerializer) {
            return TypeSerializerSchemaCompatibility.compatibleAsIs();
        }
    }
}
