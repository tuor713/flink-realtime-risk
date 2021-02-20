package org.uwh.flink.data.generic;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.reflect.Nullable;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.types.RowKind;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

public class RecordType extends TypeInformation<Record> {
    // TODO Add distinction between mandatory and optional fields
    @SuppressWarnings("rawtypes")
    private final TreeSet<Field> fields = new TreeSet<>();
    private final Map<Field,Integer> indices = new HashMap<>();
    private final Set<Field> nullable = new HashSet<>();
    private final ExecutionConfig config;
    private final FieldGetter[] getters;
    private final FieldSetter[] setters;
    private final SerializableAvroSchema schema;
    public static final Field<RowKind> F_ROW_KIND = new Field("_record","row-kind",TypeInformation.of(RowKind.class));

    // copied from Flink internal
    final static class SerializableAvroSchema implements Serializable {
        private static final long serialVersionUID = 1L;
        @Nullable
        private transient Schema schema;

        SerializableAvroSchema() {
        }

        SerializableAvroSchema(Schema schema) {
            this.schema = schema;
        }

        Schema getAvroSchema() {
            return this.schema;
        }

        private void writeObject(ObjectOutputStream oos) throws IOException {
            if (this.schema == null) {
                oos.writeBoolean(false);
            } else {
                oos.writeBoolean(true);
                oos.writeUTF(this.schema.toString(false));
            }

        }

        private void readObject(ObjectInputStream ois) throws ClassNotFoundException, IOException {
            if (ois.readBoolean()) {
                String schema = ois.readUTF();
                this.schema = (new Schema.Parser()).parse(schema);
            } else {
                this.schema = null;
            }

        }
    }

    @FunctionalInterface
    public interface FieldGetter<T> extends Serializable {
        T get(GenericData.Record data);
    }

    @FunctionalInterface
    public interface FieldSetter<T> extends Serializable {
        void set(GenericData.Record data, T value);
    }

    @SuppressWarnings("rawtypes")
    public RecordType(ExecutionConfig config, Field... fields) {
        this(config, Arrays.asList(fields));
    }

    public RecordType(ExecutionConfig config, Collection<Field> fields) {
        this(config, fields, Collections.emptySet());
    }

    @SuppressWarnings("rawtypes")
    public RecordType(ExecutionConfig config, Collection<Field> fields, Set<Field> nullable) {
        this.config = config;
        this.fields.addAll(fields);
        this.nullable.addAll(nullable);

        this.schema = new SerializableAvroSchema(buildSchema());

        getters = new FieldGetter[this.fields.size()+1];
        setters = new FieldSetter[this.fields.size()+1];

        indices.put(F_ROW_KIND, 0);
        getters[0] = F_ROW_KIND.getFieldGetter(config, false, 0);
        setters[0] = F_ROW_KIND.getFieldSetter(config, false, 0);

        int idx = 1;
        for (Field f : this.fields) {
            indices.put(f, idx);

            getters[idx] = f.getFieldGetter(config, this.nullable.contains(f), idx);
            setters[idx] = f.getFieldSetter(config, this.nullable.contains(f), idx);

            idx++;
        }
    }

    private Schema buildSchema() {
        List<Schema.Field> fs = new ArrayList<>();
        fs.add(buildField(F_ROW_KIND));
        fs.addAll(fields.stream().map(this::buildField).collect(Collectors.toList()));
        return Schema.createRecord("schema_"+UUID.randomUUID().toString().replaceAll("-",""), "", "_record", false, fs);
    }

    private Schema.Field buildField(Field f) {
        Schema schema = f.getSchema(config);

        if (nullable.contains(f)) {
            schema = Schema.createUnion(Schema.create(Schema.Type.NULL), schema);
        }

        return new Schema.Field(f.getFullName(), schema);
    }

    public Schema getSchema() {
        return schema.getAvroSchema();
    }

    public<T> T get(GenericData.Record data, Field<T> field) {
        return (T) getters[indexOf(field)].get(data);
    }

    public<T> void set(GenericData.Record data, Field<T> field, T value) {
        setters[indexOf(field)].set(data, value);
    }

    @SuppressWarnings("rawtypes")
    public NavigableSet<Field> getFields() {
        return Collections.unmodifiableNavigableSet(fields);
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

    public ExecutionConfig getConfig() {
        return config;
    }

    @Override
    public boolean isBasicType() {
        return false;
    }

    @Override
    public boolean isTupleType() {
        return true;
    }

    @Override
    public int getArity() {
        return fields.size();
    }

    @Override
    public int getTotalFields() {
        return fields.size();
    }

    @Override
    public Class<Record> getTypeClass() {
        return Record.class;
    }

    @Override
    public boolean isKeyType() {
        return false;
    }

    @Override
    public TypeSerializer<Record> createSerializer(ExecutionConfig executionConfig) {
        return new RecordSerializer(config, this);
    }

    @Override
    public String toString() {
        return "Record["+fields+"]";
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof RecordType)) {
            return false;
        } else {
            return fields.equals(((RecordType) o).fields);
        }
    }

    @Override
    public int hashCode() {
        return fields.hashCode();
    }

    @Override
    public boolean canEqual(Object o) {
        return o instanceof Record;
    }
}
