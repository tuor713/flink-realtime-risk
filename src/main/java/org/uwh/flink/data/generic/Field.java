package org.uwh.flink.data.generic;

import org.apache.avro.Schema;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.binary.BinaryRawValueData;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.EnumMap;
import java.util.Objects;

public class Field<T> implements Serializable, Comparable<Field<T>>, Expression<T> {
    private final String namespace;
    private final String name;
    private final String fullName;
    private final Class<T> clazz;
    private final TypeInformation<T> type;
    private final int hashCode;

    public Field(String namespace, String name, Class<T> clazz, TypeInformation<T> type) {
        this.namespace = namespace;
        this.name = name;
        this.clazz = clazz;
        this.type = type;
        this.fullName = (namespace + "_" + name).replaceAll("[-.]","_");
        this.hashCode = Objects.hash(namespace, name);
    }

    public Field(String namespace, String name, TypeInformation<T> type) {
        this(namespace, name, type.getTypeClass(), type);
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

    public String getFullName() {
        return fullName;
    }

    public TypeInformation<T> getType() {
        return type;
    }

    public Schema getSchema(ExecutionConfig config) {
        return getFieldType(config).getSchema();
    }

    public RecordType.FieldGetter<T> getFieldGetter(ExecutionConfig config, boolean nullable, int index) {
        return getFieldType(config).getFieldGetter(nullable, index);
    }

    public RecordType.FieldSetter<T> getFieldSetter(ExecutionConfig config, boolean nullable, int index) {
        return getFieldType(config).getFieldSetter(nullable, index);
    }

    private FieldType<T> getFieldType(ExecutionConfig config) {
        if (getType().equals(Types.STRING)) {
            Schema res = Schema.create(Schema.Type.STRING);
            res.addProp("avro.java.string", "String");
            return new SimpleFieldType<T>(res);
        } else if (getType().equals(Types.DOUBLE)) {
            return new SimpleFieldType<T>(Schema.create(Schema.Type.DOUBLE));
        } else if (getType().equals(Types.LONG)) {
            return new SimpleFieldType<T>(Schema.create(Schema.Type.LONG));
        } else if (clazz.isEnum()) {
            return new FieldType<T>() {
                @Override
                public Schema getSchema() {
                    return Schema.create(Schema.Type.INT);
                }

                @Override
                public RecordType.FieldGetter<T> getFieldGetter(boolean nullable, int index) {
                    Class<Enum> eclazz = (Class) clazz;
                    Enum[] values = eclazz.getEnumConstants();
                    if (nullable) {
                        return data -> {
                            Integer val = (Integer) data.get(index);
                            return val != null ? (T) values[val] : null;
                        };
                    } else {
                        return data -> (T) values[(int) data.get(index)];
                    }
                }

                @Override
                public RecordType.FieldSetter<T> getFieldSetter(boolean nullable, int index) {
                    Class<Enum> eclazz = (Class) clazz;
                    EnumMap mapping = new EnumMap(eclazz);
                    int i = 0;
                    for (Enum val : (Enum[]) eclazz.getEnumConstants()) {
                        mapping.put(val, i++);
                    }

                    if (nullable) {
                        return (data, value) -> {
                            if (value == null) {
                                data.put(index, null);
                            } else {
                                data.put(index, mapping.get(value));
                            }
                        };
                    } else {
                        return (data, value) -> data.put(index, mapping.get(value));
                    }
                }
            };
        } else {
            TypeSerializer<T> serializer = type.createSerializer(config);
            return new FieldType<T>() {
                @Override
                public Schema getSchema() {
                    return Schema.create(Schema.Type.BYTES);
                }

                @Override
                public RecordType.FieldGetter<T> getFieldGetter(boolean nullable, int index) {
                    if (nullable) {
                        return data -> {
                            ByteBuffer bytes = (ByteBuffer) data.get(index);
                            if (bytes != null) {
                                return RawValueData.<T>fromBytes(bytes.array()).toObject(serializer);
                            } else {
                                return null;
                            }
                        };
                    } else {
                        return data -> RawValueData.<T>fromBytes(((ByteBuffer) data.get(index)).array()).toObject(serializer);
                    }
                }

                @Override
                public RecordType.FieldSetter<T> getFieldSetter(boolean nullable, int index) {
                    return (data, value) -> {
                        if (value == null) {
                            data.put(index, null);
                        } else {
                            byte[] bytes = BinaryRawValueData.fromObject(value).toBytes(serializer);
                            data.put(index, ByteBuffer.wrap(bytes));
                        }
                    };
                }
            };
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
        return hashCode;
    }

    @Override
    public String toString() {
        return namespace + "/" + name;
    }

    private interface FieldType<T> {
        Schema getSchema();
        RecordType.FieldGetter<T> getFieldGetter(boolean nullable, int index);
        RecordType.FieldSetter<T> getFieldSetter(boolean nullable, int index);
    }

    private static class SimpleFieldType<T> implements FieldType<T> {
        private final Schema schema;

        public SimpleFieldType(Schema schema) {
            this.schema = schema;
        }

        @Override
        public Schema getSchema() {
            return schema;
        }

        @Override
        public RecordType.FieldGetter<T> getFieldGetter(boolean nullable, int index) {
            return data -> (T) data.get(index);
        }

        @Override
        public RecordType.FieldSetter<T> getFieldSetter(boolean nullable, int index) {
            return (data, value) -> data.put(index, value);
        }
    }
}
