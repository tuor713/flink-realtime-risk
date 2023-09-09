package org.uwh.flink.util;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * TODO Can we support de-duplication?
 * TODO Can we support TTL / cleanup?
 */
public class MultiLeftJoin<K, X> extends KeyedCoProcessFunction<K, X, Tuple, Tuple> {
    private final int rhsArity;
    private final TypeInformation<X> lhsType;
    private final TypeInformation[] rhsTypes;
    private transient ValueState<X> leftCache;
    private transient ValueState[] rhsCache;

    private MultiLeftJoin(TypeInformation<X> lhsType, TypeInformation[] rhsTypes) {
        this.lhsType = lhsType;
        this.rhsArity = rhsTypes.length;
        this.rhsTypes = rhsTypes;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.rhsCache = new ValueState[rhsArity];
        leftCache = getRuntimeContext().getState(new ValueStateDescriptor<>("left", lhsType));
        for (int i=0; i<rhsArity; i++) {
            rhsCache[i] = getRuntimeContext().getState(new ValueStateDescriptor<>("right"+i, rhsTypes[i]));
        }
    }

    @Override
    public void processElement1(X x, KeyedCoProcessFunction<K, X, Tuple, Tuple>.Context context, Collector<Tuple> collector) throws Exception {
        leftCache.update(x);
        Tuple result = Tuple.newInstance(rhsArity+1);
        result.setField(x, 0);
        for (int i=0; i<rhsArity; i++) {
            result.setField(rhsCache[i].value(), i+1);
        }
        collector.collect(result);
    }

    @Override
    public void processElement2(Tuple y, KeyedCoProcessFunction<K, X, Tuple, Tuple>.Context context, Collector<Tuple> collector) throws Exception {
        int idx = y.getField(0);
        rhsCache[idx].update(y.getField(idx+1));

        // only one field will be non-null
//        for (int i=0; i<rhsArity; i++) {
//            if (y.getField(i) != null) {
//                rhsCache[i].update(y.getField(i));
//                break;
//            }
//        }

        X x = leftCache.value();
        if (x != null) {
            Tuple result = Tuple.newInstance(rhsArity + 1);
            result.setField(x, 0);
            for (int i = 0; i < rhsArity; i++) {
                result.setField(rhsCache[i].value(), i + 1);
            }
            collector.collect(result);
        }
    }

    public static class TaggedObject {
        private int tag;
        private Object value;

        public TaggedObject() {}

        public TaggedObject(int tag, Object value) {
            this.tag = tag;
            this.value = value;
        }
    }

    public static class TaggedObjectTypeInformation extends TypeInformation<TaggedObject> {
        private final TypeInformation[] types;

        public TaggedObjectTypeInformation(TypeInformation[] types) {
            this.types = types;
        }

        @Override
        public boolean isBasicType() {
            return false;
        }

        @Override
        public boolean isTupleType() {
            return false;
        }

        @Override
        public int getArity() {
            return 1;
        }

        @Override
        public int getTotalFields() {
            return 2;
        }

        @Override
        public Class<TaggedObject> getTypeClass() {
            return TaggedObject.class;
        }

        @Override
        public boolean isKeyType() {
            return false;
        }

        @Override
        public TypeSerializer<TaggedObject> createSerializer(ExecutionConfig executionConfig) {
            TypeSerializer[] serializers = new TypeSerializer[types.length];
            for (int i=0; i<types.length; i++) {
                serializers[i] = types[i].createSerializer(executionConfig);
            }

            return new TaggedObjectSerializer(serializers);
        }

        @Override
        public String toString() {
            return "TaggedObjectTypeInformation<" + Arrays.asList(types) + ">";
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof TaggedObjectTypeInformation)) {
                return false;
            }
            TaggedObjectTypeInformation that = (TaggedObjectTypeInformation) o;
            return Arrays.equals(this.types, that.types);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(types);
        }

        @Override
        public boolean canEqual(Object o) {
            return o instanceof TaggedObjectTypeInformation;
        }
    }

    public static class TaggedObjectSerializer extends TypeSerializer<TaggedObject> {
        private final TypeSerializer[] serializers;

        public TaggedObjectSerializer(TypeSerializer[] serializers) {
            this.serializers = serializers;
        }

        @Override
        public boolean isImmutableType() {
            return false;
        }

        @Override
        public TypeSerializer<TaggedObject> duplicate() {
            return this;
        }

        @Override
        public TaggedObject createInstance() {
            return new TaggedObject();
        }

        @Override
        public TaggedObject copy(TaggedObject taggedObject) {
            return new TaggedObject(taggedObject.tag, taggedObject.value);
        }

        @Override
        public TaggedObject copy(TaggedObject taggedObject, TaggedObject t1) {
            t1.tag = taggedObject.tag;
            t1.value = taggedObject.value;
            return t1;
        }

        @Override
        public int getLength() {
            return -1;
        }

        @Override
        public void serialize(TaggedObject taggedObject, DataOutputView dataOutputView) throws IOException {
            dataOutputView.writeByte(taggedObject.tag);
            serializers[taggedObject.tag].serialize(taggedObject.value, dataOutputView);
        }

        @Override
        public TaggedObject deserialize(DataInputView dataInputView) throws IOException {
            int tag = dataInputView.readByte();
            return new TaggedObject(tag, serializers[tag].deserialize(dataInputView));
        }

        @Override
        public TaggedObject deserialize(TaggedObject taggedObject, DataInputView dataInputView) throws IOException {
            taggedObject.tag = dataInputView.readByte();
            taggedObject.value = serializers[taggedObject.tag].deserialize(dataInputView);
            return taggedObject;
        }

        @Override
        public void copy(DataInputView dataInputView, DataOutputView dataOutputView) throws IOException {
            int tag = dataInputView.readByte();
            dataOutputView.writeByte(tag);
            serializers[tag].copy(dataInputView, dataOutputView);
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof TaggedObjectSerializer)) {
                return false;
            }
            TaggedObjectSerializer that = (TaggedObjectSerializer) o;
            return Arrays.equals(this.serializers, that.serializers);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(serializers);
        }

        @Override
        public TypeSerializerSnapshot<TaggedObject> snapshotConfiguration() {
            // TODO
            return null;
        }
    }

    public static class Builder<K, X> implements Serializable {
        private transient final DataStream<X> lhs;
        private final KeySelector<X, K> leftKey;
        private final TypeInformation<X> lhsType;
        private final TypeInformation<K> keyType;
        private transient final List<DataStream> joins;
        private final List<KeySelector> joinKeys;
        private final List<TypeInformation> rhsTypes;

        public Builder(DataStream<X> lhs, KeySelector<X, K> leftKey, TypeInformation<X> lhsType, TypeInformation<K> keyType) {
            this.lhs = lhs;
            this.leftKey = leftKey;
            this.lhsType = lhsType;
            this.keyType = keyType;
            joins = new ArrayList<>();
            joinKeys = new ArrayList<>();
            rhsTypes = new ArrayList<>();
        }

        public <Y> Builder<K, X> addJoin(DataStream<Y> join, KeySelector<Y, K> joinKey, TypeInformation<Y> type) {
            joins.add(join);
            joinKeys.add(joinKey);
            rhsTypes.add(type);
            return this;
        }

        public DataStream<Tuple> build() {
            KeyedStream<X, K> leftSide = lhs.keyBy(leftKey);

            final int size = joins.size()+1;

            List<TypeInformation> inputTypes = new ArrayList<>();
            inputTypes.add(TypeInformation.of(Integer.class));
            inputTypes.addAll(rhsTypes);

            DataStream rightSide = null;
            TypeInformation rhsTupleType = new TupleTypeInfo<>(inputTypes.toArray(new TypeInformation[inputTypes.size()]));

            for (int i = 0; i < joins.size(); i++) {
                final int j = i;
                DataStream<Tuple> newRHS = joins.get(i).map(o -> {
                    Tuple t = Tuple.newInstance(size);
                    t.setField(j, 0);
                    t.setField(o, j+1);
                    return t;
                }).returns(rhsTupleType);
                if (i==0) {
                    rightSide = newRHS;
                } else {
                    rightSide = rightSide.union(newRHS);
                }
            }

            KeyedStream rhsKeyed = rightSide.keyBy(new KeySelector<Tuple, K>() {
                @Override
                public K getKey(Tuple o) throws Exception {
                    int idx = o.getField(0);
                    return (K) joinKeys.get(idx).getKey(o.getField(idx+1));

//                    for (int i= 0; i<size; i++) {
//                        if (o.getField(i) != null) {
//                            return (K) joinKeys.get(i).getKey(o.getField(i));
//                        }
//                    }
//
//                    throw new IllegalStateException("Tuple with no element set");
                }
            }, keyType);

            TypeInformation[] returnTypes = new TypeInformation[rhsTypes.size()+1];
            returnTypes[0] = lhsType;
            for (int i=0; i<rhsTypes.size(); i++) {
                returnTypes[i+1] = rhsTypes.get(i);
            }

            return leftSide
                    .connect(rhsKeyed)
                    .process(new MultiLeftJoin<K, X>(lhsType, rhsTypes.toArray(new TypeInformation[rhsTypes.size()])))
                    .returns(new TupleTypeInfo(returnTypes));
        }
    }
}
