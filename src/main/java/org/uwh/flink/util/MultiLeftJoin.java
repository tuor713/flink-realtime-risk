package org.uwh.flink.util;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.StateTtlConfig;
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
import java.util.Optional;

public class MultiLeftJoin<K, X> extends KeyedCoProcessFunction<K, X, MultiLeftJoin.TaggedObject, Tuple> {
    private final int rhsArity;
    private final TypeInformation<X> lhsType;
    private final FilterFunction<X> lhsFilter;
    private final TypeInformation[] rhsTypes;
    private final FilterFunction[] rhsFilters;
    private transient ValueState<X> leftCache;
    private transient ValueState[] rhsCache;

    private final StateTtlConfig ttlConfig;

    private MultiLeftJoin(TypeInformation<X> lhsType, TypeInformation[] rhsTypes, FilterFunction<X> lhsFilter, FilterFunction[] rhsFilters, StateTtlConfig ttlConfig) {
        this.lhsType = lhsType;
        this.rhsArity = rhsTypes.length;
        this.rhsTypes = rhsTypes;
        this.lhsFilter = lhsFilter;
        this.rhsFilters = rhsFilters;
        this.ttlConfig = ttlConfig;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.rhsCache = new ValueState[rhsArity];

        ValueStateDescriptor<X> lhsDescriptor = new ValueStateDescriptor<>("left", lhsType);
        if (ttlConfig != null) {
            lhsDescriptor.enableTimeToLive(ttlConfig);
        }
        leftCache = getRuntimeContext().getState(lhsDescriptor);

        for (int i=0; i<rhsArity; i++) {
            ValueStateDescriptor rhsDescriptor = new ValueStateDescriptor<>("rhs" + i, rhsTypes[i]);
            if (ttlConfig != null) {
                rhsDescriptor.enableTimeToLive(ttlConfig);
            }
            rhsCache[i] = getRuntimeContext().getState(rhsDescriptor);
        }
    }

    @Override
    public void processElement1(X x, KeyedCoProcessFunction<K, X, TaggedObject, Tuple>.Context context, Collector<Tuple> collector) throws Exception {
        if (lhsFilter != null) {
            X previous = leftCache.value();
            if (previous == null || lhsFilter.filter(previous, x)) {
                leftCache.update(x);
                emitResult(x, collector);
            }
        } else {
            leftCache.update(x);
            emitResult(x, collector);
        }
    }

    @Override
    public void processElement2(TaggedObject y, KeyedCoProcessFunction<K, X, TaggedObject, Tuple>.Context context, Collector<Tuple> collector) throws Exception {
        if (rhsFilters[y.tag] != null) {
            Object prev = rhsCache[y.tag].value();
            if (prev == null || rhsFilters[y.tag].filter(prev, y.value)) {
                rhsCache[y.tag].update(y.value);

                X x = leftCache.value();
                if (x != null) {
                    emitResult(x, collector);
                }
            }
        } else {
            rhsCache[y.tag].update(y.value);

            X x = leftCache.value();
            if (x != null) {
                emitResult(x, collector);
            }
        }
    }

    private void emitResult(X x, Collector<Tuple> collector) throws Exception {
        Tuple result = Tuple.newInstance(rhsArity + 1);
        result.setField(x, 0);
        for (int i = 0; i < rhsArity; i++) {
            result.setField(rhsCache[i].value(), i + 1);
        }
        collector.collect(result);
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

    public static interface FilterFunction<X> extends Serializable {
        boolean filter(X prev, X next);
    }

    public static class RHSKey<K> implements KeySelector<TaggedObject, K> {
        private final List<KeySelector> joinKeys;

        public RHSKey(List<KeySelector> joinKeys) {
            this.joinKeys = joinKeys;
        }

        @Override
        public K getKey(TaggedObject taggedObject) throws Exception {
            return (K) joinKeys.get(taggedObject.tag).getKey(taggedObject.value);
        }
    }

    public static class Builder<K, X> implements Serializable {
        private transient final DataStream<X> lhs;
        private final KeySelector<X, K> leftKey;
        private final TypeInformation<X> lhsType;
        private final TypeInformation<K> keyType;
        private final Optional<FilterFunction<X>> lhsFilter;
        private transient final List<DataStream> joins;
        private final List<KeySelector> joinKeys;
        private final List<TypeInformation> rhsTypes;
        private final List<Optional<FilterFunction>> filters;
        private StateTtlConfig ttlConfig;

        public Builder(DataStream<X> lhs, KeySelector<X, K> leftKey, TypeInformation<X> lhsType, TypeInformation<K> keyType) {
            this(lhs, leftKey, lhsType, keyType, Optional.empty());
        }


        public Builder(DataStream<X> lhs, KeySelector<X, K> leftKey, TypeInformation<X> lhsType, TypeInformation<K> keyType, Optional<FilterFunction<X>> filter) {
            this.lhs = lhs;
            this.leftKey = leftKey;
            this.lhsType = lhsType;
            this.keyType = keyType;
            joins = new ArrayList<>();
            joinKeys = new ArrayList<>();
            rhsTypes = new ArrayList<>();
            filters = new ArrayList<>();
            lhsFilter = filter;
        }

        public <Y> Builder<K, X> addJoin(DataStream<Y> join, KeySelector<Y, K> joinKey, TypeInformation<Y> type) {
            return addJoin(join, joinKey, type, Optional.empty());
        }

        public <Y> Builder<K, X> addJoin(DataStream<Y> join, KeySelector<Y, K> joinKey, TypeInformation<Y> type, Optional<FilterFunction<Y>> filter) {
            joins.add(join);
            joinKeys.add(joinKey);
            rhsTypes.add(type);
            filters.add((Optional) filter);
            return this;
        }

        public Builder<K, X> withTtl(StateTtlConfig ttlConfig) {
            this.ttlConfig = ttlConfig;
            return this;
        }

        public DataStream<Tuple> build() {
            KeyedStream<X, K> leftSide = lhs.keyBy(leftKey);

            final int size = joins.size();

            DataStream<TaggedObject> rightSide = null;
            TypeInformation<TaggedObject> rhsSumType = new TaggedObjectTypeInformation(rhsTypes.toArray(new TypeInformation[rhsTypes.size()]));

            for (int i = 0; i < size; i++) {
                final int j = i;
                DataStream<TaggedObject> newRHS = joins.get(i).map(o -> new TaggedObject(j, o)).returns(rhsSumType);
                if (i==0) {
                    rightSide = newRHS;
                } else {
                    rightSide = rightSide.union(newRHS);
                }
            }

            KeyedStream rhsKeyed = rightSide.keyBy(new RHSKey<>(joinKeys), keyType);

            TypeInformation<?>[] returnTypes = new TypeInformation[rhsTypes.size()+1];
            returnTypes[0] = lhsType;
            for (int i=0; i<rhsTypes.size(); i++) {
                returnTypes[i+1] = rhsTypes.get(i);
            }

            return leftSide
                    .connect(rhsKeyed)
                    .process(new MultiLeftJoin<K, X>(
                            lhsType,
                            rhsTypes.toArray(new TypeInformation[rhsTypes.size()]),
                            lhsFilter.orElse(null),
                            filters.stream().map(f -> f.orElse(null)).toArray(s -> new FilterFunction[s]),
                            ttlConfig
                    ))
                    .returns(new TupleTypeInfo(returnTypes));
        }
    }
}
