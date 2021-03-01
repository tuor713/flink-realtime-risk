package org.uwh.flink.data.generic;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.uwh.flink.util.DeltaJoinFunction;
import org.uwh.flink.util.LogSink;
import org.uwh.flink.util.ManyToOneJoin;
import org.uwh.flink.util.OneToOneJoin;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

public class Stream implements Serializable {
    private transient final DataStream<Record> stream;
    private final RecordType type;
    private final ChangeLogMode changeLogMode;

    private static final Field<Integer> F_GROUPING_SET_ID = new Field<>("_internal", "grouping-set-id", Types.INT);

    public enum ChangeLogMode {
        // only Insert records
        APPEND,
        // Insert, Upsert_before, Upsert_after and Delete
        CHANGE_LOG,
        // Upsert_after and Delete
        KEYED_CHANGE_LOG
    }

    public Stream(DataStream<Record> stream, RecordType type, ChangeLogMode mode) {
        this.stream = stream;
        this.type = type;
        changeLogMode = mode;
    }

    public static<T> Stream fromDataStream(DataStream<T> stream, Expressions.FieldConverter<T,?>... converters) {
        RecordType type = new RecordType(stream.getExecutionConfig(), Arrays.stream(converters).map(Expressions.FieldConverter::getField).collect(Collectors.toList()));

        return new Stream(
                stream.map(obj -> {
                    Record rec = new Record(RowKind.INSERT, type);

                    for (Expressions.FieldConverter<T,Object> conv : (Expressions.FieldConverter<T, Object>[]) converters) {
                        rec.set(conv.getField(), conv.map(obj));
                    }

                    return rec;
                }, type).name("Transform to Stream"),
                type,
                ChangeLogMode.APPEND
        );
    }

    public Stream toAppendMode() {
        if (changeLogMode == ChangeLogMode.APPEND) {
            return this;
        }

        return new Stream(
                stream.filter(rec -> rec.getKind() == RowKind.INSERT || rec.getKind() == RowKind.UPDATE_AFTER)
                        .map(rec -> new Record(RowKind.INSERT, rec)),
                type,
                ChangeLogMode.APPEND
        );
    }

    public Stream toKeyedChangeLogMode() {
        if (changeLogMode == ChangeLogMode.KEYED_CHANGE_LOG) {
            return this;
        }

        return new Stream(
                stream.filter(rec -> rec.getKind() != RowKind.UPDATE_BEFORE).map(rec -> {
                    if (rec.getKind() == RowKind.INSERT) {
                        return new Record(RowKind.UPDATE_AFTER, rec);
                    } else {
                        return rec;
                    }
                }),
                type,
                ChangeLogMode.KEYED_CHANGE_LOG
        );
    }

    public<T> Stream toChangeLogMode(KeySelector<Record,T> primaryKey) {
        if (changeLogMode == ChangeLogMode.CHANGE_LOG) {
            return this;
        }

        DataStream<Record> resStream = stream.keyBy(primaryKey)
                .process(new KeyedProcessFunction<>() {
                    private transient ValueState<Record> latestState;

                    @Override
                    public void open(Configuration parameters) {
                        latestState = getRuntimeContext().getState(new ValueStateDescriptor<>("latest", type));
                    }

                    @Override
                    public void processElement(Record rec, Context context, Collector<Record> collector) throws Exception {
                        Record previous = latestState.value();
                        latestState.update(rec);

                        if (previous == null) {
                            if (rec.getKind() != RowKind.DELETE) {
                                collector.collect(new Record(RowKind.INSERT, rec));
                            }
                        } else if (rec.getKind() == RowKind.DELETE) {
                            collector.collect(rec);
                        } else {
                            collector.collect(new Record(RowKind.UPDATE_BEFORE, previous));
                            collector.collect(new Record(RowKind.UPDATE_AFTER, rec));
                        }
                    }
                });

        return new Stream(resStream, type, ChangeLogMode.CHANGE_LOG);
    }

    private String createSelectName(Expression[] expressions) {
        return "SELECT " + String.join(", ", Arrays.stream(expressions).map(Object::toString).collect(Collectors.toList()));
    }

    public Stream select(Expression... expressions) {
        RecordType resultType = new RecordType(type.getConfig(), Arrays.stream(expressions).flatMap(
                exp -> {
                    if (exp.isStar()) {
                        return type.getFields().stream();
                    } else {
                        return java.util.stream.Stream.of(exp.getResultField());
                    }
                }
        ).collect(Collectors.toList()));

        return new Stream(
                stream.map(cur -> {
                    Record res = new Record(cur.getKind(), resultType);

                    for (Expression exp : expressions) {
                        if (exp.isStar()) {
                            for (Field f : type.getFields()) {
                                res.set(f, cur.get(f));
                            }
                        } else {
                            res.set(exp.getResultField(), exp.map(cur));
                        }
                    }

                    return res;
                }, resultType).name(createSelectName(expressions)),
                resultType,
                changeLogMode
        );
    }

    public<T> Stream joinManyToOne(Stream right, Field key, Field<T> leftPrimaryKey) {
        return joinManyToOne(right, key, rec -> rec.get(leftPrimaryKey), leftPrimaryKey.getType()).name("JOIN[N-1] ON "+key);
    }

    public<T> Stream joinManyToOne(Stream right, Field key, KeySelector<Record, T> leftPrimaryKey, TypeInformation<T> leftPrimaryKeyType) {
        return joinManyToOne(right, rec -> rec.get(key), key.getType(), leftPrimaryKey, leftPrimaryKeyType).name("JOIN[N-1] ON "+key);
    }

    public<U,V> Stream joinManyToOne(Stream right, KeySelector<Record, U> key, TypeInformation<U> keyType, KeySelector<Record, V> leftPrimaryKey, TypeInformation<V> leftPrimaryKeyType) {
        RecordType resType = type.join(right.type);
        DataStream<Record> resStream = stream.keyBy(key, keyType)
                .connect(right.stream.keyBy(key, keyType))
                .process(new ManyToOneJoin<>(
                            (currentLeft, currentRight, newLeft, newRight) -> defaultJoin(currentLeft, currentRight, newLeft, newRight, resType),
                                    type,
                                    leftPrimaryKeyType,
                                    leftPrimaryKey,
                                    right.type
                ), resType).name("JOIN[N-1]");

        return new Stream(resStream, resType, changeLogMode);
    }

    private Collection<Record> defaultJoin(Record curLeft, Record curRight, Record newLeft, Record newRight, RecordType resType) {
        Record leftRec;
        Record rightRec;
        RowKind kind;

        // Is this logic correct - it depends on all inputs sending UPSERT_BEFORE & UPSERT_AFTER correctly, no?
        if (newLeft != null) {
            leftRec = newLeft;
            rightRec = curRight;
            kind = newLeft.getKind();
        } else {
            leftRec = curLeft;
            rightRec = newRight;
            kind = newRight.getKind();
        }

        Record output = new Record(kind, resType);

        for (Field f : leftRec.getType().getFields()) {
            output.set(f, leftRec.get(f));
        }
        for (Field f : rightRec.getType().getFields()) {
            output.set(f, rightRec.get(f));
        }

        return Collections.singleton(output);
    }

    public<T> Stream joinOneToOne(Stream right, Field<T> key) {
        RecordType resType = type.join(right.type);
        return joinOneToOne(
                right,
                rec -> rec.get(key),
                key.getType(),
                (curLeft, curRight, newLeft, newRight) -> defaultJoin(curLeft, curRight, newLeft, newRight, resType),
                resType,
                changeLogMode).name("JOIN[1-1] ON " + key);
    }

    public<U> Stream joinOneToOne(Stream right, KeySelector<Record, U> key, TypeInformation<U> keyType, DeltaJoinFunction<Record, Record, Record> join, RecordType resType, ChangeLogMode mode) {
        DataStream<Record> resStream = stream.keyBy(key, keyType)
                .connect(right.stream.keyBy(key, keyType))
                .process(new OneToOneJoin<>(join, type, right.type), resType).name("JOIN[1-1]");

        return new Stream(resStream, resType, mode);
    }

    public Stream aggregateFields(Collection<Field> dimensions, Collection<Field<Double>> aggregations, long throttleMs) {
        return aggregate(dimensions, aggregations.stream().map(f -> Expressions.sum(f)).collect(Collectors.toList()), throttleMs);
    }

    public Stream aggregate(Collection<Field> dimensions, Collection<Expressions.Aggregation> aggregations, long throttleMs) {
        List<Field> resFields = new ArrayList<>(dimensions);
        aggregations.forEach(agg -> resFields.add(agg.getOutputField()));

        RecordType dimType = new RecordType(type.getConfig(), dimensions);
        RecordType resType = new RecordType(type.getConfig(), resFields);

        DataStream<Record> resStream = stream.keyBy(rec -> new Record(RowKind.INSERT, dimType, rec), dimType)
                .process(new AggregationFunction(dimensions, aggregations, type, resType, throttleMs), resType)
                .name("AGGREGATE "
                        + String.join(", ", aggregations.stream().map(f -> f.toString()).collect(Collectors.toList()))
                        + " ON " + String.join(", ", dimensions.stream().map(f -> f.toString()).collect(Collectors.toList())));

        return new Stream(resStream, resType, ChangeLogMode.CHANGE_LOG);
    }

    public Stream rollup(List<Set<Field>> groupingSets, Collection<Expressions.Aggregation> aggregations) {
        /*
        Strategy similar to Blink implementation:
        1. Add virtual grouping_set_id field
        2. Duplicate records once for each grouping set
        3. Key by and aggregation by grouping values _and_ grouping_set_id
         */

        Set<Field> allDimensions = groupingSets.stream().flatMap(Collection::stream).collect(Collectors.toSet());

        List<Field> resFields = new ArrayList<>(allDimensions);
        aggregations.forEach(agg -> resFields.add(agg.getOutputField()));

        Set<Field> aggInFields = aggregations.stream().map(agg -> agg.getInputField()).collect(Collectors.toSet());

        Set<Field> inputFields = new HashSet<>(allDimensions);
        inputFields.add(F_GROUPING_SET_ID);
        inputFields.addAll(aggInFields);

        Set<Field> nullables = new HashSet<>(type.getNullable());
        nullables.addAll(allDimensions);
        RecordType inputType = new RecordType(type.getConfig(), inputFields, nullables);
        RecordType dimType = new RecordType(type.getConfig(), allDimensions, new HashSet<>(allDimensions));
        RecordType dimAndGroupIdType = dimType.withFields(F_GROUPING_SET_ID);
        RecordType resType = new RecordType(type.getConfig(), resFields, new HashSet<>(allDimensions));

        DataStream<Record> resStream = stream
                .flatMap(new FlatMapFunction<>() {
                    @Override
                    public void flatMap(Record record, Collector<Record> collector) {
                        for (int i = 0; i < groupingSets.size(); i++) {
                            Record res = new Record(record.getKind(), inputType);
                            // copy only fields needed - inputs to aggregation & dimensions of the current grouping set
                            res.copyAll(record, aggInFields);
                            res.copyAll(record, groupingSets.get(i));
                            res.set(F_GROUPING_SET_ID, i);
                            collector.collect(res);
                        }
                    }
                }, inputType)
                .keyBy(rec -> new Record(RowKind.INSERT, dimAndGroupIdType, rec), dimAndGroupIdType)
                // note the dimensions used for output do *not* include the grouping set id
                .process(new AggregationFunction(allDimensions, aggregations, inputType, resType, -1), resType);

        return new Stream(resStream, resType, ChangeLogMode.CHANGE_LOG);
    }

    public Stream name(String name) {
        if (!(stream instanceof SingleOutputStreamOperator)) {
            throw new IllegalStateException("Cannot set name on " + stream);
        }

        ((SingleOutputStreamOperator<?>) stream).name(name);

        return this;
    }

    public void print(String label) {
        stream.map(Record::toString).print(label);
    }

    public void log(String label, long interval) {
        stream.addSink(new LogSink<>(label, interval)).name("LogSink "+label);
    }

    public RecordType getRecordType() {
        return type;
    }

    public DataStream<Record> getDataStream() {
        return stream;
    }

    public ChangeLogMode getMode() {
        return changeLogMode;
    }
}
