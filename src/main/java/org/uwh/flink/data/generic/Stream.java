package org.uwh.flink.data.generic;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.data.RowData;
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
    private transient final DataStream<RowData> stream;
    private final RecordType type;
    private final ChangeLogMode changeLogMode;

    public enum ChangeLogMode {
        // only Insert records
        APPEND,
        // Insert, Upsert_before, Upsert_after and Delete
        // Upsert_after and Delete
        CHANGE_LOG,
        KEYED_CHANGE_LOG
    }

    private Stream(DataStream<RowData> stream, RecordType type, ChangeLogMode mode) {
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

                    return rec.getRow();
                }, type.getProducedType()),
                type,
                ChangeLogMode.APPEND
        );
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
                stream.map(row -> {
                    Record cur  = new Record(row, type);
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

                    return res.getRow();
                }, resultType.getProducedType()),
                resultType,
                changeLogMode
        );
    }

    public<T> Stream joinManyToOne(Stream right, Field key, Field<T> leftPrimaryKey) {
        return joinManyToOne(right, key, rec -> rec.get(leftPrimaryKey), leftPrimaryKey.getType());
    }

    public<T> Stream joinManyToOne(Stream right, Field key, KeySelector<Record, T> leftPrimaryKey, TypeInformation<T> leftPrimaryKeyType) {
        return joinManyToOne(right, rec -> rec.get(key), key.getType(), leftPrimaryKey, leftPrimaryKeyType);
    }

    public<U,V> Stream joinManyToOne(Stream right, KeySelector<Record, U> key, TypeInformation<U> keyType, KeySelector<Record, V> leftPrimaryKey, TypeInformation<V> leftPrimaryKeyType) {
        RecordType resType = type.join(right.type);
        DataStream<RowData> resStream = stream.keyBy(row -> key.getKey(new Record(row, type)), keyType)
                .connect(right.stream.keyBy(row -> key.getKey(new Record(row, right.type)), keyType))
                .process(new ManyToOneJoin<>(
                            (currentLeft, currentRight, newLeft, newRight) -> defaultJoin(currentLeft, currentRight, newLeft, newRight, type, right.type, resType),
                                    type.getProducedType(),
                                    leftPrimaryKeyType,
                                    row -> leftPrimaryKey.getKey(new Record(row, type)),
                                    right.type.getProducedType()
                ), resType.getProducedType()).name("Join N-1");

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

    private Collection<RowData> defaultJoin(RowData curLeft, RowData curRight, RowData newLeft, RowData newRight, RecordType leftType, RecordType rightType, RecordType resType) {
        Collection<Record> res = defaultJoin(
                (curLeft != null) ? new Record(curLeft, leftType) : null,
                (curRight != null) ? new Record(curRight, rightType) : null,
                (newLeft != null) ? new Record(newLeft, leftType) : null,
                (newRight != null) ? new Record(newRight, rightType) : null,
                resType
        );

        return res.stream().map(Record::getRow).collect(Collectors.toList());
    }

    public<T> Stream joinOneToOne(Stream right, Field<T> key) {
        RecordType resType = type.join(right.type);
        return joinOneToOne(
                right,
                rec -> rec.get(key),
                key.getType(),
                (curLeft, curRight, newLeft, newRight) -> defaultJoin(curLeft, curRight, newLeft, newRight, resType),
                resType,
                changeLogMode);
    }

    public<U> Stream joinOneToOne(Stream right, KeySelector<Record, U> key, TypeInformation<U> keyType, DeltaJoinFunction<Record, Record, Record> join, RecordType resType, ChangeLogMode mode) {
        DataStream<RowData> resStream = stream.keyBy(row -> key.getKey(new Record(row, type)), keyType)
                .connect(right.stream.keyBy(row -> key.getKey(new Record(row, right.type)), keyType))
                .process(new OneToOneJoin<>(
                        (curLeft, curRight, newLeft, newRight) -> {
                            Collection<Record> res = join.join(
                                    (curLeft != null) ? new Record(curLeft, type) : null,
                                    (curRight != null) ? new Record(curRight, right.type) : null,
                                    (newLeft != null) ? new Record(newLeft, type) : null,
                                    (newRight != null) ? new Record(newRight, right.type) : null
                            );
                            return res.stream().map(Record::getRow).collect(Collectors.toList());
                        },
                        type.getProducedType(),
                        right.type.getProducedType()
                ), resType.getProducedType()).name("Join 1-1");

        return new Stream(resStream, resType, mode);
    }

    public Stream aggregate(Collection<Field> dimensions, Collection<Field<Double>> aggregations, long throttleMs) {
        List<Field> resFields = new ArrayList<>(dimensions);
        resFields.addAll(aggregations);
        RecordType dimType = new RecordType(type.getConfig(), dimensions);
        RecordType resType = new RecordType(type.getConfig(), resFields);

        DataStream<RowData> resStream = stream.keyBy(row -> new Record(RowKind.INSERT, dimType, new Record(row, type)).getRow(), dimType.getProducedType())
                .process(new KeyedProcessFunction<>() {
                    private transient ValueState<RowData> pending;
                    private transient ValueState<RowData> lastPublished;

                    @Override
                    public void open(Configuration parameters) {
                        pending = getRuntimeContext().getState(new ValueStateDescriptor<>("pending", resType.getProducedType()));
                        lastPublished = getRuntimeContext().getState(new ValueStateDescriptor<>("lastPublished", resType.getProducedType()));
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<RowData> out) throws Exception {
                        Record next = new Record(pending.value(), resType);
                        RowData lastRow = lastPublished.value();
                        Record last = (lastRow != null) ? new Record(lastPublished.value(), resType) : null;

                        pending.clear();

                        // Record has been removed
                        if (next.getKind() == RowKind.DELETE) {
                            if (last == null) {
                                // record was never sent, do nothing
                            } else {
                                // retract previous record, clear state
                                lastPublished.clear();
                                out.collect(new Record(RowKind.DELETE, resType, last).getRow());
                            }
                        } else if (last == null) {
                            Record publish = new Record(RowKind.INSERT, resType, next);
                            lastPublished.update(publish.getRow());
                            out.collect(publish.getRow());
                        } else {
                            // retract previous value, inject new value
                            Record retract = new Record(RowKind.UPDATE_BEFORE, resType, last);
                            Record upsert = new Record(RowKind.UPDATE_AFTER, resType, next);
                            lastPublished.update(upsert.getRow());
                            out.collect(retract.getRow());
                            out.collect(upsert.getRow());
                        }
                    }

                    @Override
                    public void processElement(RowData rowData, Context context, Collector<RowData> collector) throws Exception {
                        RowData last = pending.value();
                        if (last == null) {
                            last = lastPublished.value();
                            context.timerService().registerProcessingTimeTimer(context.timerService().currentProcessingTime()+throttleMs);
                        }

                        Record res = update(last != null ? new Record(last, resType) : null, new Record(rowData, type));
                        pending.update(res.getRow());
                    }

                    private Record update(Record current, Record next) {
                        Record res = new Record(next.getKind(), resType);
                        for (Field dim : dimensions) {
                            res.set(dim, next.get(dim));
                        }

                        for (Field<Double> agg : aggregations) {
                            double value = next.get(agg);
                            if (next.getKind() == RowKind.DELETE || next.getKind() == RowKind.UPDATE_BEFORE) {
                                value = -value;
                            }
                            res.set(agg, ((current != null) ? current.get(agg) : 0) + value);
                        }

                        return res;
                    }
                }, resType.getProducedType()).name("Aggregate");

        return new Stream(resStream, resType, ChangeLogMode.CHANGE_LOG);
    }

    public void print(String label) {
        stream.map(row -> new Record(row, type).toString()).print(label);
    }

    public void log(String label, long interval) {
        stream.map(row -> new Record(row ,type).toString()).addSink(new LogSink<>(label, interval)).disableChaining().name("LogSink "+label);
    }

    public RecordType getRecordType() {
        return type;
    }

    public DataStream<RowData> getDataStream() {
        return stream;
    }

    public ChangeLogMode getMode() {
        return changeLogMode;
    }
}
