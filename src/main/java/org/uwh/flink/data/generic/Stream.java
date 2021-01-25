package org.uwh.flink.data.generic;

import org.apache.flink.api.common.functions.MapFunction;
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

    private Stream(DataStream<RowData> stream, RecordType type) {
        this.stream = stream;
        this.type = type;
    }

    public static<T> Stream fromDataStream(DataStream<T> stream, MapFunction<T, Record> mapper, RecordType type) {
        return new Stream(
                stream.map(it -> mapper.map(it).getRow(), type.getProducedType()),
                type
        );
    }

    public Stream select(Map<Field,Field> mapping) {
        RecordType resultType = type.mapped(mapping);

        return map(record -> {
            Record resRecord = new Record(record.getKind(), resultType);
            for (Map.Entry<Field, Field> e : mapping.entrySet()) {
                resRecord.set(e.getValue(), record.get(e.getKey()));
            }
            return resRecord;
        }, resultType);
    }

    public Stream map(MapFunction<Record,Record> function, RecordType resultType) {
        DataStream<RowData> resStream = stream.map(row -> {
            Record input = new Record(row, type);
            Record output = function.map(input);
            return output.getRow();
        }, resultType.getProducedType());

        return new Stream(resStream, resultType);
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
                            (currentLeft, currentRight, newLeft, newRight) -> {
                                Record leftRec;
                                Record rightRec;
                                RowKind kind;

                                // Is this logic correct - it depends on all inputs sending UPSERT_BEFORE & UPSERT_AFTER correctly, no?
                                if (newLeft != null) {
                                    leftRec = new Record(newLeft, type);
                                    rightRec = new Record(currentRight, right.type);
                                    kind = newLeft.getRowKind();
                                } else {
                                    leftRec = new Record(currentLeft, type);
                                    rightRec = new Record(newRight, right.type);
                                    kind = newRight.getRowKind();
                                }

                                Record output = new Record(kind, resType);

                                for (Field f : type.getFields()) {
                                    output.set(f, leftRec.get(f));
                                }
                                for (Field f : right.type.getFields()) {
                                    output.set(f, rightRec.get(f));
                                }

                                return Collections.singleton(output.getRow());
                            },
                                    type.getProducedType(),
                                    leftPrimaryKeyType,
                                    row -> leftPrimaryKey.getKey(new Record(row, type)),
                                    right.type.getProducedType()
                ), resType.getProducedType()).name("Join N-1");

        return new Stream(resStream, resType);
    }

    public<U> Stream joinOneToOne(Stream right, KeySelector<Record, U> key, TypeInformation<U> keyType, DeltaJoinFunction<Record, Record, Record> join, RecordType resType) {
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

        return new Stream(resStream, resType);
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

        return new Stream(resStream, resType);
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
}
