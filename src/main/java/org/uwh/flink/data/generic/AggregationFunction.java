package org.uwh.flink.data.generic;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class AggregationFunction extends KeyedProcessFunction<Record,Record,Record> {
    private final RecordType resType;
    private final RecordType inType;
    private final Collection<Field> dimensions;
    private final Collection<Expressions.Aggregation> aggregations;
    private final long throttleMs;

    private transient ValueState<Record> lastPublished;
    private transient ValueState<Boolean> isDirty;
    private transient List<ValueState> aggStates;
    private transient List<FieldRef> inputFieldRefs;

    public AggregationFunction(Collection<Field> dimensions, Collection<Expressions.Aggregation> aggregations, RecordType inType, RecordType resType, long throttleMs) {
        this.resType = resType;
        this.inType = inType;
        this.dimensions = dimensions;
        this.aggregations = aggregations;
        this.throttleMs = throttleMs;
    }

    @Override
    public void open(Configuration parameters) {
        lastPublished = getRuntimeContext().getState(new ValueStateDescriptor<>("lastPublished", resType));
        isDirty = getRuntimeContext().getState(new ValueStateDescriptor<>("dirty", Types.BOOLEAN));
        aggStates = aggregations.stream().map(agg -> {
            ValueStateDescriptor desc = new ValueStateDescriptor(agg.getOutputField().getFullName(), agg.getOutputField().getType());
            return getRuntimeContext().getState(desc);
        }).collect(Collectors.toList());
        inputFieldRefs = aggregations.stream().map(agg -> inType.getFieldRef(agg.getInputField())).collect(Collectors.toList());
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Record> out) throws Exception {
        emitResults(ctx.getCurrentKey(), out);
    }

    private void emitResults(Record currentKey, Collector<Record> out) throws Exception {
        Record next = currentRecord(currentKey);
        Record last = lastPublished.value();

        isDirty.update(false);

        // Record has been removed
        if (next.getKind() == RowKind.DELETE) {
            if (last == null) {
                // record was never sent, do nothing
            } else {
                // retract previous record, clear state
                lastPublished.clear();
                out.collect(new Record(RowKind.DELETE, resType, last));
            }
        } else if (last == null) {
            Record publish = new Record(RowKind.INSERT, resType, next);
            lastPublished.update(publish);
            out.collect(publish);
        } else {
            // retract previous value, inject new value
            Record retract = new Record(RowKind.UPDATE_BEFORE, resType, last);
            Record upsert = new Record(RowKind.UPDATE_AFTER, resType, next);
            lastPublished.update(upsert);
            out.collect(retract);
            out.collect(upsert);
        }

    }

    private Record currentRecord(Record currentKey) throws Exception {
        Record res = new Record(resType);
        res.copyAll(currentKey, dimensions);
        int i=0;
        for (Expressions.Aggregation agg : aggregations) {
            res.set(agg.getOutputField(), aggStates.get(i).value());
            i++;
        }
        return res;
    }

    @Override
    public void processElement(Record rec, Context context, Collector<Record> collector) throws Exception {
        updateAggs(rec);

        if (throttleMs > 0) {
            Boolean dirty = isDirty.value();

            if (dirty == null || !dirty) {
                isDirty.update(true);
                context.timerService().registerProcessingTimeTimer(context.timerService().currentProcessingTime() + throttleMs);
            }
        } else {
            emitResults(context.getCurrentKey(), collector);
        }
    }

    private void updateAggs(Record next) throws Exception {
        int i=0;
        for (Expressions.Aggregation agg : aggregations) {
            ValueState aggState = aggStates.get(i);
            Object prev = aggState.value();
            Object value = next.get(inputFieldRefs.get(i));
            i++;

            boolean retract = next.getKind() == RowKind.DELETE || next.getKind() == RowKind.UPDATE_BEFORE;
            if (prev == null) {
                aggState.update(agg.init(value, retract));
            } else {
                aggState.update(agg.update(value, prev, retract));
            }
        }
    }

}
