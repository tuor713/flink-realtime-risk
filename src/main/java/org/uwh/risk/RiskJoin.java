package org.uwh.risk;

import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.types.Either;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.uwh.flink.data.generic.Field;
import org.uwh.flink.data.generic.Record;
import org.uwh.flink.data.generic.RecordType;

import java.util.*;

import static org.uwh.risk.Fields.*;

/*
Heart piece of the risk streaming join
- Joins issuer risk batch with risk position & issuer data
- Then decomposes issuer risk batch into issuer risk
 */
public class RiskJoin extends KeyedBroadcastProcessFunction<String, Either<Record,Record>, Record, Record> implements ResultTypeQueryable<Record> {
    private ValueState<Tuple2<Record,Record>> current;
    private final RecordType riskType;
    private final Field<List<Record>> fieldRiskLines;
    private final RecordType posType;
    private final RecordType resType;
    private final MapStateDescriptor<String,Record> broadcastStateDescriptor;

    private final RecordType.Copier issuerCopier;
    private final RecordType.Copier joinCopier;
    private final RecordType.Copier positionCopier;
    private final RecordType.Copier riskCopier;

    public RiskJoin(RecordType riskType, Field<List<Record>> fieldRiskLines, RecordType riskLineType, RecordType posType, RecordType issuerType) {
        this.riskType = riskType;
        this.fieldRiskLines = fieldRiskLines;
        this.posType = posType;
        broadcastStateDescriptor = new MapStateDescriptor<>("broadcast", Types.STRING, issuerType);

        // cannot use riskType because that is the batch level risk
        resType = new RecordType(riskType.getConfig(), F_RISK_ISSUER_JTD, F_RISK_ISSUER_CR01, F_RISK_ISSUER_JTD_ROLLDOWN).join(posType).join(issuerType);

        issuerCopier = new RecordType.Copier(issuerType, resType);
        joinCopier = new RecordType.Copier(
                resType,
                resType,
                new RecordType(riskType.getConfig(), F_ISSUER_ID, F_RISK_ISSUER_JTD, F_RISK_ISSUER_CR01, F_RISK_ISSUER_JTD_ROLLDOWN).join(posType).getFields());
        positionCopier = new RecordType.Copier(posType, resType);
        riskCopier = new RecordType.Copier(riskLineType, resType, List.of(F_ISSUER_ID, F_RISK_ISSUER_JTD, F_RISK_ISSUER_CR01, F_RISK_ISSUER_JTD_ROLLDOWN));
    }

    public MapStateDescriptor<String,Record> getMapStateDescriptor() {
        return broadcastStateDescriptor;
    }

    @Override
    public RecordType getProducedType() {
        return resType;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        current = getRuntimeContext().getState(new ValueStateDescriptor<>("current", new TupleTypeInfo<>(riskType, posType)));
    }

    @Override
    public void processElement(Either<Record,Record> input, ReadOnlyContext readOnlyContext, Collector<Record> collector) throws Exception {
        Collection<Record> recs = Collections.emptyList();

        if (input.isLeft()) {
            // left => IssuerRisk
            Tuple2<Record,Record> cur = current.value();
            if (cur != null && cur.f1 != null) {
                recs = joinRiskAndPosition(resType, cur.f0, cur.f1, input.left(), null);
            }

            current.update(Tuple2.of(input.left(), cur != null ? cur.f1 : null));
        } else {
            // right => RiskPosition
            Tuple2<Record,Record> cur = current.value();
            if (cur != null && cur.f0 != null) {
                recs = joinRiskAndPosition(resType, cur.f0, cur.f1, null, input.right());
            }

            current.update(Tuple2.of(cur != null ? cur.f0 : null, input.right()));
        }

        ReadOnlyBroadcastState<String, Record> state = readOnlyContext.getBroadcastState(broadcastStateDescriptor);
        for (Record out : recs) {
            Record res = issuerJoin(out, state);
            if (res != null) {
                collector.collect(res);
            }
        }
    }

    @Override
    public void processBroadcastElement(Record record, Context context, Collector<Record> collector) throws Exception {
        BroadcastState<String, Record> state = context.getBroadcastState(broadcastStateDescriptor);
        state.put(record.get(F_ISSUER_ID), record);

        context.applyToKeyedState(new ValueStateDescriptor<Tuple2<Record,Record>>("current", new TupleTypeInfo<>(riskType, posType)),
                (key, keyState) -> {
                    Tuple2<Record,Record> cur = keyState.value();
                    if (cur.f0 != null && cur.f1 != null) {
                        for (Record risk : cur.f0.get(fieldRiskLines)) {
                            if (risk.get(F_ISSUER_ID).equals(record.get(F_ISSUER_ID))) {
                                Record res = joinRecord(record.getKind(), resType, risk, cur.f1);
                                res.copyInto(record);
                                collector.collect(res);
                            }
                        }
                    }
                });
    }

    private Record issuerJoin(Record riskAndPos, ReadOnlyBroadcastState<String, Record> state) throws Exception {
        String smci = riskAndPos.get(F_ISSUER_ID);
        Record issuer = state.get(smci);
        if (issuer != null) {
            Record res = new Record(riskAndPos.getKind(), resType);
            joinCopier.copy(riskAndPos, res);
            issuerCopier.copy(issuer, res);
            return res;
        } else {
            return null;
        }
    }

    private Collection<Record> joinRiskAndPosition(RecordType joinType, Record curRisk, Record curPosition, Record newRisk, Record newPosition) {
        List<Record> res = new ArrayList<>();
        if (newPosition != null) {
            // Position update
            for (Record risk : curRisk.get(fieldRiskLines)) {
                if (curPosition != null) {
                    res.add(joinRecord(RowKind.UPDATE_BEFORE, joinType, risk, curPosition));
                    res.add(joinRecord(RowKind.UPDATE_AFTER, joinType, risk, newPosition));
                } else {
                    res.add(joinRecord(RowKind.INSERT, joinType, risk, newPosition));
                }
            }
        } else {
            // Risk update
            Set<String> newIssuers = new HashSet<>();

            List<Record> risks = newRisk.get(fieldRiskLines);
            risks.forEach(risk -> newIssuers.add(risk.get(F_ISSUER_ID)));

            Set<String> oldIssuers = new HashSet<>();
            if (curRisk != null) {
                for (Record risk : curRisk.get(fieldRiskLines)) {
                    oldIssuers.add(risk.get(F_ISSUER_ID));
                    res.add(joinRecord(
                            newIssuers.contains(risk.get(F_ISSUER_ID)) ? RowKind.UPDATE_BEFORE : RowKind.DELETE,
                            joinType,
                            risk,
                            curPosition
                    ));
                }
            }

            for (Record risk : risks) {
                res.add(joinRecord(
                        oldIssuers.contains(risk.get(F_ISSUER_ID)) ? RowKind.UPDATE_AFTER : RowKind.INSERT,
                        joinType,
                        risk,
                        curPosition
                ));
            }
        }

        return res;
    }

    private Record joinRecord(RowKind kind, RecordType joinType, Record risk, Record position) {
        Record res = new Record(kind, joinType);
        positionCopier.copy(position, res);
        riskCopier.copy(risk, res);
        return res;
    }
}
