package org.uwh.sparta;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.uwh.IssuerRisk;
import org.uwh.RiskPosition;

public class RiskPositionJoin2 extends KeyedCoProcessFunction<String, IssuerRisk, RiskPosition, Tuple3<RowKind, IssuerRisk, RiskPosition>> implements ResultTypeQueryable<Tuple3<RowKind, IssuerRisk, RiskPosition>> {
    private transient ValueState<RiskPosition> posState;
    private transient ValueState<IssuerRisk> riskState;
    private boolean emitUpsertBefore;

    public RiskPositionJoin2() {
        emitUpsertBefore = true;
    }

    public RiskPositionJoin2(boolean emitUpsertBefore) {
        this.emitUpsertBefore = emitUpsertBefore;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        posState = getRuntimeContext().getState(new ValueStateDescriptor<>("position", RiskPosition.class));
        riskState = getRuntimeContext().getState(new ValueStateDescriptor<>("risk", IssuerRisk.class));
    }

    @Override
    public TypeInformation<Tuple3<RowKind, IssuerRisk, RiskPosition>> getProducedType() {
        return new TupleTypeInfo<>(TypeInformation.of(RowKind.class), TypeInformation.of(IssuerRisk.class), TypeInformation.of(RiskPosition.class));
    }

    @Override
    public void processElement1(IssuerRisk newRisk, Context context, Collector<Tuple3<RowKind, IssuerRisk, RiskPosition>> collector) throws Exception {
        IssuerRisk current = riskState.value();

        // Ignore out-of-order messages
        if (current != null && current.getAuditDateTimeUTC() > newRisk.getAuditDateTimeUTC()) {
            return;
        }

        riskState.update(newRisk);
        RiskPosition pos = posState.value();

        if (pos != null) {
            emit(collector, pos, current, pos, newRisk);
        }
    }

    @Override
    public void processElement2(RiskPosition newPos, Context context, Collector<Tuple3<RowKind, IssuerRisk, RiskPosition>> collector) throws Exception {
        RiskPosition current = posState.value();

        // Ignore out-of-order messages
        if (current != null && current.getAuditDateTimeUTC() > newPos.getAuditDateTimeUTC()) {
            return;
        }

        posState.update(newPos);
        IssuerRisk risk = riskState.value();

        if (risk != null) {
            emit(collector, current, risk, newPos, risk);
        }
    }

    private void emit(Collector<Tuple3<RowKind, IssuerRisk, RiskPosition>> collector, RiskPosition prevPos, IssuerRisk prevRisk, RiskPosition newPos, IssuerRisk newRisk) {
        if (prevPos != null && prevRisk != null) {
            if (emitUpsertBefore) {
                collector.collect(Tuple3.of(RowKind.UPDATE_BEFORE, prevRisk, prevPos));
            }

            collector.collect(Tuple3.of(RowKind.UPDATE_AFTER, newRisk, newPos));
        } else {
            collector.collect(Tuple3.of(RowKind.INSERT, newRisk, newPos));
        }
    }
}