package org.uwh.sparta;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.uwh.IssuerRisk;
import org.uwh.RiskPosition;

public class RiskPositionJoin extends KeyedCoProcessFunction<String, IssuerRisk, RiskPosition, RowData> {
    private transient ValueState<RiskPosition> posState;
    private transient ValueState<IssuerRisk> riskState;
    private boolean emitUpsertBefore;

    public RiskPositionJoin() {
        emitUpsertBefore = true;
    }

    public RiskPositionJoin(boolean emitUpsertBefore) {
        this.emitUpsertBefore = emitUpsertBefore;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        posState = getRuntimeContext().getState(new ValueStateDescriptor<>("position", RiskPosition.class));
        riskState = getRuntimeContext().getState(new ValueStateDescriptor<>("risk", IssuerRisk.class));
    }

    @Override
    public void processElement1(IssuerRisk newRisk, Context context, Collector<RowData> collector) throws Exception {
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
    public void processElement2(RiskPosition newPos, Context context, Collector<RowData> collector) throws Exception {
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

    private void emit(Collector<RowData> collector, RiskPosition prevPos, IssuerRisk prevRisk, RiskPosition newPos, IssuerRisk newRisk) {
        GenericRowData prev = null;
        if (prevPos != null && prevRisk != null) {
            prev = join(prevPos, prevRisk);
        }

        GenericRowData current = join(newPos, newRisk);

        if (prev != null) {
            if (emitUpsertBefore) {
                prev.setRowKind(RowKind.UPDATE_BEFORE);
                collector.collect(prev);
            }

            current.setRowKind(RowKind.UPDATE_AFTER);
            collector.collect(current);
        } else {
            collector.collect(current);
        }
    }

    private GenericRowData join(RiskPosition pos, IssuerRisk risk) {
        GenericRowData res = new GenericRowData(6);
        res.setField(TwoStageDataStreamFlowBuilder.RISK_AND_POS_FIELD_UIDTYPE, StringData.fromString(pos.getUIDType().toString()));
        res.setField(TwoStageDataStreamFlowBuilder.RISK_AND_POS_FIELD_UID, StringData.fromString(pos.getUID()));
        res.setField(TwoStageDataStreamFlowBuilder.RISK_AND_POS_FIELD_FIRMACCOUNT, StringData.fromString(pos.getFirmAccountMnemonic()));
        res.setField(TwoStageDataStreamFlowBuilder.RISK_AND_POS_FIELD_SMCI, StringData.fromString(risk.getSMCI()));
        res.setField(TwoStageDataStreamFlowBuilder.RISK_AND_POS_FIELD_CR01, risk.getCR01());
        res.setField(TwoStageDataStreamFlowBuilder.RISK_AND_POS_FIELD_JTD, risk.getJTD());

        return res;
    }
}