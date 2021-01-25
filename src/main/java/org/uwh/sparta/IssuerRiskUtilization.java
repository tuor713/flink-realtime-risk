package org.uwh.sparta;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.uwh.*;

import java.util.HashMap;
import java.util.Map;

public class IssuerRiskUtilization implements MapFunction<Tuple2<RiskAggregate<String>, RiskThreshold>, RiskThresholdUtilization> {
    @Override
    public RiskThresholdUtilization map(Tuple2<RiskAggregate<String>, RiskThreshold> input) throws Exception {
        RiskAggregate<String> agg = input.f0;
        RiskThreshold limit = input.f1;

        Map<String, Utilization> utilizations = new HashMap<>();
        double threshold = limit.getThresholds().get(RiskMeasure.CR01.name());
        utilizations.put(RiskMeasure.CR01.name(), new Utilization(threshold, agg.getCr01(), 100*(agg.getCr01()/threshold)));
        threshold = limit.getThresholds().get(RiskMeasure.JTD.name());
        utilizations.put(RiskMeasure.JTD.name(), new Utilization(threshold, agg.getJtd(), 100*(agg.getJtd()/threshold)));

        return new RiskThresholdUtilization(RiskFactorType.Issuer, agg.getDimensions(), utilizations);
    }
}
