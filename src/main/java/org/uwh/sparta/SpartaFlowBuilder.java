package org.uwh.sparta;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.uwh.flink.util.LatestSum;

import static org.apache.flink.table.api.Expressions.$;

public class SpartaFlowBuilder {
    private StreamExecutionEnvironment env;

    private DataStream<Tuple4<String,String,Double,Long>> riskSource;
    private DataStream<Tuple2<Boolean, Row>> aggregateIssuerStream;

    public SpartaFlowBuilder(StreamExecutionEnvironment env) {
        this.env = env;
    }

    public void setRiskSource(DataStream<Tuple4<String, String, Double, Long>> riskSource) {
        this.riskSource = riskSource;
    }

    public DataStream<Tuple2<Boolean, Row>> getAggregateIssuerStream() {
        return aggregateIssuerStream;
    }

    public void build() {
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        Table liveRisk = tenv.fromDataStream(
                riskSource,
                $("f0").as("issuer"),
                $("f1").as("uid"),
                $("f2").as("jtd"),
                $("f3").as("eventtime")
        );

        tenv.createTemporaryView("LiveRisk", liveRisk);

        tenv.registerFunction("latestSum", new LatestSum());
        // tenv.registerFunction("latestLong", new SQLJob.LatestLongValue());
        // tenv.registerFunction("latestDouble", new SQLJob.LatestDoubleValue());
        // tenv.registerFunction("latestString", new SQLJob.LatestStringValue());

        // Inner group by creates retraction stream in cases issuer changes
        Table issuerRisk = tenv.sqlQuery(
                "select issuer, sum(jtd) as jtd, proctime() as proctime " +
                        "from " +
                        " (select uid, latestDouble(jtd, eventtime) as jtd, latestLong(eventtime, eventtime) as eventtime, latestString(issuer, eventtime) as issuer, " +
                        " proctime() as proctime " +
                        "from LiveRisk group by uid) " +
                        "LatestLiveRisk " +
                        "group by issuer"
        );

        aggregateIssuerStream = tenv.toRetractStream(issuerRisk, new RowTypeInfo(
                Types.STRING,
                Types.DOUBLE
        ));
    }
}
