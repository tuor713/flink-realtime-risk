package org.uwh.risk;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.Test;
import org.uwh.flink.util.LogSink;

import java.util.List;

import static org.apache.flink.table.api.Expressions.$;

public class CubeExperiment {

    @Test
    public void testCube() throws Exception {
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(4);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        DataStream<Exposure> exposures = env.fromCollection(SAMPLE_DATA);
        Table tExposure = tenv.fromDataStream(exposures,
                $("business"),
                $("desk"),
                $("product"),
                $("issuer"),
                $("cr01"),
                $("jtd"));

        // CUBE (business, desk, product)                                       => 32 records
        // ROLLUP (business, desk, product)                                     => 17 records
        // GROUPING SETS ((business), (business, desk), (business, product))    => 13 records

        // Blink implementation is two-stage process:
        // 1) Duplicate records according to the various grouping sets e.g.
        //    - Cube: 8 grouping sets, so 8*3 = 24 records
        //    - Rollup: 4 grouping sets, so 4*3 = 12 records
        // 2) Run aggregation for each grouping set
        // Essentially no savings compared to hand coding as group bys + union

        Table res = tenv.sqlQuery("SELECT business, desk, product, sum(cr01) FROM " + tExposure + " GROUP BY ROLLUP (business, desk, product)");
        tenv.toRetractStream(res, RowData.class).print("RES");

        Generators.issuerRisk(env,1000,1000).addSink(new LogSink<>("RISK", 10_000_000));

        env.execute();
    }

    private static List<Exposure> SAMPLE_DATA = List.of(
            new Exposure("FLOW", "NAM.IG", "CDS", "IBM", 1_000.0, 1_000_000.0),
            new Exposure("FLOW", "NAM.IG", "BOND", "IBM", 500.0, 400_000.0),
            new Exposure("FLOW", "EMEA.IG", "BOND", "BMW", 200.0, 300_000.0)
    );

    public static class Exposure {
        private String business;
        private String desk;
        private String product;
        private String issuer;
        private double cr01;
        private double jtd;

        public Exposure() {}

        public Exposure(String business, String desk, String product, String issuer, double cr01, double jtd) {
            this.business = business;
            this.desk = desk;
            this.product = product;
            this.issuer = issuer;
            this.cr01 = cr01;
            this.jtd = jtd;
        }

        public void setBusiness(String business) {
            this.business = business;
        }

        public void setDesk(String desk) {
            this.desk = desk;
        }

        public void setProduct(String product) {
            this.product = product;
        }

        public void setIssuer(String issuer) {
            this.issuer = issuer;
        }

        public void setCr01(double cr01) {
            this.cr01 = cr01;
        }

        public void setJtd(double jtd) {
            this.jtd = jtd;
        }

        public String getBusiness() {
            return business;
        }

        public String getDesk() {
            return desk;
        }

        public String getProduct() {
            return product;
        }

        public String getIssuer() {
            return issuer;
        }

        public double getCr01() {
            return cr01;
        }

        public double getJtd() {
            return jtd;
        }
    }

}
