package org.uwh.sparta;

import java.util.Map;

public class RiskAggregate<DIM> {
    private final DIM dimensions;
    private final double cr01;
    private final double jtd;
    private final int count;

    public RiskAggregate(DIM dimensions, double cr01, double jtd, int count) {
        this.dimensions = dimensions;
        this.cr01 = cr01;
        this.jtd = jtd;
        this.count = count;
    }

    public DIM getDimensions() {
        return dimensions;
    }

    public double getCr01() {
        return cr01;
    }

    public double getJtd() {
        return jtd;
    }

    public int getCount() {
        return count;
    }

    @Override
    public String toString() {
        return "RiskAggregate{" +
                "dimensions=" + dimensions +
                ", cr01=" + cr01 +
                ", jtd=" + jtd +
                ", count=" + count +
                '}';
    }
}
