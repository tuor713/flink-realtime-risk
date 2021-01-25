package org.uwh.flink.util;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogSink<T> extends RichSinkFunction<T> {
    private static final Logger logger = LoggerFactory.getLogger(LogSink.class);
    private String label;
    private long interval;
    private transient long count = 0;
    private transient long firstRecord = -1;

    public LogSink(String label, long interval) {
        this.interval = interval;
        this.label = label;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        firstRecord = -1;
    }

    @Override
    public void invoke(T value) throws Exception {
        if (firstRecord == -1) {
            firstRecord = System.currentTimeMillis();
        }

        count++;
        if (count % interval == 0) {
            double tps = ((double) 1000 * count) / (System.currentTimeMillis()-firstRecord);
            logger.info(label + "[" + getRuntimeContext().getIndexOfThisSubtask() + "] TPS: " + String.format("%,.0f", tps) + " => " + value);
        }
    }
}
