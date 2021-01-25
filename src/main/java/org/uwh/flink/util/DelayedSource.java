package org.uwh.flink.util;

import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Source function that delays delegate source.
 * TODO there are a few remaining gaps
 * - No support for parallelization
 * - No support for checkpointing
 */
public class DelayedSource<T> extends RichSourceFunction<T> implements ResultTypeQueryable<T> {
    private SourceFunction<T> delegate;
    private TypeInformation<T> tinfo;
    private long millis;
    private static Logger logger  = LoggerFactory.getLogger(DelayedSource.class);

    public DelayedSource(SourceFunction<T> delegate, TypeInformation<T> tinfo, long millis) {
        this.delegate = delegate;
        this.tinfo = tinfo;
        this.millis = millis;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        if (delegate instanceof RichFunction) {
            ((RichFunction) delegate).open(parameters);
        }
    }

    @Override
    public void close() throws Exception {
        if (delegate instanceof RichFunction) {
            ((RichFunction) delegate).close();
        }
    }

    @Override
    public void run(SourceContext<T> sourceContext) throws Exception {
        logger.info("Starting delay");
        Thread.sleep(millis);
        logger.info("Finished delay");
        delegate.run(sourceContext);
    }

    @Override
    public void cancel() {
        delegate.cancel();
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return tinfo;
    }
}
