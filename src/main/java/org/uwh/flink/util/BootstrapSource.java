package org.uwh.flink.util;

import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class BootstrapSource<T> extends RichParallelSourceFunction<T> implements ResultTypeQueryable<T> {
    private final SourceFunction<T> delegate;
    private final TypeInformation<T> tinfo;
    private final long bootstrapTimeMillis;
    private transient ExecutorService exec;

    public BootstrapSource(SourceFunction<T> delegate, TypeInformation<T> tinfo, long bootstrapTimeMillis) {
        this.delegate = delegate;
        this.tinfo = tinfo;
        this.bootstrapTimeMillis = bootstrapTimeMillis;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        if (delegate instanceof RichFunction) {
            ((RichFunction) delegate).open(parameters);
        }

        exec = Executors.newSingleThreadExecutor();
    }

    @Override
    public void close() throws Exception {
        if (delegate instanceof RichFunction) {
            ((RichFunction) delegate).close();
        }
    }

    @Override
    public void run(SourceContext<T> sourceContext) throws Exception {
        exec.submit(() -> {
            try {
                Thread.sleep(bootstrapTimeMillis);
            } catch (Exception e) {}
            delegate.cancel();
        });

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
