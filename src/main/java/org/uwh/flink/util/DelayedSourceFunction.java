package org.uwh.flink.util;

import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Source function that delays delegate source.
 */
public class DelayedSourceFunction<T> extends RichParallelSourceFunction<T> implements ResultTypeQueryable<T>, CheckpointedFunction {
    private final SourceFunction<T> delegate;
    private final TypeInformation<T> tinfo;
    private final long millis;
    private static final Logger logger = LoggerFactory.getLogger(DelayedSourceFunction.class);

    public static<T> DataStreamSource<T> delay(StreamExecutionEnvironment env, SourceFunction<T> source, TypeInformation<T> tinfo, long millis) {
        DataStreamSource<T> res = env.addSource(new DelayedSourceFunction<>(source, tinfo, millis));
        if (!(source instanceof ParallelSourceFunction)) {
            res.setParallelism(1);
        }
        return res;
    }

    public static<T,U extends SourceFunction<T> & ResultTypeQueryable<T>> DataStreamSource<T> delay(StreamExecutionEnvironment env, U source, long millis) {
        DataStreamSource<T> res = env.addSource(new DelayedSourceFunction<>(source, source.getProducedType(), millis));
        if (!(source instanceof ParallelSourceFunction)) {
            res.setParallelism(1);
        }
        return res;
    }


    public DelayedSourceFunction(SourceFunction<T> delegate, TypeInformation<T> tinfo, long millis) {
        this.delegate = delegate;
        this.tinfo = tinfo;
        this.millis = millis;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        if (!(delegate instanceof ParallelSourceFunction) && getRuntimeContext().getNumberOfParallelSubtasks() > 1) {
            throw new IllegalStateException("Delaying a non-parallel source with parallelism > 1");
        }

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
    public void setRuntimeContext(RuntimeContext t) {
        super.setRuntimeContext(t);
        if (delegate instanceof RichFunction) {
            ((RichFunction) delegate).setRuntimeContext(t);
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        if (delegate instanceof CheckpointedFunction) {
            ((CheckpointedFunction) delegate).snapshotState(context);
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        if (delegate instanceof CheckpointedFunction) {
            ((CheckpointedFunction) delegate).initializeState(context);
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
