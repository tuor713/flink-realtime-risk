package org.uwh.flink.util;

import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

public class DelegateSourceFunction<T> extends RichSourceFunction<T> implements SourceFunction<T>, CheckpointedFunction {
    protected SourceFunction<T> delegate;

    public static class DelegateSourceContext<T> implements SourceContext<T> {
        protected SourceContext<T> delegate;

        public DelegateSourceContext(SourceContext<T> delegate) {
            this.delegate = delegate;
        }

        @Override
        public void collect(T t) {
            delegate.collect(t);
        }

        @Override
        public void collectWithTimestamp(T t, long l) {
            delegate.collectWithTimestamp(t, l);
        }

        @Override
        public void emitWatermark(Watermark watermark) {
            delegate.emitWatermark(watermark);
        }

        @Override
        public void markAsTemporarilyIdle() {
            delegate.markAsTemporarilyIdle();
        }

        @Override
        public Object getCheckpointLock() {
            return delegate.getCheckpointLock();
        }

        @Override
        public void close() {
            delegate.close();
        }
    }

    public DelegateSourceFunction(SourceFunction<T> delegate) {
        this.delegate = delegate;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        // TODO
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
        // TODO
    }

    @Override
    public void run(SourceContext<T> sourceContext) throws Exception {
        delegate.run(sourceContext);
    }

    @Override
    public void cancel() {
        delegate.cancel();
    }
}
