package org.uwh.flink.util;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class ConcatSource<T> extends RichSourceFunction<T> implements EnhancedSourceFunction<T> {
    private transient StreamExecutionEnvironment env;
    private TypeInformation<T> tinfo;
    private SourceFunction<T> first;
    private SourceFunction<T> second;

    public ConcatSource(StreamExecutionEnvironment env, SourceFunction<T> first, SourceFunction<T> second, TypeInformation<T> tinfo) {
        this.env = env;
        this.first = first;
        this.second = second;
        this.tinfo = tinfo;
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return tinfo;
    }

    @Override
    public void run(SourceContext<T> sourceContext) throws Exception {
        first.run(sourceContext);
        second.run(sourceContext);
    }

    @Override
    public void cancel() {
        first.cancel();
        second.cancel();
    }

    @Override
    public StreamExecutionEnvironment getEnv() {
        return env;
    }
}
