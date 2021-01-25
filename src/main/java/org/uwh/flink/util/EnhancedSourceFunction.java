package org.uwh.flink.util;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Collection;

public interface EnhancedSourceFunction<T> extends SourceFunction<T>, ResultTypeQueryable<T> {
    default EnhancedSourceFunction<T> then(SourceFunction<T> next) {
        return new ConcatSource<>(this.getEnv(), this, next, this.getProducedType());
    }

    default EnhancedSourceFunction<T> then(Collection<T> items) throws Exception {
        TypeInformation<T> tinfo = TypeExtractor.getForObject(items.iterator().next());
        return this.then(new Sources.EnhancedFromElementsSource<>(this.getEnv(), tinfo.createSerializer(this.getEnv().getConfig()), tinfo, items));
    }

    default EnhancedSourceFunction<T> delay(long millis) {
        return this.then(new Sources.DelaySource<>(this.getEnv(), this.getProducedType(), millis));
    }

    default DataStream<T> toStream() {
        return this.getEnv().addSource(this);
    }

    public StreamExecutionEnvironment getEnv();
}
