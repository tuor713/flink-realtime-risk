package org.uwh.flink.util;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FromElementsFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Sources {
    public static<T> EnhancedSourceFunction<T> start(StreamExecutionEnvironment env, TypeInformation<T> tinfo) {
        return new EmptySource<>(env, tinfo);
    }

    private static class EmptySource<T> implements EnhancedSourceFunction<T> {
        private transient StreamExecutionEnvironment env;
        private TypeInformation<T> tinfo;

        public EmptySource(StreamExecutionEnvironment env, TypeInformation<T> tinfo) {
            this.env = env;
            this.tinfo = tinfo;
        }

        @Override
        public StreamExecutionEnvironment getEnv() {
            return env;
        }

        @Override
        public TypeInformation<T> getProducedType() {
            return tinfo;
        }

        @Override
        public void run(SourceFunction.SourceContext<T> sourceContext) throws Exception {}

        @Override
        public void cancel() {}
    }

    public static class DelaySource<T> implements EnhancedSourceFunction<T> {
        private static Logger logger = LoggerFactory.getLogger(DelaySource.class);
        private transient StreamExecutionEnvironment env;
        private TypeInformation<T> tinfo;
        private long millis;

        public DelaySource(StreamExecutionEnvironment env, TypeInformation<T> tinfo, long millis) {
            this.env = env;
            this.tinfo = tinfo;
            this.millis = millis;
        }

        @Override
        public void run(SourceFunction.SourceContext<T> sourceContext) throws Exception {
            logger.info("Starting delay");
            Thread.sleep(millis);
            logger.info("Finished delay");
        }

        @Override
        public void cancel() {}

        @Override
        public StreamExecutionEnvironment getEnv() {
            return env;
        }

        @Override
        public TypeInformation<T> getProducedType() {
            return tinfo;
        }
    }

    public static class EnhancedFromElementsSource<T> extends FromElementsFunction<T> implements EnhancedSourceFunction<T> {
        private TypeInformation<T> tinfo;
        private transient StreamExecutionEnvironment env;

        public EnhancedFromElementsSource(StreamExecutionEnvironment env, TypeSerializer<T> serializer, TypeInformation<T> tinfo, Iterable<T> items) throws Exception {
            super(serializer, items);
            this.env = env;
            this.tinfo = tinfo;
        }

        @Override
        public TypeInformation<T> getProducedType() {
            return tinfo;
        }

        @Override
        public StreamExecutionEnvironment getEnv() {
            return env;
        }
    }
}
