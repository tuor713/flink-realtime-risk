package org.uwh.flink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.uwh.flink.util.MultiLeftJoin;
import org.uwh.flink.util.OneToOneJoin;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class MultiLeftJoinTest {
    @BeforeEach
    public void setUp() {
        CountingSink.counter = 0;
    }

    @Test
    public void testMultiJoin() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);

        DataStream<IntBean> ints = env.fromCollection(List.of(1, 2, 3, 4, 5)).map(i -> new IntBean(i));
        DataStream<IntBean> doubles = env.fromCollection(List.of(2, 4, 6, 8, 10)).map(i -> new IntBean(i));
        DataStream<IntBean> squares = env.fromCollection(List.of(1, 4, 9, 16, 25)).map(i -> new IntBean(i));

        new MultiLeftJoin.Builder<>(ints, i -> i.getNumber(), TypeInformation.of(IntBean.class), TypeInformation.of(Integer.class))
                .addJoin(doubles, i -> i.getNumber()/2, TypeInformation.of(IntBean.class))
                .addJoin(squares, i -> (int) Math.sqrt(i.getNumber()), TypeInformation.of(IntBean.class))
                .build()
                .addSink(new SinkFunction<Tuple>() {
                    @Override
                    public void invoke(Tuple value, Context context) throws Exception {
                        System.out.println(">>>: " + value);
                    }
                });

        env.execute();
    }

    // 10MM/P1 => 36s, 43s, 44s, 45s, 43s
    // 10MM/P2 => 28s, 29s
    // 10MM/P4 => 20s, 20s
    // 10MM/P1/ObjectReuse => 32s, 31s
    // 1M/P1 => 9.8s, 9.1s
    // 1k/P1 => 4.5s, 4.5s
    @Test
    public void volumeTest() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
        env.getConfig().disableGenericTypes();

        long N = 10_000_000;
        DataStream<IntBean> ints = env.fromSequence(1, N).map(i -> new IntBean((i.intValue())));
        DataStream<IntBean> doubles = env.fromSequence(1, N).map(i -> new IntBean((i.intValue()*2)));
        DataStream<IntBean> triples = env.fromSequence(1, N).map(i -> new IntBean((i.intValue()*3)));

        new MultiLeftJoin.Builder<>(ints, i -> i.getNumber(), TypeInformation.of(IntBean.class), TypeInformation.of(Integer.class))
                .addJoin(doubles, i -> i.getNumber()/2, TypeInformation.of(IntBean.class))
                .addJoin(triples, i -> i.getNumber()/3, TypeInformation.of(IntBean.class))
                .build()
                .addSink(new CountingSink());

        env.execute();

        System.out.println(CountingSink.counter);
    }

    // - Difference in performance might be the emitting of partial join results
    // 10MM/P1 => 30s, 37s, 35s
    @Test
    public void testSingleJoinPerformance() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);

        long N = 10_000_000;
        DataStream<IntBean> ints = env.fromSequence(1, N).map(i -> new IntBean((i.intValue())));
        DataStream<IntBean> doubles = env.fromSequence(1, N).map(i -> new IntBean((i.intValue()*2)));
        DataStream<IntBean> triples = env.fromSequence(1, N).map(i -> new IntBean((i.intValue()*3)));

        OneToOneJoin<IntBean, IntBean, Tuple2<IntBean, IntBean>> firstJoin =
                new OneToOneJoin<>((currentX, currentY, newX, newY) -> Collections.singleton(Tuple2.of(newX != null ? newX : currentX, newY != null ? newY : currentY)),
                        ints.getType(),
                        doubles.getType());

        DataStream<Tuple2<IntBean, IntBean>> join2 = ints.keyBy(i -> i.getNumber()).connect(doubles.keyBy(i -> i.getNumber()/2)).process(firstJoin)
                .returns(new TupleTypeInfo<>(TypeInformation.of(IntBean.class), TypeInformation.of(IntBean.class)));

        OneToOneJoin<Tuple2<IntBean,IntBean>, IntBean, Tuple3<IntBean, IntBean, IntBean>> secondJoin =
                new OneToOneJoin<>((currentX, currentY, newX, newY) -> {
                    Tuple2<IntBean, IntBean> t2 = newX != null ? newX : currentX;
                    return Collections.singleton(Tuple3.of(t2.f0, t2.f1, newY != null ? newY : currentY));
                },
                new TupleTypeInfo<>(TypeInformation.of(IntBean.class), TypeInformation.of(IntBean.class)),
                doubles.getType());

        join2.keyBy(t -> t.f0.getNumber()).connect(triples.keyBy(i -> i.getNumber()/3))
                .process(secondJoin)
                .returns(new TupleTypeInfo<>(TypeInformation.of(IntBean.class), TypeInformation.of(IntBean.class), TypeInformation.of(IntBean.class)))
                .addSink(new CountingSink());

        env.execute();

        System.out.println(CountingSink.counter);
    }


    // Need this because Integer itself is not nullable in a Tuple
    public static class IntBean {
        private int number;

        public IntBean() {
            number = 0;
        }

        public IntBean(int number) {
            this.number = number;
        }

        public int getNumber() {
            return number;
        }

        public void setNumber(int number) {
            this.number = number;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            IntBean intBean = (IntBean) o;
            return number == intBean.number;
        }

        @Override
        public int hashCode() {
            return Objects.hash(number);
        }

        @Override
        public String toString() {
            return String.valueOf(number);
        }
    }

    public static class CountingSink<X extends Tuple> implements SinkFunction<X> {
        public static long counter = 0;
        @Override
        public void invoke(Tuple t, Context context) throws Exception {
            Tuple3<IntBean,IntBean,IntBean> value = (Tuple3<IntBean,IntBean,IntBean>) t;
            if (value.f0 != null && value.f1 != null && value.f2 != null) {
                counter++;
            }
        }
    }
}
