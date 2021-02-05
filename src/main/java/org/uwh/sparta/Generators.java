package org.uwh.sparta;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.uwh.*;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Generators {
    public static final int NO_ISSUER = 100_000;
    public static final int NO_ACCOUNT = 10_000;
    public static final int NO_USED_ACCOUNT = 1_000;
    public static final int NO_USED_ISSUER = 10_000;
    public static final int NO_ULTIMATE = 1_000;
    public static final int NO_POSITION = 500_000;
    public static final int NO_ISSUERS_FOR_INDEX = 125;

    private static final String COB = DateTimeFormatter.ofPattern("yyyyMMdd").format(LocalDateTime.now());

    private static class GeneratorIterator<T> implements Iterator<T>, Serializable {
        private final Supplier<T> generator;

        public GeneratorIterator(Supplier<T> generator) {
            this.generator = generator;
        }

        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        public T next() {
            return generator.get();
        }
    }

    private static Tuple2<UIDType, String> generateUID(int i, int posLimit) {
        UIDType uidType = (i < posLimit/5) ? UIDType.UIPID : UIDType.UITID;
        return Tuple2.of(uidType, String.valueOf(i));
    }

    private static double randomCR01(Random r) {
        return r.nextDouble()*2000-1000;
    }

    private static double randomJTD(Random r) {
        return r.nextDouble()*2000000-1000000;
    }

    private static IssuerRisk randomRisk(Random r, int tradeId, int posLimit, int issuerLimit) {
        int issuerId;

        if (tradeId % 2 == 0) {
            issuerId = tradeId % (issuerLimit/10);
        } else {
            issuerId = tradeId % issuerLimit;
        }

        Tuple2<UIDType,String> uid = generateUID(tradeId, posLimit);

        return new IssuerRisk(uid.f0, uid.f1, String.valueOf(issuerId), COB, randomCR01(r), randomJTD(r), System.currentTimeMillis());
    }

    private static IssuerRisk randomRisk(Random r, int posLimit, int issuerLimit) {
        int tradeId = r.nextInt(posLimit);
        return randomRisk(r, tradeId, posLimit, issuerLimit);
    }

    private static boolean isIndex(UIDType type, int r) {
        return type == UIDType.UITID && r % 4 == 0;
    }

    private static IssuerRiskBatch randomBatchRisk(Random r, int id, int posLimit, int issuerLimit) {
        Tuple2<UIDType, String> uid = generateUID(id, posLimit);

        List<IssuerRiskLine> risks;
        if (isIndex(uid.f0, id)) {
            risks = new ArrayList<>(NO_ISSUERS_FOR_INDEX);
            int index = id % 4;
            for (int issuerId = NO_ISSUERS_FOR_INDEX*index; issuerId < NO_ISSUERS_FOR_INDEX*(index+1); issuerId++) {
                risks.add(new IssuerRiskLine(String.valueOf(issuerId), randomCR01(r), randomJTD(r)));
            }
        } else {
            String issuerId = String.valueOf(((id & 1) == 1) ? id % issuerLimit : id % (issuerLimit/10));
            risks = List.of(new IssuerRiskLine(issuerId, randomCR01(r), randomJTD(r)));
        }

        return new IssuerRiskBatch(uid.f0, uid.f1, COB, System.currentTimeMillis(), risks);
    }

    /*
    Number of results:
    - 1/5 UIPID => 100,000
    - 4/5 UITID
        - 1/4 index => 12,500,000
        - 3/4 non-index => 300,000
     => Total: 12.9 million
     */
    private static IssuerRiskBatch randomBatchRisk(Random r, int posLimit, int issuerLimit) {
        int id = r.nextInt(posLimit);
        return randomBatchRisk(r, id, posLimit, issuerLimit);
    }

    public static List<IssuerRisk> issuerRiskList(int num, int posLimit, int issuerLimit) {
        Random r = new Random();
        return IntStream.range(0, num).mapToObj(i -> randomRisk(r, posLimit, issuerLimit)).collect(Collectors.toList());
    }

    private static class IssuerRiskGen implements Supplier<IssuerRisk>, Serializable {
        private final int posLimit;
        private final int issuerLimit;
        private final Random r;

        public IssuerRiskGen(int posLimit, int issuerLimit) {
            this.posLimit = posLimit;
            this.issuerLimit = issuerLimit;
            this.r = new Random();
        }

        public IssuerRisk get() {
            return randomRisk(r, posLimit, issuerLimit);
        }
    }

    public static Iterator<IssuerRisk> issuerRisk(int posLimit, int issuerLimit) {
        return new GeneratorIterator<>(new IssuerRiskGen(posLimit, issuerLimit));
    }

    public static DataStream<IssuerRisk> oneTimeIssuerRisk(StreamExecutionEnvironment env, int posLimit, int issuerLimit) {
        Random r = new Random();
        List<IssuerRisk> risks = IntStream.range(0, posLimit).mapToObj(id -> randomRisk(r, id, posLimit, issuerLimit)).collect(Collectors.toList());
        return env.fromCollection(risks);
    }

    public static DataStream<IssuerRisk> issuerRisk(StreamExecutionEnvironment env, int posLimit, int issuerLimit) {
        return env.addSource(new RichParallelSourceFunction<IssuerRisk>() {
            @Override
            public void run(SourceContext<IssuerRisk> sourceContext) {
                Iterator<IssuerRisk> gen = issuerRisk(posLimit, issuerLimit);
                while (gen.hasNext()) {
                    sourceContext.collect(gen.next());
                }
            }

            @Override
            public void cancel() {}
        }).name("Issuer Risk");
    }

    public static DataStream<IssuerRiskBatch> oneTimeBatchRisk(StreamExecutionEnvironment env, int posLimit, int issuerLimit) {
        Random r = new Random();
        List<IssuerRiskBatch> risks = IntStream.range(0, posLimit).mapToObj(id -> randomBatchRisk(r, id, posLimit, issuerLimit)).collect(Collectors.toList());
        return env.fromCollection(risks);
    }

    public static RichParallelSourceFunction<IssuerRiskBatch> batchRisk(int posLimit, int issuerLimit) {
        return new RichParallelSourceFunction<>() {
            @Override
            public void run(SourceContext<IssuerRiskBatch> sourceContext) {
                Random r = new Random();
                while (true) {
                    sourceContext.collect(randomBatchRisk(r, posLimit, issuerLimit));
                }
            }

            @Override
            public void cancel() {}
        };
    }

    public static DataStream<IssuerRiskBatch> batchRisk(StreamExecutionEnvironment env, int posLimit, int issuerLimit) {
        return env.addSource(batchRisk(posLimit, issuerLimit)).name("Issuer Risk");
    }

    public static List<RiskPosition> positionList(int limit, int noAccounts) {
        List<RiskPosition> positions = new ArrayList<>();
        Random r = new Random();
        ProductType[] products = ProductType.values();

        for (int i=0; i<limit; i++) {
            Tuple2<UIDType, String> uid = generateUID(i, limit);
            int accountId = ((int) (r.nextDouble() * r.nextDouble() * noAccounts)) % noAccounts;
            String book = "ACC" + String.format("%06d", accountId);
            ProductType product = products[r.nextInt(products.length)];

            positions.add(new RiskPosition(uid.f0, uid.f1, book, product, System.currentTimeMillis()));
        }

        return positions;
    }

    public static DataStream<RiskPosition> positions(StreamExecutionEnvironment env, int limit, int noAccounts) {
        return env.fromCollection(positionList(limit, noAccounts)).name("Risk Positions");
    }

    public static List<Issuer> issuers(int limit, int ultimates) {
        return IntStream.range(0, limit)
                .mapToObj(id -> new Issuer(String.valueOf(id), "Issuer " + id, (id < ultimates) ? null : String.valueOf(id % ultimates)))
                .collect(Collectors.toList());
    }

    public static DataStream<Issuer> issuers(StreamExecutionEnvironment env, int limit, int ultimates) {
        return env.fromCollection(issuers(limit, ultimates)).name("Issuers");
    }

    public static List<RiskThreshold> thresholds(int issuerLimit) {
        Random r = new Random();
        int[] jtdLimits = new int[] { 25000000, 50000000, 100000000, 200000000 };
        int[] cr01Limits = new int[] { 25000, 50000, 100000 };

        return IntStream.range(0, issuerLimit)
                .mapToObj(id -> {
                    Map<String,Double> map = new HashMap<>();
                    map.put(RiskMeasure.JTD.name(), (double) jtdLimits[r.nextInt(jtdLimits.length)]);
                    map.put(RiskMeasure.CR01.name(),(double) cr01Limits[r.nextInt(cr01Limits.length)]);
                    return new RiskThreshold(RiskFactorType.Issuer, String.valueOf(id), map);
                })
                .collect(Collectors.toList());
    }

    public static DataStream<RiskThreshold> thresholds(StreamExecutionEnvironment env, int issuerLimit) {
        return env.fromCollection(thresholds(issuerLimit)).name("Risk Limit Thresholds");
    }

    public static List<FirmAccount> accounts(int limit) {
        List<FirmAccount> accounts = new ArrayList<>();
        Random r = new Random();
        String[] strategies = new String[] { "001", "002", "097", "78E", "78F" };

        for (int i=0; i<limit; i++) {
            accounts.add(new FirmAccount("ACC" + String.format("%06d", i), strategies[r.nextInt(strategies.length)], System.currentTimeMillis()));
        }

        return accounts;
    }

    public static DataStream<FirmAccount> accounts(StreamExecutionEnvironment env, int limit) {
        return env.fromCollection(accounts(limit)).name("Firm Accounts");
    }

    public static DataStream<Long> ticks(StreamExecutionEnvironment env, int num, TimeUnit unit) {
        return env.addSource(new SourceFunction<>() {
            private volatile boolean running = true;
            private volatile long inc = 0;

            @Override
            public void run(SourceContext<Long> sourceContext) throws Exception {
                running = true;
                while (running) {
                    sourceContext.collect(inc++);
                    Thread.sleep(unit.toMillis(num));
                }
            }

            @Override
            public void cancel() {
                running = false;
            }
        });
    }
}
