package org.uwh.sparta;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.FromElementsFunction;
import org.apache.flink.streaming.api.functions.source.FromIteratorFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.uwh.FirmAccount;
import org.uwh.Issuer;
import org.uwh.RiskPosition;
import org.uwh.flink.util.DelegateSourceFunction;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

public class SampleTableSourceFactory implements DynamicTableSourceFactory {
    private static final ConfigOption<String> DATASET = ConfigOptions.key("dataset").stringType().noDefaultValue();
    private static final Map<String, Object> datasets;
    private static final Map<String, MapFunction<RowData, String>> keyExtractors;

    private static Logger logger = LoggerFactory.getLogger(SampleTableSourceFactory.class);

    private static class MappedIterator<T,O> implements Iterator<O>, Serializable {
        private Iterator<T> delegate;
        private MapFunction<T,O> map;

        public MappedIterator(Iterator<T> delegate, MapFunction<T,O> map) {
            this.delegate = delegate;
            this.map = map;
        }

        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }

        @Override
        public O next() {
            try {
                return map.map(delegate.next());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    static {
        datasets = new HashMap<>();
        keyExtractors = new HashMap<>();

        // issuerid, uid, jtd, eventtime
        datasets.put("risk", List.<RowData>of(
                GenericRowData.of(StringData.fromString("IBM"), StringData.fromString("T1"), 100.0, 1L),
                GenericRowData.of(StringData.fromString("IBM"), StringData.fromString("T2"), -50.0, 2L),
                GenericRowData.of(StringData.fromString("MSFT"), StringData.fromString("T3"), 1000.0, 3L),
                GenericRowData.of(StringData.fromString("MSFT"), StringData.fromString("T2"), -50.0, 4L)
        ));
        keyExtractors.put("risk", rowData -> rowData.getString(1).toString());

        // uid, book
        datasets.put("position", List.<RowData>of(
                GenericRowData.of(StringData.fromString("T1"), StringData.fromString("STBMIG")),
                GenericRowData.of(StringData.fromString("T2"), StringData.fromString("LB007")),
                GenericRowData.of(StringData.fromString("T3"), StringData.fromString("CDSSOV")),
                GenericRowData.of(StringData.fromString("T1"), StringData.fromString("CDSSOV"))
        ));
        keyExtractors.put("position", rowData -> rowData.getString(0).toString());

        // book, strategy, desk
        datasets.put("account", List.<RowData>of(
                GenericRowData.of(StringData.fromString("STBMIG"), StringData.fromString("001"), StringData.fromString("US IG")),
                GenericRowData.of(StringData.fromString("LB007"), StringData.fromString("002"), StringData.fromString("EMCT")),
                GenericRowData.of(StringData.fromString("CDSSOV"), StringData.fromString("003"), StringData.fromString("EMEA IG"))
        ));
        keyExtractors.put("account", rowData -> rowData.getString(0).toString());

        // uidtype, uid, book, product
        List<RiskPosition> positions = Generators.positionList(Generators.NO_POSITION, Generators.NO_USED_ACCOUNT);
        datasets.put("real-position", positions.stream()
                .map(pos -> GenericRowData.of(
                        StringData.fromString(pos.getUIDType().name()),
                        StringData.fromString(pos.getUID()),
                        StringData.fromString(pos.getFirmAccountMnemonic()),
                        StringData.fromString(pos.getProductType().name())
                        ))
                .collect(Collectors.toList()));
        keyExtractors.put("real-position", rowData -> rowData.getString(0).toString() + ":" + rowData.getString(1).toString());

        List<FirmAccount> accounts = Generators.accounts(Generators.NO_ACCOUNT);
        datasets.put("real-account", accounts.stream()
                .map(acc -> GenericRowData.of(StringData.fromString(acc.getMnemonic()), StringData.fromString(acc.getStrategyCode())))
                .collect(Collectors.toList())
        );
        keyExtractors.put("real-account", rowData -> rowData.getString(0).toString());

        // uidtype, uid, smci, cr01, jtd
        Iterator<RowData> risk = new MappedIterator<>(
                Generators.issuerRisk(Generators.NO_POSITION, Generators.NO_ISSUER),
                r -> GenericRowData.of(
                        StringData.fromString(r.getUIDType().name()),
                        StringData.fromString(r.getUID()),
                        StringData.fromString(r.getSMCI()),
                        r.getCR01(),
                        r.getJTD()
                )
        );
        datasets.put("real-risk", risk);
        keyExtractors.put("real-risk", rowData -> rowData.getString(0).toString() + ":" + rowData.getString(1).toString() + ":" + rowData.getString(2).toString());

        // smci, name, ultimateparentsmci
        List<Issuer> issuers = Generators.issuers(Generators.NO_ISSUER, Generators.NO_ULTIMATE);
        datasets.put("real-issuer",
                issuers.stream()
                .map(issuer -> GenericRowData.of(StringData.fromString(issuer.getSMCI()), StringData.fromString(issuer.getName()), StringData.fromString(issuer.getUltimateParentSMCI())))
                .collect(Collectors.toList())
                );
        keyExtractors.put("real-issuer", rowData -> rowData.getString(0).toString());
    }

    private static class AppendToChangelogSourceFunction extends DelegateSourceFunction<RowData> {
        private transient Map<String, RowData> map;
        private MapFunction<RowData, String> keyExtractor;
        private String name;

        private class AppendSourceContext extends DelegateSourceContext<RowData> {
            private transient long count = 0;

            public AppendSourceContext(SourceContext<RowData> delegate) {
                super(delegate);
            }

            // TODO
            //  - more needs to be done for checkpoint persistence - push the map into the ListState
            //  - handle the size of the state given RocksDB is not an option here?

            @Override
            public void collect(RowData rowData) {
                count++;
                if (count % 10000 == 0) {
                    logger.info("Emitted " + count + " records for " + name + ", unique keys " + map.size());
                }
                try {
                    String key = keyExtractor.map(rowData);

                    // TODO this is not always warranted
                    GenericRowData previous = (GenericRowData) map.get(key);
                    if (previous != null) {
                        GenericRowData prevRetract = new GenericRowData(RowKind.DELETE, previous.getArity());
                        for (int i=0; i<previous.getArity(); i++) {
                            prevRetract.setField(i, previous.getField(i));
                        }
                        // logger.info("Emiting "+prevRetract);
                        delegate.collect(prevRetract);
                    }
                    map.put(key, rowData);
                    // logger.info("Emiting "+rowData);
                    delegate.collect(rowData);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }

        public AppendToChangelogSourceFunction(String name, SourceFunction<RowData> delegate, MapFunction<RowData, String> keyExtractor) {
            super(delegate);
            this.name = name;
            this.keyExtractor = keyExtractor;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            map = new HashMap<>();
        }

        @Override
        public void run(SourceContext<RowData> sourceContext) throws Exception {
            SourceContext<RowData> ctx = new AppendSourceContext(sourceContext);
            delegate.run(ctx);
        }
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();

        String dataset = helper.getOptions().get(DATASET);

        return new ScanTableSource() {
            @Override
            public ChangelogMode getChangelogMode() {
                return ChangelogMode.newBuilder()
                        .addContainedKind(RowKind.INSERT)
                        .addContainedKind(RowKind.DELETE)
                        .build();
            }

            @Override
            public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext)  {
                SourceFunction<RowData> target;
                Object data = datasets.get(dataset);

                if (data instanceof Iterator) {
                    target = new FromIteratorFunction<>((Iterator<RowData>) data);
                } else {
                    try {
                        target = new FromElementsFunction<>(
                                TypeInformation.of(RowData.class).createSerializer(new ExecutionConfig()),
                                (Iterable<RowData>) data
                        );
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }

                return SourceFunctionProvider.of(
                        new AppendToChangelogSourceFunction(
                                dataset,
                                target,
                                keyExtractors.get(dataset)
                        ),
                        false);
            }

            @Override
            public DynamicTableSource copy() {
                return this;
            }

            @Override
            public String asSummaryString() {
                return "Sample source";
            }
        };
    }

    @Override
    public String factoryIdentifier() {
        return "sample";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.singleton(DATASET);
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Collections.emptySet();
    }
}
