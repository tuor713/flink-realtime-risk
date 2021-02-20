package org.uwh.risk;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.formats.avro.typeutils.AvroSerializer;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.uwh.Issuer;
import org.uwh.IssuerRisk;
import org.uwh.RolldownItem;
import org.uwh.UIDType;
import org.uwh.flink.data.generic.Field;
import org.uwh.flink.data.generic.Record;
import org.uwh.flink.data.generic.RecordType;

import java.time.LocalDate;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SerializationTest {
    private static final Field<String> F_ISSUER_ID = new Field<>("issuer","id", String.class, Types.STRING);
    private static final Field<String> F_ISSUER_NAME = new Field<>("issuer", "name", String.class, Types.STRING);
    private static final Field<String> F_ISSUER_ULTIMATE_PARENT_ID = new Field<>("issuer", "ultimate-parent-id", F_ISSUER_ID);

    private static final Field<String> F_POS_UID = new Field<>("position", "uid", String.class, Types.STRING);
    private static final Field<String> F_POS_UID_TYPE = new Field<>("position", "uid-type", String.class, Types.STRING);
    private static final Field<String> F_CLOSEDATE = new Field<>("risk", "close-date", String.class, Types.STRING);
    private static final Field<Long> F_AUDIT_DATE_TIME = new Field<>("general", "audit-date-time", Long.class, Types.LONG);

    private static final Field<Double> F_RISK_ISSUER_CR01 = new Field<>("issuer-risk","cr01", Double.class, Types.DOUBLE);
    private static final Field<Double> F_RISK_ISSUER_JTD = new Field<>("issuer-risk","jtd", Double.class, Types.DOUBLE);

    @Test
    public void testSizes() throws Exception {
        ExecutionConfig config = new ExecutionConfig();
        config.disableGenericTypes();

        // Avro serialization is about 70% size of RowData
        // - Note that this is still not as efficient as the default Avro binary serialization.
        //   For example for ints Avro uses variable length encoding whereas in Flink the encoder chooses a fixed
        //   4 byte format.
        // POJO is a little bit smaller still

        System.out.println("=== Issuer ===");
        System.out.println("Avro: " + serializedLength(new Issuer("1","Issuer 1","1"), TypeInformation.of(Issuer.class)));
        System.out.println("Tuple[RowKind,Avro]: " +
                serializedLength(Tuple2.of(RowKind.INSERT, new Issuer("1","Issuer 1","1")),
                        new TupleTypeInfo<>(TypeInformation.of(RowKind.class), TypeInformation.of(Issuer.class))));

        Schema schema = Schema.createRecord("record", "", "", false, List.of(
                new Schema.Field("RowKind", Schema.create(Schema.Type.INT)),
                new Schema.Field("SMCI", Schema.create(Schema.Type.STRING)),
                new Schema.Field("Name", Schema.create(Schema.Type.STRING)),
                new Schema.Field("UltimateParentSMCI", Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING)))
        ));
        AvroSerializer<GenericRecord> serializer = new AvroSerializer<>(GenericRecord.class, schema);
        GenericRecord rec = new GenericData.Record(schema);
        rec.put("RowKind", RowKind.INSERT.toByteValue());
        rec.put("SMCI", "1");
        rec.put("Name", "Issuer 1");
        rec.put("UltimateParentSMCI", "1");
        System.out.println("Generic Avro (incl. RowKind): " + serializedLength(rec, serializer));

        System.out.println("POJO: " + serializedLength(new IssuerPOJO("1", "Issuer 1", "1"), TypeInformation.of(IssuerPOJO.class)));

        RecordType type = new RecordType(config, List.of(F_ISSUER_ID, F_ISSUER_NAME, F_ISSUER_ULTIMATE_PARENT_ID), Set.of(F_ISSUER_ULTIMATE_PARENT_ID));
        System.out.println("RowData: " + serializedLength(new Record(type).with(F_ISSUER_ID, "1").with(F_ISSUER_NAME, "Issuer 1").with(F_ISSUER_ULTIMATE_PARENT_ID, "1"), type));

        System.out.println("=== Issuer Risk ===");
        System.out.println("Avro: " + serializedLength(
                new IssuerRisk(UIDType.UIPID, "BOOK:123", "1", "20201231", 100.0, 1000000.0, Collections.emptyList(), System.currentTimeMillis()),
                TypeInformation.of(IssuerRisk.class)
        ));

        // date (int) + float = 8 bytes per entry
        //  = 160 bytes payload
        //  Total overhead 161 bytes = 160 + 1 byte for length
        System.out.println("Avro + 20 rolldown: " + serializedLength(
                new IssuerRisk(UIDType.UIPID, "BOOK:123", "1", "20201231", 100.0, 1000000.0,
                        IntStream.range(0,20).mapToObj(i -> new RolldownItem(LocalDate.now(), (float) 1000000.0)).collect(Collectors.toList()),
                        System.currentTimeMillis()),
                TypeInformation.of(IssuerRisk.class)
        ));


        System.out.println("Tuple[RowKind,Avro]: " + serializedLength(
                Tuple2.of(RowKind.INSERT, new IssuerRisk(UIDType.UIPID, "BOOK:123", "1", "20201231", 100.0, 1000000.0, Collections.emptyList(), System.currentTimeMillis())),
                new TupleTypeInfo<>(TypeInformation.of(RowKind.class), TypeInformation.of(IssuerRisk.class))
        ));

        type = new RecordType(config, F_POS_UID_TYPE, F_POS_UID, F_ISSUER_ID, F_CLOSEDATE, F_RISK_ISSUER_CR01, F_RISK_ISSUER_JTD, F_AUDIT_DATE_TIME);
        System.out.println(
                "RowData: " +
                serializedLength(
                        new Record(type).with(F_POS_UID_TYPE, UIDType.UIPID.name())
                            .with(F_POS_UID, "BOOK:123")
                            .with(F_ISSUER_ID, "1")
                            .with(F_CLOSEDATE, "20201231")
                            .with(F_RISK_ISSUER_CR01, 100.0)
                            .with(F_RISK_ISSUER_JTD, 1000000.0)
                            .with(F_AUDIT_DATE_TIME, System.currentTimeMillis()),
                        type
                )
        );
    }

    private<T> int serializedLength(T t, TypeInformation<T> type) throws Exception {
        ExecutionConfig config = new ExecutionConfig();
        config.disableGenericTypes();

        TypeSerializer<T> serializer = type.createSerializer(config);
        return serializedLength(t, serializer);
    }

    private<T> int serializedLength(T t, TypeSerializer<T> serializer) throws Exception {
        DataOutputSerializer out = new DataOutputSerializer(100);
        serializer.serialize(t, out);
        return out.length();
    }

    public static class IssuerPOJO {
        private RowKind kind = RowKind.INSERT;
        private String id;
        private String name;
        private String ultimateParentId;

        public IssuerPOJO() {}

        public IssuerPOJO(String id, String name, String ultimateParentId) {
            this.id = id;
            this.name = name;
            this.ultimateParentId = ultimateParentId;
        }

        public RowKind getKind() {
            return kind;
        }

        public void setKind(RowKind kind) {
            this.kind = kind;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getUltimateParentId() {
            return ultimateParentId;
        }

        public void setUltimateParentId(String ultimateParentId) {
            this.ultimateParentId = ultimateParentId;
        }
    }
}
