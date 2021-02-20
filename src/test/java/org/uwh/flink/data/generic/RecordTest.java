package org.uwh.flink.data.generic;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.uwh.Issuer;
import org.uwh.RolldownItem;
import org.uwh.UIDType;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RecordTest {
    private static final Field<String> F_POS_UID = new Field<>("position", "uid", Types.STRING);
    private static final Field<UIDType> F_POS_UID_TYPE = new Field<>("position", "uid-type", TypeInformation.of(UIDType.class));
    private static final Field<String> F_CLOSEDATE = new Field<>("risk", "close-date", Types.STRING);
    private static final Field<Double> F_RISK_ISSUER_JTD = new Field<>("issuer-risk","jtd", Types.DOUBLE);
    private static final Field<Long> F_AUDIT_DATE_TIME = new Field<>("general", "audit-date-time", Types.LONG);
    private static final Field<Issuer> F_ISSUER = new Field<>("general", "issuer", TypeInformation.of(Issuer.class));
    private static final Field<List<RolldownItem>> F_RISK_ISSUER_JTD_ROLLDOWN = new Field<>("issuer-risk", "jtd-rolldown", new ListTypeInfo<>(RolldownItem.class));
    private static final ExecutionConfig config = new ExecutionConfig();

    @Test
    public void testKinds() throws Exception {
        RecordType type = new RecordType(config, F_POS_UID);

        for (RowKind kind : RowKind.values()) {
            Record rec = new Record(kind, type).with(F_POS_UID, "123");
            assertEquals(kind, rec.getKind());
            assertEquals(kind, serializeDeserialize(rec).getKind());
        }
    }

    @Test
    public void testLongField() throws Exception {
        RecordType type = new RecordType(config, F_AUDIT_DATE_TIME);
        Record rec = new Record(type);
        rec.set(F_AUDIT_DATE_TIME, 10L);

        assertEquals(10L, rec.get(F_AUDIT_DATE_TIME));

        rec = serializeDeserialize(rec);
        assertEquals(10L, rec.get(F_AUDIT_DATE_TIME));

        assertTrue(serializedLength(rec) <= 20, "Message length: "+serializedLength(rec));
    }

    @Test
    public void testStringField() throws Exception {
        RecordType type = new RecordType(config, F_POS_UID);
        Record rec = new Record(type).with(F_POS_UID, "BOOK:123");

        assertEquals("BOOK:123", rec.get(F_POS_UID));

        rec = serializeDeserialize(rec);
        assertEquals("BOOK:123", rec.get(F_POS_UID));

        assertTrue(serializedLength(rec) <= 30, "Message length: "+serializedLength(rec));
    }

    @Test
    public void testNullFields() throws Exception {
        List<Field> fields = List.of(F_RISK_ISSUER_JTD, F_POS_UID, F_AUDIT_DATE_TIME, F_POS_UID_TYPE);

        for (Field field : fields) {
            RecordType type = new RecordType(config, List.of(field), Set.of(field));
            Record rec = new Record(type);
            assertEquals(null, rec.get(field), "Uninitialized record of field " + field + " is not null");

            rec.set(field, null);
            assertEquals(null, rec.get(field), "Explicitly record of field " + field + " is not null");

            rec = serializeDeserialize(rec);
            assertEquals(null, rec.get(field), "Round-tripped record of field " + field + " is not null");
        }
    }

    @Test
    public void testEnumField() throws Exception {
        RecordType type = new RecordType(config, F_POS_UID_TYPE);
        Record rec = new Record(type).with(F_POS_UID_TYPE, UIDType.UIPID);
        assertEquals(UIDType.UIPID, rec.get(F_POS_UID_TYPE));

        assertTrue(serializedLength(rec) <= 20, "Message length: "+serializedLength(rec));
    }

    @Test
    public void testTupleWithNull() throws Exception {
        RecordType atype = new RecordType(config, F_POS_UID_TYPE);
        RecordType btype = new RecordType(config, F_POS_UID);
        TupleTypeInfo<Tuple2<Record,Record>> tinfo = new TupleTypeInfo<>(atype, btype);
        TypeSerializer<Tuple2<Record, Record>> serializer = tinfo.createSerializer(config);
        Tuple2<Record,Record> tup = Tuple2.of(new Record(atype).with(F_POS_UID_TYPE, UIDType.UITID), null);
        DataOutputSerializer out = new DataOutputSerializer(100);
        serializer.serialize(tup, out);

        DataInputDeserializer in = new DataInputDeserializer(out.getCopyOfBuffer());
        Tuple2<Record,Record> tup2 = serializer.deserialize(in);
        assertEquals(tup, tup2);

    }

    @Test
    public void testMultipleFields() throws Exception {
        RecordType type = new RecordType(config, F_POS_UID_TYPE, F_POS_UID, F_RISK_ISSUER_JTD);
        Record rec = new Record(type).with(F_POS_UID_TYPE, UIDType.UITID).with(F_POS_UID, "123").with(F_RISK_ISSUER_JTD, 1000.0);

        assertEquals(3, type.getFields().size());

        assertEquals(UIDType.UITID, rec.get(F_POS_UID_TYPE));
        assertEquals("123", rec.get(F_POS_UID));
        assertEquals(1000.0, rec.get(F_RISK_ISSUER_JTD), 0.1);

        rec = serializeDeserialize(rec);
        assertEquals(UIDType.UITID, rec.get(F_POS_UID_TYPE));
        assertEquals("123", rec.get(F_POS_UID));
        assertEquals(1000.0, rec.get(F_RISK_ISSUER_JTD), 0.1);
    }

    @Test
    public void testNestedRecords() throws Exception {
        RecordType innerType = new RecordType(config, F_POS_UID_TYPE, F_POS_UID, F_RISK_ISSUER_JTD);
        Field<Record> innerField = new Field<>("record", "inner", innerType);
        RecordType outerType = new RecordType(config, innerField);
        Record inner = new Record(innerType).with(F_POS_UID_TYPE, UIDType.UIPID).with(F_POS_UID,"123").with(F_RISK_ISSUER_JTD,1000.0);
        Record outer = new Record(outerType).with(innerField, inner);

        assertEquals("123", outer.get(innerField).get(F_POS_UID));

        outer = serializeDeserialize(outer);
        assertEquals("123", outer.get(innerField).get(F_POS_UID));
    }

    @Test
    public void testNestedAvroObject() throws Exception {
        RecordType type = new RecordType(config, F_ISSUER);
        Record rec = new Record(type).with(F_ISSUER, new Issuer("123", "Issuer 123", "1"));
        assertEquals("123", rec.get(F_ISSUER).getSMCI());

        rec = serializeDeserialize(rec);
        assertEquals("123", rec.get(F_ISSUER).getSMCI());
    }

    @Test
    public void testNestedList() throws Exception {
        RecordType type = new RecordType(config, F_RISK_ISSUER_JTD_ROLLDOWN);
        Record rec = new Record(type).with(F_RISK_ISSUER_JTD_ROLLDOWN, List.of(new RolldownItem(1, 1000.0), new RolldownItem(2, 2000.0)));
        assertEquals(2, rec.get(F_RISK_ISSUER_JTD_ROLLDOWN).size());
        assertEquals(2000.0, rec.get(F_RISK_ISSUER_JTD_ROLLDOWN).get(1).getJTD(), 0.1);

        rec = serializeDeserialize(rec);
        assertEquals(2, rec.get(F_RISK_ISSUER_JTD_ROLLDOWN).size());
        assertEquals(2000.0, rec.get(F_RISK_ISSUER_JTD_ROLLDOWN).get(1).getJTD(), 0.1);
    }

    private int serializedLength(Record rec) throws Exception {
        DataOutputSerializer out = new DataOutputSerializer(100);
        TypeSerializer<Record> serializer = rec.getType().createSerializer(config);
        serializer.serialize(rec, out);
        System.out.println("Serialized length: " + out.length());
        return out.length();
    }

    private Record serializeDeserialize(Record rec) throws Exception {
        DataOutputSerializer out = new DataOutputSerializer(100);
        TypeSerializer<Record> serializer = rec.getType().createSerializer(config);
        serializer.serialize(rec, out);

        DataInputDeserializer in = new DataInputDeserializer(out.getCopyOfBuffer());
        return serializer.deserialize(in);
    }
}
