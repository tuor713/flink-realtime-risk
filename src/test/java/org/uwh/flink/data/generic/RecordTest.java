package org.uwh.flink.data.generic;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.uwh.UIDType;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RecordTest {
    private static final Field<String> F_POS_UID = new Field<>("position", "uid", String.class, Types.STRING);
    private static final Field<UIDType> F_POS_UID_TYPE = new Field<>("position", "uid-type", UIDType.class, TypeInformation.of(UIDType.class));
    private static final Field<String> F_CLOSEDATE = new Field<>("risk", "close-date", String.class, Types.STRING);
    private static final Field<Double> F_RISK_ISSUER_JTD = new Field<>("issuer-risk","jtd", Double.class, Types.DOUBLE);
    private static final Field<Long> F_AUDIT_DATE_TIME = new Field<>("general", "audit-date-time", Long.class, Types.LONG);
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
            RecordType type = new RecordType(config, field);
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
    public void testMultipleFields() throws Exception {
        RecordType type = new RecordType(config, F_POS_UID_TYPE, F_POS_UID, F_RISK_ISSUER_JTD);
        Record rec = new Record(type).with(F_POS_UID_TYPE, UIDType.UITID).with(F_POS_UID, "123").with(F_RISK_ISSUER_JTD, 1000.0);

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
