/**
 * Autogenerated by Avro
 *
 * DO NOT EDIT DIRECTLY
 */
package org.uwh;

import org.apache.avro.generic.GenericArray;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.util.Utf8;
import org.apache.avro.message.BinaryMessageEncoder;
import org.apache.avro.message.BinaryMessageDecoder;
import org.apache.avro.message.SchemaStore;

@org.apache.avro.specific.AvroGenerated
public class IssuerRisk extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = -6522850179304667579L;
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"IssuerRisk\",\"namespace\":\"org.uwh\",\"fields\":[{\"name\":\"UIDType\",\"type\":{\"type\":\"enum\",\"name\":\"UIDType\",\"symbols\":[\"UIPID\",\"UITID\"]}},{\"name\":\"UID\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}},{\"name\":\"SMCI\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}},{\"name\":\"BusinessDate\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}},{\"name\":\"CR01\",\"type\":\"double\"},{\"name\":\"JTD\",\"type\":\"double\"},{\"name\":\"JTDRolldown\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"RolldownItem\",\"fields\":[{\"name\":\"Date\",\"type\":{\"type\":\"int\",\"logicalType\":\"date\"}},{\"name\":\"JTD\",\"type\":\"double\"}]}}},{\"name\":\"AuditDateTimeUTC\",\"type\":\"long\"}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }

  private static SpecificData MODEL$ = new SpecificData();
static {
    MODEL$.addLogicalTypeConversion(new org.apache.avro.data.TimeConversions.DateConversion());
  }

  private static final BinaryMessageEncoder<IssuerRisk> ENCODER =
      new BinaryMessageEncoder<IssuerRisk>(MODEL$, SCHEMA$);

  private static final BinaryMessageDecoder<IssuerRisk> DECODER =
      new BinaryMessageDecoder<IssuerRisk>(MODEL$, SCHEMA$);

  /**
   * Return the BinaryMessageEncoder instance used by this class.
   * @return the message encoder used by this class
   */
  public static BinaryMessageEncoder<IssuerRisk> getEncoder() {
    return ENCODER;
  }

  /**
   * Return the BinaryMessageDecoder instance used by this class.
   * @return the message decoder used by this class
   */
  public static BinaryMessageDecoder<IssuerRisk> getDecoder() {
    return DECODER;
  }

  /**
   * Create a new BinaryMessageDecoder instance for this class that uses the specified {@link SchemaStore}.
   * @param resolver a {@link SchemaStore} used to find schemas by fingerprint
   * @return a BinaryMessageDecoder instance for this class backed by the given SchemaStore
   */
  public static BinaryMessageDecoder<IssuerRisk> createDecoder(SchemaStore resolver) {
    return new BinaryMessageDecoder<IssuerRisk>(MODEL$, SCHEMA$, resolver);
  }

  /**
   * Serializes this IssuerRisk to a ByteBuffer.
   * @return a buffer holding the serialized data for this instance
   * @throws java.io.IOException if this instance could not be serialized
   */
  public java.nio.ByteBuffer toByteBuffer() throws java.io.IOException {
    return ENCODER.encode(this);
  }

  /**
   * Deserializes a IssuerRisk from a ByteBuffer.
   * @param b a byte buffer holding serialized data for an instance of this class
   * @return a IssuerRisk instance decoded from the given buffer
   * @throws java.io.IOException if the given bytes could not be deserialized into an instance of this class
   */
  public static IssuerRisk fromByteBuffer(
      java.nio.ByteBuffer b) throws java.io.IOException {
    return DECODER.decode(b);
  }

   private org.uwh.UIDType UIDType;
   private java.lang.String UID;
   private java.lang.String SMCI;
   private java.lang.String BusinessDate;
   private double CR01;
   private double JTD;
   private java.util.List<org.uwh.RolldownItem> JTDRolldown;
   private long AuditDateTimeUTC;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public IssuerRisk() {}

  /**
   * All-args constructor.
   * @param UIDType The new value for UIDType
   * @param UID The new value for UID
   * @param SMCI The new value for SMCI
   * @param BusinessDate The new value for BusinessDate
   * @param CR01 The new value for CR01
   * @param JTD The new value for JTD
   * @param JTDRolldown The new value for JTDRolldown
   * @param AuditDateTimeUTC The new value for AuditDateTimeUTC
   */
  public IssuerRisk(org.uwh.UIDType UIDType, java.lang.String UID, java.lang.String SMCI, java.lang.String BusinessDate, java.lang.Double CR01, java.lang.Double JTD, java.util.List<org.uwh.RolldownItem> JTDRolldown, java.lang.Long AuditDateTimeUTC) {
    this.UIDType = UIDType;
    this.UID = UID;
    this.SMCI = SMCI;
    this.BusinessDate = BusinessDate;
    this.CR01 = CR01;
    this.JTD = JTD;
    this.JTDRolldown = JTDRolldown;
    this.AuditDateTimeUTC = AuditDateTimeUTC;
  }

  public org.apache.avro.specific.SpecificData getSpecificData() { return MODEL$; }
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call.
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return UIDType;
    case 1: return UID;
    case 2: return SMCI;
    case 3: return BusinessDate;
    case 4: return CR01;
    case 5: return JTD;
    case 6: return JTDRolldown;
    case 7: return AuditDateTimeUTC;
    default: throw new IndexOutOfBoundsException("Invalid index: " + field$);
    }
  }

  // Used by DatumReader.  Applications should not call.
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: UIDType = (org.uwh.UIDType)value$; break;
    case 1: UID = value$ != null ? value$.toString() : null; break;
    case 2: SMCI = value$ != null ? value$.toString() : null; break;
    case 3: BusinessDate = value$ != null ? value$.toString() : null; break;
    case 4: CR01 = (java.lang.Double)value$; break;
    case 5: JTD = (java.lang.Double)value$; break;
    case 6: JTDRolldown = (java.util.List<org.uwh.RolldownItem>)value$; break;
    case 7: AuditDateTimeUTC = (java.lang.Long)value$; break;
    default: throw new IndexOutOfBoundsException("Invalid index: " + field$);
    }
  }

  /**
   * Gets the value of the 'UIDType' field.
   * @return The value of the 'UIDType' field.
   */
  public org.uwh.UIDType getUIDType() {
    return UIDType;
  }


  /**
   * Sets the value of the 'UIDType' field.
   * @param value the value to set.
   */
  public void setUIDType(org.uwh.UIDType value) {
    this.UIDType = value;
  }

  /**
   * Gets the value of the 'UID' field.
   * @return The value of the 'UID' field.
   */
  public java.lang.String getUID() {
    return UID;
  }


  /**
   * Sets the value of the 'UID' field.
   * @param value the value to set.
   */
  public void setUID(java.lang.String value) {
    this.UID = value;
  }

  /**
   * Gets the value of the 'SMCI' field.
   * @return The value of the 'SMCI' field.
   */
  public java.lang.String getSMCI() {
    return SMCI;
  }


  /**
   * Sets the value of the 'SMCI' field.
   * @param value the value to set.
   */
  public void setSMCI(java.lang.String value) {
    this.SMCI = value;
  }

  /**
   * Gets the value of the 'BusinessDate' field.
   * @return The value of the 'BusinessDate' field.
   */
  public java.lang.String getBusinessDate() {
    return BusinessDate;
  }


  /**
   * Sets the value of the 'BusinessDate' field.
   * @param value the value to set.
   */
  public void setBusinessDate(java.lang.String value) {
    this.BusinessDate = value;
  }

  /**
   * Gets the value of the 'CR01' field.
   * @return The value of the 'CR01' field.
   */
  public double getCR01() {
    return CR01;
  }


  /**
   * Sets the value of the 'CR01' field.
   * @param value the value to set.
   */
  public void setCR01(double value) {
    this.CR01 = value;
  }

  /**
   * Gets the value of the 'JTD' field.
   * @return The value of the 'JTD' field.
   */
  public double getJTD() {
    return JTD;
  }


  /**
   * Sets the value of the 'JTD' field.
   * @param value the value to set.
   */
  public void setJTD(double value) {
    this.JTD = value;
  }

  /**
   * Gets the value of the 'JTDRolldown' field.
   * @return The value of the 'JTDRolldown' field.
   */
  public java.util.List<org.uwh.RolldownItem> getJTDRolldown() {
    return JTDRolldown;
  }


  /**
   * Sets the value of the 'JTDRolldown' field.
   * @param value the value to set.
   */
  public void setJTDRolldown(java.util.List<org.uwh.RolldownItem> value) {
    this.JTDRolldown = value;
  }

  /**
   * Gets the value of the 'AuditDateTimeUTC' field.
   * @return The value of the 'AuditDateTimeUTC' field.
   */
  public long getAuditDateTimeUTC() {
    return AuditDateTimeUTC;
  }


  /**
   * Sets the value of the 'AuditDateTimeUTC' field.
   * @param value the value to set.
   */
  public void setAuditDateTimeUTC(long value) {
    this.AuditDateTimeUTC = value;
  }

  /**
   * Creates a new IssuerRisk RecordBuilder.
   * @return A new IssuerRisk RecordBuilder
   */
  public static org.uwh.IssuerRisk.Builder newBuilder() {
    return new org.uwh.IssuerRisk.Builder();
  }

  /**
   * Creates a new IssuerRisk RecordBuilder by copying an existing Builder.
   * @param other The existing builder to copy.
   * @return A new IssuerRisk RecordBuilder
   */
  public static org.uwh.IssuerRisk.Builder newBuilder(org.uwh.IssuerRisk.Builder other) {
    if (other == null) {
      return new org.uwh.IssuerRisk.Builder();
    } else {
      return new org.uwh.IssuerRisk.Builder(other);
    }
  }

  /**
   * Creates a new IssuerRisk RecordBuilder by copying an existing IssuerRisk instance.
   * @param other The existing instance to copy.
   * @return A new IssuerRisk RecordBuilder
   */
  public static org.uwh.IssuerRisk.Builder newBuilder(org.uwh.IssuerRisk other) {
    if (other == null) {
      return new org.uwh.IssuerRisk.Builder();
    } else {
      return new org.uwh.IssuerRisk.Builder(other);
    }
  }

  /**
   * RecordBuilder for IssuerRisk instances.
   */
  @org.apache.avro.specific.AvroGenerated
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<IssuerRisk>
    implements org.apache.avro.data.RecordBuilder<IssuerRisk> {

    private org.uwh.UIDType UIDType;
    private java.lang.String UID;
    private java.lang.String SMCI;
    private java.lang.String BusinessDate;
    private double CR01;
    private double JTD;
    private java.util.List<org.uwh.RolldownItem> JTDRolldown;
    private long AuditDateTimeUTC;

    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     * @param other The existing Builder to copy.
     */
    private Builder(org.uwh.IssuerRisk.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.UIDType)) {
        this.UIDType = data().deepCopy(fields()[0].schema(), other.UIDType);
        fieldSetFlags()[0] = other.fieldSetFlags()[0];
      }
      if (isValidValue(fields()[1], other.UID)) {
        this.UID = data().deepCopy(fields()[1].schema(), other.UID);
        fieldSetFlags()[1] = other.fieldSetFlags()[1];
      }
      if (isValidValue(fields()[2], other.SMCI)) {
        this.SMCI = data().deepCopy(fields()[2].schema(), other.SMCI);
        fieldSetFlags()[2] = other.fieldSetFlags()[2];
      }
      if (isValidValue(fields()[3], other.BusinessDate)) {
        this.BusinessDate = data().deepCopy(fields()[3].schema(), other.BusinessDate);
        fieldSetFlags()[3] = other.fieldSetFlags()[3];
      }
      if (isValidValue(fields()[4], other.CR01)) {
        this.CR01 = data().deepCopy(fields()[4].schema(), other.CR01);
        fieldSetFlags()[4] = other.fieldSetFlags()[4];
      }
      if (isValidValue(fields()[5], other.JTD)) {
        this.JTD = data().deepCopy(fields()[5].schema(), other.JTD);
        fieldSetFlags()[5] = other.fieldSetFlags()[5];
      }
      if (isValidValue(fields()[6], other.JTDRolldown)) {
        this.JTDRolldown = data().deepCopy(fields()[6].schema(), other.JTDRolldown);
        fieldSetFlags()[6] = other.fieldSetFlags()[6];
      }
      if (isValidValue(fields()[7], other.AuditDateTimeUTC)) {
        this.AuditDateTimeUTC = data().deepCopy(fields()[7].schema(), other.AuditDateTimeUTC);
        fieldSetFlags()[7] = other.fieldSetFlags()[7];
      }
    }

    /**
     * Creates a Builder by copying an existing IssuerRisk instance
     * @param other The existing instance to copy.
     */
    private Builder(org.uwh.IssuerRisk other) {
      super(SCHEMA$);
      if (isValidValue(fields()[0], other.UIDType)) {
        this.UIDType = data().deepCopy(fields()[0].schema(), other.UIDType);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.UID)) {
        this.UID = data().deepCopy(fields()[1].schema(), other.UID);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.SMCI)) {
        this.SMCI = data().deepCopy(fields()[2].schema(), other.SMCI);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.BusinessDate)) {
        this.BusinessDate = data().deepCopy(fields()[3].schema(), other.BusinessDate);
        fieldSetFlags()[3] = true;
      }
      if (isValidValue(fields()[4], other.CR01)) {
        this.CR01 = data().deepCopy(fields()[4].schema(), other.CR01);
        fieldSetFlags()[4] = true;
      }
      if (isValidValue(fields()[5], other.JTD)) {
        this.JTD = data().deepCopy(fields()[5].schema(), other.JTD);
        fieldSetFlags()[5] = true;
      }
      if (isValidValue(fields()[6], other.JTDRolldown)) {
        this.JTDRolldown = data().deepCopy(fields()[6].schema(), other.JTDRolldown);
        fieldSetFlags()[6] = true;
      }
      if (isValidValue(fields()[7], other.AuditDateTimeUTC)) {
        this.AuditDateTimeUTC = data().deepCopy(fields()[7].schema(), other.AuditDateTimeUTC);
        fieldSetFlags()[7] = true;
      }
    }

    /**
      * Gets the value of the 'UIDType' field.
      * @return The value.
      */
    public org.uwh.UIDType getUIDType() {
      return UIDType;
    }


    /**
      * Sets the value of the 'UIDType' field.
      * @param value The value of 'UIDType'.
      * @return This builder.
      */
    public org.uwh.IssuerRisk.Builder setUIDType(org.uwh.UIDType value) {
      validate(fields()[0], value);
      this.UIDType = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    /**
      * Checks whether the 'UIDType' field has been set.
      * @return True if the 'UIDType' field has been set, false otherwise.
      */
    public boolean hasUIDType() {
      return fieldSetFlags()[0];
    }


    /**
      * Clears the value of the 'UIDType' field.
      * @return This builder.
      */
    public org.uwh.IssuerRisk.Builder clearUIDType() {
      UIDType = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /**
      * Gets the value of the 'UID' field.
      * @return The value.
      */
    public java.lang.String getUID() {
      return UID;
    }


    /**
      * Sets the value of the 'UID' field.
      * @param value The value of 'UID'.
      * @return This builder.
      */
    public org.uwh.IssuerRisk.Builder setUID(java.lang.String value) {
      validate(fields()[1], value);
      this.UID = value;
      fieldSetFlags()[1] = true;
      return this;
    }

    /**
      * Checks whether the 'UID' field has been set.
      * @return True if the 'UID' field has been set, false otherwise.
      */
    public boolean hasUID() {
      return fieldSetFlags()[1];
    }


    /**
      * Clears the value of the 'UID' field.
      * @return This builder.
      */
    public org.uwh.IssuerRisk.Builder clearUID() {
      UID = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    /**
      * Gets the value of the 'SMCI' field.
      * @return The value.
      */
    public java.lang.String getSMCI() {
      return SMCI;
    }


    /**
      * Sets the value of the 'SMCI' field.
      * @param value The value of 'SMCI'.
      * @return This builder.
      */
    public org.uwh.IssuerRisk.Builder setSMCI(java.lang.String value) {
      validate(fields()[2], value);
      this.SMCI = value;
      fieldSetFlags()[2] = true;
      return this;
    }

    /**
      * Checks whether the 'SMCI' field has been set.
      * @return True if the 'SMCI' field has been set, false otherwise.
      */
    public boolean hasSMCI() {
      return fieldSetFlags()[2];
    }


    /**
      * Clears the value of the 'SMCI' field.
      * @return This builder.
      */
    public org.uwh.IssuerRisk.Builder clearSMCI() {
      SMCI = null;
      fieldSetFlags()[2] = false;
      return this;
    }

    /**
      * Gets the value of the 'BusinessDate' field.
      * @return The value.
      */
    public java.lang.String getBusinessDate() {
      return BusinessDate;
    }


    /**
      * Sets the value of the 'BusinessDate' field.
      * @param value The value of 'BusinessDate'.
      * @return This builder.
      */
    public org.uwh.IssuerRisk.Builder setBusinessDate(java.lang.String value) {
      validate(fields()[3], value);
      this.BusinessDate = value;
      fieldSetFlags()[3] = true;
      return this;
    }

    /**
      * Checks whether the 'BusinessDate' field has been set.
      * @return True if the 'BusinessDate' field has been set, false otherwise.
      */
    public boolean hasBusinessDate() {
      return fieldSetFlags()[3];
    }


    /**
      * Clears the value of the 'BusinessDate' field.
      * @return This builder.
      */
    public org.uwh.IssuerRisk.Builder clearBusinessDate() {
      BusinessDate = null;
      fieldSetFlags()[3] = false;
      return this;
    }

    /**
      * Gets the value of the 'CR01' field.
      * @return The value.
      */
    public double getCR01() {
      return CR01;
    }


    /**
      * Sets the value of the 'CR01' field.
      * @param value The value of 'CR01'.
      * @return This builder.
      */
    public org.uwh.IssuerRisk.Builder setCR01(double value) {
      validate(fields()[4], value);
      this.CR01 = value;
      fieldSetFlags()[4] = true;
      return this;
    }

    /**
      * Checks whether the 'CR01' field has been set.
      * @return True if the 'CR01' field has been set, false otherwise.
      */
    public boolean hasCR01() {
      return fieldSetFlags()[4];
    }


    /**
      * Clears the value of the 'CR01' field.
      * @return This builder.
      */
    public org.uwh.IssuerRisk.Builder clearCR01() {
      fieldSetFlags()[4] = false;
      return this;
    }

    /**
      * Gets the value of the 'JTD' field.
      * @return The value.
      */
    public double getJTD() {
      return JTD;
    }


    /**
      * Sets the value of the 'JTD' field.
      * @param value The value of 'JTD'.
      * @return This builder.
      */
    public org.uwh.IssuerRisk.Builder setJTD(double value) {
      validate(fields()[5], value);
      this.JTD = value;
      fieldSetFlags()[5] = true;
      return this;
    }

    /**
      * Checks whether the 'JTD' field has been set.
      * @return True if the 'JTD' field has been set, false otherwise.
      */
    public boolean hasJTD() {
      return fieldSetFlags()[5];
    }


    /**
      * Clears the value of the 'JTD' field.
      * @return This builder.
      */
    public org.uwh.IssuerRisk.Builder clearJTD() {
      fieldSetFlags()[5] = false;
      return this;
    }

    /**
      * Gets the value of the 'JTDRolldown' field.
      * @return The value.
      */
    public java.util.List<org.uwh.RolldownItem> getJTDRolldown() {
      return JTDRolldown;
    }


    /**
      * Sets the value of the 'JTDRolldown' field.
      * @param value The value of 'JTDRolldown'.
      * @return This builder.
      */
    public org.uwh.IssuerRisk.Builder setJTDRolldown(java.util.List<org.uwh.RolldownItem> value) {
      validate(fields()[6], value);
      this.JTDRolldown = value;
      fieldSetFlags()[6] = true;
      return this;
    }

    /**
      * Checks whether the 'JTDRolldown' field has been set.
      * @return True if the 'JTDRolldown' field has been set, false otherwise.
      */
    public boolean hasJTDRolldown() {
      return fieldSetFlags()[6];
    }


    /**
      * Clears the value of the 'JTDRolldown' field.
      * @return This builder.
      */
    public org.uwh.IssuerRisk.Builder clearJTDRolldown() {
      JTDRolldown = null;
      fieldSetFlags()[6] = false;
      return this;
    }

    /**
      * Gets the value of the 'AuditDateTimeUTC' field.
      * @return The value.
      */
    public long getAuditDateTimeUTC() {
      return AuditDateTimeUTC;
    }


    /**
      * Sets the value of the 'AuditDateTimeUTC' field.
      * @param value The value of 'AuditDateTimeUTC'.
      * @return This builder.
      */
    public org.uwh.IssuerRisk.Builder setAuditDateTimeUTC(long value) {
      validate(fields()[7], value);
      this.AuditDateTimeUTC = value;
      fieldSetFlags()[7] = true;
      return this;
    }

    /**
      * Checks whether the 'AuditDateTimeUTC' field has been set.
      * @return True if the 'AuditDateTimeUTC' field has been set, false otherwise.
      */
    public boolean hasAuditDateTimeUTC() {
      return fieldSetFlags()[7];
    }


    /**
      * Clears the value of the 'AuditDateTimeUTC' field.
      * @return This builder.
      */
    public org.uwh.IssuerRisk.Builder clearAuditDateTimeUTC() {
      fieldSetFlags()[7] = false;
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public IssuerRisk build() {
      try {
        IssuerRisk record = new IssuerRisk();
        record.UIDType = fieldSetFlags()[0] ? this.UIDType : (org.uwh.UIDType) defaultValue(fields()[0]);
        record.UID = fieldSetFlags()[1] ? this.UID : (java.lang.String) defaultValue(fields()[1]);
        record.SMCI = fieldSetFlags()[2] ? this.SMCI : (java.lang.String) defaultValue(fields()[2]);
        record.BusinessDate = fieldSetFlags()[3] ? this.BusinessDate : (java.lang.String) defaultValue(fields()[3]);
        record.CR01 = fieldSetFlags()[4] ? this.CR01 : (java.lang.Double) defaultValue(fields()[4]);
        record.JTD = fieldSetFlags()[5] ? this.JTD : (java.lang.Double) defaultValue(fields()[5]);
        record.JTDRolldown = fieldSetFlags()[6] ? this.JTDRolldown : (java.util.List<org.uwh.RolldownItem>) defaultValue(fields()[6]);
        record.AuditDateTimeUTC = fieldSetFlags()[7] ? this.AuditDateTimeUTC : (java.lang.Long) defaultValue(fields()[7]);
        return record;
      } catch (org.apache.avro.AvroMissingFieldException e) {
        throw e;
      } catch (java.lang.Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumWriter<IssuerRisk>
    WRITER$ = (org.apache.avro.io.DatumWriter<IssuerRisk>)MODEL$.createDatumWriter(SCHEMA$);

  @Override public void writeExternal(java.io.ObjectOutput out)
    throws java.io.IOException {
    WRITER$.write(this, SpecificData.getEncoder(out));
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumReader<IssuerRisk>
    READER$ = (org.apache.avro.io.DatumReader<IssuerRisk>)MODEL$.createDatumReader(SCHEMA$);

  @Override public void readExternal(java.io.ObjectInput in)
    throws java.io.IOException {
    READER$.read(this, SpecificData.getDecoder(in));
  }

}










