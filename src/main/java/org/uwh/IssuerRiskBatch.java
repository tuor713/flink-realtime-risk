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
public class IssuerRiskBatch extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = 2488703313857040462L;
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"IssuerRiskBatch\",\"namespace\":\"org.uwh\",\"fields\":[{\"name\":\"UIDType\",\"type\":{\"type\":\"enum\",\"name\":\"UIDType\",\"symbols\":[\"UIPID\",\"UITID\"]}},{\"name\":\"UID\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}},{\"name\":\"BusinessDate\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}},{\"name\":\"AuditDateTimeUTC\",\"type\":\"long\"},{\"name\":\"risk\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"IssuerRiskLine\",\"fields\":[{\"name\":\"SMCI\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}},{\"name\":\"CR01\",\"type\":\"double\"},{\"name\":\"JTD\",\"type\":\"double\"},{\"name\":\"JTDRolldown\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"RolldownItem\",\"fields\":[{\"name\":\"Date\",\"type\":\"int\"},{\"name\":\"JTD\",\"type\":\"double\"}]}}}]}}}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }

  private static SpecificData MODEL$ = new SpecificData();

  private static final BinaryMessageEncoder<IssuerRiskBatch> ENCODER =
      new BinaryMessageEncoder<IssuerRiskBatch>(MODEL$, SCHEMA$);

  private static final BinaryMessageDecoder<IssuerRiskBatch> DECODER =
      new BinaryMessageDecoder<IssuerRiskBatch>(MODEL$, SCHEMA$);

  /**
   * Return the BinaryMessageEncoder instance used by this class.
   * @return the message encoder used by this class
   */
  public static BinaryMessageEncoder<IssuerRiskBatch> getEncoder() {
    return ENCODER;
  }

  /**
   * Return the BinaryMessageDecoder instance used by this class.
   * @return the message decoder used by this class
   */
  public static BinaryMessageDecoder<IssuerRiskBatch> getDecoder() {
    return DECODER;
  }

  /**
   * Create a new BinaryMessageDecoder instance for this class that uses the specified {@link SchemaStore}.
   * @param resolver a {@link SchemaStore} used to find schemas by fingerprint
   * @return a BinaryMessageDecoder instance for this class backed by the given SchemaStore
   */
  public static BinaryMessageDecoder<IssuerRiskBatch> createDecoder(SchemaStore resolver) {
    return new BinaryMessageDecoder<IssuerRiskBatch>(MODEL$, SCHEMA$, resolver);
  }

  /**
   * Serializes this IssuerRiskBatch to a ByteBuffer.
   * @return a buffer holding the serialized data for this instance
   * @throws java.io.IOException if this instance could not be serialized
   */
  public java.nio.ByteBuffer toByteBuffer() throws java.io.IOException {
    return ENCODER.encode(this);
  }

  /**
   * Deserializes a IssuerRiskBatch from a ByteBuffer.
   * @param b a byte buffer holding serialized data for an instance of this class
   * @return a IssuerRiskBatch instance decoded from the given buffer
   * @throws java.io.IOException if the given bytes could not be deserialized into an instance of this class
   */
  public static IssuerRiskBatch fromByteBuffer(
      java.nio.ByteBuffer b) throws java.io.IOException {
    return DECODER.decode(b);
  }

   private org.uwh.UIDType UIDType;
   private java.lang.String UID;
   private java.lang.String BusinessDate;
   private long AuditDateTimeUTC;
   private java.util.List<org.uwh.IssuerRiskLine> risk;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public IssuerRiskBatch() {}

  /**
   * All-args constructor.
   * @param UIDType The new value for UIDType
   * @param UID The new value for UID
   * @param BusinessDate The new value for BusinessDate
   * @param AuditDateTimeUTC The new value for AuditDateTimeUTC
   * @param risk The new value for risk
   */
  public IssuerRiskBatch(org.uwh.UIDType UIDType, java.lang.String UID, java.lang.String BusinessDate, java.lang.Long AuditDateTimeUTC, java.util.List<org.uwh.IssuerRiskLine> risk) {
    this.UIDType = UIDType;
    this.UID = UID;
    this.BusinessDate = BusinessDate;
    this.AuditDateTimeUTC = AuditDateTimeUTC;
    this.risk = risk;
  }

  public org.apache.avro.specific.SpecificData getSpecificData() { return MODEL$; }
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call.
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return UIDType;
    case 1: return UID;
    case 2: return BusinessDate;
    case 3: return AuditDateTimeUTC;
    case 4: return risk;
    default: throw new IndexOutOfBoundsException("Invalid index: " + field$);
    }
  }

  // Used by DatumReader.  Applications should not call.
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: UIDType = (org.uwh.UIDType)value$; break;
    case 1: UID = value$ != null ? value$.toString() : null; break;
    case 2: BusinessDate = value$ != null ? value$.toString() : null; break;
    case 3: AuditDateTimeUTC = (java.lang.Long)value$; break;
    case 4: risk = (java.util.List<org.uwh.IssuerRiskLine>)value$; break;
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
   * Gets the value of the 'risk' field.
   * @return The value of the 'risk' field.
   */
  public java.util.List<org.uwh.IssuerRiskLine> getRisk() {
    return risk;
  }


  /**
   * Sets the value of the 'risk' field.
   * @param value the value to set.
   */
  public void setRisk(java.util.List<org.uwh.IssuerRiskLine> value) {
    this.risk = value;
  }

  /**
   * Creates a new IssuerRiskBatch RecordBuilder.
   * @return A new IssuerRiskBatch RecordBuilder
   */
  public static org.uwh.IssuerRiskBatch.Builder newBuilder() {
    return new org.uwh.IssuerRiskBatch.Builder();
  }

  /**
   * Creates a new IssuerRiskBatch RecordBuilder by copying an existing Builder.
   * @param other The existing builder to copy.
   * @return A new IssuerRiskBatch RecordBuilder
   */
  public static org.uwh.IssuerRiskBatch.Builder newBuilder(org.uwh.IssuerRiskBatch.Builder other) {
    if (other == null) {
      return new org.uwh.IssuerRiskBatch.Builder();
    } else {
      return new org.uwh.IssuerRiskBatch.Builder(other);
    }
  }

  /**
   * Creates a new IssuerRiskBatch RecordBuilder by copying an existing IssuerRiskBatch instance.
   * @param other The existing instance to copy.
   * @return A new IssuerRiskBatch RecordBuilder
   */
  public static org.uwh.IssuerRiskBatch.Builder newBuilder(org.uwh.IssuerRiskBatch other) {
    if (other == null) {
      return new org.uwh.IssuerRiskBatch.Builder();
    } else {
      return new org.uwh.IssuerRiskBatch.Builder(other);
    }
  }

  /**
   * RecordBuilder for IssuerRiskBatch instances.
   */
  @org.apache.avro.specific.AvroGenerated
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<IssuerRiskBatch>
    implements org.apache.avro.data.RecordBuilder<IssuerRiskBatch> {

    private org.uwh.UIDType UIDType;
    private java.lang.String UID;
    private java.lang.String BusinessDate;
    private long AuditDateTimeUTC;
    private java.util.List<org.uwh.IssuerRiskLine> risk;

    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     * @param other The existing Builder to copy.
     */
    private Builder(org.uwh.IssuerRiskBatch.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.UIDType)) {
        this.UIDType = data().deepCopy(fields()[0].schema(), other.UIDType);
        fieldSetFlags()[0] = other.fieldSetFlags()[0];
      }
      if (isValidValue(fields()[1], other.UID)) {
        this.UID = data().deepCopy(fields()[1].schema(), other.UID);
        fieldSetFlags()[1] = other.fieldSetFlags()[1];
      }
      if (isValidValue(fields()[2], other.BusinessDate)) {
        this.BusinessDate = data().deepCopy(fields()[2].schema(), other.BusinessDate);
        fieldSetFlags()[2] = other.fieldSetFlags()[2];
      }
      if (isValidValue(fields()[3], other.AuditDateTimeUTC)) {
        this.AuditDateTimeUTC = data().deepCopy(fields()[3].schema(), other.AuditDateTimeUTC);
        fieldSetFlags()[3] = other.fieldSetFlags()[3];
      }
      if (isValidValue(fields()[4], other.risk)) {
        this.risk = data().deepCopy(fields()[4].schema(), other.risk);
        fieldSetFlags()[4] = other.fieldSetFlags()[4];
      }
    }

    /**
     * Creates a Builder by copying an existing IssuerRiskBatch instance
     * @param other The existing instance to copy.
     */
    private Builder(org.uwh.IssuerRiskBatch other) {
      super(SCHEMA$);
      if (isValidValue(fields()[0], other.UIDType)) {
        this.UIDType = data().deepCopy(fields()[0].schema(), other.UIDType);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.UID)) {
        this.UID = data().deepCopy(fields()[1].schema(), other.UID);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.BusinessDate)) {
        this.BusinessDate = data().deepCopy(fields()[2].schema(), other.BusinessDate);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.AuditDateTimeUTC)) {
        this.AuditDateTimeUTC = data().deepCopy(fields()[3].schema(), other.AuditDateTimeUTC);
        fieldSetFlags()[3] = true;
      }
      if (isValidValue(fields()[4], other.risk)) {
        this.risk = data().deepCopy(fields()[4].schema(), other.risk);
        fieldSetFlags()[4] = true;
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
    public org.uwh.IssuerRiskBatch.Builder setUIDType(org.uwh.UIDType value) {
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
    public org.uwh.IssuerRiskBatch.Builder clearUIDType() {
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
    public org.uwh.IssuerRiskBatch.Builder setUID(java.lang.String value) {
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
    public org.uwh.IssuerRiskBatch.Builder clearUID() {
      UID = null;
      fieldSetFlags()[1] = false;
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
    public org.uwh.IssuerRiskBatch.Builder setBusinessDate(java.lang.String value) {
      validate(fields()[2], value);
      this.BusinessDate = value;
      fieldSetFlags()[2] = true;
      return this;
    }

    /**
      * Checks whether the 'BusinessDate' field has been set.
      * @return True if the 'BusinessDate' field has been set, false otherwise.
      */
    public boolean hasBusinessDate() {
      return fieldSetFlags()[2];
    }


    /**
      * Clears the value of the 'BusinessDate' field.
      * @return This builder.
      */
    public org.uwh.IssuerRiskBatch.Builder clearBusinessDate() {
      BusinessDate = null;
      fieldSetFlags()[2] = false;
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
    public org.uwh.IssuerRiskBatch.Builder setAuditDateTimeUTC(long value) {
      validate(fields()[3], value);
      this.AuditDateTimeUTC = value;
      fieldSetFlags()[3] = true;
      return this;
    }

    /**
      * Checks whether the 'AuditDateTimeUTC' field has been set.
      * @return True if the 'AuditDateTimeUTC' field has been set, false otherwise.
      */
    public boolean hasAuditDateTimeUTC() {
      return fieldSetFlags()[3];
    }


    /**
      * Clears the value of the 'AuditDateTimeUTC' field.
      * @return This builder.
      */
    public org.uwh.IssuerRiskBatch.Builder clearAuditDateTimeUTC() {
      fieldSetFlags()[3] = false;
      return this;
    }

    /**
      * Gets the value of the 'risk' field.
      * @return The value.
      */
    public java.util.List<org.uwh.IssuerRiskLine> getRisk() {
      return risk;
    }


    /**
      * Sets the value of the 'risk' field.
      * @param value The value of 'risk'.
      * @return This builder.
      */
    public org.uwh.IssuerRiskBatch.Builder setRisk(java.util.List<org.uwh.IssuerRiskLine> value) {
      validate(fields()[4], value);
      this.risk = value;
      fieldSetFlags()[4] = true;
      return this;
    }

    /**
      * Checks whether the 'risk' field has been set.
      * @return True if the 'risk' field has been set, false otherwise.
      */
    public boolean hasRisk() {
      return fieldSetFlags()[4];
    }


    /**
      * Clears the value of the 'risk' field.
      * @return This builder.
      */
    public org.uwh.IssuerRiskBatch.Builder clearRisk() {
      risk = null;
      fieldSetFlags()[4] = false;
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public IssuerRiskBatch build() {
      try {
        IssuerRiskBatch record = new IssuerRiskBatch();
        record.UIDType = fieldSetFlags()[0] ? this.UIDType : (org.uwh.UIDType) defaultValue(fields()[0]);
        record.UID = fieldSetFlags()[1] ? this.UID : (java.lang.String) defaultValue(fields()[1]);
        record.BusinessDate = fieldSetFlags()[2] ? this.BusinessDate : (java.lang.String) defaultValue(fields()[2]);
        record.AuditDateTimeUTC = fieldSetFlags()[3] ? this.AuditDateTimeUTC : (java.lang.Long) defaultValue(fields()[3]);
        record.risk = fieldSetFlags()[4] ? this.risk : (java.util.List<org.uwh.IssuerRiskLine>) defaultValue(fields()[4]);
        return record;
      } catch (org.apache.avro.AvroMissingFieldException e) {
        throw e;
      } catch (java.lang.Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumWriter<IssuerRiskBatch>
    WRITER$ = (org.apache.avro.io.DatumWriter<IssuerRiskBatch>)MODEL$.createDatumWriter(SCHEMA$);

  @Override public void writeExternal(java.io.ObjectOutput out)
    throws java.io.IOException {
    WRITER$.write(this, SpecificData.getEncoder(out));
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumReader<IssuerRiskBatch>
    READER$ = (org.apache.avro.io.DatumReader<IssuerRiskBatch>)MODEL$.createDatumReader(SCHEMA$);

  @Override public void readExternal(java.io.ObjectInput in)
    throws java.io.IOException {
    READER$.read(this, SpecificData.getDecoder(in));
  }

  @Override protected boolean hasCustomCoders() { return true; }

  @Override public void customEncode(org.apache.avro.io.Encoder out)
    throws java.io.IOException
  {
    out.writeEnum(this.UIDType.ordinal());

    out.writeString(this.UID);

    out.writeString(this.BusinessDate);

    out.writeLong(this.AuditDateTimeUTC);

    long size0 = this.risk.size();
    out.writeArrayStart();
    out.setItemCount(size0);
    long actualSize0 = 0;
    for (org.uwh.IssuerRiskLine e0: this.risk) {
      actualSize0++;
      out.startItem();
      e0.customEncode(out);
    }
    out.writeArrayEnd();
    if (actualSize0 != size0)
      throw new java.util.ConcurrentModificationException("Array-size written was " + size0 + ", but element count was " + actualSize0 + ".");

  }

  @Override public void customDecode(org.apache.avro.io.ResolvingDecoder in)
    throws java.io.IOException
  {
    org.apache.avro.Schema.Field[] fieldOrder = in.readFieldOrderIfDiff();
    if (fieldOrder == null) {
      this.UIDType = org.uwh.UIDType.values()[in.readEnum()];

      this.UID = in.readString();

      this.BusinessDate = in.readString();

      this.AuditDateTimeUTC = in.readLong();

      long size0 = in.readArrayStart();
      java.util.List<org.uwh.IssuerRiskLine> a0 = this.risk;
      if (a0 == null) {
        a0 = new SpecificData.Array<org.uwh.IssuerRiskLine>((int)size0, SCHEMA$.getField("risk").schema());
        this.risk = a0;
      } else a0.clear();
      SpecificData.Array<org.uwh.IssuerRiskLine> ga0 = (a0 instanceof SpecificData.Array ? (SpecificData.Array<org.uwh.IssuerRiskLine>)a0 : null);
      for ( ; 0 < size0; size0 = in.arrayNext()) {
        for ( ; size0 != 0; size0--) {
          org.uwh.IssuerRiskLine e0 = (ga0 != null ? ga0.peek() : null);
          if (e0 == null) {
            e0 = new org.uwh.IssuerRiskLine();
          }
          e0.customDecode(in);
          a0.add(e0);
        }
      }

    } else {
      for (int i = 0; i < 5; i++) {
        switch (fieldOrder[i].pos()) {
        case 0:
          this.UIDType = org.uwh.UIDType.values()[in.readEnum()];
          break;

        case 1:
          this.UID = in.readString();
          break;

        case 2:
          this.BusinessDate = in.readString();
          break;

        case 3:
          this.AuditDateTimeUTC = in.readLong();
          break;

        case 4:
          long size0 = in.readArrayStart();
          java.util.List<org.uwh.IssuerRiskLine> a0 = this.risk;
          if (a0 == null) {
            a0 = new SpecificData.Array<org.uwh.IssuerRiskLine>((int)size0, SCHEMA$.getField("risk").schema());
            this.risk = a0;
          } else a0.clear();
          SpecificData.Array<org.uwh.IssuerRiskLine> ga0 = (a0 instanceof SpecificData.Array ? (SpecificData.Array<org.uwh.IssuerRiskLine>)a0 : null);
          for ( ; 0 < size0; size0 = in.arrayNext()) {
            for ( ; size0 != 0; size0--) {
              org.uwh.IssuerRiskLine e0 = (ga0 != null ? ga0.peek() : null);
              if (e0 == null) {
                e0 = new org.uwh.IssuerRiskLine();
              }
              e0.customDecode(in);
              a0.add(e0);
            }
          }
          break;

        default:
          throw new java.io.IOException("Corrupt ResolvingDecoder.");
        }
      }
    }
  }
}










