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
public class FirmAccount extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = -8187569917887421122L;
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"FirmAccount\",\"namespace\":\"org.uwh\",\"fields\":[{\"name\":\"Mnemonic\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}},{\"name\":\"StrategyCode\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}},{\"name\":\"AuditDateTimeUTC\",\"type\":\"long\"}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }

  private static SpecificData MODEL$ = new SpecificData();

  private static final BinaryMessageEncoder<FirmAccount> ENCODER =
      new BinaryMessageEncoder<FirmAccount>(MODEL$, SCHEMA$);

  private static final BinaryMessageDecoder<FirmAccount> DECODER =
      new BinaryMessageDecoder<FirmAccount>(MODEL$, SCHEMA$);

  /**
   * Return the BinaryMessageEncoder instance used by this class.
   * @return the message encoder used by this class
   */
  public static BinaryMessageEncoder<FirmAccount> getEncoder() {
    return ENCODER;
  }

  /**
   * Return the BinaryMessageDecoder instance used by this class.
   * @return the message decoder used by this class
   */
  public static BinaryMessageDecoder<FirmAccount> getDecoder() {
    return DECODER;
  }

  /**
   * Create a new BinaryMessageDecoder instance for this class that uses the specified {@link SchemaStore}.
   * @param resolver a {@link SchemaStore} used to find schemas by fingerprint
   * @return a BinaryMessageDecoder instance for this class backed by the given SchemaStore
   */
  public static BinaryMessageDecoder<FirmAccount> createDecoder(SchemaStore resolver) {
    return new BinaryMessageDecoder<FirmAccount>(MODEL$, SCHEMA$, resolver);
  }

  /**
   * Serializes this FirmAccount to a ByteBuffer.
   * @return a buffer holding the serialized data for this instance
   * @throws java.io.IOException if this instance could not be serialized
   */
  public java.nio.ByteBuffer toByteBuffer() throws java.io.IOException {
    return ENCODER.encode(this);
  }

  /**
   * Deserializes a FirmAccount from a ByteBuffer.
   * @param b a byte buffer holding serialized data for an instance of this class
   * @return a FirmAccount instance decoded from the given buffer
   * @throws java.io.IOException if the given bytes could not be deserialized into an instance of this class
   */
  public static FirmAccount fromByteBuffer(
      java.nio.ByteBuffer b) throws java.io.IOException {
    return DECODER.decode(b);
  }

   private java.lang.String Mnemonic;
   private java.lang.String StrategyCode;
   private long AuditDateTimeUTC;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public FirmAccount() {}

  /**
   * All-args constructor.
   * @param Mnemonic The new value for Mnemonic
   * @param StrategyCode The new value for StrategyCode
   * @param AuditDateTimeUTC The new value for AuditDateTimeUTC
   */
  public FirmAccount(java.lang.String Mnemonic, java.lang.String StrategyCode, java.lang.Long AuditDateTimeUTC) {
    this.Mnemonic = Mnemonic;
    this.StrategyCode = StrategyCode;
    this.AuditDateTimeUTC = AuditDateTimeUTC;
  }

  public org.apache.avro.specific.SpecificData getSpecificData() { return MODEL$; }
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call.
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return Mnemonic;
    case 1: return StrategyCode;
    case 2: return AuditDateTimeUTC;
    default: throw new IndexOutOfBoundsException("Invalid index: " + field$);
    }
  }

  // Used by DatumReader.  Applications should not call.
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: Mnemonic = value$ != null ? value$.toString() : null; break;
    case 1: StrategyCode = value$ != null ? value$.toString() : null; break;
    case 2: AuditDateTimeUTC = (java.lang.Long)value$; break;
    default: throw new IndexOutOfBoundsException("Invalid index: " + field$);
    }
  }

  /**
   * Gets the value of the 'Mnemonic' field.
   * @return The value of the 'Mnemonic' field.
   */
  public java.lang.String getMnemonic() {
    return Mnemonic;
  }


  /**
   * Sets the value of the 'Mnemonic' field.
   * @param value the value to set.
   */
  public void setMnemonic(java.lang.String value) {
    this.Mnemonic = value;
  }

  /**
   * Gets the value of the 'StrategyCode' field.
   * @return The value of the 'StrategyCode' field.
   */
  public java.lang.String getStrategyCode() {
    return StrategyCode;
  }


  /**
   * Sets the value of the 'StrategyCode' field.
   * @param value the value to set.
   */
  public void setStrategyCode(java.lang.String value) {
    this.StrategyCode = value;
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
   * Creates a new FirmAccount RecordBuilder.
   * @return A new FirmAccount RecordBuilder
   */
  public static org.uwh.FirmAccount.Builder newBuilder() {
    return new org.uwh.FirmAccount.Builder();
  }

  /**
   * Creates a new FirmAccount RecordBuilder by copying an existing Builder.
   * @param other The existing builder to copy.
   * @return A new FirmAccount RecordBuilder
   */
  public static org.uwh.FirmAccount.Builder newBuilder(org.uwh.FirmAccount.Builder other) {
    if (other == null) {
      return new org.uwh.FirmAccount.Builder();
    } else {
      return new org.uwh.FirmAccount.Builder(other);
    }
  }

  /**
   * Creates a new FirmAccount RecordBuilder by copying an existing FirmAccount instance.
   * @param other The existing instance to copy.
   * @return A new FirmAccount RecordBuilder
   */
  public static org.uwh.FirmAccount.Builder newBuilder(org.uwh.FirmAccount other) {
    if (other == null) {
      return new org.uwh.FirmAccount.Builder();
    } else {
      return new org.uwh.FirmAccount.Builder(other);
    }
  }

  /**
   * RecordBuilder for FirmAccount instances.
   */
  @org.apache.avro.specific.AvroGenerated
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<FirmAccount>
    implements org.apache.avro.data.RecordBuilder<FirmAccount> {

    private java.lang.String Mnemonic;
    private java.lang.String StrategyCode;
    private long AuditDateTimeUTC;

    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     * @param other The existing Builder to copy.
     */
    private Builder(org.uwh.FirmAccount.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.Mnemonic)) {
        this.Mnemonic = data().deepCopy(fields()[0].schema(), other.Mnemonic);
        fieldSetFlags()[0] = other.fieldSetFlags()[0];
      }
      if (isValidValue(fields()[1], other.StrategyCode)) {
        this.StrategyCode = data().deepCopy(fields()[1].schema(), other.StrategyCode);
        fieldSetFlags()[1] = other.fieldSetFlags()[1];
      }
      if (isValidValue(fields()[2], other.AuditDateTimeUTC)) {
        this.AuditDateTimeUTC = data().deepCopy(fields()[2].schema(), other.AuditDateTimeUTC);
        fieldSetFlags()[2] = other.fieldSetFlags()[2];
      }
    }

    /**
     * Creates a Builder by copying an existing FirmAccount instance
     * @param other The existing instance to copy.
     */
    private Builder(org.uwh.FirmAccount other) {
      super(SCHEMA$);
      if (isValidValue(fields()[0], other.Mnemonic)) {
        this.Mnemonic = data().deepCopy(fields()[0].schema(), other.Mnemonic);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.StrategyCode)) {
        this.StrategyCode = data().deepCopy(fields()[1].schema(), other.StrategyCode);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.AuditDateTimeUTC)) {
        this.AuditDateTimeUTC = data().deepCopy(fields()[2].schema(), other.AuditDateTimeUTC);
        fieldSetFlags()[2] = true;
      }
    }

    /**
      * Gets the value of the 'Mnemonic' field.
      * @return The value.
      */
    public java.lang.String getMnemonic() {
      return Mnemonic;
    }


    /**
      * Sets the value of the 'Mnemonic' field.
      * @param value The value of 'Mnemonic'.
      * @return This builder.
      */
    public org.uwh.FirmAccount.Builder setMnemonic(java.lang.String value) {
      validate(fields()[0], value);
      this.Mnemonic = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    /**
      * Checks whether the 'Mnemonic' field has been set.
      * @return True if the 'Mnemonic' field has been set, false otherwise.
      */
    public boolean hasMnemonic() {
      return fieldSetFlags()[0];
    }


    /**
      * Clears the value of the 'Mnemonic' field.
      * @return This builder.
      */
    public org.uwh.FirmAccount.Builder clearMnemonic() {
      Mnemonic = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /**
      * Gets the value of the 'StrategyCode' field.
      * @return The value.
      */
    public java.lang.String getStrategyCode() {
      return StrategyCode;
    }


    /**
      * Sets the value of the 'StrategyCode' field.
      * @param value The value of 'StrategyCode'.
      * @return This builder.
      */
    public org.uwh.FirmAccount.Builder setStrategyCode(java.lang.String value) {
      validate(fields()[1], value);
      this.StrategyCode = value;
      fieldSetFlags()[1] = true;
      return this;
    }

    /**
      * Checks whether the 'StrategyCode' field has been set.
      * @return True if the 'StrategyCode' field has been set, false otherwise.
      */
    public boolean hasStrategyCode() {
      return fieldSetFlags()[1];
    }


    /**
      * Clears the value of the 'StrategyCode' field.
      * @return This builder.
      */
    public org.uwh.FirmAccount.Builder clearStrategyCode() {
      StrategyCode = null;
      fieldSetFlags()[1] = false;
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
    public org.uwh.FirmAccount.Builder setAuditDateTimeUTC(long value) {
      validate(fields()[2], value);
      this.AuditDateTimeUTC = value;
      fieldSetFlags()[2] = true;
      return this;
    }

    /**
      * Checks whether the 'AuditDateTimeUTC' field has been set.
      * @return True if the 'AuditDateTimeUTC' field has been set, false otherwise.
      */
    public boolean hasAuditDateTimeUTC() {
      return fieldSetFlags()[2];
    }


    /**
      * Clears the value of the 'AuditDateTimeUTC' field.
      * @return This builder.
      */
    public org.uwh.FirmAccount.Builder clearAuditDateTimeUTC() {
      fieldSetFlags()[2] = false;
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public FirmAccount build() {
      try {
        FirmAccount record = new FirmAccount();
        record.Mnemonic = fieldSetFlags()[0] ? this.Mnemonic : (java.lang.String) defaultValue(fields()[0]);
        record.StrategyCode = fieldSetFlags()[1] ? this.StrategyCode : (java.lang.String) defaultValue(fields()[1]);
        record.AuditDateTimeUTC = fieldSetFlags()[2] ? this.AuditDateTimeUTC : (java.lang.Long) defaultValue(fields()[2]);
        return record;
      } catch (org.apache.avro.AvroMissingFieldException e) {
        throw e;
      } catch (java.lang.Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumWriter<FirmAccount>
    WRITER$ = (org.apache.avro.io.DatumWriter<FirmAccount>)MODEL$.createDatumWriter(SCHEMA$);

  @Override public void writeExternal(java.io.ObjectOutput out)
    throws java.io.IOException {
    WRITER$.write(this, SpecificData.getEncoder(out));
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumReader<FirmAccount>
    READER$ = (org.apache.avro.io.DatumReader<FirmAccount>)MODEL$.createDatumReader(SCHEMA$);

  @Override public void readExternal(java.io.ObjectInput in)
    throws java.io.IOException {
    READER$.read(this, SpecificData.getDecoder(in));
  }

  @Override protected boolean hasCustomCoders() { return true; }

  @Override public void customEncode(org.apache.avro.io.Encoder out)
    throws java.io.IOException
  {
    out.writeString(this.Mnemonic);

    out.writeString(this.StrategyCode);

    out.writeLong(this.AuditDateTimeUTC);

  }

  @Override public void customDecode(org.apache.avro.io.ResolvingDecoder in)
    throws java.io.IOException
  {
    org.apache.avro.Schema.Field[] fieldOrder = in.readFieldOrderIfDiff();
    if (fieldOrder == null) {
      this.Mnemonic = in.readString();

      this.StrategyCode = in.readString();

      this.AuditDateTimeUTC = in.readLong();

    } else {
      for (int i = 0; i < 3; i++) {
        switch (fieldOrder[i].pos()) {
        case 0:
          this.Mnemonic = in.readString();
          break;

        case 1:
          this.StrategyCode = in.readString();
          break;

        case 2:
          this.AuditDateTimeUTC = in.readLong();
          break;

        default:
          throw new java.io.IOException("Corrupt ResolvingDecoder.");
        }
      }
    }
  }
}










