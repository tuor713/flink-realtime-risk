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
public class RolldownItem extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = -8396342249704156444L;
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"RolldownItem\",\"namespace\":\"org.uwh\",\"fields\":[{\"name\":\"Date\",\"type\":{\"type\":\"int\",\"logicalType\":\"date\"}},{\"name\":\"JTD\",\"type\":\"double\"}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }

  private static SpecificData MODEL$ = new SpecificData();
static {
    MODEL$.addLogicalTypeConversion(new org.apache.avro.data.TimeConversions.DateConversion());
  }

  private static final BinaryMessageEncoder<RolldownItem> ENCODER =
      new BinaryMessageEncoder<RolldownItem>(MODEL$, SCHEMA$);

  private static final BinaryMessageDecoder<RolldownItem> DECODER =
      new BinaryMessageDecoder<RolldownItem>(MODEL$, SCHEMA$);

  /**
   * Return the BinaryMessageEncoder instance used by this class.
   * @return the message encoder used by this class
   */
  public static BinaryMessageEncoder<RolldownItem> getEncoder() {
    return ENCODER;
  }

  /**
   * Return the BinaryMessageDecoder instance used by this class.
   * @return the message decoder used by this class
   */
  public static BinaryMessageDecoder<RolldownItem> getDecoder() {
    return DECODER;
  }

  /**
   * Create a new BinaryMessageDecoder instance for this class that uses the specified {@link SchemaStore}.
   * @param resolver a {@link SchemaStore} used to find schemas by fingerprint
   * @return a BinaryMessageDecoder instance for this class backed by the given SchemaStore
   */
  public static BinaryMessageDecoder<RolldownItem> createDecoder(SchemaStore resolver) {
    return new BinaryMessageDecoder<RolldownItem>(MODEL$, SCHEMA$, resolver);
  }

  /**
   * Serializes this RolldownItem to a ByteBuffer.
   * @return a buffer holding the serialized data for this instance
   * @throws java.io.IOException if this instance could not be serialized
   */
  public java.nio.ByteBuffer toByteBuffer() throws java.io.IOException {
    return ENCODER.encode(this);
  }

  /**
   * Deserializes a RolldownItem from a ByteBuffer.
   * @param b a byte buffer holding serialized data for an instance of this class
   * @return a RolldownItem instance decoded from the given buffer
   * @throws java.io.IOException if the given bytes could not be deserialized into an instance of this class
   */
  public static RolldownItem fromByteBuffer(
      java.nio.ByteBuffer b) throws java.io.IOException {
    return DECODER.decode(b);
  }

   private java.time.LocalDate Date;
   private double JTD;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public RolldownItem() {}

  /**
   * All-args constructor.
   * @param Date The new value for Date
   * @param JTD The new value for JTD
   */
  public RolldownItem(java.time.LocalDate Date, java.lang.Double JTD) {
    this.Date = Date;
    this.JTD = JTD;
  }

  public org.apache.avro.specific.SpecificData getSpecificData() { return MODEL$; }
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call.
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return Date;
    case 1: return JTD;
    default: throw new IndexOutOfBoundsException("Invalid index: " + field$);
    }
  }

  private static final org.apache.avro.Conversion<?>[] conversions =
      new org.apache.avro.Conversion<?>[] {
      new org.apache.avro.data.TimeConversions.DateConversion(),
      null,
      null
  };

  @Override
  public org.apache.avro.Conversion<?> getConversion(int field) {
    return conversions[field];
  }

  // Used by DatumReader.  Applications should not call.
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: Date = (java.time.LocalDate)value$; break;
    case 1: JTD = (java.lang.Double)value$; break;
    default: throw new IndexOutOfBoundsException("Invalid index: " + field$);
    }
  }

  /**
   * Gets the value of the 'Date' field.
   * @return The value of the 'Date' field.
   */
  public java.time.LocalDate getDate() {
    return Date;
  }


  /**
   * Sets the value of the 'Date' field.
   * @param value the value to set.
   */
  public void setDate(java.time.LocalDate value) {
    this.Date = value;
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
   * Creates a new RolldownItem RecordBuilder.
   * @return A new RolldownItem RecordBuilder
   */
  public static org.uwh.RolldownItem.Builder newBuilder() {
    return new org.uwh.RolldownItem.Builder();
  }

  /**
   * Creates a new RolldownItem RecordBuilder by copying an existing Builder.
   * @param other The existing builder to copy.
   * @return A new RolldownItem RecordBuilder
   */
  public static org.uwh.RolldownItem.Builder newBuilder(org.uwh.RolldownItem.Builder other) {
    if (other == null) {
      return new org.uwh.RolldownItem.Builder();
    } else {
      return new org.uwh.RolldownItem.Builder(other);
    }
  }

  /**
   * Creates a new RolldownItem RecordBuilder by copying an existing RolldownItem instance.
   * @param other The existing instance to copy.
   * @return A new RolldownItem RecordBuilder
   */
  public static org.uwh.RolldownItem.Builder newBuilder(org.uwh.RolldownItem other) {
    if (other == null) {
      return new org.uwh.RolldownItem.Builder();
    } else {
      return new org.uwh.RolldownItem.Builder(other);
    }
  }

  /**
   * RecordBuilder for RolldownItem instances.
   */
  @org.apache.avro.specific.AvroGenerated
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<RolldownItem>
    implements org.apache.avro.data.RecordBuilder<RolldownItem> {

    private java.time.LocalDate Date;
    private double JTD;

    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     * @param other The existing Builder to copy.
     */
    private Builder(org.uwh.RolldownItem.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.Date)) {
        this.Date = data().deepCopy(fields()[0].schema(), other.Date);
        fieldSetFlags()[0] = other.fieldSetFlags()[0];
      }
      if (isValidValue(fields()[1], other.JTD)) {
        this.JTD = data().deepCopy(fields()[1].schema(), other.JTD);
        fieldSetFlags()[1] = other.fieldSetFlags()[1];
      }
    }

    /**
     * Creates a Builder by copying an existing RolldownItem instance
     * @param other The existing instance to copy.
     */
    private Builder(org.uwh.RolldownItem other) {
      super(SCHEMA$);
      if (isValidValue(fields()[0], other.Date)) {
        this.Date = data().deepCopy(fields()[0].schema(), other.Date);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.JTD)) {
        this.JTD = data().deepCopy(fields()[1].schema(), other.JTD);
        fieldSetFlags()[1] = true;
      }
    }

    /**
      * Gets the value of the 'Date' field.
      * @return The value.
      */
    public java.time.LocalDate getDate() {
      return Date;
    }


    /**
      * Sets the value of the 'Date' field.
      * @param value The value of 'Date'.
      * @return This builder.
      */
    public org.uwh.RolldownItem.Builder setDate(java.time.LocalDate value) {
      validate(fields()[0], value);
      this.Date = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    /**
      * Checks whether the 'Date' field has been set.
      * @return True if the 'Date' field has been set, false otherwise.
      */
    public boolean hasDate() {
      return fieldSetFlags()[0];
    }


    /**
      * Clears the value of the 'Date' field.
      * @return This builder.
      */
    public org.uwh.RolldownItem.Builder clearDate() {
      fieldSetFlags()[0] = false;
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
    public org.uwh.RolldownItem.Builder setJTD(double value) {
      validate(fields()[1], value);
      this.JTD = value;
      fieldSetFlags()[1] = true;
      return this;
    }

    /**
      * Checks whether the 'JTD' field has been set.
      * @return True if the 'JTD' field has been set, false otherwise.
      */
    public boolean hasJTD() {
      return fieldSetFlags()[1];
    }


    /**
      * Clears the value of the 'JTD' field.
      * @return This builder.
      */
    public org.uwh.RolldownItem.Builder clearJTD() {
      fieldSetFlags()[1] = false;
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public RolldownItem build() {
      try {
        RolldownItem record = new RolldownItem();
        record.Date = fieldSetFlags()[0] ? this.Date : (java.time.LocalDate) defaultValue(fields()[0]);
        record.JTD = fieldSetFlags()[1] ? this.JTD : (java.lang.Double) defaultValue(fields()[1]);
        return record;
      } catch (org.apache.avro.AvroMissingFieldException e) {
        throw e;
      } catch (java.lang.Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumWriter<RolldownItem>
    WRITER$ = (org.apache.avro.io.DatumWriter<RolldownItem>)MODEL$.createDatumWriter(SCHEMA$);

  @Override public void writeExternal(java.io.ObjectOutput out)
    throws java.io.IOException {
    WRITER$.write(this, SpecificData.getEncoder(out));
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumReader<RolldownItem>
    READER$ = (org.apache.avro.io.DatumReader<RolldownItem>)MODEL$.createDatumReader(SCHEMA$);

  @Override public void readExternal(java.io.ObjectInput in)
    throws java.io.IOException {
    READER$.read(this, SpecificData.getDecoder(in));
  }

}










