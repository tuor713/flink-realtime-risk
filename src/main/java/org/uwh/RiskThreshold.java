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
public class RiskThreshold extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = -5269254478072329145L;
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"RiskThreshold\",\"namespace\":\"org.uwh\",\"fields\":[{\"name\":\"RiskFactorType\",\"type\":{\"type\":\"enum\",\"name\":\"RiskFactorType\",\"symbols\":[\"Issuer\"]}},{\"name\":\"RiskFactor\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}},{\"name\":\"Thresholds\",\"type\":{\"type\":\"map\",\"values\":\"double\",\"avro.java.string\":\"String\"}}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }

  private static SpecificData MODEL$ = new SpecificData();

  private static final BinaryMessageEncoder<RiskThreshold> ENCODER =
      new BinaryMessageEncoder<RiskThreshold>(MODEL$, SCHEMA$);

  private static final BinaryMessageDecoder<RiskThreshold> DECODER =
      new BinaryMessageDecoder<RiskThreshold>(MODEL$, SCHEMA$);

  /**
   * Return the BinaryMessageEncoder instance used by this class.
   * @return the message encoder used by this class
   */
  public static BinaryMessageEncoder<RiskThreshold> getEncoder() {
    return ENCODER;
  }

  /**
   * Return the BinaryMessageDecoder instance used by this class.
   * @return the message decoder used by this class
   */
  public static BinaryMessageDecoder<RiskThreshold> getDecoder() {
    return DECODER;
  }

  /**
   * Create a new BinaryMessageDecoder instance for this class that uses the specified {@link SchemaStore}.
   * @param resolver a {@link SchemaStore} used to find schemas by fingerprint
   * @return a BinaryMessageDecoder instance for this class backed by the given SchemaStore
   */
  public static BinaryMessageDecoder<RiskThreshold> createDecoder(SchemaStore resolver) {
    return new BinaryMessageDecoder<RiskThreshold>(MODEL$, SCHEMA$, resolver);
  }

  /**
   * Serializes this RiskThreshold to a ByteBuffer.
   * @return a buffer holding the serialized data for this instance
   * @throws java.io.IOException if this instance could not be serialized
   */
  public java.nio.ByteBuffer toByteBuffer() throws java.io.IOException {
    return ENCODER.encode(this);
  }

  /**
   * Deserializes a RiskThreshold from a ByteBuffer.
   * @param b a byte buffer holding serialized data for an instance of this class
   * @return a RiskThreshold instance decoded from the given buffer
   * @throws java.io.IOException if the given bytes could not be deserialized into an instance of this class
   */
  public static RiskThreshold fromByteBuffer(
      java.nio.ByteBuffer b) throws java.io.IOException {
    return DECODER.decode(b);
  }

   private org.uwh.RiskFactorType RiskFactorType;
   private java.lang.String RiskFactor;
   private java.util.Map<java.lang.String,java.lang.Double> Thresholds;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public RiskThreshold() {}

  /**
   * All-args constructor.
   * @param RiskFactorType The new value for RiskFactorType
   * @param RiskFactor The new value for RiskFactor
   * @param Thresholds The new value for Thresholds
   */
  public RiskThreshold(org.uwh.RiskFactorType RiskFactorType, java.lang.String RiskFactor, java.util.Map<java.lang.String,java.lang.Double> Thresholds) {
    this.RiskFactorType = RiskFactorType;
    this.RiskFactor = RiskFactor;
    this.Thresholds = Thresholds;
  }

  public org.apache.avro.specific.SpecificData getSpecificData() { return MODEL$; }
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call.
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return RiskFactorType;
    case 1: return RiskFactor;
    case 2: return Thresholds;
    default: throw new IndexOutOfBoundsException("Invalid index: " + field$);
    }
  }

  // Used by DatumReader.  Applications should not call.
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: RiskFactorType = (org.uwh.RiskFactorType)value$; break;
    case 1: RiskFactor = value$ != null ? value$.toString() : null; break;
    case 2: Thresholds = (java.util.Map<java.lang.String,java.lang.Double>)value$; break;
    default: throw new IndexOutOfBoundsException("Invalid index: " + field$);
    }
  }

  /**
   * Gets the value of the 'RiskFactorType' field.
   * @return The value of the 'RiskFactorType' field.
   */
  public org.uwh.RiskFactorType getRiskFactorType() {
    return RiskFactorType;
  }


  /**
   * Sets the value of the 'RiskFactorType' field.
   * @param value the value to set.
   */
  public void setRiskFactorType(org.uwh.RiskFactorType value) {
    this.RiskFactorType = value;
  }

  /**
   * Gets the value of the 'RiskFactor' field.
   * @return The value of the 'RiskFactor' field.
   */
  public java.lang.String getRiskFactor() {
    return RiskFactor;
  }


  /**
   * Sets the value of the 'RiskFactor' field.
   * @param value the value to set.
   */
  public void setRiskFactor(java.lang.String value) {
    this.RiskFactor = value;
  }

  /**
   * Gets the value of the 'Thresholds' field.
   * @return The value of the 'Thresholds' field.
   */
  public java.util.Map<java.lang.String,java.lang.Double> getThresholds() {
    return Thresholds;
  }


  /**
   * Sets the value of the 'Thresholds' field.
   * @param value the value to set.
   */
  public void setThresholds(java.util.Map<java.lang.String,java.lang.Double> value) {
    this.Thresholds = value;
  }

  /**
   * Creates a new RiskThreshold RecordBuilder.
   * @return A new RiskThreshold RecordBuilder
   */
  public static org.uwh.RiskThreshold.Builder newBuilder() {
    return new org.uwh.RiskThreshold.Builder();
  }

  /**
   * Creates a new RiskThreshold RecordBuilder by copying an existing Builder.
   * @param other The existing builder to copy.
   * @return A new RiskThreshold RecordBuilder
   */
  public static org.uwh.RiskThreshold.Builder newBuilder(org.uwh.RiskThreshold.Builder other) {
    if (other == null) {
      return new org.uwh.RiskThreshold.Builder();
    } else {
      return new org.uwh.RiskThreshold.Builder(other);
    }
  }

  /**
   * Creates a new RiskThreshold RecordBuilder by copying an existing RiskThreshold instance.
   * @param other The existing instance to copy.
   * @return A new RiskThreshold RecordBuilder
   */
  public static org.uwh.RiskThreshold.Builder newBuilder(org.uwh.RiskThreshold other) {
    if (other == null) {
      return new org.uwh.RiskThreshold.Builder();
    } else {
      return new org.uwh.RiskThreshold.Builder(other);
    }
  }

  /**
   * RecordBuilder for RiskThreshold instances.
   */
  @org.apache.avro.specific.AvroGenerated
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<RiskThreshold>
    implements org.apache.avro.data.RecordBuilder<RiskThreshold> {

    private org.uwh.RiskFactorType RiskFactorType;
    private java.lang.String RiskFactor;
    private java.util.Map<java.lang.String,java.lang.Double> Thresholds;

    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     * @param other The existing Builder to copy.
     */
    private Builder(org.uwh.RiskThreshold.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.RiskFactorType)) {
        this.RiskFactorType = data().deepCopy(fields()[0].schema(), other.RiskFactorType);
        fieldSetFlags()[0] = other.fieldSetFlags()[0];
      }
      if (isValidValue(fields()[1], other.RiskFactor)) {
        this.RiskFactor = data().deepCopy(fields()[1].schema(), other.RiskFactor);
        fieldSetFlags()[1] = other.fieldSetFlags()[1];
      }
      if (isValidValue(fields()[2], other.Thresholds)) {
        this.Thresholds = data().deepCopy(fields()[2].schema(), other.Thresholds);
        fieldSetFlags()[2] = other.fieldSetFlags()[2];
      }
    }

    /**
     * Creates a Builder by copying an existing RiskThreshold instance
     * @param other The existing instance to copy.
     */
    private Builder(org.uwh.RiskThreshold other) {
      super(SCHEMA$);
      if (isValidValue(fields()[0], other.RiskFactorType)) {
        this.RiskFactorType = data().deepCopy(fields()[0].schema(), other.RiskFactorType);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.RiskFactor)) {
        this.RiskFactor = data().deepCopy(fields()[1].schema(), other.RiskFactor);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.Thresholds)) {
        this.Thresholds = data().deepCopy(fields()[2].schema(), other.Thresholds);
        fieldSetFlags()[2] = true;
      }
    }

    /**
      * Gets the value of the 'RiskFactorType' field.
      * @return The value.
      */
    public org.uwh.RiskFactorType getRiskFactorType() {
      return RiskFactorType;
    }


    /**
      * Sets the value of the 'RiskFactorType' field.
      * @param value The value of 'RiskFactorType'.
      * @return This builder.
      */
    public org.uwh.RiskThreshold.Builder setRiskFactorType(org.uwh.RiskFactorType value) {
      validate(fields()[0], value);
      this.RiskFactorType = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    /**
      * Checks whether the 'RiskFactorType' field has been set.
      * @return True if the 'RiskFactorType' field has been set, false otherwise.
      */
    public boolean hasRiskFactorType() {
      return fieldSetFlags()[0];
    }


    /**
      * Clears the value of the 'RiskFactorType' field.
      * @return This builder.
      */
    public org.uwh.RiskThreshold.Builder clearRiskFactorType() {
      RiskFactorType = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /**
      * Gets the value of the 'RiskFactor' field.
      * @return The value.
      */
    public java.lang.String getRiskFactor() {
      return RiskFactor;
    }


    /**
      * Sets the value of the 'RiskFactor' field.
      * @param value The value of 'RiskFactor'.
      * @return This builder.
      */
    public org.uwh.RiskThreshold.Builder setRiskFactor(java.lang.String value) {
      validate(fields()[1], value);
      this.RiskFactor = value;
      fieldSetFlags()[1] = true;
      return this;
    }

    /**
      * Checks whether the 'RiskFactor' field has been set.
      * @return True if the 'RiskFactor' field has been set, false otherwise.
      */
    public boolean hasRiskFactor() {
      return fieldSetFlags()[1];
    }


    /**
      * Clears the value of the 'RiskFactor' field.
      * @return This builder.
      */
    public org.uwh.RiskThreshold.Builder clearRiskFactor() {
      RiskFactor = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    /**
      * Gets the value of the 'Thresholds' field.
      * @return The value.
      */
    public java.util.Map<java.lang.String,java.lang.Double> getThresholds() {
      return Thresholds;
    }


    /**
      * Sets the value of the 'Thresholds' field.
      * @param value The value of 'Thresholds'.
      * @return This builder.
      */
    public org.uwh.RiskThreshold.Builder setThresholds(java.util.Map<java.lang.String,java.lang.Double> value) {
      validate(fields()[2], value);
      this.Thresholds = value;
      fieldSetFlags()[2] = true;
      return this;
    }

    /**
      * Checks whether the 'Thresholds' field has been set.
      * @return True if the 'Thresholds' field has been set, false otherwise.
      */
    public boolean hasThresholds() {
      return fieldSetFlags()[2];
    }


    /**
      * Clears the value of the 'Thresholds' field.
      * @return This builder.
      */
    public org.uwh.RiskThreshold.Builder clearThresholds() {
      Thresholds = null;
      fieldSetFlags()[2] = false;
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public RiskThreshold build() {
      try {
        RiskThreshold record = new RiskThreshold();
        record.RiskFactorType = fieldSetFlags()[0] ? this.RiskFactorType : (org.uwh.RiskFactorType) defaultValue(fields()[0]);
        record.RiskFactor = fieldSetFlags()[1] ? this.RiskFactor : (java.lang.String) defaultValue(fields()[1]);
        record.Thresholds = fieldSetFlags()[2] ? this.Thresholds : (java.util.Map<java.lang.String,java.lang.Double>) defaultValue(fields()[2]);
        return record;
      } catch (org.apache.avro.AvroMissingFieldException e) {
        throw e;
      } catch (java.lang.Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumWriter<RiskThreshold>
    WRITER$ = (org.apache.avro.io.DatumWriter<RiskThreshold>)MODEL$.createDatumWriter(SCHEMA$);

  @Override public void writeExternal(java.io.ObjectOutput out)
    throws java.io.IOException {
    WRITER$.write(this, SpecificData.getEncoder(out));
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumReader<RiskThreshold>
    READER$ = (org.apache.avro.io.DatumReader<RiskThreshold>)MODEL$.createDatumReader(SCHEMA$);

  @Override public void readExternal(java.io.ObjectInput in)
    throws java.io.IOException {
    READER$.read(this, SpecificData.getDecoder(in));
  }

  @Override protected boolean hasCustomCoders() { return true; }

  @Override public void customEncode(org.apache.avro.io.Encoder out)
    throws java.io.IOException
  {
    out.writeEnum(this.RiskFactorType.ordinal());

    out.writeString(this.RiskFactor);

    long size0 = this.Thresholds.size();
    out.writeMapStart();
    out.setItemCount(size0);
    long actualSize0 = 0;
    for (java.util.Map.Entry<java.lang.String, java.lang.Double> e0: this.Thresholds.entrySet()) {
      actualSize0++;
      out.startItem();
      out.writeString(e0.getKey());
      java.lang.Double v0 = e0.getValue();
      out.writeDouble(v0);
    }
    out.writeMapEnd();
    if (actualSize0 != size0)
      throw new java.util.ConcurrentModificationException("Map-size written was " + size0 + ", but element count was " + actualSize0 + ".");

  }

  @Override public void customDecode(org.apache.avro.io.ResolvingDecoder in)
    throws java.io.IOException
  {
    org.apache.avro.Schema.Field[] fieldOrder = in.readFieldOrderIfDiff();
    if (fieldOrder == null) {
      this.RiskFactorType = org.uwh.RiskFactorType.values()[in.readEnum()];

      this.RiskFactor = in.readString();

      long size0 = in.readMapStart();
      java.util.Map<java.lang.String,java.lang.Double> m0 = this.Thresholds; // Need fresh name due to limitation of macro system
      if (m0 == null) {
        m0 = new java.util.HashMap<java.lang.String,java.lang.Double>((int)size0);
        this.Thresholds = m0;
      } else m0.clear();
      for ( ; 0 < size0; size0 = in.mapNext()) {
        for ( ; size0 != 0; size0--) {
          java.lang.String k0 = null;
          k0 = in.readString();
          java.lang.Double v0 = null;
          v0 = in.readDouble();
          m0.put(k0, v0);
        }
      }

    } else {
      for (int i = 0; i < 3; i++) {
        switch (fieldOrder[i].pos()) {
        case 0:
          this.RiskFactorType = org.uwh.RiskFactorType.values()[in.readEnum()];
          break;

        case 1:
          this.RiskFactor = in.readString();
          break;

        case 2:
          long size0 = in.readMapStart();
          java.util.Map<java.lang.String,java.lang.Double> m0 = this.Thresholds; // Need fresh name due to limitation of macro system
          if (m0 == null) {
            m0 = new java.util.HashMap<java.lang.String,java.lang.Double>((int)size0);
            this.Thresholds = m0;
          } else m0.clear();
          for ( ; 0 < size0; size0 = in.mapNext()) {
            for ( ; size0 != 0; size0--) {
              java.lang.String k0 = null;
              k0 = in.readString();
              java.lang.Double v0 = null;
              v0 = in.readDouble();
              m0.put(k0, v0);
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










