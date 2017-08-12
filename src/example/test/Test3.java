/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package example.test;  
@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class Test3 extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Test3\",\"namespace\":\"example.test\",\"fields\":[{\"name\":\"test3_ck\",\"type\":\"long\",\"order\":\"ignore\"},{\"name\":\"ftest2\",\"type\":{\"type\":\"array\",\"items\":[{\"type\":\"record\",\"name\":\"Test2\",\"fields\":[{\"name\":\"test2_ok\",\"type\":\"long\"},{\"name\":\"test2_ck\",\"type\":\"long\"},{\"name\":\"ftest1\",\"type\":{\"type\":\"array\",\"items\":[{\"type\":\"record\",\"name\":\"Test1\",\"fields\":[{\"name\":\"test1_ok\",\"type\":\"long\"},{\"name\":\"test1\",\"type\":\"string\"}]}]}}]}]}}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }
  @Deprecated public long test3_ck;
  @Deprecated public java.util.List<java.lang.Object> ftest2;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>. 
   */
  public Test3() {}

  /**
   * All-args constructor.
   */
  public Test3(java.lang.Long test3_ck, java.util.List<java.lang.Object> ftest2) {
    this.test3_ck = test3_ck;
    this.ftest2 = ftest2;
  }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call. 
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return test3_ck;
    case 1: return ftest2;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call. 
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: test3_ck = (java.lang.Long)value$; break;
    case 1: ftest2 = (java.util.List<java.lang.Object>)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'test3_ck' field.
   */
  public java.lang.Long getTest3Ck() {
    return test3_ck;
  }

  /**
   * Sets the value of the 'test3_ck' field.
   * @param value the value to set.
   */
  public void setTest3Ck(java.lang.Long value) {
    this.test3_ck = value;
  }

  /**
   * Gets the value of the 'ftest2' field.
   */
  public java.util.List<java.lang.Object> getFtest2() {
    return ftest2;
  }

  /**
   * Sets the value of the 'ftest2' field.
   * @param value the value to set.
   */
  public void setFtest2(java.util.List<java.lang.Object> value) {
    this.ftest2 = value;
  }

  /** Creates a new Test3 RecordBuilder */
  public static example.test.Test3.Builder newBuilder() {
    return new example.test.Test3.Builder();
  }
  
  /** Creates a new Test3 RecordBuilder by copying an existing Builder */
  public static example.test.Test3.Builder newBuilder(example.test.Test3.Builder other) {
    return new example.test.Test3.Builder(other);
  }
  
  /** Creates a new Test3 RecordBuilder by copying an existing Test3 instance */
  public static example.test.Test3.Builder newBuilder(example.test.Test3 other) {
    return new example.test.Test3.Builder(other);
  }
  
  /**
   * RecordBuilder for Test3 instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<Test3>
    implements org.apache.avro.data.RecordBuilder<Test3> {

    private long test3_ck;
    private java.util.List<java.lang.Object> ftest2;

    /** Creates a new Builder */
    private Builder() {
      super(example.test.Test3.SCHEMA$);
    }
    
    /** Creates a Builder by copying an existing Builder */
    private Builder(example.test.Test3.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.test3_ck)) {
        this.test3_ck = data().deepCopy(fields()[0].schema(), other.test3_ck);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.ftest2)) {
        this.ftest2 = data().deepCopy(fields()[1].schema(), other.ftest2);
        fieldSetFlags()[1] = true;
      }
    }
    
    /** Creates a Builder by copying an existing Test3 instance */
    private Builder(example.test.Test3 other) {
            super(example.test.Test3.SCHEMA$);
      if (isValidValue(fields()[0], other.test3_ck)) {
        this.test3_ck = data().deepCopy(fields()[0].schema(), other.test3_ck);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.ftest2)) {
        this.ftest2 = data().deepCopy(fields()[1].schema(), other.ftest2);
        fieldSetFlags()[1] = true;
      }
    }

    /** Gets the value of the 'test3_ck' field */
    public java.lang.Long getTest3Ck() {
      return test3_ck;
    }
    
    /** Sets the value of the 'test3_ck' field */
    public example.test.Test3.Builder setTest3Ck(long value) {
      validate(fields()[0], value);
      this.test3_ck = value;
      fieldSetFlags()[0] = true;
      return this; 
    }
    
    /** Checks whether the 'test3_ck' field has been set */
    public boolean hasTest3Ck() {
      return fieldSetFlags()[0];
    }
    
    /** Clears the value of the 'test3_ck' field */
    public example.test.Test3.Builder clearTest3Ck() {
      fieldSetFlags()[0] = false;
      return this;
    }

    /** Gets the value of the 'ftest2' field */
    public java.util.List<java.lang.Object> getFtest2() {
      return ftest2;
    }
    
    /** Sets the value of the 'ftest2' field */
    public example.test.Test3.Builder setFtest2(java.util.List<java.lang.Object> value) {
      validate(fields()[1], value);
      this.ftest2 = value;
      fieldSetFlags()[1] = true;
      return this; 
    }
    
    /** Checks whether the 'ftest2' field has been set */
    public boolean hasFtest2() {
      return fieldSetFlags()[1];
    }
    
    /** Clears the value of the 'ftest2' field */
    public example.test.Test3.Builder clearFtest2() {
      ftest2 = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    @Override
    public Test3 build() {
      try {
        Test3 record = new Test3();
        record.test3_ck = fieldSetFlags()[0] ? this.test3_ck : (java.lang.Long) defaultValue(fields()[0]);
        record.ftest2 = fieldSetFlags()[1] ? this.ftest2 : (java.util.List<java.lang.Object>) defaultValue(fields()[1]);
        return record;
      } catch (Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }
}
