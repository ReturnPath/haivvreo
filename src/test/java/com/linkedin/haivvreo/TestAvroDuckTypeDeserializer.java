/*
 * Copyright 2011 LinkedIn
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.haivvreo;


import java.util.*;

import static org.junit.Assert.*;
import org.junit.*;

import org.apache.avro.file.*;
import org.apache.avro.util.Utf8;
import org.apache.avro.generic.*;
import org.apache.avro.Schema;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.*;


public class TestAvroDuckTypeDeserializer {

  public static final String RECORD_SCHEMA = "{\n" +
    "  \"namespace\": \"testing.test.mctesty\",\n" +
    "  \"name\": \"aRecord\",\n" +
    "  \"type\": \"record\",\n" +
    "  \"fields\": [\n" +
    "     {\n" +
    "       \"name\":\"int1\",\n" +
    "       \"type\":\"int\"\n" +
    "     },{\n" +
    "       \"name\":\"boolean1\",\n" +
    "       \"type\":\"boolean\"\n" +
    "     },{\n" +
    "       \"name\":\"long1\",\n" +
    "       \"type\":\"long\"\n" +
    "     }\n" +
    "  ]}\n";

  private final GenericData GENERIC_DATA = GenericData.get();

  private Deserializer de;
  private Schema s;
  private GenericData.Record record;

  @Before public void setup() throws SerDeException, java.io.IOException {

    //Schema s = Schema.parse(TestAvroObjectInspectorGenerator.KITCHEN_SINK_SCHEMA);
    s = Schema.parse(RECORD_SCHEMA);
    record = new GenericData.Record(s);
    //string1,string2,int1,boolean1,long1,float1,double1,inner_record1,enum1,array1,map1,union1,fixed1,null1,UnionNullInt,bytes1
    //record.put("string1", "42");
    record.put("int1", 42);
    record.put("boolean1", true);
    record.put("long1", 42432234234l);
    //record.put("UnionNullInt", 42);
    assertTrue(GENERIC_DATA.validate(s, record)); 

    String fieldNames = "boolean1,long1,fake1,int1".toLowerCase();
    String fieldTypes = "boolean,bigint,string,int".toLowerCase();

    //String fieldNames = "string1,int1,boolean1,long1,UnionNullInt,fake1".toLowerCase();
    //String fieldTypes = "string,int,boolean,bigint,int,string".toLowerCase();
     Properties props = new Properties();
    props.setProperty(Constants.LIST_COLUMNS, fieldNames);
    props.setProperty(Constants.LIST_COLUMN_TYPES, fieldTypes);
    de = new AvroDuckTypeDeserializer();
    de.initialize(new Configuration(), props);
  }

  @Test public void lazyInitialize() {
    AvroDuckTypeDeserializer adtde = (AvroDuckTypeDeserializer) de;
    List l = adtde.lazyInitialize(s, adtde.getColumnNames());
    assertEquals(l.get(0), 1);
    assertEquals(l.get(1), 2);
    assertNull(l.get(2));
    assertEquals(l.get(3), 0);
  }

  @Test public void deserializer() throws SerDeException, java.io.IOException {
    AvroGenericRecordWritable garw = Utils.serializeAndDeserializeRecord(record);
    StandardStructObjectInspector oi = (StandardStructObjectInspector)de.getObjectInspector();
    Object row = de.deserialize(garw);
    StructField fieldRef = oi.getStructFieldRef("int1");
    Object int1 = oi.getStructFieldData(row, fieldRef);
    assertEquals(42, int1);

    List<Object> z = oi.getStructFieldsDataAsList(row);
    assertEquals(true, z.get(0));
    assertEquals(42432234234l, z.get(1));
    assertEquals(null, z.get(2));
    assertEquals(42, z.get(3));
  }
}
