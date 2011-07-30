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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroJob;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.HashMap;
import java.util.MissingFormatArgumentException;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AvroDuckTypeDeserializer implements Deserializer {

  private ObjectInspector oi;
  private List<String> columnNames;
  private ArrayList<Object> row;
  private ArrayList<Integer> columnOrderToRecordOrder = null;

  @Override
  public void initialize(Configuration configuration, Properties properties) throws SerDeException {
    String columnNameProperty = properties.getProperty(Constants.LIST_COLUMNS);
    String columnTypeProperty = properties.getProperty(Constants.LIST_COLUMN_TYPES);

    columnNames = Arrays.asList(columnNameProperty.split(","));
    List<TypeInfo> columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);

    AvroObjectInspectorGenerator aoig = new AvroObjectInspectorGenerator(columnNames, columnTypes);
    oi = aoig.getObjectInspector();

    row = new ArrayList<Object>(columnNames.size());
    for (String n : columnNames) {
      row.add(null);
    }
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return oi;
  }

  @Override
  public Object deserialize(Writable writable) throws SerDeException {
    AvroGenericRecordWritable recordWritable = (AvroGenericRecordWritable) writable;
    GenericRecord record = recordWritable.getRecord();

    if (columnOrderToRecordOrder == null) {
      columnOrderToRecordOrder = lazyInitialize(record.getSchema(), columnNames);
    }

    int c = 0;
    for (Integer i : columnOrderToRecordOrder) {
      //System.out.printf("Get %d -> %d\n", c, i);
      Object val = (i == null) ? null : record.get(i);
      row.set(c++, val);
    }

    return row;
  }

  public List<String> getColumnNames() {
    return columnNames;
  }

  /* lazyInitialize.
   * My understanding is that each map get a unique file, or part of a file, so
   * the schema should be valid for the life of the AvroDuckTypeDeserializer
   * object. If it is reused for combinefileinputformat or jvm reuse, this will
   * fail on multiple files of different schema - if those schema are
   * inconsistent.
   */
  public ArrayList<Integer> lazyInitialize(Schema schema, List<String> columns) {
    HashMap<String, Integer> lowerCasefields = new HashMap<String, Integer>();
    for (Schema.Field f : schema.getFields()) {
      lowerCasefields.put(f.name().toLowerCase(), f.pos());
    }
    ArrayList<Integer> orderMap = new ArrayList<Integer>();
    for (int c = 0; c < columns.size(); c++) {
      String name = columns.get(c);
      Integer i = lowerCasefields.get(name.toLowerCase());
      orderMap.add(i);
    }
    return orderMap;
  }
}
