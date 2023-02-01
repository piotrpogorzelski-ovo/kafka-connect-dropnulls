/*
 * Copyright © 2019 Christopher Matta (chris.matta@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.lepoitr.kafka.connect.smt;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;


import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Arrays;
import java.util.List;
import java.util.Collections;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class DropNulls<R extends ConnectRecord<R>> implements Transformation<R> {

  private static final Logger log = LoggerFactory.getLogger(DropNulls.class);

  public static final String OVERVIEW_DOC =
    "remove top level fields with null value";

  private interface ConfigName {
    String DROPNULLS_VERBOSE = "dropnulls.verbose";
  }

  public static final ConfigDef CONFIG_DEF = new ConfigDef().define(ConfigName.DROPNULLS_VERBOSE, ConfigDef.Type.INT, 0, ConfigDef.Importance.LOW,
      "Log message verbosity level");

  private static final String PURPOSE = "verbosesity: 0 - warn/error only, 1+ - processing info";

  private int verbose = 0;

  @Override
  public void configure(Map<String, ?> props) {
    final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
    verbose = config.getInt(ConfigName.DROPNULLS_VERBOSE);
    log.info("DropNulls SMT configured with verbose=[{}].",verbose);
  }


  @Override
  public R apply(R record) {
    if (operatingSchema(record) == null) {
      return applySchemaless(record);
    } else {
      return applyWithSchema(record);
    }
  }

  private R applySchemaless(R record) {
    //TODO
    log.error("DropNulls: Schemaless records not supported yet.");
    return record;
  }

private Boolean isNullOrEmpty(Field field, Object fieldvalue){
  if(fieldvalue == null){
    if (verbose > 0) log.info("{} skipped - value is null",field);
    
  }else if(field.schema().type().equals(Schema.Type.ARRAY)){
        List<Object> al = (List<Object>)fieldvalue;
        if (al.size() == 0) {
	        if (verbose > 0) log.info("{} skipped - empty array",field);
        
        }else{
          if (verbose > 0) log.info("{} not empy array ",field);
          return false;
        }
   
  }else{
    if (verbose > 0) log.info("{} not an array and not null value",field);
    return false ;
  }
  return true;
}


private Object addIfNullOrEmpty(SchemaBuilder builder, Field field,Object fieldValue){
  SchemaBuilder subBuilder = SchemaBuilder.struct();
  Object retVal = null;
  Map<String,Object> newValues = new HashMap<>();
  if(field.schema().type().equals(Schema.Type.STRUCT)){
    Struct fieldValueStruct = (Struct) fieldValue;
    for(Field subField: field.schema().fields()){
      Object subFieldValue =  fieldValueStruct.get(subField);
      Object newValue = addIfNullOrEmpty(subBuilder, subField, subFieldValue);
      if (newValue != null) {
        newValues.put(subField.name(), newValue);
      }
    }
    if( subBuilder.fields().size() > 0){
      Schema subSchema = subBuilder.build();
      Struct retValStruct = new Struct(subSchema);
      for(Field newField: subSchema.fields()){
        retValStruct.put(newField,newValues.get(newField.name()));
      }
    builder.field(field.name(),subSchema) ;
    retVal = (Object) retValStruct;
    }
  }else{
    if( ! isNullOrEmpty(field, fieldValue) ){
    //if( isNullorEmpty( field, value.get(field) ) {
      builder.field(field.name(), field.schema());
      retVal = fieldValue;
    }
  }
  return retVal;
}

  public R applyWithSchema2(R record) {
    final Struct value = requireStruct(operatingValue(record), PURPOSE);

   
    final SchemaBuilder builder = SchemaUtil.copySchemaBasics(value.schema(), SchemaBuilder.struct());

      final Map<String,Object> newValues = new HashMap<>();
    for (Field field: value.schema().fields()) {
      Object fieldValue =  value.get(field);
      Object newValue = addIfNullOrEmpty(builder, field, fieldValue);
      if(null != newValue){
        newValues.put(field.name(),newValue);
      }
    }

    
    Schema updatedSchema = builder.build();
    final Struct updatedValue = new Struct(updatedSchema);

      for (Field field: updatedSchema.fields()) {

        if (verbose > 0) log.info("{} added",field);
        
        updatedValue.put(field.name(), newValues.get(field.name()));  
    }
  

    return newRecord(record, updatedSchema, updatedValue);
  }

  private R applyWithSchema(R record) {
    final Struct value = requireStruct(operatingValue(record), PURPOSE);

   
    final SchemaBuilder builder = SchemaUtil.copySchemaBasics(value.schema(), SchemaBuilder.struct());
   
    for (Field field: value.schema().fields()) {
      if(value.get(field) != null){    
        if (verbose > 0) log.info("{}not null",field);
        builder.field(field.name(), field.schema());

      }else{
        log.info("{} null -should be skipped",field);
      }
    }

      Schema updatedSchema = builder.build();

      final Struct updatedValue = new Struct(updatedSchema);
      for (Field field: updatedSchema.fields()) {
        if (verbose > 0) log.info("{} added",field);
        
        updatedValue.put(field.name(), value.get(field.name()));  
    }


    return newRecord(record, updatedSchema, updatedValue);
  }



  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  @Override
  public void close() {
  }


  protected abstract Schema operatingSchema(R record);

  protected abstract Object operatingValue(R record);

  protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

  public static class Key<R extends ConnectRecord<R>> extends DropNulls<R> {

    @Override
    protected Schema operatingSchema(R record) {
      return record.keySchema();
    }

    @Override
    protected Object operatingValue(R record) {
      return record.key();
    }

    @Override
    protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
      return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue, record.valueSchema(), record.value(), record.timestamp());
    }

  }

  public static class Value<R extends ConnectRecord<R>> extends DropNulls<R> {

    @Override
    protected Schema operatingSchema(R record) {
      return record.valueSchema();
    }

    @Override
    protected Object operatingValue(R record) {
      return record.value();
    }

    @Override
    protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
      return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
    }

  }
}


