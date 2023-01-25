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


import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class DropNulls<R extends ConnectRecord<R>> implements Transformation<R> {

  private static final Logger log = LoggerFactory.getLogger(DropNulls.class);

  public static final String OVERVIEW_DOC =
    "remove top level fields with null value";

  private interface ConfigName {
    //String DROPNULS_RECURSIVE = "dropnulls.recursive";
  }

  public static final ConfigDef CONFIG_DEF = new ConfigDef();
    //.define(ConfigName.DROPNULLS_RECURSIVE, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.HIGH,
    //  "Remove nulls on all levels of document structure");

  private static final String PURPOSE = "remove top level fields with null value";

//  private Boolean recursive;

  @Override
  public void configure(Map<String, ?> props) {
    final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
    //recursive = config.getString(ConfigName.DROPNULLS_RECURSIVE);
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
    return record;
  }

  private R applyWithSchema(R record) {
    final Struct value = requireStruct(operatingValue(record), PURPOSE);

   
    final SchemaBuilder builder = SchemaUtil.copySchemaBasics(value.schema(), SchemaBuilder.struct());
   
    for (Field field: value.schema().fields()) {
      if(value.get(field) != null){    
        log.info("{}not null",field);
        builder.field(field.name(), field.schema());

      }else{
        log.info("{} null -should be skipped",field);
      }
    }
      Schema updatedSchema = builder.build();

      final Struct updatedValue = new Struct(updatedSchema);
      for (Field field: updatedSchema.fields()) {
        log.info("{} added",field);
        
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


