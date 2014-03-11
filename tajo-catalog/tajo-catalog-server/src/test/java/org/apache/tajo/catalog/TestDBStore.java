/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.catalog;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.partition.PartitionMethodDesc;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.catalog.store.AbstractDBStore;
import org.apache.tajo.catalog.store.DerbyStore;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.util.CommonTestingUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.apache.tajo.catalog.CatalogConstants.*;
import static org.junit.Assert.*;

public class TestDBStore {
  protected static final Log LOG = LogFactory.getLog(TestDBStore.class);
  protected static Configuration conf;
  protected static AbstractDBStore store;

  @BeforeClass
  public static void setUp() throws Exception {
    conf = new TajoConf();
    Path testDir = CommonTestingUtil.getTestDir("target/test-data/TestDBSTore");
    File absolutePath = new File(testDir.toUri());
    conf.set(CatalogConstants.CATALOG_URI, "jdbc:derby:"+absolutePath.getAbsolutePath()+"/db;create=true");
    LOG.info("derby repository is set to "+conf.get(CatalogConstants.CATALOG_URI));

    store = new DerbyStore(conf);
    store.createTablespace(DEFAULT_TABLESPACE_NAME, testDir.toUri().toString());
    store.createDatabase(DEFAULT_DATABASE_NAME, DEFAULT_TABLESPACE_NAME);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    store.close();
  }

  @Test
  public final void testAddAndDeleteTable() throws Exception {
    Schema schema = new Schema();
    schema.addColumn("id", Type.INT4)
    .addColumn("name", Type.TEXT)
    .addColumn("age", Type.INT4)
    .addColumn("score", Type.FLOAT8);
    
    String tableName = "addedtable";
    Options opts = new Options();
    opts.put("file.delimiter", ",");
    TableMeta meta = CatalogUtil.newTableMeta(StoreType.CSV, opts);
    TableDesc desc = new TableDesc(tableName, schema, meta, new Path(CommonTestingUtil.getTestDir(), "addedtable"));
    assertFalse(store.existTable(DEFAULT_DATABASE_NAME, DEFAULT_NAMESPACE, tableName));
    store.createTable(desc.getProto());
    assertTrue(store.existTable(DEFAULT_DATABASE_NAME, DEFAULT_NAMESPACE, tableName));

    TableDesc retrieved = new TableDesc(store.getTable(DEFAULT_DATABASE_NAME, DEFAULT_NAMESPACE, tableName));
    // Schema order check
    assertSchemaOrder(desc.getSchema(), retrieved.getSchema());
    store.dropTable(DEFAULT_DATABASE_NAME, DEFAULT_NAMESPACE, tableName);
    assertFalse(store.existTable(DEFAULT_DATABASE_NAME, DEFAULT_NAMESPACE, tableName));
  }
  
  @Test
  public final void testGetTable() throws Exception {
    Schema schema = new Schema();
    schema.addColumn("gettable.id", Type.INT4)
    .addColumn("gettable.name", Type.TEXT)
    .addColumn("gettable.age", Type.INT4)
    .addColumn("gettable.score", Type.FLOAT8);
    
    String tableName = "gettable";
    Options opts = new Options();
    opts.put("file.delimiter", ",");
    TableMeta meta = CatalogUtil.newTableMeta(StoreType.CSV, opts);

    TableStats stat = new TableStats();
    stat.setNumRows(957685);
    stat.setNumBytes(1023234);

    TableDesc desc = new TableDesc(tableName, schema, meta, new Path(CommonTestingUtil.getTestDir(), "gettable"));
    desc.setExternal(true);
    desc.setStats(stat);

    store.createTable(desc.getProto());
    TableDesc retrieved = new TableDesc(store.getTable(DEFAULT_DATABASE_NAME, DEFAULT_NAMESPACE, tableName));
    assertEquals(",", retrieved.getMeta().getOption("file.delimiter"));
    assertTrue(desc.equals(retrieved));
    assertTrue(957685 == desc.getStats().getNumRows());
    assertTrue(1023234 == desc.getStats().getNumBytes());
    // Schema order check
    assertSchemaOrder(desc.getSchema(), retrieved.getSchema());
    store.dropTable(DEFAULT_DATABASE_NAME, DEFAULT_NAMESPACE, tableName);
  }
  
  @Test
  public final void testGetAllTableNames() throws Exception {
    Schema schema = new Schema();
    schema.addColumn("id", Type.INT4)
    .addColumn("name", Type.TEXT)
    .addColumn("age", Type.INT4)
    .addColumn("score", Type.FLOAT8);
    
    int numTables = 5;
    for (int i = 0; i < numTables; i++) {
      String tableName = "tableA_" + i;
      TableMeta meta = CatalogUtil.newTableMeta(StoreType.CSV);
      TableDesc desc = new TableDesc(tableName, schema, meta,
          new Path(CommonTestingUtil.getTestDir(), "tableA_" + i));
      store.createTable(desc.getProto());
    }
    
    assertEquals(numTables, store.getAllTableNames(DEFAULT_DATABASE_NAME, DEFAULT_NAMESPACE).size());
  }  
  
  @Test
  public final void testAddAndDeleteIndex() throws Exception {
    TableDesc table = prepareTable();
    store.createTable(table.getProto());
    
    store.createIndex(TestCatalog.desc1.getProto());
    assertTrue(store.existIndexByName(DEFAULT_DATABASE_NAME, DEFAULT_NAMESPACE, TestCatalog.desc1.getIndexName()));
    store.dropIndex(DEFAULT_DATABASE_NAME, DEFAULT_NAMESPACE, TestCatalog.desc1.getIndexName());
    assertFalse(store.existIndexByName(DEFAULT_DATABASE_NAME, DEFAULT_NAMESPACE, TestCatalog.desc1.getIndexName()));
    store.dropTable(DEFAULT_DATABASE_NAME, DEFAULT_NAMESPACE, table.getName());
  }
  
  @Test
  public final void testGetIndex() throws Exception {
    
    TableDesc table = prepareTable();
    store.createTable(table.getProto());
    
    store.createIndex(TestCatalog.desc2.getProto());
    CatalogProtos.IndexDescProto proto = store.getIndexByName(DEFAULT_DATABASE_NAME, DEFAULT_NAMESPACE,
        TestCatalog.desc2.getIndexName());

    assertEquals(
        new IndexDesc(TestCatalog.desc2.getProto()), new IndexDesc(proto));
    store.dropIndex(DEFAULT_DATABASE_NAME, DEFAULT_NAMESPACE, TestCatalog.desc2.getIndexName());
    store.dropTable(DEFAULT_DATABASE_NAME, DEFAULT_NAMESPACE, table.getName());
  }
  
  @Test
  public final void testGetIndexByTableAndColumn() throws Exception {
    
    TableDesc table = prepareTable();
    store.createTable(table.getProto());
    
    store.createIndex(TestCatalog.desc2.getProto());
    
    String tableId = TestCatalog.desc2.getTableName();
    String columnName = "score";
    CatalogProtos.IndexDescProto retrieved =
        store.getIndexByColumn(DEFAULT_DATABASE_NAME, DEFAULT_NAMESPACE, tableId, columnName);
    assertEquals(
        new IndexDesc(TestCatalog.desc2.getProto()),
        new IndexDesc(retrieved));
    store.dropIndex(DEFAULT_DATABASE_NAME, DEFAULT_NAMESPACE, TestCatalog.desc2.getIndexName());
    
    store.dropTable(DEFAULT_DATABASE_NAME, DEFAULT_NAMESPACE, table.getName());
  }
  
  @Test
  public final void testGetAllIndexes() throws Exception {
    
    TableDesc table = prepareTable();
    store.createTable(table.getProto());
    
    store.createIndex(TestCatalog.desc1.getProto());
    store.createIndex(TestCatalog.desc2.getProto());
        
    assertEquals(2, 
        store.getIndexes(DEFAULT_DATABASE_NAME, DEFAULT_NAMESPACE, TestCatalog.desc2.getTableName()).length);
    store.dropIndex(DEFAULT_DATABASE_NAME, DEFAULT_NAMESPACE, TestCatalog.desc1.getIndexName());
    store.dropIndex(DEFAULT_DATABASE_NAME, DEFAULT_NAMESPACE, TestCatalog.desc2.getIndexName());
    
    store.dropTable(DEFAULT_DATABASE_NAME, DEFAULT_NAMESPACE, table.getName());
  }
  
  public static TableDesc prepareTable() throws IOException {
    Schema schema = new Schema();
    schema.addColumn("indexed.id", Type.INT4)
    .addColumn("indexed.name", Type.TEXT)
    .addColumn("indexed.age", Type.INT4)
    .addColumn("indexed.score", Type.FLOAT8);
    
    String tableName = "indexed";
    
    TableMeta meta = CatalogUtil.newTableMeta(StoreType.CSV);
    return new TableDesc(tableName, schema, meta, new Path(CommonTestingUtil.getTestDir(), "indexed"));
  }

  public static void assertSchemaOrder(Schema s1, Schema s2) {
    // Schema order check
    assertEquals(s1.size(),
        s2.size());

    for (int i = 0; i < s1.size(); i++) {
      assertEquals(s1.getColumn(i).getSimpleName(),
          s2.getColumn(i).getSimpleName());
    }
  }

  @Test
  public final void testAddAndDeleteTablePartitionByHash1() throws Exception {
    Schema schema = new Schema();
    schema.addColumn("id", Type.INT4)
        .addColumn("name", Type.TEXT)
        .addColumn("age", Type.INT4)
        .addColumn("score", Type.FLOAT8);

    String tableName = "addedtable";
    Options opts = new Options();
    opts.put("file.delimiter", ",");
    TableMeta meta = CatalogUtil.newTableMeta(StoreType.CSV, opts);

    Schema partSchema = new Schema();
    partSchema.addColumn("id", Type.INT4);
    PartitionMethodDesc partitionDesc =
        new PartitionMethodDesc(DEFAULT_DATABASE_NAME, DEFAULT_NAMESPACE, tableName, CatalogProtos.PartitionType.HASH,
            "id", partSchema);

    TableDesc desc =
        new TableDesc(tableName, schema, meta, new Path(CommonTestingUtil.getTestDir(), "addedtable"));
    desc.setPartitionMethod(partitionDesc);
    assertFalse(store.existTable(DEFAULT_DATABASE_NAME, DEFAULT_NAMESPACE, tableName));
    store.createTable(desc.getProto());
    assertTrue(store.existTable(DEFAULT_DATABASE_NAME, DEFAULT_NAMESPACE, tableName));

    TableDesc retrieved = new TableDesc(store.getTable(DEFAULT_DATABASE_NAME, DEFAULT_NAMESPACE, tableName));

    // Schema order check
    assertSchemaOrder(desc.getSchema(), retrieved.getSchema());
    store.dropTable(DEFAULT_DATABASE_NAME, DEFAULT_NAMESPACE, tableName);
    assertFalse(store.existTable(DEFAULT_DATABASE_NAME, DEFAULT_NAMESPACE, tableName));
  }

  @Test
  public final void testAddAndDeleteTablePartitionByHash2() throws Exception {
    Schema schema = new Schema();
    schema.addColumn("id", Type.INT4)
        .addColumn("name", Type.TEXT)
        .addColumn("age", Type.INT4)
        .addColumn("score", Type.FLOAT8);

    String tableName = "addedtable";
    Options opts = new Options();
    opts.put("file.delimiter", ",");
    TableMeta meta = CatalogUtil.newTableMeta(StoreType.CSV, opts);

    Schema partSchema = new Schema();
    partSchema.addColumn("id", Type.INT4);
    PartitionMethodDesc partitionDesc =
        new PartitionMethodDesc(DEFAULT_DATABASE_NAME, DEFAULT_NAMESPACE, tableName, CatalogProtos.PartitionType.HASH,
            "id", partSchema);

    TableDesc desc =
        new TableDesc(tableName, schema, meta, new Path(CommonTestingUtil.getTestDir(), "addedtable"));
    desc.setPartitionMethod(partitionDesc);
    assertFalse(store.existTable(DEFAULT_DATABASE_NAME, DEFAULT_NAMESPACE, tableName));
    store.createTable(desc.getProto());
    assertTrue(store.existTable(DEFAULT_DATABASE_NAME, DEFAULT_NAMESPACE, tableName));

    TableDesc retrieved = new TableDesc(store.getTable(DEFAULT_DATABASE_NAME, DEFAULT_NAMESPACE, tableName));

    // Schema order check
    assertSchemaOrder(desc.getSchema(), retrieved.getSchema());
    store.dropTable(DEFAULT_DATABASE_NAME, DEFAULT_NAMESPACE, tableName);
    assertFalse(store.existTable(DEFAULT_DATABASE_NAME, DEFAULT_NAMESPACE, tableName));
  }

  @Test
  public final void testAddAndDeleteTablePartitionByList() throws Exception {
    Schema schema = new Schema();
    schema.addColumn("id", Type.INT4)
        .addColumn("name", Type.TEXT)
        .addColumn("age", Type.INT4)
        .addColumn("score", Type.FLOAT8);

    String tableName = "addedtable";
    Options opts = new Options();
    opts.put("file.delimiter", ",");
    TableMeta meta = CatalogUtil.newTableMeta(StoreType.CSV, opts);

    Schema partSchema = new Schema();
    partSchema.addColumn("id", Type.INT4);
    PartitionMethodDesc partitionDesc =
        new PartitionMethodDesc(DEFAULT_DATABASE_NAME, DEFAULT_NAMESPACE, tableName, CatalogProtos.PartitionType.LIST,
            "id", partSchema);

    TableDesc desc =
        new TableDesc(tableName, schema, meta, new Path(CommonTestingUtil.getTestDir(), "addedtable"));
    desc.setPartitionMethod(partitionDesc);
    assertFalse(store.existTable(DEFAULT_DATABASE_NAME, DEFAULT_NAMESPACE, tableName));
    store.createTable(desc.getProto());
    assertTrue(store.existTable(DEFAULT_DATABASE_NAME, DEFAULT_NAMESPACE, tableName));

    TableDesc retrieved = new TableDesc(store.getTable(DEFAULT_DATABASE_NAME, DEFAULT_NAMESPACE, tableName));

    // Schema order check
    assertSchemaOrder(desc.getSchema(), retrieved.getSchema());
    store.dropTable(DEFAULT_DATABASE_NAME, DEFAULT_NAMESPACE, tableName);
    assertFalse(store.existTable(DEFAULT_DATABASE_NAME, DEFAULT_NAMESPACE, tableName));
  }

  @Test
  public final void testAddAndDeleteTablePartitionByRange() throws Exception {
    Schema schema = new Schema();
    schema.addColumn("id", Type.INT4)
        .addColumn("name", Type.TEXT)
        .addColumn("age", Type.INT4)
        .addColumn("score", Type.FLOAT8);

    String tableName = "addedtable";
    Options opts = new Options();
    opts.put("file.delimiter", ",");
    TableMeta meta = CatalogUtil.newTableMeta(StoreType.CSV, opts);

    Schema partSchema = new Schema();
    partSchema.addColumn("id", Type.INT4);
    PartitionMethodDesc partitionDesc =
        new PartitionMethodDesc(DEFAULT_DATABASE_NAME, DEFAULT_NAMESPACE, tableName, CatalogProtos.PartitionType.RANGE,
            "id", partSchema);

    TableDesc desc =
        new TableDesc(tableName, schema, meta, new Path(CommonTestingUtil.getTestDir(), "addedtable"));
    desc.setPartitionMethod(partitionDesc);
    assertFalse(store.existTable(DEFAULT_DATABASE_NAME, DEFAULT_NAMESPACE, tableName));
    store.createTable(desc.getProto());
    assertTrue(store.existTable(DEFAULT_DATABASE_NAME, DEFAULT_NAMESPACE, tableName));

    TableDesc retrieved = new TableDesc(store.getTable(DEFAULT_DATABASE_NAME, DEFAULT_NAMESPACE, tableName));

    // Schema order check
    assertSchemaOrder(desc.getSchema(), retrieved.getSchema());
    store.dropTable(DEFAULT_DATABASE_NAME, DEFAULT_NAMESPACE, tableName);
    assertFalse(store.existTable(DEFAULT_DATABASE_NAME, DEFAULT_NAMESPACE, tableName));
  }

  @Test
  public final void testAddAndDeleteTablePartitionByColumn() throws Exception {
    Schema schema = new Schema();
    schema.addColumn("id", Type.INT4)
        .addColumn("name", Type.TEXT)
        .addColumn("age", Type.INT4)
        .addColumn("score", Type.FLOAT8);

    String tableName = "addedtable";
    Options opts = new Options();
    opts.put("file.delimiter", ",");
    TableMeta meta = CatalogUtil.newTableMeta(StoreType.CSV, opts);

    Schema partSchema = new Schema();
    partSchema.addColumn("id", Type.INT4);
    PartitionMethodDesc partitionDesc =
        new PartitionMethodDesc(DEFAULT_DATABASE_NAME, DEFAULT_NAMESPACE, tableName, CatalogProtos.PartitionType.COLUMN,
            "id", partSchema);

    TableDesc desc =
        new TableDesc(tableName, schema, meta, new Path(CommonTestingUtil.getTestDir(), "addedtable"));
    desc.setPartitionMethod(partitionDesc);
    assertFalse(store.existTable(DEFAULT_DATABASE_NAME, DEFAULT_NAMESPACE, tableName));
    store.createTable(desc.getProto());
    assertTrue(store.existTable(DEFAULT_DATABASE_NAME, DEFAULT_NAMESPACE, tableName));

    TableDesc retrieved = new TableDesc(store.getTable(DEFAULT_DATABASE_NAME, DEFAULT_NAMESPACE, tableName));

    // Schema order check
    assertSchemaOrder(desc.getSchema(), retrieved.getSchema());
    store.dropTable(DEFAULT_DATABASE_NAME, DEFAULT_NAMESPACE, tableName);
    assertFalse(store.existTable(DEFAULT_DATABASE_NAME, DEFAULT_NAMESPACE, tableName));
  }



}
