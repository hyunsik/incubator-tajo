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

package org.apache.tajo.storage.index;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.storage.*;
import org.apache.tajo.storage.index.bst.BSTIndex;
import org.apache.tajo.storage.index.bst.BSTIndex.BSTIndexReader;
import org.apache.tajo.storage.index.bst.BSTIndex.BSTIndexWriter;
import org.apache.tajo.storage.v2.FileScannerV2;
import org.apache.tajo.util.CommonTestingUtil;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestBSTIndex {
  private TajoConf conf;
  private Schema schema;
  private TableMeta meta;

  private static final int TUPLE_NUM = 10000;
  private static final int LOAD_NUM = 100;
  private static final String TEST_PATH = "target/test-data/TestIndex";
  private Path testDir;
  private FileSystem fs;
  
  public TestBSTIndex() {
    conf = new TajoConf();
    conf.setVar(TajoConf.ConfVars.ROOT_DIR, TEST_PATH);
    schema = new Schema();
    schema.addColumn(new Column("int", Type.INT4));
    schema.addColumn(new Column("long", Type.INT8));
    schema.addColumn(new Column("double", Type.FLOAT8));
    schema.addColumn(new Column("float", Type.FLOAT4));
    schema.addColumn(new Column("string", Type.TEXT));
  }

   
  @Before
  public void setUp() throws Exception {
    testDir = CommonTestingUtil.getTestDir(TEST_PATH);
    fs = testDir.getFileSystem(conf);
  }
  
  @Test
  public void testFindValueInCSV() throws IOException {
    meta = CatalogUtil.newTableMeta(schema, StoreType.CSV);
    
    Path tablePath = new Path(testDir, "FindValueInCSV.csv");
    Appender appender  = StorageManagerFactory.getStorageManager(conf).getAppender(meta, tablePath);
    appender.init();
    Tuple tuple;
    for(int i = 0 ; i < TUPLE_NUM; i ++ ) {
        tuple = new VTuple(5);
        tuple.put(0, DatumFactory.createInt4(i));
        tuple.put(1, DatumFactory.createInt8(i));
        tuple.put(2, DatumFactory.createFloat8(i));
        tuple.put(3, DatumFactory.createFloat4(i));
        tuple.put(4, DatumFactory.createText("field_" + i));
        appender.addTuple(tuple);
      }
    appender.close();

    FileStatus status = fs.getFileStatus(tablePath);
    long fileLen = status.getLen();
    Fragment tablet = new Fragment("table1_1", status.getPath(), meta, 0, fileLen);
    
    SortSpec [] sortKeys = new SortSpec[2];
    sortKeys[0] = new SortSpec(schema.getColumnByFQN("long"), true, false);
    sortKeys[1] = new SortSpec(schema.getColumnByFQN("double"), true, false);

    Schema keySchema = new Schema();
    keySchema.addColumn(new Column("long", Type.INT8));
    keySchema.addColumn(new Column("double", Type.FLOAT8));

    TupleComparator comp = new TupleComparator(keySchema, sortKeys);
    
    BSTIndex bst = new BSTIndex(conf);
    BSTIndexWriter creater = bst.getIndexWriter(new Path(testDir, "FindValueInCSV.idx"),
        BSTIndex.TWO_LEVEL_INDEX,
        keySchema, comp);    
    creater.setLoadNum(LOAD_NUM);
    creater.open();

    SeekableScanner scanner = StorageManagerFactory.getSeekableScanner(conf, meta, tablet, meta.getSchema());
    scanner.init();

    Tuple keyTuple;
    long offset;
    while (true) {
      keyTuple = new VTuple(2);
      offset = scanner.getNextOffset();
      tuple = scanner.next();
      if (tuple == null) break;
      
      keyTuple.put(0, tuple.get(1));
      keyTuple.put(1, tuple.get(2));
      creater.write(keyTuple, offset);
    }
    
    creater.flush();
    creater.close();
    scanner.close();
    
    tuple = new VTuple(keySchema.getColumnNum());
    BSTIndexReader reader = bst.getIndexReader(new Path(testDir, "FindValueInCSV.idx"), keySchema, comp);
    reader.open();
    scanner = StorageManagerFactory.getSeekableScanner(conf, meta, tablet, meta.getSchema());
    scanner.init();
    if(scanner instanceof FileScannerV2) {
      ((FileScannerV2)scanner).waitScanStart();
    }
    for(int i = 0 ; i < TUPLE_NUM -1 ; i ++) {
      tuple.put(0, DatumFactory.createInt8(i));
      tuple.put(1, DatumFactory.createFloat8(i));
      long offsets = reader.find(tuple);
      scanner.seek(offsets);
      tuple = scanner.next();
      assertTrue("seek check [" + (i) + " ," +(tuple.get(1).asInt8())+ "]" , (i) == (tuple.get(1).asInt8()));
      assertTrue("seek check [" + (i) + " ,"  +(tuple.get(2).asFloat8())+"]" , (i) == (tuple.get(2).asFloat8()));
      
      offsets = reader.next();
      if (offsets == -1) {
        continue;
      }
      scanner.seek(offsets);
      tuple = scanner.next();
      assertTrue("[seek check " + (i + 1) + " ]" , (i + 1) == (tuple.get(0).asInt4()));
      assertTrue("[seek check " + (i + 1) + " ]" , (i + 1) == (tuple.get(1).asInt8()));
    }
  }

  @Test
  public void testBuildIndexWithAppender() throws IOException {
    meta = CatalogUtil.newTableMeta(schema, StoreType.CSV);

    Path tablePath = new Path(testDir, "BuildIndexWithAppender.csv");
    FileAppender appender  = (FileAppender) StorageManagerFactory.getStorageManager(conf).getAppender(meta, tablePath);
    appender.init();

    SortSpec [] sortKeys = new SortSpec[2];
    sortKeys[0] = new SortSpec(schema.getColumnByFQN("long"), true, false);
    sortKeys[1] = new SortSpec(schema.getColumnByFQN("double"), true, false);

    Schema keySchema = new Schema();
    keySchema.addColumn(new Column("long", Type.INT8));
    keySchema.addColumn(new Column("double", Type.FLOAT8));

    TupleComparator comp = new TupleComparator(keySchema, sortKeys);

    BSTIndex bst = new BSTIndex(conf);
    BSTIndexWriter creater = bst.getIndexWriter(new Path(testDir, "BuildIndexWithAppender.idx"),
        BSTIndex.TWO_LEVEL_INDEX, keySchema, comp);
    creater.setLoadNum(LOAD_NUM);
    creater.open();

    Tuple tuple;
    long offset;
    for(int i = 0 ; i < TUPLE_NUM; i ++ ) {
      tuple = new VTuple(5);
      tuple.put(0, DatumFactory.createInt4(i));
      tuple.put(1, DatumFactory.createInt8(i));
      tuple.put(2, DatumFactory.createFloat8(i));
      tuple.put(3, DatumFactory.createFloat4(i));
      tuple.put(4, DatumFactory.createText("field_" + i));

      offset = appender.getOffset();
      appender.addTuple(tuple);
      creater.write(tuple, offset);
    }
    appender.flush();
    appender.close();

    creater.flush();
    creater.close();


    FileStatus status = fs.getFileStatus(tablePath);
    long fileLen = status.getLen();
    Fragment tablet = new Fragment("table1_1", status.getPath(), meta, 0, fileLen);

    tuple = new VTuple(keySchema.getColumnNum());
    BSTIndexReader reader = bst.getIndexReader(new Path(testDir, "BuildIndexWithAppender.idx"),
        keySchema, comp);
    reader.open();
    SeekableScanner scanner = StorageManagerFactory.getSeekableScanner(conf, meta, tablet, meta.getSchema());
    scanner.init();

    for(int i = 0 ; i < TUPLE_NUM -1 ; i ++) {
      tuple.put(0, DatumFactory.createInt8(i));
      tuple.put(1, DatumFactory.createFloat8(i));
      long offsets = reader.find(tuple);
      scanner.seek(offsets);
      tuple = scanner.next();
      assertTrue("[seek check " + (i) + " ]", (i) == (tuple.get(1).asInt8()));
      assertTrue("[seek check " + (i) + " ]" , (i) == (tuple.get(2).asFloat8()));

      offsets = reader.next();
      if (offsets == -1) {
        continue;
      }
      scanner.seek(offsets);
      tuple = scanner.next();
      assertTrue("[seek check " + (i + 1) + " ]" , (i + 1) == (tuple.get(0).asInt4()));
      assertTrue("[seek check " + (i + 1) + " ]" , (i + 1) == (tuple.get(1).asInt8()));
    }
  }
  
  @Test
  public void testFindOmittedValueInCSV() throws IOException {
    meta = CatalogUtil.newTableMeta(schema, StoreType.CSV);
    
    Path tablePath = StorageUtil.concatPath(testDir, "FindOmittedValueInCSV.csv");
    Appender appender = StorageManagerFactory.getStorageManager(conf).getAppender(meta, tablePath);
    appender.init();
    Tuple tuple;
    for(int i = 0 ; i < TUPLE_NUM; i += 2 ) {
        tuple = new VTuple(5);
        tuple.put(0, DatumFactory.createInt4(i));
        tuple.put(1, DatumFactory.createInt8(i));
        tuple.put(2, DatumFactory.createFloat8(i));
        tuple.put(3, DatumFactory.createFloat4(i));
        tuple.put(4, DatumFactory.createText("field_" + i));
        appender.addTuple(tuple);
      }
    appender.close();

    FileStatus status = fs.getFileStatus(tablePath);
    Fragment tablet = new Fragment("table1_1", status.getPath(), meta, 0, status.getLen());
    
    SortSpec [] sortKeys = new SortSpec[2];
    sortKeys[0] = new SortSpec(schema.getColumnByFQN("long"), true, false);
    sortKeys[1] = new SortSpec(schema.getColumnByFQN("double"), true, false);

    Schema keySchema = new Schema();
    keySchema.addColumn(new Column("long", Type.INT8));
    keySchema.addColumn(new Column("double", Type.FLOAT8));

    TupleComparator comp = new TupleComparator(keySchema, sortKeys);
    
    BSTIndex bst = new BSTIndex(conf);
    BSTIndexWriter creater = bst.getIndexWriter(new Path(testDir, "FindOmittedValueInCSV.idx"),
        BSTIndex.TWO_LEVEL_INDEX, keySchema, comp);
    creater.setLoadNum(LOAD_NUM);
    creater.open();

    SeekableScanner scanner  = StorageManagerFactory.getSeekableScanner(conf, meta, tablet, meta.getSchema());
    scanner.init();

    Tuple keyTuple;
    long offset;
    while (true) {
      keyTuple = new VTuple(2);
      offset = scanner.getNextOffset();
      tuple = scanner.next();
      if (tuple == null) break;
      
      keyTuple.put(0, tuple.get(1));
      keyTuple.put(1, tuple.get(2));
      creater.write(keyTuple, offset);
    }
    
    creater.flush();
    creater.close();
    scanner.close();

    BSTIndexReader reader = bst.getIndexReader(new Path(testDir, "FindOmittedValueInCSV.idx"), keySchema, comp);
    reader.open();
    for(int i = 1 ; i < TUPLE_NUM -1 ; i+=2) {
      keyTuple.put(0, DatumFactory.createInt8(i));
      keyTuple.put(1, DatumFactory.createFloat8(i));
      long offsets = reader.find(keyTuple);
      assertEquals(-1, offsets);
    }
  }
  
  @Test
  public void testFindNextKeyValueInCSV() throws IOException {
    meta = CatalogUtil.newTableMeta(schema, StoreType.CSV);

    Path tablePath = new Path(testDir, "FindNextKeyValueInCSV.csv");
    Appender appender = StorageManagerFactory.getStorageManager(conf).getAppender(meta, tablePath);
    appender.init();
    Tuple tuple;
    for(int i = 0 ; i < TUPLE_NUM; i ++ ) {
      tuple = new VTuple(5);
      tuple.put(0, DatumFactory.createInt4(i));
      tuple.put(1, DatumFactory.createInt8(i));
      tuple.put(2, DatumFactory.createFloat8(i));
      tuple.put(3, DatumFactory.createFloat4(i));
      tuple.put(4, DatumFactory.createText("field_" + i));
      appender.addTuple(tuple);
    }
    appender.close();

    FileStatus status = fs.getFileStatus(tablePath);
    long fileLen = status.getLen();
    Fragment tablet = new Fragment("table1_1", status.getPath(), meta, 0, fileLen);
    
    SortSpec [] sortKeys = new SortSpec[2];
    sortKeys[0] = new SortSpec(schema.getColumnByFQN("int"), true, false);
    sortKeys[1] = new SortSpec(schema.getColumnByFQN("long"), true, false);

    Schema keySchema = new Schema();
    keySchema.addColumn(new Column("int", Type.INT4));
    keySchema.addColumn(new Column("long", Type.INT8));

    TupleComparator comp = new TupleComparator(keySchema, sortKeys);
    
    BSTIndex bst = new BSTIndex(conf);
    BSTIndexWriter creater = bst.getIndexWriter(new Path(testDir, "FindNextKeyValueInCSV.idx"),
        BSTIndex.TWO_LEVEL_INDEX, keySchema, comp);
    creater.setLoadNum(LOAD_NUM);
    creater.open();

    SeekableScanner scanner  = StorageManagerFactory.getSeekableScanner(conf, meta, tablet, meta.getSchema());
    scanner.init();

    Tuple keyTuple;
    long offset;
    while (true) {
      keyTuple = new VTuple(2);
      offset = scanner.getNextOffset();
      tuple = scanner.next();
      if (tuple == null) break;
      
      keyTuple.put(0, tuple.get(0));
      keyTuple.put(1, tuple.get(1));
      creater.write(keyTuple, offset);
    }
    
    creater.flush();
    creater.close();
    scanner.close();
    
    BSTIndexReader reader = bst.getIndexReader(new Path(testDir, "FindNextKeyValueInCSV.idx"),
        keySchema, comp);
    reader.open();
    scanner  = StorageManagerFactory.getSeekableScanner(conf, meta, tablet, meta.getSchema());
    scanner.init();

    Tuple result;
    for(int i = 0 ; i < TUPLE_NUM -1 ; i ++) {
      keyTuple = new VTuple(2);
      keyTuple.put(0, DatumFactory.createInt4(i));
      keyTuple.put(1, DatumFactory.createInt8(i));
      long offsets = reader.find(keyTuple, true);
      scanner.seek(offsets);
      result = scanner.next();
      assertTrue("[seek check " + (i + 1) + " ]",
          (i + 1) == (result.get(0).asInt4()));
      assertTrue("[seek check " + (i + 1) + " ]" , (i + 1) == (result.get(1).asInt8()));
      
      offsets = reader.next();
      if (offsets == -1) {
        continue;
      }
      scanner.seek(offsets);
      result = scanner.next();
      assertTrue("[seek check " + (i + 2) + " ]" , (i + 2) == (result.get(0).asInt8()));
      assertTrue("[seek check " + (i + 2) + " ]" , (i + 2) == (result.get(1).asFloat8()));
    }
  }
  
  @Test
  public void testFindNextKeyOmittedValueInCSV() throws IOException {
    meta = CatalogUtil.newTableMeta(schema, StoreType.CSV);

    Path tablePath = new Path(testDir, "FindNextKeyOmittedValueInCSV.csv");
    Appender appender = StorageManagerFactory.getStorageManager(conf).getAppender(meta, tablePath);
    appender.init();
    Tuple tuple;
    for(int i = 0 ; i < TUPLE_NUM; i+=2) {
      tuple = new VTuple(5);
      tuple.put(0, DatumFactory.createInt4(i));
      tuple.put(1, DatumFactory.createInt8(i));
      tuple.put(2, DatumFactory.createFloat8(i));
      tuple.put(3, DatumFactory.createFloat4(i));
      tuple.put(4, DatumFactory.createText("field_" + i));
      appender.addTuple(tuple);
    }
    appender.close();

    FileStatus status = fs.getFileStatus(tablePath);
    long fileLen = status.getLen();
    Fragment tablet = new Fragment("table1_1", status.getPath(), meta, 0, fileLen);
    
    SortSpec [] sortKeys = new SortSpec[2];
    sortKeys[0] = new SortSpec(schema.getColumnByFQN("int"), true, false);
    sortKeys[1] = new SortSpec(schema.getColumnByFQN("long"), true, false);

    Schema keySchema = new Schema();
    keySchema.addColumn(new Column("int", Type.INT4));
    keySchema.addColumn(new Column("long", Type.INT8));

    TupleComparator comp = new TupleComparator(keySchema, sortKeys);
    
    BSTIndex bst = new BSTIndex(conf);
    BSTIndexWriter creater = bst.getIndexWriter(new Path(testDir,
        "FindNextKeyOmittedValueInCSV.idx"), BSTIndex.TWO_LEVEL_INDEX, keySchema, comp);
    creater.setLoadNum(LOAD_NUM);
    creater.open();

    SeekableScanner scanner  = StorageManagerFactory.getSeekableScanner(conf, meta, tablet, meta.getSchema());
    scanner.init();

    Tuple keyTuple;
    long offset;
    while (true) {
      keyTuple = new VTuple(2);
      offset = scanner.getNextOffset();
      tuple = scanner.next();
      if (tuple == null) break;
      
      keyTuple.put(0, tuple.get(0));
      keyTuple.put(1, tuple.get(1));
      creater.write(keyTuple, offset);
    }
    
    creater.flush();
    creater.close();
    scanner.close();
    
    BSTIndexReader reader = bst.getIndexReader(new Path(testDir, "FindNextKeyOmittedValueInCSV.idx"),
        keySchema, comp);
    reader.open();
    scanner  = StorageManagerFactory.getSeekableScanner(conf, meta, tablet, meta.getSchema());
    scanner.init();

    if(scanner instanceof FileScannerV2) {
      ((FileScannerV2)scanner).waitScanStart();
    }

    Tuple result;
    for(int i = 1 ; i < TUPLE_NUM -1 ; i+=2) {
      keyTuple = new VTuple(2);
      keyTuple.put(0, DatumFactory.createInt4(i));
      keyTuple.put(1, DatumFactory.createInt8(i));
      long offsets = reader.find(keyTuple, true);
      scanner.seek(offsets);
      result = scanner.next();
      assertTrue("[seek check " + (i + 1) + " ]" , (i + 1) == (result.get(0).asInt4()));
      assertTrue("[seek check " + (i + 1) + " ]" , (i + 1) == (result.get(1).asInt8()));
    }
  }

  /*
  @Test
  public void testFindValueInRaw() throws IOException {
    meta = TCatUtil.newTableMeta(schema, StoreType.RAW);
    
    sm.initTableBase(meta, "table1");
    Appender appender  = sm.getAppender(meta, "table1", "table1.csv");
    Tuple tuple;
    for(int i = 0 ; i < TUPLE_NUM; i ++ ) {
        tuple = new VTuple(5);
        tuple.put(0, DatumFactory.createInt4(i));
        tuple.put(1, DatumFactory.createInt8(i));
        tuple.put(2, DatumFactory.createFloat8(i));
        tuple.put(3, DatumFactory.createFloat4(i));
        tuple.put(4, DatumFactory.createText("field_"+i));
        appender.addTuple(tuple);
      }
    appender.close();
    
    appender.close();

    FileStatus status = sm.listTableFiles("table1")[0];
    long fileLen = status.getLen();
    Fragment tablet = new Fragment("table1_1", status.getPath(), meta, 0, fileLen, null);
    
    SortSpec [] sortKeys = new SortSpec[2];
    sortKeys[0] = new SortSpec(schema.getColumnByFQN("long"), false, false);
    sortKeys[1] = new SortSpec(schema.getColumnByFQN("double"), true, false);

    Schema keySchema = new Schema();
    keySchema.addColumn(new Column("long", DataType.LONG));
    keySchema.addColumn(new Column("double", DataType.DOUBLE));

    TupleComparator comp = new TupleComparator(keySchema, sortKeys);
    
    BSTIndex bst = new BSTIndex(conf);
    BSTIndexWriter creater = bst.getIndexWriter(new Path(testDir, "FindValueInRawBSTIndex.idx"),
        BSTIndex.TWO_LEVEL_INDEX, keySchema, comp);
    creater.setLoadNum(LOAD_NUM);
    creater.open();

    SeekableScanner scanner  = (SeekableScanner)(sm.getScanner(meta, new Fragment[]{tablet}));
    Tuple keyTuple;
    long offset;
    while (true) {
      keyTuple = new VTuple(2);
      offset = scanner.getNextOffset();
      tuple = scanner.next();
      if (tuple == null) break;
      
      keyTuple.put(0, tuple.get(1));
      keyTuple.put(1, tuple.get(2));
      creater.write(keyTuple, offset);
    }
    
    creater.flush();
    creater.close();
    scanner.close();
    
    tuple = new VTuple(keySchema.getColumnNum());
    BSTIndexReader reader = bst.getIndexReader(new Path(testDir, "FindValueInRawBSTIndex.idx"), keySchema, comp);
    reader.open();
    scanner  = (SeekableScanner)(sm.getScanner(meta, new Fragment[]{tablet}));
    for(int i = 0 ; i < TUPLE_NUM -1 ; i ++) {
      tuple.put(0, DatumFactory.createInt8(i));
      tuple.put(1, DatumFactory.createFloat8(i));
      long offsets = reader.find(tuple, false);
      scanner.seek(offsets);
      tuple = scanner.next();
      assertTrue("[seek check " + (i) + " ]" , (i) == (tuple.get(1).asLong()));
      assertTrue("[seek check " + (i) + " ]" , (i) == (tuple.get(2).asDouble()));
    }
  }
  
  @Test
  public void testFindOmittedValueInRaw() throws IOException {
    meta = TCatUtil.newTableMeta(schema, StoreType.RAW);
    
    sm.initTableBase(meta, "table1");
    Appender appender  = sm.getAppender(meta, "table1", "table1.csv");
    Tuple tuple;
    for(int i = 0 ; i < TUPLE_NUM; i += 2 ) {
        tuple = new VTuple(5);
        tuple.put(0, DatumFactory.createInt4(i));
        tuple.put(1, DatumFactory.createInt8(i));
        tuple.put(2, DatumFactory.createFloat8(i));
        tuple.put(3, DatumFactory.createFloat4(i));
        tuple.put(4, DatumFactory.createText("field_"+i));
        appender.addTuple(tuple);
      }
    appender.close();
    
    appender.close();

    FileStatus status = sm.listTableFiles("table1")[0];
    long fileLen = status.getLen();
    Fragment tablet = new Fragment("table1_1", status.getPath(), meta, 0, fileLen, null);
    
    SortSpec [] sortKeys = new SortSpec[2];
    sortKeys[0] = new SortSpec(schema.getColumnByFQN("long"), false, false);
    sortKeys[1] = new SortSpec(schema.getColumnByFQN("double"), true, false);

    Schema keySchema = new Schema();
    keySchema.addColumn(new Column("long", DataType.LONG));
    keySchema.addColumn(new Column("double", DataType.DOUBLE));

    TupleComparator comp = new TupleComparator(keySchema, sortKeys);
    
    BSTIndex bst = new BSTIndex(conf);
    BSTIndexWriter creater = bst.getIndexWriter(new Path(testDir, "FindOmittedValueInRaw.idx"),
        BSTIndex.TWO_LEVEL_INDEX, keySchema, comp);
    creater.setLoadNum(LOAD_NUM);
    creater.open();

    SeekableScanner scanner  = (SeekableScanner)(sm.getScanner(meta, new Fragment[]{tablet}));
    Tuple keyTuple;
    long offset;
    while (true) {
      keyTuple = new VTuple(2);
      offset = scanner.getNextOffset();
      tuple = scanner.next();
      if (tuple == null) break;
      
      keyTuple.put(0, tuple.get(1));
      keyTuple.put(1, tuple.get(2));
      creater.write(keyTuple, offset);
    }
    
    creater.flush();
    creater.close();
    scanner.close();
    
    tuple = new VTuple(keySchema.getColumnNum());
    BSTIndexReader reader = bst.getIndexReader(new Path(testDir, "FindOmittedValueInRaw.idx"),
        keySchema, comp);
    reader.open();
    for(int i = 1 ; i < TUPLE_NUM -1 ; i+=2) {
      tuple.put(0, DatumFactory.createInt8(i));
      tuple.put(1, DatumFactory.createFloat8(i));
      long offsets = reader.find(tuple, false);
      assertEquals(-1, offsets);
    }
  }
  
  @Test
  public void testFindNextKeyValueInRaw() throws IOException {
    meta = TCatUtil.newTableMeta(schema, StoreType.RAW);

    sm.initTableBase(meta, "table1");
    Appender appender = sm.getAppender(meta, "table1", "table1.csv");
    Tuple tuple;
    for(int i = 0 ; i < TUPLE_NUM; i ++ ) {
      tuple = new VTuple(5);
      tuple.put(0, DatumFactory.createInt4(i));
      tuple.put(1, DatumFactory.createInt8(i));
      tuple.put(2, DatumFactory.createFloat8(i));
      tuple.put(3, DatumFactory.createFloat4(i));
      tuple.put(4, DatumFactory.createText("field_"+i));
      appender.addTuple(tuple);
    }
    appender.close();

    FileStatus status = sm.listTableFiles("table1")[0];
    long fileLen = status.getLen();
    Fragment tablet = new Fragment("table1_1", status.getPath(), meta, 0, fileLen, null);
    
    SortSpec [] sortKeys = new SortSpec[2];
    sortKeys[0] = new SortSpec(schema.getColumnByFQN("int"), true, false);
    sortKeys[1] = new SortSpec(schema.getColumnByFQN("long"), true, false);

    Schema keySchema = new Schema();
    keySchema.addColumn(new Column("int", DataType.INT));
    keySchema.addColumn(new Column("long", DataType.LONG));

    TupleComparator comp = new TupleComparator(keySchema, sortKeys);
    
    BSTIndex bst = new BSTIndex(conf);
    BSTIndexWriter creater = bst.getIndexWriter(new Path(testDir, "FindOmittedValueInRaw.idx"),
        BSTIndex.TWO_LEVEL_INDEX, keySchema, comp);
    creater.setLoadNum(LOAD_NUM);
    creater.open();

    SeekableScanner scanner  = (SeekableScanner)(sm.getScanner(meta, new Fragment[]{tablet}));
    Tuple keyTuple;
    long offset;
    while (true) {
      keyTuple = new VTuple(2);
      offset = scanner.getNextOffset();
      tuple = scanner.next();
      if (tuple == null) break;
      
      keyTuple.put(0, tuple.get(0));
      keyTuple.put(1, tuple.get(1));
      creater.write(keyTuple, offset);
    }
    
    creater.flush();
    creater.close();
    scanner.close();
    
    BSTIndexReader reader = bst.getIndexReader(new Path(testDir, "FindOmittedValueInRaw.idx"), keySchema, comp);
    reader.open();
    scanner  = (SeekableScanner)(sm.getScanner(meta, new Fragment[]{tablet}));
    Tuple result;
    for(int i = 0 ; i < TUPLE_NUM -1 ; i ++) {
      keyTuple = new VTuple(2);
      keyTuple.put(0, DatumFactory.createInt4(i));
      keyTuple.put(1, DatumFactory.createInt8(i));
      long offsets = reader.find(keyTuple, true);
      scanner.seek(offsets);
      result = scanner.next();
      assertTrue("[seek check " + (i + 1) + " ]",
          (i + 1) == (result.get(0).asInt()));
      assertTrue("[seek check " + (i + 1) + " ]" , (i + 1) == (result.get(1).asLong()));
      
      offsets = reader.next();
      if (offsets == -1) {
        continue;
      }
      scanner.seek(offsets);
      result = scanner.next();
      assertTrue("[seek check " + (i + 2) + " ]" , (i + 2) == (result.get(0).asLong()));
      assertTrue("[seek check " + (i + 2) + " ]" , (i + 2) == (result.get(1).asDouble()));
    }
  }
  
  @Test
  public void testFindNextKeyOmittedValueInRaw() throws IOException {
    meta = TCatUtil.newTableMeta(schema, StoreType.RAW);

    sm.initTableBase(meta, "table1");
    Appender appender = sm.getAppender(meta, "table1", "table1.csv");
    Tuple tuple;
    for(int i = 0 ; i < TUPLE_NUM; i+=2) {
      tuple = new VTuple(5);
      tuple.put(0, DatumFactory.createInt4(i));
      tuple.put(1, DatumFactory.createInt8(i));
      tuple.put(2, DatumFactory.createFloat8(i));
      tuple.put(3, DatumFactory.createFloat4(i));
      tuple.put(4, DatumFactory.createText("field_"+i));
      appender.addTuple(tuple);
    }
    appender.close();

    FileStatus status = sm.listTableFiles("table1")[0];
    long fileLen = status.getLen();
    Fragment tablet = new Fragment("table1_1", status.getPath(), meta, 0, fileLen, null);
    
    SortSpec [] sortKeys = new SortSpec[2];
    sortKeys[0] = new SortSpec(schema.getColumnByFQN("int"), true, false);
    sortKeys[1] = new SortSpec(schema.getColumnByFQN("long"), true, false);

    Schema keySchema = new Schema();
    keySchema.addColumn(new Column("int", DataType.INT));
    keySchema.addColumn(new Column("long", DataType.LONG));

    TupleComparator comp = new TupleComparator(keySchema, sortKeys);
    
    BSTIndex bst = new BSTIndex(conf);
    BSTIndexWriter creater = bst.getIndexWriter(new Path(testDir, "FindNextKeyOmittedValueInRaw.idx"),
        BSTIndex.TWO_LEVEL_INDEX, keySchema, comp);
    creater.setLoadNum(LOAD_NUM);
    creater.open();

    SeekableScanner scanner  = (SeekableScanner)(sm.getScanner(meta, new Fragment[]{tablet}));
    Tuple keyTuple;
    long offset;
    while (true) {
      keyTuple = new VTuple(2);
      offset = scanner.getNextOffset();
      tuple = scanner.next();
      if (tuple == null) break;
      
      keyTuple.put(0, tuple.get(0));
      keyTuple.put(1, tuple.get(1));
      creater.write(keyTuple, offset);
    }
    
    creater.flush();
    creater.close();
    scanner.close();
    
    BSTIndexReader reader = bst.getIndexReader(new Path(testDir, "FindNextKeyOmittedValueInRaw.idx"),
        keySchema, comp);
    reader.open();
    scanner  = (SeekableScanner)(sm.getScanner(meta, new Fragment[]{tablet}));
    Tuple result;
    for(int i = 1 ; i < TUPLE_NUM -1 ; i+=2) {
      keyTuple = new VTuple(2);
      keyTuple.put(0, DatumFactory.createInt4(i));
      keyTuple.put(1, DatumFactory.createInt8(i));
      long offsets = reader.find(keyTuple, true);
      scanner.seek(offsets);
      result = scanner.next();
      assertTrue("[seek check " + (i + 1) + " ]" , (i + 1) == (result.get(0).asInt()));
      assertTrue("[seek check " + (i + 1) + " ]" , (i + 1) == (result.get(1).asLong()));      
    }
  }
  
  @Test
  public void testNextInRaw() throws IOException {
    meta = TCatUtil.newTableMeta(schema, StoreType.RAW);

    sm.initTableBase(meta, "table1");
    Appender appender = sm.getAppender(meta, "table1", "table1.csv");
    Tuple tuple;
    for(int i = 0 ; i < TUPLE_NUM; i ++ ) {
      tuple = new VTuple(5);
      tuple.put(0, DatumFactory.createInt4(i));
      tuple.put(1, DatumFactory.createInt8(i));
      tuple.put(2, DatumFactory.createFloat8(i));
      tuple.put(3, DatumFactory.createFloat4(i));
      tuple.put(4, DatumFactory.createText("field_"+i));
      appender.addTuple(tuple);
    }
    appender.close();

    FileStatus status = sm.listTableFiles("table1")[0];
    long fileLen = status.getLen();
    Fragment tablet = new Fragment("table1_1", status.getPath(), meta, 0, fileLen, null);
    
    SortSpec [] sortKeys = new SortSpec[2];
    sortKeys[0] = new SortSpec(schema.getColumnByFQN("int"), true, false);
    sortKeys[1] = new SortSpec(schema.getColumnByFQN("long"), true, false);

    Schema keySchema = new Schema();
    keySchema.addColumn(new Column("int", DataType.INT));
    keySchema.addColumn(new Column("long", DataType.LONG));

    TupleComparator comp = new TupleComparator(keySchema, sortKeys);
    
    BSTIndex bst = new BSTIndex(conf);
    BSTIndexWriter creater = bst.getIndexWriter(new Path(testDir, "FindNextKeyOmittedValueInRaw.idx"),
        BSTIndex.TWO_LEVEL_INDEX, keySchema, comp);
    creater.setLoadNum(LOAD_NUM);
    creater.open();

    SeekableScanner scanner  = (SeekableScanner)(sm.getScanner(meta, new Fragment[]{tablet}));
    Tuple keyTuple;
    long offset;
    while (true) {
      keyTuple = new VTuple(2);
      offset = scanner.getNextOffset();
      tuple = scanner.next();
      if (tuple == null) break;
      
      keyTuple.put(0, tuple.get(0));
      keyTuple.put(1, tuple.get(1));
      creater.write(keyTuple, offset);
    }
    
    creater.flush();
    creater.close();
    scanner.close();
    
    BSTIndexReader reader = bst.getIndexReader(new Path(testDir, "FindNextKeyOmittedValueInRaw.idx"),
        keySchema, comp);
    reader.open();
    scanner  = (SeekableScanner)(sm.getScanner(meta, new Fragment[]{tablet}));
    Tuple result;

    keyTuple = new VTuple(2);
    keyTuple.put(0, DatumFactory.createInt4(0));
    keyTuple.put(1, DatumFactory.createInt8(0));
    long offsets = reader.find(keyTuple);
    scanner.seek(offsets);
    result = scanner.next();
    assertTrue("[seek check " + 0 + " ]" , (0) == (result.get(0).asInt()));
    assertTrue("[seek check " + 0 + " ]" , (0) == (result.get(1).asLong()));
      
    for (int i = 1; i < TUPLE_NUM; i++) {
      offsets = reader.next();
      
      scanner.seek(offsets);
      result = scanner.next();
      assertEquals(i, result.get(0).asInt());
      assertEquals(i, result.get(1).asLong());
    }
  }

  @Test
  public void testFindMinValue() throws IOException {
    meta = TCatUtil.newTableMeta(schema, StoreType.CSV);

    sm.initTableBase(meta, "table1");
    Appender appender  = sm.getAppender(meta, "table1", "table1.csv");
    Tuple tuple;
    for(int i = 5 ; i < TUPLE_NUM + 5; i ++ ) {
      tuple = new VTuple(5);
      tuple.put(0, DatumFactory.createInt4(i));
      tuple.put(1, DatumFactory.createInt8(i));
      tuple.put(2, DatumFactory.createFloat8(i));
      tuple.put(3, DatumFactory.createFloat4(i));
      tuple.put(4, DatumFactory.createText("field_"+i));
      appender.addTuple(tuple);
    }
    appender.close();

    appender.close();

    FileStatus status = sm.listTableFiles("table1")[0];
    long fileLen = status.getLen();
    Fragment tablet = new Fragment("table1_1", status.getPath(), meta, 0, fileLen, null);

    SortSpec [] sortKeys = new SortSpec[2];
    sortKeys[0] = new SortSpec(schema.getColumnByFQN("long"), true, false);
    sortKeys[1] = new SortSpec(schema.getColumnByFQN("double"), true, false);

    Schema keySchema = new Schema();
    keySchema.addColumn(new Column("long", DataType.LONG));
    keySchema.addColumn(new Column("double", DataType.DOUBLE));

    TupleComparator comp = new TupleComparator(keySchema, sortKeys);

    BSTIndex bst = new BSTIndex(conf);
    BSTIndexWriter creater = bst.getIndexWriter(new Path(testDir, "Test.idx"), BSTIndex.TWO_LEVEL_INDEX,
        keySchema, comp);
    creater.setLoadNum(LOAD_NUM);
    creater.open();

    SeekableScanner scanner  = (SeekableScanner)(sm.getScanner(meta, new Fragment[]{tablet}));
    Tuple keyTuple;
    long offset;
    while (true) {
      keyTuple = new VTuple(2);
      offset = scanner.getNextOffset();
      tuple = scanner.next();
      if (tuple == null) break;

      keyTuple.put(0, tuple.get(1));
      keyTuple.put(1, tuple.get(2));
      creater.write(keyTuple, offset);
    }

    creater.flush();
    creater.close();
    scanner.close();

    tuple = new VTuple(keySchema.getColumnNum());
    BSTIndexReader reader = bst.getIndexReader(new Path(testDir, "Test.idx"), keySchema, comp);
    reader.open();
    scanner  = (SeekableScanner)(sm.getScanner(meta, new Fragment[]{tablet}));
    tuple.put(0, DatumFactory.createInt8(0));
    tuple.put(1, DatumFactory.createFloat8(0));

    offset = reader.find(tuple);
    assertEquals(-1, offset);

    offset = reader.find(tuple , true);
    assertTrue(offset >= 0);
    scanner.seek(offset);
    tuple = scanner.next();
    assertEquals(5, tuple.get(1).asInt());
    assertEquals(5l, tuple.get(2).asLong());
  }

  @Test
  public void testMinMax() throws IOException {
    meta = TCatUtil.newTableMeta(schema, StoreType.RAW);

    sm.initTableBase(meta, "table1");
    Appender appender = sm.getAppender(meta, "table1", "table1.csv");
    Tuple tuple;
    for(int i = 5 ; i < TUPLE_NUM; i ++ ) {
      tuple = new VTuple(5);
      tuple.put(0, DatumFactory.createInt4(i));
      tuple.put(1, DatumFactory.createInt8(i));
      tuple.put(2, DatumFactory.createFloat8(i));
      tuple.put(3, DatumFactory.createFloat4(i));
      tuple.put(4, DatumFactory.createText("field_"+i));
      appender.addTuple(tuple);
    }
    appender.close();

    FileStatus status = sm.listTableFiles("table1")[0];
    long fileLen = status.getLen();
    Fragment tablet = new Fragment("table1_1", status.getPath(), meta, 0, fileLen, null);

    SortSpec [] sortKeys = new SortSpec[2];
    sortKeys[0] = new SortSpec(schema.getColumnByFQN("int"), true, false);
    sortKeys[1] = new SortSpec(schema.getColumnByFQN("long"), true, false);

    Schema keySchema = new Schema();
    keySchema.addColumn(new Column("int", DataType.INT));
    keySchema.addColumn(new Column("long", DataType.LONG));

    TupleComparator comp = new TupleComparator(keySchema, sortKeys);

    BSTIndex bst = new BSTIndex(conf);
    BSTIndexWriter creater = bst.getIndexWriter(new Path(testDir, "FindNextKeyOmittedValueInRaw.idx"),
        BSTIndex.TWO_LEVEL_INDEX, keySchema, comp);
    creater.setLoadNum(LOAD_NUM);
    creater.open();

    SeekableScanner scanner  = (SeekableScanner)(sm.getScanner(meta, new Fragment[]{tablet}));
    Tuple keyTuple;
    long offset;
    while (true) {
      keyTuple = new VTuple(2);
      offset = scanner.getNextOffset();
      tuple = scanner.next();
      if (tuple == null) break;

      keyTuple.put(0, tuple.get(0));
      keyTuple.put(1, tuple.get(1));
      creater.write(keyTuple, offset);
    }

    creater.flush();
    creater.close();
    scanner.close();

    BSTIndexReader reader = bst.getIndexReader(new Path(testDir, "FindNextKeyOmittedValueInRaw.idx"),
        keySchema, comp);
    reader.open();

    Tuple min = reader.getFirstKey();
    assertEquals(5, min.get(0).asInt());
    assertEquals(5l, min.get(0).asLong());

    Tuple max = reader.getLastKey();
    assertEquals(TUPLE_NUM - 1, max.get(0).asInt());
    assertEquals(TUPLE_NUM - 1, max.get(0).asLong());
  }

  private class ConcurrentAccessor implements Runnable {
    final BSTIndexReader reader;
    final Random rnd = new Random(System.currentTimeMillis());
    boolean failed = false;

    ConcurrentAccessor(BSTIndexReader reader) {
      this.reader = reader;
    }

    public boolean isFailed() {
      return this.failed;
    }

    @Override
    public void run() {
      Tuple findKey = new VTuple(2);
      int keyVal;
      for (int i = 0; i < 10000; i++) {
        keyVal = rnd.nextInt(10000);
        findKey.put(0, DatumFactory.createInt4(keyVal));
        findKey.put(1, DatumFactory.createInt8(keyVal));
        try {
          assertTrue(reader.find(findKey) != -1);
        } catch (Exception e) {
          e.printStackTrace();
          this.failed = true;
        }
      }
    }
  }

  @Test
  public void testConcurrentAccess() throws IOException, InterruptedException {
    meta = TCatUtil.newTableMeta(schema, StoreType.RAW);

    sm.initTableBase(meta, "table1");
    Appender appender = sm.getAppender(meta, "table1", "table1.csv");
    Tuple tuple;
    for(int i = 0 ; i < TUPLE_NUM; i ++ ) {
      tuple = new VTuple(5);
      tuple.put(0, DatumFactory.createInt4(i));
      tuple.put(1, DatumFactory.createInt8(i));
      tuple.put(2, DatumFactory.createFloat8(i));
      tuple.put(3, DatumFactory.createFloat4(i));
      tuple.put(4, DatumFactory.createText("field_"+i));
      appender.addTuple(tuple);
    }
    appender.close();

    FileStatus status = sm.listTableFiles("table1")[0];
    long fileLen = status.getLen();
    Fragment tablet = new Fragment("table1_1", status.getPath(), meta, 0, fileLen, null);

    SortSpec [] sortKeys = new SortSpec[2];
    sortKeys[0] = new SortSpec(schema.getColumnByFQN("int"), true, false);
    sortKeys[1] = new SortSpec(schema.getColumnByFQN("long"), true, false);

    Schema keySchema = new Schema();
    keySchema.addColumn(new Column("int", DataType.INT));
    keySchema.addColumn(new Column("long", DataType.LONG));

    TupleComparator comp = new TupleComparator(keySchema, sortKeys);

    BSTIndex bst = new BSTIndex(conf);
    BSTIndexWriter creater = bst.getIndexWriter(new Path(testDir, "ConcurrentAccess.idx"),
        BSTIndex.TWO_LEVEL_INDEX, keySchema, comp);
    creater.setLoadNum(LOAD_NUM);
    creater.open();

    SeekableScanner scanner  = (SeekableScanner)(sm.getScanner(meta, new Fragment[]{tablet}));
    Tuple keyTuple;
    long offset;
    while (true) {
      keyTuple = new VTuple(2);
      offset = scanner.getNextOffset();
      tuple = scanner.next();
      if (tuple == null) break;

      keyTuple.put(0, tuple.get(0));
      keyTuple.put(1, tuple.get(1));
      creater.write(keyTuple, offset);
    }

    creater.flush();
    creater.close();
    scanner.close();

    BSTIndexReader reader = bst.getIndexReader(new Path(testDir, "ConcurrentAccess.idx"),
        keySchema, comp);
    reader.open();

    Thread [] threads = new Thread[5];
    ConcurrentAccessor [] accs = new ConcurrentAccessor[5];
    for (int i = 0; i < threads.length; i++) {
      accs[i] = new ConcurrentAccessor(reader);
      threads[i] = new Thread(accs[i]);
      threads[i].start();
    }

    for (int i = 0; i < threads.length; i++) {
      threads[i].join();
      assertFalse(accs[i].isFailed());
    }
  }

  @Test
  public void testFindValueInCSVDescOrder() throws IOException {
    meta = TCatUtil.newTableMeta(schema, StoreType.CSV);

    sm.initTableBase(meta, "table1");
    Appender appender  = sm.getAppender(meta, "table1", "table1.csv");
    Tuple tuple;
    for(int i = (TUPLE_NUM - 1); i >= 0; i -- ) {
      tuple = new VTuple(5);
      tuple.put(0, DatumFactory.createInt4(i));
      tuple.put(1, DatumFactory.createInt8(i));
      tuple.put(2, DatumFactory.createFloat8(i));
      tuple.put(3, DatumFactory.createFloat4(i));
      tuple.put(4, DatumFactory.createText("field_"+i));
      appender.addTuple(tuple);
    }
    appender.close();

    appender.close();

    FileStatus status = sm.listTableFiles("table1")[0];
    long fileLen = status.getLen();
    Fragment tablet = new Fragment("table1_1", status.getPath(), meta, 0, fileLen, null);

    SortSpec [] sortKeys = new SortSpec[2];
    sortKeys[0] = new SortSpec(schema.getColumnByFQN("long"), false, false);
    sortKeys[1] = new SortSpec(schema.getColumnByFQN("double"), false, false);

    Schema keySchema = new Schema();
    keySchema.addColumn(new Column("long", DataType.LONG));
    keySchema.addColumn(new Column("double", DataType.DOUBLE));

    TupleComparator comp = new TupleComparator(keySchema, sortKeys);

    BSTIndex bst = new BSTIndex(conf);
    BSTIndexWriter creater = bst.getIndexWriter(new Path(testDir, "FindValueInCSV.idx"), BSTIndex.TWO_LEVEL_INDEX,
        keySchema, comp);
    creater.setLoadNum(LOAD_NUM);
    creater.open();

    SeekableScanner scanner  = (SeekableScanner)(sm.getScanner(meta, new Fragment[]{tablet}));
    Tuple keyTuple;
    long offset;
    while (true) {
      keyTuple = new VTuple(2);
      offset = scanner.getNextOffset();
      tuple = scanner.next();
      if (tuple == null) break;

      keyTuple.put(0, tuple.get(1));
      keyTuple.put(1, tuple.get(2));
      creater.write(keyTuple, offset);
    }

    creater.flush();
    creater.close();
    scanner.close();

    tuple = new VTuple(keySchema.getColumnNum());
    BSTIndexReader reader = bst.getIndexReader(new Path(testDir, "FindValueInCSV.idx"), keySchema, comp);
    reader.open();
    scanner  = (SeekableScanner)(sm.getScanner(meta, new Fragment[]{tablet}));
    for(int i = (TUPLE_NUM - 1) ; i > 0  ; i --) {
      tuple.put(0, DatumFactory.createInt8(i));
      tuple.put(1, DatumFactory.createFloat8(i));
      long offsets = reader.find(tuple);
      scanner.seek(offsets);
      tuple = scanner.next();
      assertTrue("seek check [" + (i) + " ," +(tuple.get(1).asLong())+ "]" , (i) == (tuple.get(1).asLong()));
      assertTrue("seek check [" + (i) + " ,"  +(tuple.get(2).asDouble())+"]" , (i) == (tuple.get(2).asDouble()));

      offsets = reader.next();
      if (offsets == -1) {
        continue;
      }
      scanner.seek(offsets);
      tuple = scanner.next();
      assertTrue("[seek check " + (i - 1) + " ]" , (i - 1) == (tuple.get(0).asInt()));
      assertTrue("[seek check " + (i - 1) + " ]" , (i - 1) == (tuple.get(1).asLong()));
    }
  }

  @Test
  public void testFindNextKeyValueInCSVDescOrder() throws IOException {
    meta = TCatUtil.newTableMeta(schema, StoreType.CSV);

    sm.initTableBase(meta, "table1");
    Appender appender = sm.getAppender(meta, "table1", "table1.csv");
    Tuple tuple;
    for(int i = (TUPLE_NUM - 1); i >= 0; i --) {
      tuple = new VTuple(5);
      tuple.put(0, DatumFactory.createInt4(i));
      tuple.put(1, DatumFactory.createInt8(i));
      tuple.put(2, DatumFactory.createFloat8(i));
      tuple.put(3, DatumFactory.createFloat4(i));
      tuple.put(4, DatumFactory.createText("field_"+i));
      appender.addTuple(tuple);
    }
    appender.close();

    FileStatus status = sm.listTableFiles("table1")[0];
    long fileLen = status.getLen();
    Fragment tablet = new Fragment("table1_1", status.getPath(), meta, 0, fileLen, null);

    SortSpec [] sortKeys = new SortSpec[2];
    sortKeys[0] = new SortSpec(schema.getColumnByFQN("int"), false, false);
    sortKeys[1] = new SortSpec(schema.getColumnByFQN("long"), false, false);

    Schema keySchema = new Schema();
    keySchema.addColumn(new Column("int", DataType.INT));
    keySchema.addColumn(new Column("long", DataType.LONG));

    TupleComparator comp = new TupleComparator(keySchema, sortKeys);

    BSTIndex bst = new BSTIndex(conf);
    BSTIndexWriter creater = bst.getIndexWriter(new Path(testDir, "FindNextKeyValueInCSV.idx"),
        BSTIndex.TWO_LEVEL_INDEX, keySchema, comp);
    creater.setLoadNum(LOAD_NUM);
    creater.open();

    SeekableScanner scanner  = (SeekableScanner)(sm.getScanner(meta, new Fragment[]{tablet}));
    Tuple keyTuple;
    long offset;
    while (true) {
      keyTuple = new VTuple(2);
      offset = scanner.getNextOffset();
      tuple = scanner.next();
      if (tuple == null) break;

      keyTuple.put(0, tuple.get(0));
      keyTuple.put(1, tuple.get(1));
      creater.write(keyTuple, offset);
    }

    creater.flush();
    creater.close();
    scanner.close();

    BSTIndexReader reader = bst.getIndexReader(new Path(testDir, "FindNextKeyValueInCSV.idx"), keySchema, comp);
    reader.open();

    assertEquals(keySchema, reader.getKeySchema());
    assertEquals(comp, reader.getComparator());

    scanner  = (SeekableScanner)(sm.getScanner(meta, new Fragment[]{tablet}));
    Tuple result;
    for(int i = (TUPLE_NUM - 1) ; i > 0 ; i --) {
      keyTuple = new VTuple(2);
      keyTuple.put(0, DatumFactory.createInt4(i));
      keyTuple.put(1, DatumFactory.createInt8(i));
      long offsets = reader.find(keyTuple, true);
      scanner.seek(offsets);
      result = scanner.next();
      assertTrue("[seek check " + (i - 1) + " ]",
          (i - 1) == (result.get(0).asInt()));
      assertTrue("[seek check " + (i - 1) + " ]" , (i - 1) == (result.get(1).asLong()));

      offsets = reader.next();
      if (offsets == -1) {
        continue;
      }
      scanner.seek(offsets);
      result = scanner.next();
      assertTrue("[seek check " + (i - 2) + " ]" , (i - 2) == (result.get(0).asLong()));
      assertTrue("[seek check " + (i - 2) + " ]" , (i - 2) == (result.get(1).asDouble()));
    }
  }
  */
}
