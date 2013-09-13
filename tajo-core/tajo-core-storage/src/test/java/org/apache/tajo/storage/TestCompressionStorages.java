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

package org.apache.tajo.storage;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.util.NativeCodeLoader;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.catalog.statistics.TableStat;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.util.CommonTestingUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class TestCompressionStorages {
  private TajoConf conf;
  private static String TEST_PATH = "target/test-data/TestCompressionStorages";

  private StoreType storeType;
  private Path testDir;
  private FileSystem fs;

  public TestCompressionStorages(StoreType type) throws IOException {
    this.storeType = type;
    conf = new TajoConf();

    testDir = CommonTestingUtil.getTestDir(TEST_PATH);
    fs = testDir.getFileSystem(conf);
  }

  @Parameterized.Parameters
  public static Collection<Object[]> generateParameters() {
    return Arrays.asList(new Object[][]{
        {StoreType.CSV}
    });
  }

  @Test
  public void testDeflateCodecCompressionData() throws IOException {
    storageCompressionTest(storeType, DeflateCodec.class);
  }

  @Test
  public void testGzipCodecCompressionData() throws IOException {
    storageCompressionTest(storeType, GzipCodec.class);
  }

  @Test
  public void testSnappyCodecCompressionData() throws IOException {
    if (SnappyCodec.isNativeCodeLoaded()) {
      storageCompressionTest(storeType, SnappyCodec.class);
    }
  }

  @Test
  public void testBzip2CodecCompressionData() throws IOException {
    storageCompressionTest(storeType, BZip2Codec.class);
  }

  @Test
  public void testLz4CodecCompressionData() throws IOException {
    if(NativeCodeLoader.isNativeCodeLoaded() && Lz4Codec.isNativeCodeLoaded())
    storageCompressionTest(storeType, Lz4Codec.class);
  }

  @Test
  public void testSplitCompressionData() throws IOException {

    Schema schema = new Schema();
    schema.addColumn("id", Type.INT4);
    schema.addColumn("age", Type.INT8);

    TableMeta meta = CatalogUtil.newTableMeta(schema, StoreType.CSV);
    meta.putOption("compression.codec", BZip2Codec.class.getCanonicalName());

    Path tablePath = new Path(testDir, "SplitCompression");
    Appender appender = StorageManagerFactory.getStorageManager(conf).getAppender(meta, tablePath);
    appender.enableStats();

    appender.init();

    String extention = "";
    if (appender instanceof CSVFile.CSVAppender) {
      extention = ((CSVFile.CSVAppender) appender).getExtension();
    }

    int tupleNum = 100000;
    VTuple vTuple;

    for (int i = 0; i < tupleNum; i++) {
      vTuple = new VTuple(2);
      vTuple.put(0, DatumFactory.createInt4(i + 1));
      vTuple.put(1, DatumFactory.createInt8(25l));
      appender.addTuple(vTuple);
    }
    appender.close();

    TableStat stat = appender.getStats();
    assertEquals(tupleNum, stat.getNumRows().longValue());
    tablePath = tablePath.suffix(extention);

    FileStatus status = fs.getFileStatus(tablePath);
    long fileLen = status.getLen();
    long randomNum = (long) (Math.random() * fileLen) + 1;

    Fragment[] tablets = new Fragment[2];
    tablets[0] = new Fragment("SplitCompression", tablePath, meta, 0, randomNum);
    tablets[1] = new Fragment("SplitCompression", tablePath, meta, randomNum, (fileLen - randomNum));

    Scanner scanner = StorageManagerFactory.getStorageManager(conf).getScanner(meta, tablets[0], schema);
    scanner.init();
    int tupleCnt = 0;
    Tuple tuple;
    while ((tuple = scanner.next()) != null) {
      tupleCnt++;
    }
    scanner.close();

    scanner = StorageManagerFactory.getStorageManager(conf).getScanner(meta, tablets[1], schema);
    scanner.init();
    while ((tuple = scanner.next()) != null) {
      tupleCnt++;
    }

    scanner.close();
    assertEquals(tupleNum, tupleCnt);
  }

  private void storageCompressionTest(StoreType storeType, Class<? extends CompressionCodec> codec) throws IOException {
    Schema schema = new Schema();
    schema.addColumn("id", Type.INT4);
    schema.addColumn("age", Type.INT8);

    TableMeta meta = CatalogUtil.newTableMeta(schema, storeType);
    meta.putOption("compression.codec", codec.getCanonicalName());

    String fileName = "Compression_" + codec.getSimpleName();
    Path tablePath = new Path(testDir, fileName);
    Appender appender = StorageManagerFactory.getStorageManager(conf).getAppender(meta, tablePath);
    appender.enableStats();

    appender.init();

    String extension = "";
    if (appender instanceof CSVFile.CSVAppender) {
      extension = ((CSVFile.CSVAppender) appender).getExtension();
    }

    int tupleNum = 10000;
    VTuple vTuple;

    for (int i = 0; i < tupleNum; i++) {
      vTuple = new VTuple(2);
      vTuple.put(0, DatumFactory.createInt4(i + 1));
      vTuple.put(1, DatumFactory.createInt8(25l));
      appender.addTuple(vTuple);
    }
    appender.close();

    TableStat stat = appender.getStats();
    assertEquals(tupleNum, stat.getNumRows().longValue());
    tablePath = tablePath.suffix(extension);
    FileStatus status = fs.getFileStatus(tablePath);
    long fileLen = status.getLen();
    Fragment[] tablets = new Fragment[1];
    tablets[0] = new Fragment(fileName, tablePath, meta, 0, fileLen);

    Scanner scanner = StorageManagerFactory.getStorageManager(conf).getScanner(meta, tablets[0], schema);
    scanner.init();
    int tupleCnt = 0;
    Tuple tuple;
    while ((tuple = scanner.next()) != null) {
      tupleCnt++;
    }
    scanner.close();
    assertEquals(tupleCnt, tupleNum);
  }
}
