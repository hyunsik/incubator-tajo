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

import com.google.common.collect.Sets;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.TableMetaImpl;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.catalog.proto.CatalogProtos.TableProto;
import org.apache.tajo.catalog.statistics.TableStat;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.util.FileUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class TestRowFile {
  private TajoTestingCluster cluster;
  private TajoConf conf;

  @Before
  public void setup() throws Exception {
    cluster = new TajoTestingCluster();
    conf = cluster.getConfiguration();
    conf.setInt(ConfVars.RAWFILE_SYNC_INTERVAL.varname, 100);
    cluster.startMiniDFSCluster(1);
  }

  @After
  public void teardown() throws Exception {
    cluster.shutdownMiniDFSCluster();
  }

  @Test
  public void test() throws IOException {
    Schema schema = new Schema();
    schema.addColumn("id", Type.INT4);
    schema.addColumn("age", Type.INT8);
    schema.addColumn("description", Type.TEXT);

    TableMeta meta = CatalogUtil.newTableMeta(schema, StoreType.ROWFILE);

    AbstractStorageManager sm = StorageManagerFactory.getStorageManager(conf,
        new Path(conf.get(TajoConf.ConfVars.ROOT_DIR.name())));

    Path tablePath = new Path("/test");
    Path metaPath = new Path(tablePath, ".meta");
    Path dataPath = new Path(tablePath, "test.tbl");
    FileSystem fs = sm.getFileSystem();
    fs.mkdirs(tablePath);

    FileUtil.writeProto(fs, metaPath, meta.getProto());

    Appender appender = StorageManagerFactory.getStorageManager(conf).getAppender(meta, dataPath);
    appender.enableStats();
    appender.init();

    int tupleNum = 100000;
    Tuple tuple;
    Datum stringDatum = DatumFactory.createText("abcdefghijklmnopqrstuvwxyz");
    Set<Integer> idSet = Sets.newHashSet();

    tuple = new VTuple(3);
    long start = System.currentTimeMillis();
    for(int i = 0; i < tupleNum; i++) {
      tuple.put(0, DatumFactory.createInt4(i + 1));
      tuple.put(1, DatumFactory.createInt8(25l));
      tuple.put(2, stringDatum);
      appender.addTuple(tuple);
      idSet.add(i+1);
    }

    long end = System.currentTimeMillis();
    appender.close();

    TableStat stat = appender.getStats();
    assertEquals(tupleNum, stat.getNumRows().longValue());

    System.out.println("append time: " + (end - start));

    FileStatus file = fs.getFileStatus(dataPath);
    TableProto proto = (TableProto) FileUtil.loadProto(
        cluster.getDefaultFileSystem(), metaPath, TableProto.getDefaultInstance());
    meta = new TableMetaImpl(proto);
    Fragment fragment = new Fragment("test.tbl", dataPath, meta, 0, file.getLen());

    int tupleCnt = 0;
    start = System.currentTimeMillis();
    Scanner scanner = StorageManagerFactory.getStorageManager(conf).getScanner(meta, fragment);
    scanner.init();
    while ((tuple=scanner.next()) != null) {
      tupleCnt++;
    }
    scanner.close();
    end = System.currentTimeMillis();

    assertEquals(tupleNum, tupleCnt);
    System.out.println("scan time: " + (end - start));

    tupleCnt = 0;
    long fileStart = 0;
    long fileLen = file.getLen()/13;
    System.out.println("total length: " + file.getLen());

    for (int i = 0; i < 13; i++) {
      System.out.println("range: " + fileStart + ", " + fileLen);
      fragment = new Fragment("test.tbl", dataPath, meta, fileStart, fileLen);
      scanner = new RowFile.RowFileScanner(conf, meta, fragment);
      scanner.init();
      while ((tuple=scanner.next()) != null) {
        if (!idSet.remove(tuple.get(0).asInt4())) {
          System.out.println("duplicated! " + tuple.get(0).asInt4());
        }
        tupleCnt++;
      }
      System.out.println("tuple count: " + tupleCnt);
      scanner.close();
      fileStart += fileLen;
      if (i == 11) {
        fileLen = file.getLen() - fileStart;
      }
    }

    for (Integer id : idSet) {
      System.out.println("remaining id: " + id);
    }
    assertEquals(tupleNum, tupleCnt);
  }
}
