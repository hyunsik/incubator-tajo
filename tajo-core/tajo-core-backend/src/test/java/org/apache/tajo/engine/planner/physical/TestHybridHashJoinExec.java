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

package org.apache.tajo.engine.planner.physical;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.fs.Path;
import org.apache.tajo.LocalTajoTestingUtility;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.TaskAttemptContext;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.catalog.CatalogService;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.engine.parser.SQLAnalyzer;
import org.apache.tajo.engine.planner.LogicalPlanner;
import org.apache.tajo.engine.planner.PhysicalPlanner;
import org.apache.tajo.engine.planner.PhysicalPlannerImpl;
import org.apache.tajo.engine.planner.PlanningException;
import org.apache.tajo.engine.planner.logical.LogicalNode;
import org.apache.tajo.storage.AbstractStorageManager;
import org.apache.tajo.storage.Appender;
import org.apache.tajo.storage.Fragment;
import org.apache.tajo.storage.StorageManager;
import org.apache.tajo.storage.StorageManagerFactory;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.util.CommonTestingUtil;
import org.apache.tajo.util.TUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestHybridHashJoinExec {

  private final static long WORKING_MEMORY = 1000;

  private static DecimalFormat decimalFormat = new DecimalFormat("000");

  private TajoConf conf;
  private final String TEST_PATH = "target/test-data/TestHybridHashJoinExec";
  private TajoTestingCluster util;
  private CatalogService catalog;
  private SQLAnalyzer analyzer;
  private LogicalPlanner planner;
  private AbstractStorageManager sm;
  private Path testDir;

  private TableDesc employee;
  private TableDesc people;
  private TableMeta employeeMeta;

  @Before
  public void setUp() throws Exception {
    util = new TajoTestingCluster();
    util.initTestDir();
    catalog = util.startCatalogCluster().getCatalog();
    testDir = CommonTestingUtil.getTestDir(TEST_PATH);
    conf = util.getConfiguration();
    sm = StorageManagerFactory.getStorageManager(conf, testDir);

    Schema employeeSchema = new Schema();
    employeeSchema.addColumn("managerId", Type.INT4);
    employeeSchema.addColumn("empId", Type.INT4);
    employeeSchema.addColumn("memId", Type.INT4);
    employeeSchema.addColumn("deptName", Type.TEXT);

    employeeMeta = CatalogUtil.newTableMeta(employeeSchema, StoreType.CSV);
    Path employeePath = new Path(testDir, "employee.csv");
    Appender appender = StorageManagerFactory.getStorageManager(conf).getAppender(employeeMeta, employeePath);
    appender.init();
    Tuple tuple = new VTuple(employeeMeta.getSchema().getColumnNum());
    for (int i = 0; i < 1000; i++) {
      tuple.put(new Datum[] { DatumFactory.createInt4(i), DatumFactory.createInt4(i), DatumFactory.createInt4(10 + i),
          DatumFactory.createText("dept_" + decimalFormat.format(i)) });
      appender.addTuple(tuple);
    }

    appender.flush();
    appender.close();
    employee = CatalogUtil.newTableDesc("employee", employeeMeta, employeePath);
    catalog.addTable(employee);

    Schema peopleSchema = new Schema();
    peopleSchema.addColumn("empId", Type.INT4);
    peopleSchema.addColumn("fk_memId", Type.INT4);
    peopleSchema.addColumn("name", Type.TEXT);
    peopleSchema.addColumn("age", Type.INT4);
    TableMeta peopleMeta = CatalogUtil.newTableMeta(peopleSchema, StoreType.CSV);
    Path peoplePath = new Path(testDir, "people.csv");
    appender = StorageManagerFactory.getStorageManager(conf).getAppender(peopleMeta, peoplePath);
    appender.init();
    tuple = new VTuple(peopleMeta.getSchema().getColumnNum());
    for (int i = 1; i < 1000; i += 2) {
      tuple.put(new Datum[] { DatumFactory.createInt4(i), DatumFactory.createInt4(10 + i),
          DatumFactory.createText("name_" + decimalFormat.format(i)), DatumFactory.createInt4(30 + i) });
      appender.addTuple(tuple);
    }
    appender.flush();
    appender.close();

    people = CatalogUtil.newTableDesc("people", peopleMeta, peoplePath);
    catalog.addTable(people);
    analyzer = new SQLAnalyzer();
    planner = new LogicalPlanner(catalog);
  }

  @After
  public void tearDown() throws Exception {
    util.shutdownCatalogCluster();
  }

  String[] QUERIES = { "select managerId, e.empId, deptName, e.memId from employee as e inner join "
      + "people as p on e.empId = p.empId and e.memId = p.fk_memId" };

  private PhysicalExec getPhysicalExec(int histogramGranularity) throws IOException, PlanningException {
    Fragment[] empFrags = StorageManager.splitNG(conf, "e", employee.getMeta(), employee.getPath(),
        Integer.MAX_VALUE);
    Fragment[] peopleFrags = StorageManager.splitNG(conf, "p", people.getMeta(), people.getPath(),
        Integer.MAX_VALUE);

    Fragment[] merged = TUtil.concat(empFrags, peopleFrags);

    Path workDir = CommonTestingUtil.getTestDir("target/test-data/testInnerJoin");
    TaskAttemptContext ctx = new TaskAttemptContext(conf, LocalTajoTestingUtility.newQueryUnitAttemptId(), merged,
        workDir);

    if (histogramGranularity != -1) {
      // setting histogram
      Map<Integer, Long> histogram = new TreeMap<Integer, Long>();
      VTuple keyTuple = new VTuple(2);

      for (int i = 1; i < 1000; i += 2) {
        keyTuple.put(new Datum[] { DatumFactory.createInt4(i), DatumFactory.createInt4(10 + i) });

        int key = keyTuple.hashCode() + histogramGranularity - (keyTuple.hashCode() % histogramGranularity);
        long tupleSize = 20L;

        Long accumulated = histogram.get(key);
        if (accumulated != null) {
          histogram.put(key, accumulated + tupleSize);
        } else {
          histogram.put(key, tupleSize);
        }

      }
      ctx.setHistogram(histogram);
    }
    Expr expr = analyzer.parse(QUERIES[0]);
    LogicalNode plan = planner.createPlan(expr).getRootBlock().getRoot();

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf, sm);
    return phyPlanner.createPlan(ctx, plan);
  }

  @Test
  public final void testBucketZero() throws PlanningException, IOException {
    PhysicalExec exec = getPhysicalExec(100);
    Tuple tuple;
    int count = 0;
    int i = 1;
    exec.init();
    while ((tuple = exec.next()) != null) {
      count++;
      assertTrue(i == tuple.getInt(0).asInt4());
      assertTrue(i == tuple.getInt(1).asInt4());
      assertTrue(("dept_" + decimalFormat.format(i)).equals(tuple.getString(2).asChars()));
      assertTrue(10 + i == tuple.getInt(3).asInt4());
      i += 2;
    }
    exec.close();
    assertEquals(1000 / 2, count);
  }

  @Test
  public final void testBucketZeroWithoutHistogram() throws IOException, PlanningException {
    PhysicalExec exec = getPhysicalExec(-1);
    Tuple tuple;
    int count = 0;
    int i = 1;
    exec.init();
    while ((tuple = exec.next()) != null) {
      count++;
      assertTrue(i == tuple.getInt(0).asInt4());
      assertTrue(i == tuple.getInt(1).asInt4());
      assertTrue(("dept_" + decimalFormat.format(i)).equals(tuple.getString(2).asChars()));
      assertTrue(10 + i == tuple.getInt(3).asInt4());
      i += 2;
    }
    exec.close();
    assertEquals(1000 / 2, count);
  }

  @Test
  public final void testBucketPartition() throws IOException, PlanningException {
    PhysicalExec exec = getPhysicalExec(100);

    ProjectionExec proj = (ProjectionExec) exec;
    if (proj.getChild() instanceof HybridHashJoinExec) {
      ((HybridHashJoinExec) proj.getChild()).setWorkingMemory(WORKING_MEMORY);
    }
    Tuple tuple;
    int count = 0;
    exec.init();
    while ((tuple = exec.next()) != null) {
      count++;
      int base = tuple.getInt(0).asInt4();
      assertTrue(base % 2 != 0);
      assertTrue(tuple.getInt(1).asInt4() == base);
      assertTrue(("dept_" + decimalFormat.format(base)).equals(tuple.getString(2).asChars()));
      assertTrue(tuple.getInt(3).asInt4() == base + 10);
    }
    exec.close();
    assertEquals(1000 / 2, count);
  }

  @Test
  public final void testBucketOverflow() throws IOException, PlanningException {
    PhysicalExec exec = getPhysicalExec(66000);

    ProjectionExec proj = (ProjectionExec) exec;
    if (proj.getChild() instanceof HybridHashJoinExec) {
      ((HybridHashJoinExec) proj.getChild()).setWorkingMemory(WORKING_MEMORY);
    }
    Tuple tuple;
    int count = 0;
    exec.init();
    while ((tuple = exec.next()) != null) {
      count++;
      int base = tuple.getInt(0).asInt4();
      assertTrue(base % 2 != 0);
      assertTrue(tuple.getInt(1).asInt4() == base);
      assertTrue(("dept_" + decimalFormat.format(base)).equals(tuple.getString(2).asChars()));
      assertTrue(tuple.getInt(3).asInt4() == base + 10);
    }
    exec.close();
    assertEquals(1000 / 2, count);
  }
}
