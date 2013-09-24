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

import org.apache.hadoop.fs.Path;
import org.apache.tajo.LocalTajoTestingUtility;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.TaskAttemptContext;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.engine.parser.SQLAnalyzer;
import org.apache.tajo.engine.planner.*;
import org.apache.tajo.engine.planner.enforce.Enforcer;
import org.apache.tajo.engine.planner.logical.LogicalNode;
import org.apache.tajo.storage.*;
import org.apache.tajo.util.CommonTestingUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestExternalSortExec {
  private TajoConf conf;
  private TajoTestingCluster util;
  private final String TEST_PATH = "target/test-data/TestExternalSortExec";
  private CatalogService catalog;
  private SQLAnalyzer analyzer;
  private LogicalPlanner planner;
  private AbstractStorageManager sm;
  private Path testDir;


  private final int numTuple = 1000000;
  private Random rnd = new Random(System.currentTimeMillis());


  private TableDesc employee;

  @Before
  public void setUp() throws Exception {
    this.conf = new TajoConf();
    util = new TajoTestingCluster();
    catalog = util.startCatalogCluster().getCatalog();
    testDir = CommonTestingUtil.getTestDir(TEST_PATH);
    sm = StorageManagerFactory.getStorageManager(conf, testDir);

    Schema schema = new Schema();
    schema.addColumn("managerId", Type.INT4);
    schema.addColumn("empId", Type.INT4);
    schema.addColumn("deptName", Type.TEXT);

    TableMeta employeeMeta = CatalogUtil.newTableMeta(schema, StoreType.CSV);
    Path employeePath = new Path(testDir, "employee.csv");
    Appender appender = StorageManagerFactory.getStorageManager(conf).getAppender(employeeMeta, employeePath);
    appender.enableStats();
    appender.init();
    Tuple tuple = new VTuple(employeeMeta.getSchema().getColumnNum());
    for (int i = 0; i < numTuple; i++) {
      tuple.put(new Datum[] { DatumFactory.createInt4(rnd.nextInt(50)),
          DatumFactory.createInt4(rnd.nextInt(100)),
          DatumFactory.createText("dept_" + 123) });
      appender.addTuple(tuple);
    }
    appender.flush();
    appender.close();

    System.out.println("Total Rows: " + appender.getStats().getNumRows());

    employee = new TableDescImpl("employee", employeeMeta, employeePath);
    catalog.addTable(employee);
    analyzer = new SQLAnalyzer();
    planner = new LogicalPlanner(catalog);
  }

  @After
  public void tearDown() throws Exception {
    util.shutdownCatalogCluster();
  }

  String[] QUERIES = {
      "select managerId, empId, deptName from employee order by managerId, empId desc"
  };

  @Test
  public final void testNext() throws IOException, PlanningException {
    Fragment[] frags = sm.splitNG(conf, "employee", employee.getMeta(), employee.getPath(),
        Integer.MAX_VALUE);
    Path workDir = new Path(testDir, TestExternalSortExec.class.getName());
    TaskAttemptContext ctx = new TaskAttemptContext(conf,
        LocalTajoTestingUtility.newQueryUnitAttemptId(), new Fragment[] { frags[0] }, workDir);
    ctx.setEnforcer(new Enforcer());
    Expr expr = analyzer.parse(QUERIES[0]);
    LogicalPlan plan = planner.createPlan(expr);
    LogicalNode rootNode = plan.getRootBlock().getRoot();

    PhysicalPlanner phyPlanner = new PhysicalPlannerImpl(conf,sm);
    PhysicalExec exec = phyPlanner.createPlan(ctx, rootNode);
    
    ProjectionExec proj = (ProjectionExec) exec;

    // TODO - should be planed with user's optimization hint
    if (!(proj.getChild() instanceof ExternalSortExec)) {
      UnaryPhysicalExec sortExec = (UnaryPhysicalExec) proj.getChild();
      SeqScanExec scan = (SeqScanExec)sortExec.getChild();

      ExternalSortExec extSort = new ExternalSortExec(ctx, sm,
          ((MemSortExec)sortExec).getPlan(), scan);
      proj.setChild(extSort);
    }

    Tuple tuple;
    Datum preVal = null;
    Datum curVal;
    int cnt = 0;
    exec.init();
    long start = System.currentTimeMillis();

    while ((tuple = exec.next()) != null) {
      curVal = tuple.get(0);
      if (preVal != null) {
        assertTrue(preVal.lessThanEqual(curVal).asBool());
      }
      preVal = curVal;
      cnt++;
    }
    long end = System.currentTimeMillis();
    exec.close();
    assertEquals(numTuple, cnt);

    // for rescan test
    preVal = null;
    exec.rescan();
    cnt = 0;
    while ((tuple = exec.next()) != null) {
      curVal = tuple.get(0);
      if (preVal != null) {
        assertTrue(preVal.lessThanEqual(curVal).asBool());
      }
      preVal = curVal;
      cnt++;
    }
    assertEquals(numTuple, cnt);
    exec.close();
    System.out.println("Sort Time: " + (end - start) + " msc");
  }
}
