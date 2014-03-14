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

package org.apache.tajo.engine.eval;

import org.apache.tajo.LocalTajoTestingUtility;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.datum.TextDatum;
import org.apache.tajo.engine.json.CoreGsonHelper;
import org.apache.tajo.engine.parser.SQLAnalyzer;
import org.apache.tajo.engine.planner.*;
import org.apache.tajo.engine.utils.SchemaUtil;
import org.apache.tajo.master.TajoMaster;
import org.apache.tajo.master.session.Session;
import org.apache.tajo.storage.LazyTuple;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.util.Bytes;
import org.apache.tajo.util.CommonTestingUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;

import static org.apache.tajo.TajoConstants.DEFAULT_DATABASE_NAME;
import static org.apache.tajo.TajoConstants.DEFAULT_TABLESPACE_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class ExprTestBase {
  private static TajoTestingCluster util;
  private static CatalogService cat;
  private static SQLAnalyzer analyzer;
  private static PreLogicalPlanVerifier preLogicalPlanVerifier;
  private static LogicalPlanner planner;
  private static LogicalOptimizer optimizer;
  private static LogicalPlanVerifier annotatedPlanVerifier;

  @BeforeClass
  public static void setUp() throws Exception {
    util = new TajoTestingCluster();
    util.startCatalogCluster();
    cat = util.getMiniCatalogCluster().getCatalog();
    cat.createTablespace(DEFAULT_TABLESPACE_NAME, "hdfs://localhost:1234/warehouse");
    cat.createDatabase(DEFAULT_DATABASE_NAME, DEFAULT_TABLESPACE_NAME);
    for (FunctionDesc funcDesc : TajoMaster.initBuiltinFunctions()) {
      cat.createFunction(funcDesc);
    }

    analyzer = new SQLAnalyzer();
    preLogicalPlanVerifier = new PreLogicalPlanVerifier(cat);
    planner = new LogicalPlanner(cat);
    optimizer = new LogicalOptimizer(util.getConfiguration());
    annotatedPlanVerifier = new LogicalPlanVerifier(util.getConfiguration(), cat);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    util.shutdownCatalogCluster();
  }

  private static void assertJsonSerDer(EvalNode expr) {
    String json = CoreGsonHelper.toJson(expr, EvalNode.class);
    EvalNode fromJson = CoreGsonHelper.fromJson(json, EvalNode.class);
    assertEquals(expr, fromJson);
  }

  /**
   * verify query syntax and get raw targets.
   *
   * @param query a query for execution
   * @param condition this parameter means whether it is for success case or is not for failure case.
   * @return
   * @throws PlanningException
   */
  private static Target[] getRawTargets(String query, boolean condition) throws PlanningException {
    Session session = LocalTajoTestingUtility.createDummySession();
    Expr expr = analyzer.parse(query);
    VerificationState state = new VerificationState();
    preLogicalPlanVerifier.verify(session, state, expr);
    if (state.getErrorMessages().size() > 0) {
      if (!condition && state.getErrorMessages().size() > 0) {
        throw new PlanningException(state.getErrorMessages().get(0));
      }
      assertFalse(state.getErrorMessages().get(0), true);
    }
    LogicalPlan plan = planner.createPlan(session, expr, true);
    optimizer.optimize(plan);
    annotatedPlanVerifier.verify(session, state, plan);

    if (state.getErrorMessages().size() > 0) {
      assertFalse(state.getErrorMessages().get(0), true);
    }

    Target [] targets = plan.getRootBlock().getRawTargets();
    if (targets == null) {
      throw new PlanningException("Wrong query statement or query plan: " + query);
    }
    for (Target t : targets) {
      assertJsonSerDer(t.getEvalTree());
    }
    return targets;
  }

  public void testSimpleEval(String query, String [] expected) throws IOException {
    testEval(null, null, null, query, expected);
  }

  public void testSimpleEval(String query, String [] expected, boolean condition) throws IOException {
    testEval(null, null, null, query, expected, ',', condition);
  }

  public void testEval(Schema schema, String tableName, String csvTuple, String query, String [] expected)
      throws IOException {
    testEval(schema, tableName, csvTuple, query, expected, ',', true);
  }

  public void testEval(Schema schema, String tableName, String csvTuple, String query, String [] expected,
                       char delimiter, boolean condition) throws IOException {
    LazyTuple lazyTuple;
    VTuple vtuple  = null;
    String qualifiedTableName = CatalogUtil.buildFQName(DEFAULT_DATABASE_NAME, tableName);
    Schema inputSchema = null;
    if (schema != null) {
      inputSchema = SchemaUtil.clone(schema);
      inputSchema.setQualifier(qualifiedTableName);

      int targetIdx [] = new int[inputSchema.size()];
      for (int i = 0; i < targetIdx.length; i++) {
        targetIdx[i] = i;
      }

      lazyTuple =
          new LazyTuple(inputSchema, Bytes.splitPreserveAllTokens(csvTuple.getBytes(), delimiter, targetIdx),0);
      vtuple = new VTuple(inputSchema.size());
      for (int i = 0; i < inputSchema.size(); i++) {
        // If null value occurs, null datum is manually inserted to an input tuple.
        if (lazyTuple.get(i) instanceof TextDatum && lazyTuple.get(i).asChars().equals("")) {
          vtuple.put(i, NullDatum.get());
        } else {
          vtuple.put(i, lazyTuple.get(i));
        }
      }
      cat.createTable(new TableDesc(qualifiedTableName, inputSchema,
          CatalogProtos.StoreType.CSV, new Options(), CommonTestingUtil.getTestDir()));
    }

    Target [] targets;

    try {
      targets = getRawTargets(query, condition);

      Tuple outTuple = new VTuple(targets.length);
      for (int i = 0; i < targets.length; i++) {
        EvalNode eval = targets[i].getEvalTree();
        outTuple.put(i, eval.eval(inputSchema, vtuple));
      }

      for (int i = 0; i < expected.length; i++) {
        assertEquals(query, expected[i], outTuple.get(i).asChars());
      }
    } catch (PlanningException e) {
      // In failure test case, an exception must occur while executing query.
      // So, we should check an error message, and return it.
      if (!condition) {
        assertEquals(expected[0], e.getMessage());
      } else {
        assertFalse(e.getMessage(), true);
      }
    } finally {
      if (schema != null) {
        cat.dropTable(qualifiedTableName);
      }
    }
  }
}
