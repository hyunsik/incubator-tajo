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

package org.apache.tajo.engine.planner;

import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.proto.CatalogProtos.FunctionType;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.engine.eval.*;
import org.apache.tajo.engine.function.builtin.SumInt;
import org.apache.tajo.engine.parser.SQLAnalyzer;
import org.apache.tajo.engine.planner.logical.*;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.TupleComparator;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.util.CommonTestingUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class TestPlannerUtil {
  private static TajoTestingCluster util;
  private static CatalogService catalog;
  private static SQLAnalyzer analyzer;
  private static LogicalPlanner planner;

  @BeforeClass
  public static void setUp() throws Exception {
    util = new TajoTestingCluster();
    util.startCatalogCluster();
    catalog = util.getMiniCatalogCluster().getCatalog();

    Schema schema = new Schema();
    schema.addColumn("name", Type.TEXT);
    schema.addColumn("empId", CatalogUtil.newSimpleDataType(Type.INT4));
    schema.addColumn("deptName", Type.TEXT);

    Schema schema2 = new Schema();
    schema2.addColumn("deptName", Type.TEXT);
    schema2.addColumn("manager", Type.TEXT);

    Schema schema3 = new Schema();
    schema3.addColumn("deptName", Type.TEXT);
    schema3.addColumn("score", CatalogUtil.newSimpleDataType(Type.INT4));

    TableMeta meta = CatalogUtil.newTableMeta(StoreType.CSV);
    TableDesc people = new TableDesc("employee", schema, meta, CommonTestingUtil.getTestDir());
    catalog.addTable(people);

    TableDesc student = new TableDesc("dept", schema2, StoreType.CSV, new Options(), CommonTestingUtil.getTestDir());
    catalog.addTable(student);

    TableDesc score = new TableDesc("score", schema3, StoreType.CSV, new Options(), CommonTestingUtil.getTestDir());
    catalog.addTable(score);

    FunctionDesc funcDesc = new FunctionDesc("sumtest", SumInt.class, FunctionType.AGGREGATION,
        CatalogUtil.newSimpleDataType(Type.INT4),
        CatalogUtil.newSimpleDataTypeArray(Type.INT4));

    catalog.createFunction(funcDesc);
    analyzer = new SQLAnalyzer();
    planner = new LogicalPlanner(catalog);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    util.shutdownCatalogCluster();
  }

  @Test
  public final void testFindTopNode() throws CloneNotSupportedException, PlanningException {
    // two relations
    Expr expr = analyzer.parse(TestLogicalPlanner.QUERIES[1]);
    LogicalNode plan = planner.createPlan(expr).getRootBlock().getRoot();

    assertEquals(NodeType.ROOT, plan.getType());
    LogicalRootNode root = (LogicalRootNode) plan;
    TestLogicalNode.testCloneLogicalNode(root);

    assertEquals(NodeType.PROJECTION, root.getChild().getType());
    ProjectionNode projNode = (ProjectionNode) root.getChild();

    assertEquals(NodeType.JOIN, projNode.getChild().getType());
    JoinNode joinNode = (JoinNode) projNode.getChild();

    assertEquals(NodeType.SCAN, joinNode.getLeftChild().getType());
    ScanNode leftNode = (ScanNode) joinNode.getLeftChild();
    assertEquals("employee", leftNode.getTableName());
    assertEquals(NodeType.SCAN, joinNode.getRightChild().getType());
    ScanNode rightNode = (ScanNode) joinNode.getRightChild();
    assertEquals("dept", rightNode.getTableName());
    
    LogicalNode node = PlannerUtil.findTopNode(root, NodeType.ROOT);
    assertEquals(NodeType.ROOT, node.getType());
    
    node = PlannerUtil.findTopNode(root, NodeType.PROJECTION);
    assertEquals(NodeType.PROJECTION, node.getType());
    
    node = PlannerUtil.findTopNode(root, NodeType.JOIN);
    assertEquals(NodeType.JOIN, node.getType());
    
    node = PlannerUtil.findTopNode(root, NodeType.SCAN);
    assertEquals(NodeType.SCAN, node.getType());
  }

  @Test
  public final void testIsJoinQual() {
    FieldEval f1 = new FieldEval("part.p_partkey", CatalogUtil.newSimpleDataType(Type.INT4));
    FieldEval f2 = new FieldEval("partsupp.ps_partkey",
        CatalogUtil.newSimpleDataType(Type.INT4));


    BinaryEval [] joinQuals = new BinaryEval[5];
    int idx = 0;
    joinQuals[idx++] = new BinaryEval(EvalType.EQUAL, f1, f2);
    joinQuals[idx++] = new BinaryEval(EvalType.LEQ, f1, f2);
    joinQuals[idx++] = new BinaryEval(EvalType.LTH, f1, f2);
    joinQuals[idx++] = new BinaryEval(EvalType.GEQ, f1, f2);
    joinQuals[idx] = new BinaryEval(EvalType.GTH, f1, f2);
    for (int i = 0; i < idx; i++) {
      assertTrue(PlannerUtil.isJoinQual(joinQuals[idx]));
    }

    BinaryEval [] wrongJoinQuals = new BinaryEval[5];
    idx = 0;
    wrongJoinQuals[idx++] = new BinaryEval(EvalType.OR, f1, f2);
    wrongJoinQuals[idx++] = new BinaryEval(EvalType.PLUS, f1, f2);
    wrongJoinQuals[idx++] = new BinaryEval(EvalType.LIKE, f1, f2);

    ConstEval f3 = new ConstEval(DatumFactory.createInt4(1));
    wrongJoinQuals[idx] = new BinaryEval(EvalType.EQUAL, f1, f3);

    for (int i = 0; i < idx; i++) {
      assertFalse(PlannerUtil.isJoinQual(wrongJoinQuals[idx]));
    }
  }

  @Test
  public final void testGetJoinKeyPairs() {
    Schema outerSchema = new Schema();
    outerSchema.addColumn("employee.id1", CatalogUtil.newSimpleDataType(Type.INT4));
    outerSchema.addColumn("employee.id2", CatalogUtil.newSimpleDataType(Type.INT4));
    Schema innerSchema = new Schema();
    innerSchema.addColumn("people.fid1", CatalogUtil.newSimpleDataType(Type.INT4));
    innerSchema.addColumn("people.fid2", CatalogUtil.newSimpleDataType(Type.INT4));

    FieldEval f1 = new FieldEval("employee.id1", CatalogUtil.newSimpleDataType(Type.INT4));
    FieldEval f2 = new FieldEval("people.fid1", CatalogUtil.newSimpleDataType(Type.INT4));
    FieldEval f3 = new FieldEval("employee.id2", CatalogUtil.newSimpleDataType(Type.INT4));
    FieldEval f4 = new FieldEval("people.fid2", CatalogUtil.newSimpleDataType(Type.INT4));

    EvalNode joinQual = new BinaryEval(EvalType.EQUAL, f1, f2);

    // the case where part is the outer and partsupp is the inner.
    List<Column[]> pairs = PlannerUtil.getJoinKeyPairs(joinQual, outerSchema,  innerSchema);
    assertEquals(1, pairs.size());
    assertEquals("employee.id1", pairs.get(0)[0].getQualifiedName());
    assertEquals("people.fid1", pairs.get(0)[1].getQualifiedName());

    // after exchange of outer and inner
    pairs = PlannerUtil.getJoinKeyPairs(joinQual, innerSchema, outerSchema);
    assertEquals("people.fid1", pairs.get(0)[0].getQualifiedName());
    assertEquals("employee.id1", pairs.get(0)[1].getQualifiedName());

    // composited join key test
    EvalNode joinQual2 = new BinaryEval(EvalType.EQUAL, f3, f4);
    EvalNode compositedJoinQual = new BinaryEval(EvalType.AND, joinQual, joinQual2);
    pairs = PlannerUtil.getJoinKeyPairs(compositedJoinQual, outerSchema,  innerSchema);
    assertEquals(2, pairs.size());
    assertEquals("employee.id1", pairs.get(0)[0].getQualifiedName());
    assertEquals("people.fid1", pairs.get(0)[1].getQualifiedName());
    assertEquals("employee.id2", pairs.get(1)[0].getQualifiedName());
    assertEquals("people.fid2", pairs.get(1)[1].getQualifiedName());

    // after exchange of outer and inner
    pairs = PlannerUtil.getJoinKeyPairs(compositedJoinQual, innerSchema,  outerSchema);
    assertEquals(2, pairs.size());
    assertEquals("people.fid1", pairs.get(0)[0].getQualifiedName());
    assertEquals("employee.id1", pairs.get(0)[1].getQualifiedName());
    assertEquals("people.fid2", pairs.get(1)[0].getQualifiedName());
    assertEquals("employee.id2", pairs.get(1)[1].getQualifiedName());
  }

  @Test
  public final void testGetSortKeysFromJoinQual() {
    Schema outerSchema = new Schema();
    outerSchema.addColumn("employee.id1", CatalogUtil.newSimpleDataType(Type.INT4));
    outerSchema.addColumn("employee.id2", CatalogUtil.newSimpleDataType(Type.INT4));
    Schema innerSchema = new Schema();
    innerSchema.addColumn("people.fid1", CatalogUtil.newSimpleDataType(Type.INT4));
    innerSchema.addColumn("people.fid2", CatalogUtil.newSimpleDataType(Type.INT4));

    FieldEval f1 = new FieldEval("employee.id1", CatalogUtil.newSimpleDataType(Type.INT4));
    FieldEval f2 = new FieldEval("people.fid1", CatalogUtil.newSimpleDataType(Type.INT4));
    FieldEval f3 = new FieldEval("employee.id2", CatalogUtil.newSimpleDataType(Type.INT4));
    FieldEval f4 = new FieldEval("people.fid2", CatalogUtil.newSimpleDataType(Type.INT4));

    EvalNode joinQual = new BinaryEval(EvalType.EQUAL, f1, f2);
    SortSpec[][] sortSpecs = PlannerUtil.getSortKeysFromJoinQual(joinQual, outerSchema, innerSchema);
    assertEquals(2, sortSpecs.length);
    assertEquals(1, sortSpecs[0].length);
    assertEquals(1, sortSpecs[1].length);
    assertEquals(outerSchema.getColumnByName("id1"), sortSpecs[0][0].getSortKey());
    assertEquals(innerSchema.getColumnByName("fid1"), sortSpecs[1][0].getSortKey());

    // tests for composited join key
    EvalNode joinQual2 = new BinaryEval(EvalType.EQUAL, f3, f4);
    EvalNode compositedJoinQual = new BinaryEval(EvalType.AND, joinQual, joinQual2);

    sortSpecs = PlannerUtil.getSortKeysFromJoinQual(compositedJoinQual, outerSchema, innerSchema);
    assertEquals(2, sortSpecs.length);
    assertEquals(2, sortSpecs[0].length);
    assertEquals(2, sortSpecs[1].length);
    assertEquals(outerSchema.getColumnByName("id1"), sortSpecs[0][0].getSortKey());
    assertEquals(outerSchema.getColumnByName("id2"), sortSpecs[0][1].getSortKey());
    assertEquals(innerSchema.getColumnByName("fid1"), sortSpecs[1][0].getSortKey());
    assertEquals(innerSchema.getColumnByName("fid2"), sortSpecs[1][1].getSortKey());
  }

  @Test
  public final void testComparatorsFromJoinQual() {
    Schema outerSchema = new Schema();
    outerSchema.addColumn("employee.id1", CatalogUtil.newSimpleDataType(Type.INT4));
    outerSchema.addColumn("employee.id2", CatalogUtil.newSimpleDataType(Type.INT4));
    Schema innerSchema = new Schema();
    innerSchema.addColumn("people.fid1", CatalogUtil.newSimpleDataType(Type.INT4));
    innerSchema.addColumn("people.fid2", CatalogUtil.newSimpleDataType(Type.INT4));

    FieldEval f1 = new FieldEval("employee.id1", CatalogUtil.newSimpleDataType(Type.INT4));
    FieldEval f2 = new FieldEval("people.fid1", CatalogUtil.newSimpleDataType(Type.INT4));
    FieldEval f3 = new FieldEval("employee.id2", CatalogUtil.newSimpleDataType(Type.INT4));
    FieldEval f4 = new FieldEval("people.fid2", CatalogUtil.newSimpleDataType(Type.INT4));

    EvalNode joinQual = new BinaryEval(EvalType.EQUAL, f1, f2);
    TupleComparator [] comparators = PlannerUtil.getComparatorsFromJoinQual(joinQual, outerSchema, innerSchema);

    Tuple t1 = new VTuple(2);
    t1.put(0, DatumFactory.createInt4(1));
    t1.put(1, DatumFactory.createInt4(2));

    Tuple t2 = new VTuple(2);
    t2.put(0, DatumFactory.createInt4(2));
    t2.put(1, DatumFactory.createInt4(3));

    TupleComparator outerComparator = comparators[0];
    assertTrue(outerComparator.compare(t1, t2) < 0);
    assertTrue(outerComparator.compare(t2, t1) > 0);

    TupleComparator innerComparator = comparators[1];
    assertTrue(innerComparator.compare(t1, t2) < 0);
    assertTrue(innerComparator.compare(t2, t1) > 0);

    // tests for composited join key
    EvalNode joinQual2 = new BinaryEval(EvalType.EQUAL, f3, f4);
    EvalNode compositedJoinQual = new BinaryEval(EvalType.AND, joinQual, joinQual2);
    comparators = PlannerUtil.getComparatorsFromJoinQual(compositedJoinQual, outerSchema, innerSchema);

    outerComparator = comparators[0];
    assertTrue(outerComparator.compare(t1, t2) < 0);
    assertTrue(outerComparator.compare(t2, t1) > 0);

    innerComparator = comparators[1];
    assertTrue(innerComparator.compare(t1, t2) < 0);
    assertTrue(innerComparator.compare(t2, t1) > 0);
  }
}
