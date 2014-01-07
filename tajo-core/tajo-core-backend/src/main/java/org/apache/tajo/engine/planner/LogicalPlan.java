/*
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

import com.google.common.collect.Lists;
import org.apache.tajo.algebra.*;
import org.apache.tajo.annotation.NotThreadSafe;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.engine.exception.NoSuchColumnException;
import org.apache.tajo.engine.exception.VerifyException;
import org.apache.tajo.engine.planner.graph.DirectedGraphCursor;
import org.apache.tajo.engine.planner.graph.SimpleDirectedGraph;
import org.apache.tajo.engine.planner.logical.*;
import org.apache.tajo.util.TUtil;

import java.util.*;

/**
 * This represents and keeps every information about a query plan for a query.
 */
@NotThreadSafe
public class LogicalPlan {
  private final LogicalPlanner planner;

  /** the prefix character for virtual tables */
  public static final char VIRTUAL_TABLE_PREFIX='@';
  /** it indicates the root block */
  public static final String ROOT_BLOCK = VIRTUAL_TABLE_PREFIX + "ROOT";
  public static final String NONAME_BLOCK_PREFIX = VIRTUAL_TABLE_PREFIX + "NONAME_";
  private int nextPid = 0;
  private Integer noNameBlockId = 0;
  private Integer noNameColumnId = 0;

  /** a map from between a block name to a block plan */
  private Map<String, QueryBlock> queryBlocks = new LinkedHashMap<String, QueryBlock>();
  private Map<Integer, LogicalNode> nodeMap = new HashMap<Integer, LogicalNode>();
  private Map<Integer, QueryBlock> queryBlockByPID = new HashMap<Integer, QueryBlock>();
  private SimpleDirectedGraph<String, BlockEdge> queryBlockGraph = new SimpleDirectedGraph<String, BlockEdge>();

  /** planning and optimization log */
  private List<String> planingHistory = Lists.newArrayList();

  public LogicalPlan(LogicalPlanner planner) {
    this.planner = planner;
  }

  /**
   * Create a new {@link QueryBlock} and Get
   *
   * @param blockName the query block name
   * @return a created query block
   */
  public QueryBlock newAndGetBlock(String blockName) {
    QueryBlock block = new QueryBlock(blockName);
    queryBlocks.put(blockName, block);
    return block;
  }

  public int newPID() {
    return nextPid++;
  }

  public QueryBlock newNoNameBlock() {
    return newAndGetBlock(NONAME_BLOCK_PREFIX + (noNameBlockId++));
  }

  public String newNonameColumnName(String prefix) {
    String suffix = String.valueOf(noNameColumnId);
    noNameColumnId++;
    return "$" + prefix.toLowerCase() + "_" + suffix;
  }

  public String newGneratedColumnName(Expr expr) {
    String prefix;

    switch (expr.getType()) {
    case CountRowsFunction:
      prefix = "count";
      break;
    case GeneralSetFunction:
      GeneralSetFunctionExpr setFunction = (GeneralSetFunctionExpr) expr;
      prefix = setFunction.getSignature();
      break;
    case Function:
      FunctionExpr function = (FunctionExpr) expr;
      prefix = function.getSignature();
      break;
    default:
      prefix = expr.getType().name();
    }

    String suffix = String.valueOf(noNameColumnId);
    noNameColumnId++;
    return "$" + prefix.toLowerCase() + "_" + suffix;
  }

  /**
   * Check if a query block exists
   * @param blockName the query block name to be checked
   * @return true if exists. Otherwise, false
   */
  public boolean existBlock(String blockName) {
    return queryBlocks.containsKey(blockName);
  }

  public QueryBlock getRootBlock() {
    return queryBlocks.get(ROOT_BLOCK);
  }

  public QueryBlock getBlock(String blockName) {
    return queryBlocks.get(blockName);
  }

  public QueryBlock getBlock(LogicalNode node) {
    return queryBlockByPID.get(node.getPID());
  }

  public void removeBlock(QueryBlock block) {
    queryBlocks.remove(block.getName());
    List<Integer> tobeRemoved = new ArrayList<Integer>();
    for (Map.Entry<Integer, QueryBlock> entry : queryBlockByPID.entrySet()) {
      tobeRemoved.add(entry.getKey());
    }
    for (Integer rn : tobeRemoved) {
      queryBlockByPID.remove(rn);
    }
  }

  public void connectBlocks(QueryBlock srcBlock, QueryBlock targetBlock, BlockType type) {
    queryBlockGraph.addEdge(srcBlock.getName(), targetBlock.getName(), new BlockEdge(srcBlock, targetBlock, type));
  }

  public QueryBlock getParentBlock(QueryBlock block) {
    return queryBlocks.get(queryBlockGraph.getParent(block.getName()));
  }

  public List<QueryBlock> getChildBlocks(QueryBlock block) {
    List<QueryBlock> childBlocks = TUtil.newList();
    for (String blockName : queryBlockGraph.getChilds(block.getName())) {
      childBlocks.add(queryBlocks.get(blockName));
    }
    return childBlocks;
  }

  public Collection<QueryBlock> getQueryBlocks() {
    return queryBlocks.values();
  }

  public SimpleDirectedGraph<String, BlockEdge> getQueryBlockGraph() {
    return queryBlockGraph;
  }

  /**
   * It resolves a column.
   */
  public Column resolveColumn(QueryBlock block, LogicalNode currentNode, ColumnReferenceExpr columnRef)
      throws PlanningException {

    if (columnRef.hasQualifier()) { // if a column reference is qualified

      RelationNode relationOp = block.getRelation(columnRef.getQualifier());

      // if a column name is outside of this query block
      if (relationOp == null) {
        // TODO - nested query can only refer outer query block? or not?
        for (QueryBlock eachBlock : queryBlocks.values()) {
          if (eachBlock.containRelation(columnRef.getQualifier())) {
            relationOp = eachBlock.getRelation(columnRef.getQualifier());
          }
        }
      }

      if (relationOp == null) {
        throw new NoSuchColumnException(columnRef.getCanonicalName());
      }

      Schema schema = relationOp.getTableSchema();

      Column column = schema.getColumnByFQN(columnRef.getCanonicalName());
      if (column == null) {
        throw new VerifyException("ERROR: no such a column '"+ columnRef.getCanonicalName() + "'");
      }

      return column;

    } else { // if a column reference is not qualified

      // Trying to find the column within the current block

      if (block.getLatestNode() != null) {
        Column found = block.getLatestNode().getOutSchema().getColumnByName(columnRef.getName());
        if (found != null) {
          return found;
        }
      }

      // Trying to find columns from other relations in the current block
      List<Column> candidates = TUtil.newList();
      for (RelationNode rel : block.getRelations()) {
        Column found = rel.getOutSchema().getColumnByName(columnRef.getName());
        if (found != null) {
          candidates.add(found);
        }
      }

      if (!candidates.isEmpty()) {
        return ensureUniqueColumn(candidates);
      }

      // Trying to find columns from other relations in other blocks
      for (QueryBlock eachBlock : queryBlocks.values()) {
        for (RelationNode rel : eachBlock.getRelations()) {
          Column found = rel.getOutSchema().getColumnByName(columnRef.getName());
          if (found != null) {
            candidates.add(found);
          }
        }
      }

      if (!candidates.isEmpty()) {
        return ensureUniqueColumn(candidates);
      }

//      Target target = block.targetListManager.getTarget(columnRef.getName());
//      if (target != null) {
//        candidates.add(target.getColumnSchema());
//      }
//      if (!candidates.isEmpty()) {
//        return ensureUniqueColumn(candidates);
//      }

      // Trying to find columns from schema in current block.
      if (block.getSchema() != null) {
        Column found = block.getSchema().getColumnByName(columnRef.getName());
        if (found != null) {
          candidates.add(found);
        }
      }

      if (!candidates.isEmpty()) {
        return ensureUniqueColumn(candidates);
      }

      throw new VerifyException("ERROR: no such a column name "+ columnRef.getCanonicalName());
    }
  }

  /**
   * replace the found column if the column is renamed to an alias name
   */
  public Column getColumnOrAliasedColumn(QueryBlock block, Column column) throws PlanningException {
    return column;
  }

  private static Column ensureUniqueColumn(List<Column> candidates)
      throws VerifyException {
    if (candidates.size() == 1) {
      return candidates.get(0);
    } else if (candidates.size() > 2) {
      StringBuilder sb = new StringBuilder();
      boolean first = true;
      for (Column column : candidates) {
        if (first) {
          first = false;
        } else {
          sb.append(", ");
        }
        sb.append(column);
      }
      throw new VerifyException("Ambiguous Column Name: " + sb.toString());
    } else {
      return null;
    }
  }

  public String getQueryGraphAsString() {
    StringBuilder sb = new StringBuilder();

    sb.append("\n-----------------------------\n");
    sb.append("Query Block Graph\n");
    sb.append("-----------------------------\n");
    sb.append(queryBlockGraph.toStringGraph(getRootBlock().getName()));
    sb.append("-----------------------------\n");
    sb.append("Optimization Log:\n");
    DirectedGraphCursor<String, BlockEdge> cursor =
        new DirectedGraphCursor<String, BlockEdge>(queryBlockGraph, getRootBlock().getName());
    while(cursor.hasNext()) {
      QueryBlock block = getBlock(cursor.nextBlock());
      if (block.getPlaningHistory().size() > 0) {
        sb.append("\n[").append(block.getName()).append("]\n");
        for (String log : block.getPlaningHistory()) {
          sb.append("> ").append(log).append("\n");
        }
      }
    }
    sb.append("-----------------------------\n");
    sb.append("\n");

    sb.append(getLogicalPlanAsString());

    return sb.toString();
  }

  public String getLogicalPlanAsString() {
    ExplainLogicalPlanVisitor explain = new ExplainLogicalPlanVisitor();

    StringBuilder explains = new StringBuilder();
    try {
      ExplainLogicalPlanVisitor.Context explainContext = explain.getBlockPlanStrings(this, ROOT_BLOCK);
      while(!explainContext.explains.empty()) {
        explains.append(
            ExplainLogicalPlanVisitor.printDepthString(explainContext.getMaxDepth(), explainContext.explains.pop()));
      }
    } catch (PlanningException e) {
      e.printStackTrace();
    }

    return explains.toString();
  }

  public void addHistory(String string) {
    planingHistory.add(string);
  }

  public List<String> getHistory() {
    return planingHistory;
  }

  @Override
  public String toString() {
    return getQueryGraphAsString();
  }

  ///////////////////////////////////////////////////////////////////////////
  //                             Query Block
  ///////////////////////////////////////////////////////////////////////////

  public static enum BlockType {
    TableSubQuery,
    ScalarSubQuery
  }

  public static class BlockEdge {
    private String childName;
    private String parentName;
    private BlockType blockType;


    public BlockEdge(String childName, String parentName, BlockType blockType) {
      this.childName = childName;
      this.parentName = parentName;
      this.blockType = blockType;
    }

    public BlockEdge(QueryBlock child, QueryBlock parent, BlockType blockType) {
      this(child.getName(), parent.getName(), blockType);
    }

    public String getParentName() {
      return parentName;
    }

    public String getChildName() {
      return childName;
    }

    public BlockType getBlockType() {
      return blockType;
    }
  }

  public class QueryBlock {
    private String blockName;
    private LogicalNode rootNode;
    private NodeType rootType;
    private Map<String, RelationNode> relations = new HashMap<String, RelationNode>();
    private Map<OpType, List<Expr>> algebraicExprs = TUtil.newHashMap();

    // changing states
    private LogicalNode latestNode;
    private boolean resolvedGrouping = true;
    private boolean hasGrouping;
    private Projectable projectionNode;
    private GroupbyNode groupingNode;
    private JoinNode joinNode;
    private SelectionNode selectionNode;
    private StoreTableNode storeTableNode;
    private InsertNode insertNode;
    private Schema schema;

    NewTargetListManager targetListManager;

    /** It contains a planning log for this block */
    private List<String> planingHistory = Lists.newArrayList();

    public QueryBlock(String blockName) {
      this.blockName = blockName;
    }

    public String getName() {
      return blockName;
    }

    public boolean hasRoot() {
      return rootNode != null;
    }

    public void refresh() {
      setRoot(rootNode);
    }

    public void setRoot(LogicalNode blockRoot) {
      this.rootNode = blockRoot;
      if (blockRoot instanceof LogicalRootNode) {
        LogicalRootNode rootNode = (LogicalRootNode) blockRoot;
        rootType = rootNode.getChild().getType();
      }
      queryBlockByPID.put(blockRoot.getPID(), this);
    }


    public Target [] getCurrentTargets() {
      return null;
    }

    public <NODE extends LogicalNode> NODE getRoot() {
      return (NODE) rootNode;
    }

    public NodeType getRootType() {
      return rootType;
    }

    public boolean containRelation(String name) {
      return relations.containsKey(PlannerUtil.normalizeTableName(name));
    }

    public void addRelation(RelationNode relation) {
      relations.put(PlannerUtil.normalizeTableName(relation.getCanonicalName()), relation);
    }

    public RelationNode getRelation(String name) {
      return relations.get(PlannerUtil.normalizeTableName(name));
    }

    public Collection<RelationNode> getRelations() {
      return this.relations.values();
    }

    public void setSchema(Schema schema) {
      this.schema = schema;
    }

    public Schema getSchema() {
      return schema;
    }

    public boolean hasTableExpression() {
      return this.relations.size() > 0;
    }

    public void updateLatestNode(LogicalNode node) {
      this.latestNode = node;
    }

    public <T extends LogicalNode> T getLatestNode() {
      return (T) this.latestNode;
    }

    public void setAlgebraicExpr(Expr expr) {
      TUtil.putToNestedList(algebraicExprs, expr.getType(), expr);
    }

    public boolean hasAlgebraicExpr(OpType opType) {
      return algebraicExprs.containsKey(opType);
    }

    public <T extends Expr> List<T> getAlgebraicExpr(OpType opType) {
      return (List<T>) algebraicExprs.get(opType);
    }

    public <T extends Expr> T getSingletonExpr(OpType opType) {
      if (hasAlgebraicExpr(opType)) {
        return (T) algebraicExprs.get(opType).get(0);
      } else {
        return null;
      }
    }

    public boolean hasProjection() {
      return hasAlgebraicExpr(OpType.Projection);
    }

    public Projection getProjection() {
      return getSingletonExpr(OpType.Projection);
    }

    public boolean hasHaving() {
      return hasAlgebraicExpr(OpType.Having);
    }

    public Having getHaving() {
      return getSingletonExpr(OpType.Having);
    }

    public void setProjectionNode(Projectable node) {
      this.projectionNode = node;
    }

    public Projectable getProjectionNode() {
      return this.projectionNode;
    }

    public boolean isGroupingResolved() {
      return this.resolvedGrouping;
    }

    public void resolveGroupingRequired() {
      this.resolvedGrouping = true;
    }

    public void setHasGrouping() {
      hasGrouping = true;
      resolvedGrouping = false;
    }

    public boolean hasGrouping() {
      return hasGrouping || hasGroupbyNode();
    }

    public boolean hasGroupbyNode() {
      return this.groupingNode != null;
    }

    public void setGroupbyNode(GroupbyNode groupingNode) {
      this.groupingNode = groupingNode;
    }

    public GroupbyNode getGroupbyNode() {
      return this.groupingNode;
    }

    public boolean hasJoinNode() {
      return joinNode != null;
    }

    /**
     * @return the topmost JoinNode instance
     */
    public JoinNode getJoinNode() {
      return joinNode;
    }

    public void setJoinNode(JoinNode node) {
      if (joinNode == null || latestNode == node) {
        this.joinNode = node;
      } else {
        PlannerUtil.replaceNode(LogicalPlan.this, latestNode, this.joinNode, node);
      }
    }

    public boolean hasSelectionNode() {
      return this.selectionNode != null;
    }

    public void setSelectionNode(SelectionNode selectionNode) {
      this.selectionNode = selectionNode;
    }

    public SelectionNode getSelectionNode() {
      return selectionNode;
    }

    public boolean hasStoreTableNode() {
      return this.storeTableNode != null;
    }

    public void setStoreTableNode(StoreTableNode storeTableNode) {
      this.storeTableNode = storeTableNode;
    }

    public StoreTableNode getStoreTableNode() {
      return this.storeTableNode;
    }

    public boolean hasInsertNode() {
      return this.insertNode != null;
    }

    public InsertNode getInsertNode() {
      return this.insertNode;
    }

    public void setInsertNode(InsertNode insertNode) {
      this.insertNode = insertNode;
    }

    public List<String> getPlaningHistory() {
      return planingHistory;
    }

    public void addHistory(String history) {
      this.planingHistory.add(history);
    }

    public boolean postVisit(LogicalNode node, Stack<Expr> path) {
      if (nodeMap.containsKey(node.getPID())) {
        return false;
      }

      nodeMap.put(node.getPID(), node);
      updateLatestNode(node);

      // if an added operator is a relation, add it to relation set.
      switch (node.getType()) {
        case STORE:
          setStoreTableNode((StoreTableNode) node);
          break;

        case SCAN:
          ScanNode relationOp = (ScanNode) node;
          addRelation(relationOp);
          break;

        case GROUP_BY:
          resolveGroupingRequired();
          setGroupbyNode((GroupbyNode) node);
          break;

        case JOIN:
          setJoinNode((JoinNode) node);
          break;

        case SELECTION:
          setSelectionNode((SelectionNode) node);
          break;

        case INSERT:
          setInsertNode((InsertNode) node);
          break;

        case TABLE_SUBQUERY:
          TableSubQueryNode tableSubQueryNode = (TableSubQueryNode) node;
          addRelation(tableSubQueryNode);
          break;
      }

      // if this node is the topmost
      if (path.size() == 0) {
        setRoot(node);
      }

      return true;
    }

    public String toString() {
      return blockName;
    }

    ///////////////////////////////////////////////////////////////////////////
    //                 Target List Management Methods
    ///////////////////////////////////////////////////////////////////////////
    //
    // A target list means a list of expressions after SELECT keyword in SQL.
    //
    // SELECT rel1.col1, sum(rel1.col2), res1.col3 + res2.col2, ... FROM ...
    //        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    //                        TARGET LIST
    //
    ///////////////////////////////////////////////////////////////////////////
  }
}