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

package org.apache.tajo.engine.planner.rewrite;

import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.engine.eval.EvalNode;
import org.apache.tajo.engine.eval.EvalTreeUtil;
import org.apache.tajo.engine.eval.FieldEval;
import org.apache.tajo.engine.exception.InvalidQueryException;
import org.apache.tajo.engine.planner.*;
import org.apache.tajo.engine.planner.logical.*;
import org.apache.tajo.engine.utils.SchemaUtil;
import org.apache.tajo.util.TUtil;

import java.util.*;

public class ProjectionPushDownRule extends
    BasicLogicalPlanVisitor<ProjectionPushDownRule.Context, LogicalNode> implements RewriteRule {
  /** Class Logger */
  private final Log LOG = LogFactory.getLog(ProjectionPushDownRule.class);
  private static final String name = "ProjectionPushDown";

  @Override
  public String getName() {
    return name;
  }

  @Override
  public boolean isEligible(LogicalPlan plan) {
    LogicalNode toBeOptimized = plan.getRootBlock().getRoot();

    if (PlannerUtil.checkIfDDLPlan(toBeOptimized) || !plan.getRootBlock().hasTableExpression()) {
      LOG.info("This query skips the logical optimization step.");
      return false;
    }

    return true;
  }

  @Override
  public LogicalPlan rewrite(LogicalPlan plan) throws PlanningException {
    LogicalPlan.QueryBlock rootBlock = plan.getRootBlock();

    LogicalPlan.QueryBlock topmostBlock;

    // skip a non-table-expression block.
    if (plan.getRootBlock().getRootType() == NodeType.INSERT) {
      topmostBlock = plan.getChildBlocks(rootBlock).get(0);
    } else {
      topmostBlock = rootBlock;
    }

    Stack<LogicalNode> stack = new Stack<LogicalNode>();
    Context context = new Context();
    visit(context, plan, topmostBlock, topmostBlock.getRoot(), stack);

    return plan;
  }

  public static class TargetListManager {
    private LinkedHashMap<EvalNode, Boolean> requiredEvals;

    public TargetListManager() {
      requiredEvals = new LinkedHashMap<EvalNode, Boolean>();
    }

    public TargetListManager(TargetListManager targetListMgr) {
      requiredEvals = new LinkedHashMap<EvalNode, Boolean>(targetListMgr.requiredEvals);
    }

    public boolean isResolved(EvalNode evalNode) {
      return requiredEvals.get(evalNode);
    }

    public void add(EvalNode evalNode) {
      if (!requiredEvals.containsKey(evalNode)) {
        requiredEvals.put(evalNode, false);
      }
    }

    public void resolve(EvalNode evalNode) {
      requiredEvals.put(evalNode, true);
    }

    public String toString() {
      int resolved = 0;
      for (Boolean flag: requiredEvals.values()) {
        if (flag) {
          resolved++;
        }
      }
      return "eval=" + requiredEvals.size() + ", resolved=" + resolved;
    }
  }

  static class Context {
    TargetListManager targetListMgr;
    Set<String> requiredSet;

    public Context() {
      targetListMgr = new TargetListManager();
      requiredSet = new HashSet<String>();
    }

    public Context(Context upperContext) {
      targetListMgr = new TargetListManager(upperContext.targetListMgr);
    }
  }

  @Override
  public LogicalNode visitProjection(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                     ProjectionNode node, Stack<LogicalNode> stack) throws PlanningException {
    for (Target target : node.getTargets()) {
      context.targetListMgr.add(target.getEvalTree());
    }

    LogicalNode child = super.visitProjection(context, plan, block, node, stack);
    //node.setInSchema(child.getOutSchema());
    return node;
  }

  public LogicalNode visitLimit(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block, LimitNode node,
                           Stack<LogicalNode> stack) throws PlanningException {
    LogicalNode child = super.visitLimit(context, plan, block, node, stack);
    return node;
  }

  @Override
  public LogicalNode visitSort(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                               SortNode node, Stack<LogicalNode> stack) throws PlanningException {
    final int sortKeyNum = node.getSortKeys().length;
    EvalNode [] evalNodes = new EvalNode[sortKeyNum];
    for (int i = 0; i < sortKeyNum; i++) {
      SortSpec sortSpec = node.getSortKeys()[i];
      evalNodes[i] = new FieldEval(sortSpec.getSortKey());
      context.targetListMgr.add(evalNodes[i]);
    }

    LogicalNode child = super.visitSort(context, plan, block, node, stack);

    return node;
  }

  @Override
  public LogicalNode visitHaving(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block, HavingNode node,
                            Stack<LogicalNode> stack) throws PlanningException {
    context.targetListMgr.add(node.getQual());

    LogicalNode child = super.visitHaving(context, plan, block, node, stack);
    return node;
  }

  public LogicalNode visitGroupBy(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block, GroupbyNode node,
                             Stack<LogicalNode> stack) throws PlanningException {
    final int targetNum = node.getTargets().length;
    final EvalNode [] evalNodes = new EvalNode[targetNum];
    for (int i = 0; i < targetNum; i++) {
      evalNodes[i] = node.getTargets()[i].getEvalTree();
      context.targetListMgr.add(evalNodes[i]);
    }

    LogicalNode child = super.visitGroupBy(context, plan, block, node, stack);

    return node;
  }

  public LogicalNode visitFilter(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                 SelectionNode node, Stack<LogicalNode> stack) throws PlanningException {
    context.targetListMgr.add(node.getQual());

    LogicalNode child = super.visitFilter(context, plan, block, node, stack);
    return node;
  }

  public LogicalNode visitJoin(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block, JoinNode node,
                          Stack<LogicalNode> stack) throws PlanningException {

    if (node.hasJoinQual()) {
      context.targetListMgr.add(node.getJoinQual());
    }

    Map<String, EvalNode> map = new LinkedHashMap<String, EvalNode>();
    final EvalNode [] evalNodes;
    if (node.hasTargets()) {
      evalNodes = new EvalNode[node.getTargets().length];
      for (int i = 0; i < node.getTargets().length; i++) {
        evalNodes[i] = node.getTargets()[i].getEvalTree();
        context.targetListMgr.add(evalNodes[i]);
        map.put(node.getTargets()[i].getCanonicalName(), evalNodes[i]);
      }
    }

    Context newContext = new Context(context);

    stack.push(node);
    LogicalNode left = visit(newContext, plan, block, node.getLeftChild(), stack);
    LogicalNode right = visit(newContext, plan, block, node.getRightChild(), stack);
    stack.pop();

    return node;
  }

  public static boolean checkIfBeEvaluatedForScan(Target target, ScanNode node) {
    Set<Column> columnRefs = EvalTreeUtil.findDistinctRefColumns(target.getEvalTree());

    if (EvalTreeUtil.findDistinctAggFunction(target.getEvalTree()).size() > 0) {
      return false;
    }

    if (node.getInSchema().containsAll(columnRefs)) {
      return true;
    } else {
      return false;
    }
  }

  public static boolean checkIfBeEvaluatedForJoin(Target target, JoinNode joinNode) {
    Set<Column> columnRefs = EvalTreeUtil.findDistinctRefColumns(target.getEvalTree());

    if (EvalTreeUtil.findDistinctAggFunction(target.getEvalTree()).size() > 0) {
      return false;
    }

    Schema merged = SchemaUtil.merge(joinNode.getLeftChild().getOutSchema(), joinNode.getRightChild().getOutSchema());
    if (!merged.containsAll(columnRefs)) {
      return false;
    }

    if (columnRefs.size() == 1) {
      return true;
    }

    Set<String> tableIds = Sets.newHashSet();
    // getting distinct table references
    for (Column col : columnRefs) {
      if (!tableIds.contains(col.getQualifier())) {
        tableIds.add(col.getQualifier());
      }
    }

    if (tableIds.size() > 0) {
      return true;
    }

    String [] outer = PlannerUtil.getRelationLineage(joinNode.getLeftChild());
    String [] inner = PlannerUtil.getRelationLineage(joinNode.getRightChild());

    Set<String> o = Sets.newHashSet(outer);
    Set<String> i = Sets.newHashSet(inner);
    if (outer == null || inner == null) {
      throw new InvalidQueryException("ERROR: Unexpected logical plan");
    }
    Iterator<String> it = tableIds.iterator();
    if (o.contains(it.next()) && i.contains(it.next())) {
      return true;
    }

    it = tableIds.iterator();

    return i.contains(it.next()) && o.contains(it.next());
  }

  @Override
  public LogicalNode visitUnion(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block, UnionNode node,
                           Stack<LogicalNode> stack) throws PlanningException {
    LogicalPlan.QueryBlock leftBlock = plan.getBlock(node.getLeftChild());
    LogicalPlan.QueryBlock rightBlock = plan.getBlock(node.getRightChild());
    Context leftContext = new Context();
    Context rightContext = new Context();
    stack.push(node);
    LogicalNode leftChild = visit(leftContext, plan, leftBlock, node.getLeftChild(), stack);
    LogicalNode rightChild = visit(rightContext, plan, rightBlock, node.getRightChild(), stack);
    stack.pop();
    return node;
  }

  public LogicalNode visitScan(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block, ScanNode node,
                          Stack<LogicalNode> stack) throws PlanningException {

    if (node.hasTargets()) {
      for (Target target : node.getTargets()) {
        context.targetListMgr.add(target.getEvalTree());
      }
    }

    return node;
  }
}
