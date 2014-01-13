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
    Context context = new Context(plan);
    visit(context, plan, topmostBlock, topmostBlock.getRoot(), stack);

    return plan;
  }

  public static class TargetListManager {
    private Map<String, Target> requiredEvals;
    private LinkedHashSet<Column> requiredRerences;

    public TargetListManager(LogicalPlan plan) {
      requiredEvals = new LinkedHashMap<String, Target>();
      requiredRerences = new LinkedHashSet<Column>();
    }

    public TargetListManager(TargetListManager targetListMgr) {
      requiredEvals = new LinkedHashMap<String, Target>(targetListMgr.requiredEvals);
      requiredRerences = new LinkedHashSet<Column>(targetListMgr.requiredRerences);
    }

    public boolean isRequired(Column column) {
      return requiredRerences.contains(column);
    }

    public void addReferences(EvalNode evalNode) {
      Set<Column> columns = EvalTreeUtil.findDistinctRefColumns(evalNode);
      requiredRerences.addAll(columns);
      for (Column column : columns) {
        Target target = new Target(new FieldEval(column));
        addEvalNode(target);
      }
    }

    public void addEvalNode(Target target) {
      requiredEvals.put(target.getCanonicalName(), target);
      requiredRerences.addAll(EvalTreeUtil.findDistinctRefColumns(target.getEvalTree()));
    }
  }

  static class Context {
    TargetListManager targetListMgr;

    public Context(LogicalPlan plan) {
      targetListMgr = new TargetListManager(plan);
    }

    public Context(Context upperContext) {
      targetListMgr = new TargetListManager(upperContext.targetListMgr);
    }
  }

  @Override
  public LogicalNode visitProjection(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                     ProjectionNode node, Stack<LogicalNode> stack) throws PlanningException {
    block.getNamedExprsManager().reset();
    NamedExprsManager namedExprsMgr = block.getNamedExprsManager();
    for (Target target : namedExprsMgr.getAllTargets()) {
      context.targetListMgr.addEvalNode(target);
    }

    LogicalNode child = super.visitProjection(context, plan, block, node, stack);
    node.setInSchema(child.getOutSchema());
    return node;
  }

  public LogicalNode visitLimit(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block, LimitNode node,
                           Stack<LogicalNode> stack) throws PlanningException {
    LogicalNode child = super.visitLimit(context, plan, block, node, stack);
    node.setInSchema(child.getOutSchema());
    node.setOutSchema(child.getOutSchema());

    return node;
  }

  @Override
  public LogicalNode visitSort(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                               SortNode node, Stack<LogicalNode> stack) throws PlanningException {
    LogicalNode child = super.visitSort(context, plan, block, node, stack);

    node.setInSchema(child.getOutSchema());
    node.setOutSchema(child.getOutSchema());

    return node;
  }

  @Override
  public LogicalNode visitHaving(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block, HavingNode node,
                            Stack<LogicalNode> stack) throws PlanningException {
    LogicalNode child = super.visitHaving(context, plan, block, node, stack);
    node.setInSchema(child.getOutSchema());
    node.setOutSchema(child.getOutSchema());

    return node;
  }

  public LogicalNode visitGroupBy(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block, GroupbyNode node,
                             Stack<LogicalNode> stack) throws PlanningException {
    LogicalNode child = super.visitGroupBy(context, plan, block, node, stack);
    node.setInSchema(child.getOutSchema());
    node.setOutSchema(PlannerUtil.targetToSchema(node.getTargets()));

    return node;
  }

  public LogicalNode visitFilter(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                 SelectionNode node, Stack<LogicalNode> stack) throws PlanningException {
    LogicalNode child = super.visitFilter(context, plan, block, node, stack);
    node.setInSchema(child.getOutSchema());
    node.setOutSchema(child.getOutSchema());

    return node;
  }

  public LogicalNode visitJoin(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block, JoinNode node,
                          Stack<LogicalNode> stack) throws PlanningException {

    Context newContext = new Context(context);

    stack.push(node);
    LogicalNode left = visit(newContext, plan, block, node.getLeftChild(), stack);
    LogicalNode right = visit(newContext, plan, block, node.getRightChild(), stack);
    stack.pop();

    Schema schema = SchemaUtil.merge(left.getOutSchema(), right.getOutSchema());
    node.setInSchema(schema);

    if (node.hasTargets()) {
      List<Target> newTargets = TUtil.newList();
      for (Target target : context.targetListMgr.requiredEvals.values()) {
        if (checkIfBeEvaluatedForJoin(target, node)) {
          newTargets.add(target);
        }
      }

      node.setTargets(newTargets.toArray(new Target[newTargets.size()]));
      node.setOutSchema(PlannerUtil.targetToSchema(node.getTargets()));
    } else {
      List<Target> newTargets = TUtil.newList();
      for (Target target : context.targetListMgr.requiredEvals.values()) {
        if (checkIfBeEvaluatedForJoin(target, node)) {
          newTargets.add(target);
        }
      }
      node.setTargets(newTargets.toArray(new Target[newTargets.size()]));
      node.setOutSchema(PlannerUtil.targetToSchema(node.getTargets()));
    }

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

  public LogicalNode visitScan(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block, ScanNode node,
                          Stack<LogicalNode> stack) throws PlanningException {

    List<Target> requiredTargets = TUtil.newList();
    for (Map.Entry<String,Target> entry: context.targetListMgr.requiredEvals.entrySet()) {
      if (checkIfBeEvaluatedForScan(entry.getValue(), node)) {
        requiredTargets.add(entry.getValue());
      }
    }

    node.setTargets(requiredTargets.toArray(new Target[requiredTargets.size()]));
    node.setOutSchema(PlannerUtil.targetToSchema(node.getTargets()));
    return node;
  }
}
