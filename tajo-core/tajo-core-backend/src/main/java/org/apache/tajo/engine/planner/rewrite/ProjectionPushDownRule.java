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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.engine.eval.EvalNode;
import org.apache.tajo.engine.eval.EvalTreeUtil;
import org.apache.tajo.engine.eval.FieldEval;
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
    context.plan = plan;

    context.targetListMgr = new TargetListManager();


    visit(context, plan, topmostBlock, topmostBlock.getRoot(), stack);

    return plan;
  }

  public static class TargetListManager {
    private LinkedHashSet<Column> referenceSet = new LinkedHashSet<Column>();

    public boolean isRequired(Column column) {
      return referenceSet.contains(column);
    }

    public void addEvalNode(EvalNode evalNode) {
      referenceSet.addAll(EvalTreeUtil.findDistinctRefColumns(evalNode));
    }

    public void addEvalNode(Target target) {
      addEvalNode(target.getEvalTree());
    }
  }

  static class Context {
    LogicalPlan plan;
    TargetListManager targetListMgr;

    public Context() {}

    public Context(Context context) {
      this.plan = context.plan;
      this.targetListMgr = context.targetListMgr;
    }
  }

  @Override
  public LogicalNode visitProjection(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                     ProjectionNode node, Stack<LogicalNode> stack) throws PlanningException {
    for (Target target : node.getTargets()) {
      context.targetListMgr.addEvalNode(target);
    }

    LogicalNode child = super.visitProjection(context, plan, block, node, stack);
    node.setInSchema(child.getOutSchema());
    return node;
  }

  @Override
  public LogicalNode visitSort(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                               SortNode node, Stack<LogicalNode> stack) throws PlanningException {

    for (SortSpec sortSpec : node.getSortKeys()) {
      context.targetListMgr.addEvalNode(new FieldEval(sortSpec.getSortKey()));
    }

    super.visitSort(context, plan, block, node, stack);

    return node;
  }

  @Override
  public LogicalNode visitHaving(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block, HavingNode node,
                            Stack<LogicalNode> stack) throws PlanningException {
    context.targetListMgr.addEvalNode(node.getQual());

    super.visitHaving(context, plan, block, node, stack);

    return node;
  }

  public LogicalNode visitGroupBy(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block, GroupbyNode node,
                             Stack<LogicalNode> stack) throws PlanningException {
    for (Target target : node.getTargets()) {
      context.targetListMgr.addEvalNode(target);
    }

    super.visitGroupBy(context, plan, block, node, stack);

    return node;
  }

  public LogicalNode visitFilter(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                 SelectionNode node, Stack<LogicalNode> stack) throws PlanningException {
    context.targetListMgr.addEvalNode(node.getQual());

    super.visitFilter(context, plan, block, node, stack);

    return node;
  }

  public LogicalNode visitJoin(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block, JoinNode node,
                          Stack<LogicalNode> stack) throws PlanningException {

    if (node.hasJoinQual()) {
      context.targetListMgr.addEvalNode(node.getJoinQual());
    }

    if (node.hasTargets()) {
      for (Target target : node.getTargets()) {
        context.targetListMgr.addEvalNode(target);
      }
    }

    stack.push(node);
    LogicalNode left = visit(context, plan, block, node.getLeftChild(), stack);
    LogicalNode right = visit(context, plan, block, node.getRightChild(), stack);
    stack.pop();

    Schema schema = SchemaUtil.merge(left.getOutSchema(), right.getOutSchema());
    node.setInSchema(schema);

    if (node.hasTargets()) {
      List<Target> requiredTargets = TUtil.newList();
      for (Target target : node.getTargets()) {
        if (context.targetListMgr.isRequired(target.getNamedColumn())) {
          requiredTargets.add(target);
        }
      }

      node.setTargets(requiredTargets.toArray(new Target[requiredTargets.size()]));
      node.setOutSchema(PlannerUtil.targetToSchema(node.getTargets()));
    } else {
      node.setOutSchema(schema);
    }

    return node;
  }

  public LogicalNode visitScan(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block, ScanNode node,
                          Stack<LogicalNode> stack) throws PlanningException {

    List<Target> requiredTargets = TUtil.newList();
    for (Target target : node.getTargets()) {
      if (context.targetListMgr.isRequired(target.getNamedColumn())) {
        requiredTargets.add(target);
      }
    }

    node.setTargets(requiredTargets.toArray(new Target[requiredTargets.size()]));
    node.setOutSchema(PlannerUtil.targetToSchema(node.getTargets()));
    return node;
  }
}
