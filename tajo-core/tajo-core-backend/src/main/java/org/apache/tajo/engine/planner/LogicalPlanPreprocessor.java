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

import org.apache.tajo.algebra.*;
import org.apache.tajo.catalog.CatalogService;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.engine.eval.EvalNode;
import org.apache.tajo.engine.eval.EvalType;
import org.apache.tajo.engine.eval.FieldEval;
import org.apache.tajo.engine.planner.logical.*;
import org.apache.tajo.engine.utils.SchemaUtil;

import java.util.Stack;

class LogicalPlanPreprocessor extends BaseAlgebraVisitor<LogicalPlanPreprocessor.PreprocessContext, LogicalNode> {
  private ExprAnnotator annotator;

  static class PreprocessContext {
    LogicalPlan plan;
    LogicalPlan.QueryBlock currentBlock;

    public PreprocessContext(LogicalPlan plan, LogicalPlan.QueryBlock currentBlock) {
      this.plan = plan;
      this.currentBlock = currentBlock;
    }

    public PreprocessContext(PreprocessContext context, LogicalPlan.QueryBlock currentBlock) {
      this.plan = context.plan;
      this.currentBlock = currentBlock;
    }
  }

  private CatalogService catalog;

  LogicalPlanPreprocessor(CatalogService catalog, ExprAnnotator annotator) {
    this.catalog = catalog;
    this.annotator = annotator;
  }

  @Override
  public void preHook(PreprocessContext ctx, Stack<Expr> stack, Expr expr) throws PlanningException {
    ctx.currentBlock.setAlgebraicExpr(expr);
    ctx.plan.mapExprToBlock(expr, ctx.currentBlock.getName());
  }

  @Override
  public LogicalNode postHook(PreprocessContext ctx, Stack<Expr> stack, Expr expr, LogicalNode result) throws PlanningException {
    ctx.currentBlock.setNode(result);
    ctx.currentBlock.mapExprToLogicalNode(expr, result);
    return result;
  }

  @Override
  public LogicalNode visitProjection(PreprocessContext ctx, Stack<Expr> stack, Projection expr) throws PlanningException {
    stack.push(expr); // <--- push
    LogicalNode child = visit(ctx, stack, expr.getChild());

    Target [] targets;
    if (expr.isAllProjected()) {
      targets = PlannerUtil.schemaToTargets(child.getOutSchema());
    } else {
      targets = new Target[expr.getNamedExprs().length];

      for (int i = 0; i < expr.getNamedExprs().length; i++) {
        NamedExpr namedExpr = expr.getNamedExprs()[i];
        EvalNode evalNode = annotator.createEvalNode(ctx.plan, ctx.currentBlock, namedExpr.getExpr());

        if (namedExpr.hasAlias()) {
          targets[i] = new Target(evalNode, namedExpr.getAlias());
        } else if (evalNode.getType() == EvalType.FIELD) {
          targets[i] = new Target(evalNode, ((FieldEval)evalNode).getColumnRef().getQualifiedName());
        } else {
          targets[i] = new Target(evalNode, "$name_" + i);
        }
      }
    }
    stack.pop(); // <--- Pop

    ProjectionNode projectionNode = new ProjectionNode(ctx.plan.newPID());
    projectionNode.setInSchema(child.getOutSchema());
    projectionNode.setOutSchema(PlannerUtil.targetToSchema(targets));
    return projectionNode;
  }

  @Override
  public LogicalNode visitLimit(PreprocessContext ctx, Stack<Expr> stack, Limit expr) throws PlanningException {
    stack.push(expr);
    LogicalNode child = visit(ctx, stack, expr.getChild());
    stack.pop();

    LimitNode limitNode = new LimitNode(ctx.plan.newPID());
    limitNode.setInSchema(child.getOutSchema());
    limitNode.setOutSchema(child.getOutSchema());
    return limitNode;
  }

  @Override
  public LogicalNode visitSort(PreprocessContext ctx, Stack<Expr> stack, Sort expr) throws PlanningException {
    stack.push(expr);
    LogicalNode child = visit(ctx, stack, expr.getChild());
    stack.pop();

    SortNode sortNode = new SortNode(ctx.plan.newPID());
    sortNode.setInSchema(child.getOutSchema());
    sortNode.setOutSchema(child.getOutSchema());
    return sortNode;
  }

  @Override
  public LogicalNode visitHaving(PreprocessContext ctx, Stack<Expr> stack, Having expr) throws PlanningException {
    stack.push(expr);
    LogicalNode child = visit(ctx, stack, expr.getChild());
    stack.pop();

    HavingNode havingNode = new HavingNode(ctx.plan.newPID());
    havingNode.setInSchema(child.getOutSchema());
    havingNode.setOutSchema(child.getOutSchema());
    return havingNode;
  }

  @Override
  public LogicalNode visitGroupBy(PreprocessContext ctx, Stack<Expr> stack, Aggregation expr) throws PlanningException {
    stack.push(expr); // <--- push
    LogicalNode child = visit(ctx, stack, expr.getChild());

    Projection projection = ctx.currentBlock.getSingletonExpr(OpType.Projection);
    int finalTargetNum = projection.getNamedExprs().length;
    Target [] targets = new Target[finalTargetNum];

    for (int i = 0; i < finalTargetNum; i++) {
      NamedExpr namedExpr = projection.getNamedExprs()[i];
      EvalNode evalNode = annotator.createEvalNode(ctx.plan, ctx.currentBlock, namedExpr.getExpr());

      if (namedExpr.hasAlias()) {
        targets[i] = new Target(evalNode, namedExpr.getAlias());
      } else {
        targets[i] = new Target(evalNode, "$name_" + i);
      }
    }
    stack.pop();

    GroupbyNode groupByNode = new GroupbyNode(ctx.plan.newPID());
    groupByNode.setInSchema(child.getOutSchema());
    groupByNode.setOutSchema(PlannerUtil.targetToSchema(targets));
    return groupByNode;
  }

  @Override
  public LogicalNode visitUnion(PreprocessContext ctx, Stack<Expr> stack, SetOperation expr) throws PlanningException {
    LogicalPlan.QueryBlock leftBlock = ctx.plan.newQueryBlock();
    PreprocessContext leftContext = new PreprocessContext(ctx, leftBlock);
    LogicalNode leftChild = visit(leftContext, new Stack<Expr>(), expr.getLeft());
    TableSubQueryNode leftSubQuery = new TableSubQueryNode(ctx.plan.newPID(), leftBlock.getName(), leftChild);

    LogicalPlan.QueryBlock rightBlock = ctx.plan.newQueryBlock();
    PreprocessContext rightContext = new PreprocessContext(ctx, rightBlock);
    LogicalNode rightChild = visit(rightContext, new Stack<Expr>(), expr.getRight());
    TableSubQueryNode rightSubQuery = new TableSubQueryNode(ctx.plan.newPID(), rightBlock.getName(), rightChild);

    UnionNode unionNode = new UnionNode(ctx.plan.newPID());
    unionNode.setLeftChild(leftSubQuery);
    unionNode.setRightChild(rightSubQuery);
    unionNode.setInSchema(leftSubQuery.getOutSchema());
    unionNode.setOutSchema(leftSubQuery.getOutSchema());

    return unionNode;
  }

  public LogicalNode visitFilter(PreprocessContext ctx, Stack<Expr> stack, Selection expr) throws PlanningException {
    stack.push(expr);
    LogicalNode child = visit(ctx, stack, expr.getChild());
    stack.pop();

    SelectionNode selectionNode = new SelectionNode(ctx.plan.newPID());
    selectionNode.setInSchema(child.getOutSchema());
    selectionNode.setOutSchema(child.getOutSchema());
    return selectionNode;
  }

  @Override
  public LogicalNode visitJoin(PreprocessContext ctx, Stack<Expr> stack, Join expr) throws PlanningException {
    stack.push(expr);
    LogicalNode left = visit(ctx, stack, expr.getLeft());
    LogicalNode right = visit(ctx, stack, expr.getRight());
    stack.pop();
    JoinNode joinNode = new JoinNode(ctx.plan.newPID());
    Schema merged = SchemaUtil.merge(left.getOutSchema(), right.getOutSchema());
    joinNode.setInSchema(merged);
    joinNode.setOutSchema(merged);
    return joinNode;
  }

  @Override
  public LogicalNode visitRelation(PreprocessContext ctx, Stack<Expr> stack, Relation expr)
      throws PlanningException {

    Relation relation = expr;
    TableDesc desc = catalog.getTableDesc(relation.getName());

    ScanNode scanNode;
    if (relation.hasAlias()) {
      scanNode = new ScanNode(ctx.plan.newPID(), desc, relation.getAlias());
    } else {
      scanNode = new ScanNode(ctx.plan.newPID(), desc);
    }
    ctx.currentBlock.addRelation(scanNode);

    return scanNode;
  }

  @Override
  public LogicalNode visitTableSubQuery(PreprocessContext ctx, Stack<Expr> stack, TablePrimarySubQuery expr)
      throws PlanningException {

    PreprocessContext newContext;
    if (expr.hasAlias()) {
      newContext = new PreprocessContext(ctx, ctx.plan.newAndGetBlock(expr.getAlias()));
    } else {
      newContext = new PreprocessContext(ctx, ctx.plan.newQueryBlock());
    }
    LogicalNode child = super.visitTableSubQuery(newContext, stack, expr);

    // a table subquery is regarded as a relation.
    TableSubQueryNode node = new TableSubQueryNode(ctx.plan.newPID(), newContext.currentBlock.getName(), child);
    ctx.currentBlock.addRelation(node);
    return node;
  }
}
