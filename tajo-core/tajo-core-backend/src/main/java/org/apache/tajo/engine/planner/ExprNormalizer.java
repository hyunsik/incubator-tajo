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

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;


class ExprNormalizer extends SimpleAlgebraVisitor<ExprNormalizer.ExprNormalizedResult, Object> {

  public static class ExprNormalizedResult {
    private final LogicalPlan plan;
    private final LogicalPlan.QueryBlock block;
    private final ExprListManager evalList;
    Expr outer;
    List<TargetExpr> aggregation = new ArrayList<TargetExpr>();
    List<TargetExpr> inner = new ArrayList<TargetExpr>();

    ExprNormalizedResult(LogicalPlanner.PlanContext context) {
      this.plan = context.plan;
      this.block = context.currentBlock;
      this.evalList = context.evalList;
    }

    @Override
    public String toString() {
      return outer.toString() + ", agg=" + aggregation.size() + ", inner=" + inner.size();
    }
  }

  public ExprNormalizedResult normalize(LogicalPlanner.PlanContext context, Expr expr) throws PlanningException {
    ExprNormalizedResult exprNormalizedResult = new ExprNormalizedResult(context);
    Stack<Expr> stack = new Stack<Expr>();
    stack.push(expr);
    visit(exprNormalizedResult, new Stack<Expr>(), expr);
    exprNormalizedResult.outer = stack.pop();
    return exprNormalizedResult;
  }

  private boolean isAggregationFunction(Expr expr) {
    return expr.getType() == OpType.GeneralSetFunction || expr.getType() == OpType.CountRowsFunction;
  }

  @Override
  public Object visitCaseWhen(ExprNormalizedResult ctx, Stack<Expr> stack, CaseWhenPredicate expr)
      throws PlanningException {
    stack.push(expr);
    for (CaseWhenPredicate.WhenExpr when : expr.getWhens()) {
      visit(ctx, stack, when.getCondition());
      visit(ctx, stack, when.getResult());

      if (isAggregationFunction(when.getCondition())) {
        String referenceName = ctx.evalList.addExpr(when.getCondition());
        ctx.aggregation.add(new TargetExpr(when.getCondition(), referenceName));
        when.setCondition(new ColumnReferenceExpr(referenceName));
      }

      if (isAggregationFunction(when.getResult())) {
        String referenceName = ctx.evalList.addExpr(when.getResult());
        ctx.aggregation.add(new TargetExpr(when.getResult(), referenceName));
        when.setResult(new ColumnReferenceExpr(referenceName));
      }
    }

    if (expr.hasElseResult()) {
      visit(ctx, stack, expr.getElseResult());
      if (isAggregationFunction(expr.getElseResult())) {
        String referenceName = ctx.evalList.addExpr(expr.getElseResult());
        ctx.aggregation.add(new TargetExpr(expr.getElseResult(), referenceName));
        expr.setElseResult(new ColumnReferenceExpr(referenceName));
      }
    }
    stack.pop();
    return expr;
  }

  @Override
  public Expr visitUnaryOperator(ExprNormalizedResult ctx, Stack<Expr> stack, UnaryOperator expr) throws PlanningException {
    super.visitUnaryOperator(ctx, stack, expr);
    if (isAggregationFunction(expr.getChild())) {
      // Get an anonymous column name and replace the aggregation function by the column name
      String refName = ctx.evalList.addExpr(expr.getChild());
      ctx.aggregation.add(new TargetExpr(expr.getChild(), refName));
      expr.setChild(new ColumnReferenceExpr(refName));
    }

    return expr;
  }

  @Override
  public Expr visitBinaryOperator(ExprNormalizedResult ctx, Stack<Expr> stack, BinaryOperator expr) throws PlanningException {
    super.visitBinaryOperator(ctx, stack, expr);

    ////////////////////////
    // For Left Term
    ////////////////////////

    if (isAggregationFunction(expr.getLeft())) {
      String leftRefName = ctx.evalList.addExpr(expr.getLeft());
      ctx.aggregation.add(new TargetExpr(expr.getLeft(), leftRefName));
      expr.setLeft(new ColumnReferenceExpr(leftRefName));
    }


    ////////////////////////
    // For Right Term
    ////////////////////////
    if (isAggregationFunction(expr.getRight())) {
      String rightRefName = ctx.evalList.addExpr(expr.getRight());
      ctx.aggregation.add(new TargetExpr(expr.getRight(), rightRefName));
      expr.setRight(new ColumnReferenceExpr(rightRefName));
    }

    return expr;
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Function Section
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////

  @Override
  public Expr visitFunction(ExprNormalizedResult ctx, Stack<Expr> stack, FunctionExpr expr) throws PlanningException {
    stack.push(expr);

    Expr param;
    for (int i = 0; i < expr.getParams().length; i++) {
      param = expr.getParams()[i];
      visit(ctx, stack, param);

      if (isAggregationFunction(param)) {
        String referenceName = ctx.plan.newFieldReferenceName(param);
        ctx.aggregation.add(new TargetExpr(param, referenceName));
        expr.getParams()[i] = new ColumnReferenceExpr(referenceName);
      }
    }

    stack.pop();

    return expr;
  }

  @Override
  public Expr visitGeneralSetFunction(ExprNormalizedResult ctx, Stack<Expr> stack, GeneralSetFunctionExpr expr)
      throws PlanningException {
    stack.push(expr);

    Expr param;
    for (int i = 0; i < expr.getParams().length; i++) {
      param = expr.getParams()[i];
      visit(ctx, stack, param);

      String referenceName = ctx.evalList.addExpr(param);
      ctx.inner.add(new TargetExpr(param, referenceName));
      expr.getParams()[i] = new ColumnReferenceExpr(referenceName);
    }
    stack.pop();
    return expr;
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Literal Section
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////

  @Override
  public Expr visitCastExpr(ExprNormalizedResult ctx, Stack<Expr> stack, CastExpr expr) throws PlanningException {
    super.visitCastExpr(ctx, stack, expr);
    if (expr.getChild().getType() == OpType.GeneralSetFunction
        || expr.getChild().getType() == OpType.CountRowsFunction) {
      String referenceName = ctx.evalList.addExpr(expr.getChild());
      ctx.aggregation.add(new TargetExpr(expr.getChild(), referenceName));
      expr.setChild(new ColumnReferenceExpr(referenceName));
    }
    return expr;
  }

  @Override
  public Expr visitColumnReference(ExprNormalizedResult ctx, Stack<Expr> stack, ColumnReferenceExpr expr)
      throws PlanningException {
    if (!expr.hasQualifier()) {
      String normalized = ctx.plan.getNormalizedColumnName(ctx.block, expr);
      expr.setQualifiedName(normalized);
    }
    return expr;
  }
}
