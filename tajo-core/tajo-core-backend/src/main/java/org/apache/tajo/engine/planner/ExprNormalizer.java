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

/**
 * ExprNormalizer performs two kinds of works:
 *
 * <h3>1. Duplicate Removal.</h3>
 *
 * For example, assume a simple query as follows:
 * <pre>
 *   select price * rate as total_price, ..., order by price * rate
 * </pre>
 *
 * The expression <code>price * rate</code> is duplicated in both select list and order by clause.
 * Against those cases, ExprNormalizer removes duplicated expressions and replaces one with one reference.
 * In the case, ExprNormalizer replaces price * rate with total_price reference.
 *
 * <h3>2. Dissection of Expression</h3>
 *
 * A expression can be a complex expressions, including a mixed of scalar and aggregation expressions.
 * For example, assume an aggregation query as follows:
 * <pre>
 *   select sum(price * rate) * (1 - avg(discount_rate))), ...
 * </pre>
 *
 * In this case, ExprNormalizer dissects the expression 'sum(price * rate) * (1 - avg(discount_rate)))'
 * into the following expressions:
 * <ul>
 *   <li>$1 = price * rage</li>
 *   <li>$2 = sum($1)</li>
 *   <li>$3 = avg(discount_rate)</li>
 *   <li>$4 = $3 * (1 - $3)</li>
 * </ul>
 *
 * It mainly two advantages. Firstly, it makes complex expression evaluations easier across multiple physical executors.
 * Second, it gives move opportunities to remove duplicated expressions.
 */
class ExprNormalizer extends SimpleAlgebraVisitor<ExprNormalizer.ExprNormalizedResult, Object> {

  public static class ExprNormalizedResult {
    private final LogicalPlan plan;
    private final LogicalPlan.QueryBlock block;

    Expr baseExpr;
    List<NamedExpr> aggExprs = new ArrayList<NamedExpr>();
    List<NamedExpr> scalarExprs = new ArrayList<NamedExpr>();

    private ExprNormalizedResult(LogicalPlanner.PlanContext context) {
      this.plan = context.plan;
      this.block = context.queryBlock;
    }

    @Override
    public String toString() {
      return baseExpr.toString() + ", agg=" + aggExprs.size() + ", scalar=" + scalarExprs.size();
    }
  }

  public ExprNormalizedResult normalize(LogicalPlanner.PlanContext context, Expr expr) throws PlanningException {
    ExprNormalizedResult exprNormalizedResult = new ExprNormalizedResult(context);
    Stack<Expr> stack = new Stack<Expr>();
    stack.push(expr);
    visit(exprNormalizedResult, new Stack<Expr>(), expr);
    exprNormalizedResult.baseExpr = stack.pop();
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
        String referenceName = ctx.block.namedExprsMgr.addExpr(when.getCondition());
        ctx.aggExprs.add(new NamedExpr(when.getCondition(), referenceName));
        when.setCondition(new ColumnReferenceExpr(referenceName));
      }

      if (isAggregationFunction(when.getResult())) {
        String referenceName = ctx.block.namedExprsMgr.addExpr(when.getResult());
        ctx.aggExprs.add(new NamedExpr(when.getResult(), referenceName));
        when.setResult(new ColumnReferenceExpr(referenceName));
      }
    }

    if (expr.hasElseResult()) {
      visit(ctx, stack, expr.getElseResult());
      if (isAggregationFunction(expr.getElseResult())) {
        String referenceName = ctx.block.namedExprsMgr.addExpr(expr.getElseResult());
        ctx.aggExprs.add(new NamedExpr(expr.getElseResult(), referenceName));
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
      String refName = ctx.block.namedExprsMgr.addExpr(expr.getChild());
      ctx.aggExprs.add(new NamedExpr(expr.getChild(), refName));
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
      String leftRefName = ctx.block.namedExprsMgr.addExpr(expr.getLeft());
      ctx.aggExprs.add(new NamedExpr(expr.getLeft(), leftRefName));
      expr.setLeft(new ColumnReferenceExpr(leftRefName));
    }


    ////////////////////////
    // For Right Term
    ////////////////////////
    if (isAggregationFunction(expr.getRight())) {
      String rightRefName = ctx.block.namedExprsMgr.addExpr(expr.getRight());
      ctx.aggExprs.add(new NamedExpr(expr.getRight(), rightRefName));
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
        String referenceName = ctx.plan.newGeneratedFieldName(param);
        ctx.aggExprs.add(new NamedExpr(param, referenceName));
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

      String referenceName = ctx.block.namedExprsMgr.addExpr(param);
      ctx.scalarExprs.add(new NamedExpr(param, referenceName));
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
      String referenceName = ctx.block.namedExprsMgr.addExpr(expr.getChild());
      ctx.aggExprs.add(new NamedExpr(expr.getChild(), referenceName));
      expr.setChild(new ColumnReferenceExpr(referenceName));
    }
    return expr;
  }

  @Override
  public Expr visitColumnReference(ExprNormalizedResult ctx, Stack<Expr> stack, ColumnReferenceExpr expr)
      throws PlanningException {
    if (!expr.hasQualifier()) {
      if (ctx.block.namedExprsMgr.contains(expr.getCanonicalName())) {
        NamedExpr namedExpr = ctx.block.namedExprsMgr.getNamedExpr(expr.getCanonicalName());
        return new ColumnReferenceExpr(namedExpr.getAlias());
      } else {
        String normalized = ctx.plan.getNormalizedColumnName(ctx.block, expr);
        expr.setName(normalized);
      }
    }
    return expr;
  }
}
