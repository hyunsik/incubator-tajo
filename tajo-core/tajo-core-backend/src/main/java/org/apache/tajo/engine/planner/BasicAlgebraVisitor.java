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

import java.util.Stack;

public class BasicAlgebraVisitor<CONTEXT> extends BaseAlgebraVisitor<CONTEXT, Expr> {

  /**
   * The prehook is called before each expression is visited.
   */
  public void preHook(CONTEXT ctx, Stack<Expr> stack, Expr expr) throws PlanningException {
  }


  /**
   * The posthook is called before each expression is visited.
   */
  public Expr postHook(CONTEXT ctx, Stack<Expr> stack, Expr expr, Expr result) throws PlanningException {
    return result;
  }

  private Expr visitDefaultUnaryExpr(CONTEXT ctx, Stack<Expr> stack, UnaryOperator expr) throws PlanningException {
    stack.push(expr);
    visit(ctx, stack, expr.getChild());
    stack.pop();
    return expr;
  }

  private Expr visitDefaultBinaryExpr(CONTEXT ctx, Stack<Expr> stack, BinaryOperator expr)
      throws PlanningException {
    stack.push(expr);
    visit(ctx, stack, expr.getLeft());
    visit(ctx, stack, expr.getRight());
    stack.pop();
    return expr;
  }

  @Override
  public Expr visitProjection(CONTEXT ctx, Stack<Expr> stack, Projection expr) throws PlanningException {
    stack.push(expr);
    for (TargetExpr target : expr.getTargets()) {
      visit(ctx, stack, target);
    }
    visit(ctx, stack, expr.getChild());
    stack.pop();
    return expr;
  }

  @Override
  public Expr visitLimit(CONTEXT ctx, Stack<Expr> stack, Limit expr) throws PlanningException {
    stack.push(expr);
    visit(ctx, stack, expr.getFetchFirstNum());
    visit(ctx, stack, expr.getChild());
    stack.pop();
    return expr;
  }

  @Override
  public Expr visitSort(CONTEXT ctx, Stack<Expr> stack, Sort expr) throws PlanningException {
    stack.push(expr);
    for (Sort.SortSpec sortSpec : expr.getSortSpecs()) {
      visit(ctx, stack, sortSpec.getKey());
    }
    visit(ctx, stack, expr.getChild());
    return expr;
  }

  @Override
  public Expr visitHaving(CONTEXT ctx, Stack<Expr> stack, Having expr) throws PlanningException {
    stack.push(expr);
    visit(ctx, stack, expr.getQual());
    visit(ctx, stack, expr.getChild());
    stack.pop();
    return expr;
  }

  @Override
  public Expr visitGroupBy(CONTEXT ctx, Stack<Expr> stack, Aggregation expr) throws PlanningException {
    return visitDefaultUnaryExpr(ctx, stack, expr);
  }

  @Override
  public Expr visitJoin(CONTEXT ctx, Stack<Expr> stack, Join expr) throws PlanningException {
    stack.push(expr);
    visit(ctx, stack, expr.getQual());
    visit(ctx, stack, expr.getLeft());
    visit(ctx, stack, expr.getRight());
    stack.pop();
    return expr;
  }

  @Override
  public Expr visitFilter(CONTEXT ctx, Stack<Expr> stack, Selection expr) throws PlanningException {
    stack.push(expr);
    visit(ctx, stack, expr.getQual());
    Expr result = visit(ctx, stack, expr.getChild());
    stack.pop();
    return result;
  }

  @Override
  public Expr visitUnion(CONTEXT ctx, Stack<Expr> stack, SetOperation expr) throws PlanningException {
    return visitDefaultBinaryExpr(ctx, stack, expr);
  }

  @Override
  public Expr visitExcept(CONTEXT ctx, Stack<Expr> stack, SetOperation expr) throws PlanningException {
    return visitDefaultBinaryExpr(ctx, stack, expr);
  }

  @Override
  public Expr visitIntersect(CONTEXT ctx, Stack<Expr> stack, SetOperation expr) throws PlanningException {
    return visitDefaultBinaryExpr(ctx, stack, expr);
  }

  @Override
  public Expr visitSimpleTableSubQuery(CONTEXT ctx, Stack<Expr> stack, SimpleTableSubQuery expr)
      throws PlanningException {
    return visitDefaultUnaryExpr(ctx, stack, expr);
  }

  @Override
  public Expr visitTableSubQuery(CONTEXT ctx, Stack<Expr> stack, TablePrimarySubQuery expr)
      throws PlanningException {
    stack.push(expr);
    visit(ctx, stack, expr.getSubQuery());
    stack.pop();
    return expr;
  }

  @Override
  public Expr visitRelationList(CONTEXT ctx, Stack<Expr> stack, RelationList expr) throws PlanningException {
    stack.push(expr);
    for (Expr e : expr.getRelations()) {
      visit(ctx, stack, e);
    }
    stack.pop();
    return expr;
  }

  @Override
  public Expr visitRelation(CONTEXT ctx, Stack<Expr> stack, Relation expr) throws PlanningException {
    return expr;
  }

  @Override
  public Expr visitScalarSubQuery(CONTEXT ctx, Stack<Expr> stack, ScalarSubQuery expr) throws PlanningException {
    return visitDefaultUnaryExpr(ctx, stack, expr);
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Data Definition Language Section
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////

  @Override
  public Expr visitCreateTable(CONTEXT ctx, Stack<Expr> stack, CreateTable expr) throws PlanningException {
    stack.push(expr);
    if (expr.hasSubQuery()) {
      visit(ctx, stack, expr.getSubQuery());
    }
    stack.pop();
    return expr;
  }

  @Override
  public Expr visitDropTable(CONTEXT ctx, Stack<Expr> stack, DropTable expr) throws PlanningException {
    return expr;
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Insert or Update Section
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////

  public Expr visitInsert(CONTEXT ctx, Stack<Expr> stack, Insert expr) throws PlanningException {
    stack.push(expr);
    visit(ctx, stack, expr.getSubQuery());
    stack.pop();
    return expr;
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Logical Operator Section
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////

  @Override
  public Expr visitAnd(CONTEXT ctx, Stack<Expr> stack, BinaryOperator expr) throws PlanningException {
    return visitDefaultBinaryExpr(ctx, stack, expr);
  }

  @Override
  public Expr visitOr(CONTEXT ctx, Stack<Expr> stack, BinaryOperator expr) throws PlanningException {
    return visitDefaultBinaryExpr(ctx, stack, expr);
  }

  @Override
  public Expr visitNot(CONTEXT ctx, Stack<Expr> stack, NotExpr expr) throws PlanningException {
    return visitDefaultUnaryExpr(ctx, stack, expr);
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Comparison Predicates Section
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  @Override
  public Expr visitEquals(CONTEXT ctx, Stack<Expr> stack, BinaryOperator expr) throws PlanningException {
    return visitDefaultBinaryExpr(ctx, stack, expr);
  }

  @Override
  public Expr visitNotEquals(CONTEXT ctx, Stack<Expr> stack, BinaryOperator expr) throws PlanningException {
    return visitDefaultBinaryExpr(ctx, stack, expr);
  }

  @Override
  public Expr visitLessThan(CONTEXT ctx, Stack<Expr> stack, BinaryOperator expr) throws PlanningException {
    return visitDefaultBinaryExpr(ctx, stack, expr);
  }

  @Override
  public Expr visitLessThanOrEquals(CONTEXT ctx, Stack<Expr> stack, BinaryOperator expr) throws PlanningException {
    return visitDefaultBinaryExpr(ctx, stack, expr);
  }

  @Override
  public Expr visitGreaterThan(CONTEXT ctx, Stack<Expr> stack, BinaryOperator expr) throws PlanningException {
    return visitDefaultBinaryExpr(ctx, stack, expr);
  }

  @Override
  public Expr visitGreaterThanOrEquals(CONTEXT ctx, Stack<Expr> stack, BinaryOperator expr)
      throws PlanningException {
    return visitDefaultBinaryExpr(ctx, stack, expr);
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Other Predicates Section
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////

  @Override
  public Expr visitBetween(CONTEXT ctx, Stack<Expr> stack, BetweenPredicate expr) throws PlanningException {
    stack.push(expr);
    visit(ctx, stack, expr.predicand());
    visit(ctx, stack, expr.begin());
    visit(ctx, stack, expr.end());
    stack.pop();
    return expr;
  }

  @Override
  public Expr visitCaseWhen(CONTEXT ctx, Stack<Expr> stack, CaseWhenPredicate expr) throws PlanningException {
    stack.push(expr);
    for (CaseWhenPredicate.WhenExpr when : expr.getWhens()) {
      visit(ctx, stack, when.getCondition());
      visit(ctx, stack, when.getResult());
    }
    if (expr.hasElseResult()) {
      visit(ctx, stack, expr.getElseResult());
    }
    stack.pop();
    return expr;
  }

  @Override
  public Expr visitIsNullPredicate(CONTEXT ctx, Stack<Expr> stack, IsNullPredicate expr) throws PlanningException {
    return visitDefaultUnaryExpr(ctx, stack, expr);
  }

  @Override
  public Expr visitInPredicate(CONTEXT ctx, Stack<Expr> stack, InPredicate expr) throws PlanningException {
    return visitDefaultBinaryExpr(ctx, stack, expr);
  }

  @Override
  public Expr visitValueListExpr(CONTEXT ctx, Stack<Expr> stack, ValueListExpr expr) throws PlanningException {
    stack.push(expr);
    for (Expr value : expr.getValues()) {
      visit(ctx, stack, value);
    }
    stack.pop();
    return expr;
  }

  @Override
  public Expr visitExistsPredicate(CONTEXT ctx, Stack<Expr> stack, ExistsPredicate expr) throws PlanningException {
    return visitDefaultUnaryExpr(ctx, stack, expr);
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  // String Operator or Pattern Matching Predicates Section
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  @Override
  public Expr visitLikePredicate(CONTEXT ctx, Stack<Expr> stack, PatternMatchPredicate expr)
      throws PlanningException {
    return visitDefaultBinaryExpr(ctx, stack, expr);
  }

  @Override
  public Expr visitSimilarToPredicate(CONTEXT ctx, Stack<Expr> stack, PatternMatchPredicate expr)
      throws PlanningException {
    return visitDefaultBinaryExpr(ctx, stack, expr);
  }

  @Override
  public Expr visitRegexpPredicate(CONTEXT ctx, Stack<Expr> stack, PatternMatchPredicate expr)
      throws PlanningException {
    return visitDefaultBinaryExpr(ctx, stack, expr);
  }

  @Override
  public Expr visitConcatenate(CONTEXT ctx, Stack<Expr> stack, BinaryOperator expr) throws PlanningException {
    return visitDefaultBinaryExpr(ctx, stack, expr);
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Arithmetic Operators
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////

  @Override
  public Expr visitPlus(CONTEXT ctx, Stack<Expr> stack, BinaryOperator expr) throws PlanningException {
    return visitDefaultBinaryExpr(ctx, stack, expr);
  }

  @Override
  public Expr visitMinus(CONTEXT ctx, Stack<Expr> stack, BinaryOperator expr) throws PlanningException {
    return visitDefaultBinaryExpr(ctx, stack, expr);
  }

  @Override
  public Expr visitMultiply(CONTEXT ctx, Stack<Expr> stack, BinaryOperator expr) throws PlanningException {
    return visitDefaultBinaryExpr(ctx, stack, expr);
  }

  @Override
  public Expr visitDivide(CONTEXT ctx, Stack<Expr> stack, BinaryOperator expr) throws PlanningException {
    return visitDefaultBinaryExpr(ctx, stack, expr);
  }

  @Override
  public Expr visitModular(CONTEXT ctx, Stack<Expr> stack, BinaryOperator expr) throws PlanningException {
    return visitDefaultBinaryExpr(ctx, stack, expr);
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Other Expressions
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////

  @Override
  public Expr visitSign(CONTEXT ctx, Stack<Expr> stack, SignedExpr expr) throws PlanningException {
    return visitDefaultUnaryExpr(ctx, stack, expr);
  }

  @Override
  public Expr visitColumnReference(CONTEXT ctx, Stack<Expr> stack, ColumnReferenceExpr expr)
      throws PlanningException {
    return expr;
  }

  @Override
  public Expr visitTargetExpr(CONTEXT ctx, Stack<Expr> stack, TargetExpr expr) throws PlanningException {
    return visitDefaultUnaryExpr(ctx, stack, expr);
  }

  @Override
  public Expr visitFunction(CONTEXT ctx, Stack<Expr> stack, FunctionExpr expr) throws PlanningException {
    stack.push(expr);
    for (Expr param : expr.getParams()) {
      visit(ctx, stack, param);
    }
    stack.pop();
    return expr;
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  // General Set Section
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////

  @Override
  public Expr visitCountRowsFunction(CONTEXT ctx, Stack<Expr> stack, CountRowsFunctionExpr expr)
      throws PlanningException {
    return expr;
  }

  @Override
  public Expr visitGeneralSetFunction(CONTEXT ctx, Stack<Expr> stack, GeneralSetFunctionExpr expr)
      throws PlanningException {
    stack.push(expr);
    for (Expr param : expr.getParams()) {
      visit(ctx, stack, param);
    }
    stack.pop();
    return expr;
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Literal Section
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////

  @Override
  public Expr visitDataType(CONTEXT ctx, Stack<Expr> stack, DataTypeExpr expr) throws PlanningException {
    return expr;
  }

  @Override
  public Expr visitCastExpr(CONTEXT ctx, Stack<Expr> stack, CastExpr expr) throws PlanningException {
    stack.push(expr);
    visit(ctx, stack, expr.getOperand());
    stack.pop();
    return expr;
  }

  @Override
  public Expr visitLiteral(CONTEXT ctx, Stack<Expr> stack, LiteralValue expr) throws PlanningException {
    return expr;
  }

  @Override
  public Expr visitNullLiteral(CONTEXT ctx, Stack<Expr> stack, NullLiteral expr) throws PlanningException {
    return expr;
  }

  @Override
  public Expr visitTimestampLiteral(CONTEXT ctx, Stack<Expr> stack, TimestampLiteral expr) throws PlanningException {
    return expr;
  }

  @Override
  public Expr visitTimeLiteral(CONTEXT ctx, Stack<Expr> stack, TimeLiteral expr) throws PlanningException {
    return expr;
  }
}
