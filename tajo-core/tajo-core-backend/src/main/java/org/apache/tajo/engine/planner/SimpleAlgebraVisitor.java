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

public abstract class SimpleAlgebraVisitor<CONTEXT> extends BasicAlgebraVisitor<CONTEXT> {

  /**
   * The prehook is called before each expression is visited.
   */
  public void preHook(CONTEXT ctx, Stack<Expr> stack, Expr expr) throws PlanningException {
  }


  /**
   * The posthook is called before each expression is visited.
   */
  public Expr postHook(CONTEXT ctx, Stack<Expr> stack, Expr expr, Expr current) throws PlanningException {
    return current;
  }

  abstract Expr visitUnaryOperator(CONTEXT ctx, Stack<Expr> stack, UnaryOperator expr) throws PlanningException;

  abstract Expr visitBinaryOperator(CONTEXT ctx, Stack<Expr> stack, BinaryOperator expr) throws PlanningException;

  @Override
  public abstract Expr visitTableSubQuery(CONTEXT ctx, Stack<Expr> stack, TablePrimarySubQuery expr)
      throws PlanningException;

  @Override
  public abstract Expr visitRelationList(CONTEXT ctx, Stack<Expr> stack, RelationList expr) throws PlanningException;

  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Data Definition Language Section
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////

  @Override
  public abstract Expr visitCreateTable(CONTEXT ctx, Stack<Expr> stack, CreateTable expr) throws PlanningException;

  @Override
  public abstract Expr visitDropTable(CONTEXT ctx, Stack<Expr> stack, DropTable expr) throws PlanningException;

  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Insert or Update Section
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////

  public abstract Expr visitInsert(CONTEXT ctx, Stack<Expr> stack, Insert expr) throws PlanningException;


  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Other Predicates Section
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////

  @Override
  public abstract Expr visitBetween(CONTEXT ctx, Stack<Expr> stack, BetweenPredicate expr) throws PlanningException;

  @Override
  public abstract Expr visitCaseWhen(CONTEXT ctx, Stack<Expr> stack, CaseWhenPredicate expr) throws PlanningException;

  @Override
  public abstract Expr visitValueListExpr(CONTEXT ctx, Stack<Expr> stack, ValueListExpr expr) throws PlanningException;


  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  // String Operator or Pattern Matching Predicates Section
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////

  private Expr visitPatternMatchPredicate(CONTEXT ctx, Stack<Expr> stack, PatternMatchPredicate expr)
      throws PlanningException {
    stack.push(expr);
    visit(ctx, stack, expr.getPredicand());
    visit(ctx, stack, expr.getPattern());
    stack.pop();
    return expr;
  }
  @Override
  public Expr visitLikePredicate(CONTEXT ctx, Stack<Expr> stack, PatternMatchPredicate expr)
      throws PlanningException {
    return visitPatternMatchPredicate(ctx, stack, expr);
  }

  @Override
  public Expr visitSimilarToPredicate(CONTEXT ctx, Stack<Expr> stack, PatternMatchPredicate expr)
      throws PlanningException {
    return visitPatternMatchPredicate(ctx, stack, expr);
  }

  @Override
  public Expr visitRegexpPredicate(CONTEXT ctx, Stack<Expr> stack, PatternMatchPredicate expr)
      throws PlanningException {
    return visitPatternMatchPredicate(ctx, stack, expr);
  }

  @Override
  public Expr visitConcatenate(CONTEXT ctx, Stack<Expr> stack, BinaryOperator expr) throws PlanningException {
    return visitBinaryOperator(ctx, stack, expr);
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Arithmetic Operators
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////

  @Override
  public Expr visitPlus(CONTEXT ctx, Stack<Expr> stack, BinaryOperator expr) throws PlanningException {
    return visitBinaryOperator(ctx, stack, expr);
  }

  @Override
  public Expr visitMinus(CONTEXT ctx, Stack<Expr> stack, BinaryOperator expr) throws PlanningException {
    return visitBinaryOperator(ctx, stack, expr);
  }

  @Override
  public Expr visitMultiply(CONTEXT ctx, Stack<Expr> stack, BinaryOperator expr) throws PlanningException {
    return visitBinaryOperator(ctx, stack, expr);
  }

  @Override
  public Expr visitDivide(CONTEXT ctx, Stack<Expr> stack, BinaryOperator expr) throws PlanningException {
    return visitBinaryOperator(ctx, stack, expr);
  }

  @Override
  public Expr visitModular(CONTEXT ctx, Stack<Expr> stack, BinaryOperator expr) throws PlanningException {
    return visitBinaryOperator(ctx, stack, expr);
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Other Expressions
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////

  @Override
  public Expr visitSign(CONTEXT ctx, Stack<Expr> stack, SignedExpr expr) throws PlanningException {
    return visitUnaryOperator(ctx, stack, expr);
  }

  @Override
  public Expr visitColumnReference(CONTEXT ctx, Stack<Expr> stack, ColumnReferenceExpr expr)
      throws PlanningException {
    return null;
  }

  @Override
  public Expr visitTargetExpr(CONTEXT ctx, Stack<Expr> stack, TargetExpr expr) throws PlanningException {
    stack.push(expr);
    visit(ctx, stack, expr.getExpr());
    stack.pop();
    return expr;
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
