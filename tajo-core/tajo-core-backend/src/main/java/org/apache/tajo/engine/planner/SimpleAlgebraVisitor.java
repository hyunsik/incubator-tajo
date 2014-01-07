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

/**
 * <code>SimpleAlgebraVisitor</code> provides a simple and fewer visit methods. It makes building concrete class easier.
 */
public abstract class SimpleAlgebraVisitor<CONTEXT> extends BasicAlgebraVisitor<CONTEXT> {

  public Expr visit(CONTEXT ctx, Stack<Expr> stack, Expr expr) throws PlanningException {
    preHook(ctx, stack, expr);
    if (expr instanceof UnaryOperator) {
      visitUnaryOperator(ctx, stack, (UnaryOperator) expr);
    } else if (expr instanceof BinaryOperator) {
      visitBinaryOperator(ctx, stack, (BinaryOperator) expr);
    } else {
      super.visit(ctx, stack, expr);
    }
    postHook(ctx, stack, expr, expr);
    return expr;
  }

  public Expr visitUnaryOperator(CONTEXT ctx, Stack<Expr> stack, UnaryOperator expr) throws PlanningException {
    stack.push(expr);
    visit(ctx, stack, expr.getChild());
    stack.pop();
    return expr;
  }

  public Expr visitBinaryOperator(CONTEXT ctx, Stack<Expr> stack, BinaryOperator expr) throws PlanningException {
    stack.push(expr);
    visit(ctx, stack, expr.getLeft());
    visit(ctx, stack, expr.getRight());
    stack.pop();
    return expr;
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Relational Operator Section
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////

  @Override
  public Expr visitProjection(CONTEXT ctx, Stack<Expr> stack, Projection expr) throws PlanningException {
    return super.visitProjection(ctx, stack, expr);
  }

  @Override
  public Expr visitLimit(CONTEXT ctx, Stack<Expr> stack, Limit expr) throws PlanningException {
    return super.visitLimit(ctx, stack, expr);
  }

  @Override
  public Expr visitSort(CONTEXT ctx, Stack<Expr> stack, Sort expr) throws PlanningException {
    return super.visitSort(ctx, stack, expr);
  }

  @Override
  public Expr visitHaving(CONTEXT ctx, Stack<Expr> stack, Having expr) throws PlanningException {
    return super.visitHaving(ctx, stack, expr);
  }

  public Expr visitFilter(CONTEXT ctx, Stack<Expr> stack, Selection expr) throws PlanningException {
    return super.visitFilter(ctx, stack, expr);
  }

  @Override
  public Expr visitJoin(CONTEXT ctx, Stack<Expr> stack, Join expr) throws PlanningException {
    return super.visitJoin(ctx, stack, expr);
  }

  @Override
  public Expr visitTableSubQuery(CONTEXT ctx, Stack<Expr> stack, TablePrimarySubQuery expr) throws PlanningException {
    return super.visitTableSubQuery(ctx, stack, expr);
  }

  @Override
  public Expr visitRelationList(CONTEXT ctx, Stack<Expr> stack, RelationList expr) throws PlanningException {
    return super.visitRelationList(ctx, stack, expr);
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Data Definition Language Section
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////

  @Override
  public Expr visitCreateTable(CONTEXT ctx, Stack<Expr> stack, CreateTable expr) throws PlanningException {
    return super.visitCreateTable(ctx, stack, expr);
  }

  @Override
  public Expr visitDropTable(CONTEXT ctx, Stack<Expr> stack, DropTable expr) throws PlanningException {
    return super.visitDropTable(ctx, stack, expr);
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Insert or Update Section
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////

  public Expr visitInsert(CONTEXT ctx, Stack<Expr> stack, Insert expr) throws PlanningException {
    return super.visitInsert(ctx, stack, expr);
  }


  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Other Predicates Section
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////

  @Override
  public Expr visitBetween(CONTEXT ctx, Stack<Expr> stack, BetweenPredicate expr) throws PlanningException {
    return super.visitBetween(ctx, stack, expr);
  }

  @Override
  public Expr visitCaseWhen(CONTEXT ctx, Stack<Expr> stack, CaseWhenPredicate expr) throws PlanningException {
    return super.visitCaseWhen(ctx, stack, expr);
  }

  @Override
  public Expr visitValueListExpr(CONTEXT ctx, Stack<Expr> stack, ValueListExpr expr) throws PlanningException {
    return super.visitValueListExpr(ctx, stack, expr);
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Other Expressions
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////

  @Override
  public Expr visitFunction(CONTEXT ctx, Stack<Expr> stack, FunctionExpr expr) throws PlanningException {
    return super.visitFunction(ctx, stack, expr);
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  // General Set Section
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////

  @Override
  public Expr visitCountRowsFunction(CONTEXT ctx, Stack<Expr> stack, CountRowsFunctionExpr expr)
      throws PlanningException {
    return super.visitCountRowsFunction(ctx, stack, expr);
  }

  @Override
  public Expr visitGeneralSetFunction(CONTEXT ctx, Stack<Expr> stack, GeneralSetFunctionExpr expr)
      throws PlanningException {
    return super.visitGeneralSetFunction(ctx, stack, expr);
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Literal Section
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////

  @Override
  public Expr visitDataType(CONTEXT ctx, Stack<Expr> stack, DataTypeExpr expr) throws PlanningException {
    return super.visitDataType(ctx, stack, expr);
  }

  @Override
  public Expr visitCastExpr(CONTEXT ctx, Stack<Expr> stack, CastExpr expr) throws PlanningException {
    return super.visitCastExpr(ctx, stack, expr);
  }

  @Override
  public Expr visitLiteral(CONTEXT ctx, Stack<Expr> stack, LiteralValue expr) throws PlanningException {
    return super.visitLiteral(ctx, stack, expr);
  }

  @Override
  public Expr visitNullLiteral(CONTEXT ctx, Stack<Expr> stack, NullLiteral expr) throws PlanningException {
    return super.visitNullLiteral(ctx, stack, expr);
  }

  @Override
  public Expr visitTimestampLiteral(CONTEXT ctx, Stack<Expr> stack, TimestampLiteral expr) throws PlanningException {
    return super.visitTimestampLiteral(ctx, stack, expr);
  }

  @Override
  public Expr visitTimeLiteral(CONTEXT ctx, Stack<Expr> stack, TimeLiteral expr) throws PlanningException {
    return super.visitTimeLiteral(ctx, stack, expr);
  }
}
