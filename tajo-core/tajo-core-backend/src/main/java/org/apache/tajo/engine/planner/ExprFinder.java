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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
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
class ExprFinder extends SimpleAlgebraVisitor<ExprFinder.Context, Object> {

  static class Context {
    Set<Expr> set = new HashSet<Expr>();
    OpType targetType;

    Context(OpType type) {
      this.targetType = type;
    }
  }

  public static <T extends Expr> Set<T> finds(Expr expr, OpType type) {
    Context context = new Context(type);
    ExprFinder finder = new ExprFinder();
    Stack<Expr> stack = new Stack<Expr>();
    stack.push(expr);
    try {
      finder.visit(context, new Stack<Expr>(), expr);
    } catch (PlanningException e) {
      throw new RuntimeException(e);
    }
    stack.pop();
    return (Set<T>) context.set;
  }

  public Object visit(Context ctx, Stack<Expr> stack, Expr expr) throws PlanningException {
    if (expr instanceof UnaryOperator) {
      preHook(ctx, stack, expr);
      visitUnaryOperator(ctx, stack, (UnaryOperator) expr);
      postHook(ctx, stack, expr, null);
    } else if (expr instanceof BinaryOperator) {
      preHook(ctx, stack, expr);
      visitBinaryOperator(ctx, stack, (BinaryOperator) expr);
      postHook(ctx, stack, expr, null);
    } else {
      super.visit(ctx, stack, expr);
    }

    if (ctx.targetType == expr.getType()) {
      ctx.set.add(expr);
    }

    return null;
  }
}
