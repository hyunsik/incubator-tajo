/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.engine.planner;

import org.apache.tajo.algebra.ColumnReferenceExpr;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.algebra.OpType;
import org.apache.tajo.algebra.TargetExpr;
import org.apache.tajo.engine.eval.EvalNode;
import org.apache.tajo.engine.eval.FieldEval;

import java.util.*;

/**
 * It manages a list of targets.
 */
public class NewTargetListManager {
  private Map<String, EvalNode> nameToEvalMap = new LinkedHashMap<String, EvalNode>();
  private LinkedHashMap<String, Expr> nameToExprMap = new LinkedHashMap<String, Expr>();
  private LinkedHashMap<Expr, String> exprToNameMap = new LinkedHashMap<Expr, String>();
  private LinkedHashMap<String, Boolean> resolvedFlags = new LinkedHashMap<String, Boolean>();

  private LogicalPlan plan;
  private LogicalPlanner planner;

  public NewTargetListManager(LogicalPlan plan, LogicalPlanner planner, LogicalPlan.QueryBlock block) {
    this.plan = plan;
    this.planner = planner;
  }

  public boolean isResolved(String name) {
    return resolvedFlags.containsKey(name) && resolvedFlags.get(name);
  }

  public String addExpr(String alias, Expr expr) {
    if (exprToNameMap.containsKey(expr)) {
      return exprToNameMap.get(expr);
    } else {
      nameToExprMap.put(alias, expr);
      exprToNameMap.put(expr, alias);
      resolvedFlags.put(alias, false);
      return alias;
    }
  }

  public String addExpr(Expr expr) {
    String name;
    if (expr.getType() == OpType.Column) {
      name = ((ColumnReferenceExpr)expr).getCanonicalName();
    } else {
      name = plan.newNonameColumnName(expr.getType().name());
    }
    return addExpr(name, expr);
  }

  public String [] addExprArray(Expr[] exprs) {
    String [] names = new String[exprs.length];
    for (int i = 0; i < exprs.length; i++) {
      names[i] = addExpr(exprs[i]);
    }
    return names;
  }

  public String addTargetExpr(TargetExpr targetExpr) {
    if (targetExpr.hasAlias()) {
      return addExpr(targetExpr.getAlias(), targetExpr.getExpr());
    } else {
      return addExpr(targetExpr.getExpr());
    }
  }

  public String [] addTargetExprArray(TargetExpr[] targets) {
    String [] names = new String[targets.length];
    for (int i = 0; i < targets.length; i++) {
      names[i] = addTargetExpr(targets[i]);
    }
    return names;
  }

  public Collection<TargetExpr> getRawTargets() {
    List<TargetExpr> targetExprList = new ArrayList<TargetExpr>();
    for (Map.Entry<String, Expr> entry: nameToExprMap.entrySet()) {
      targetExprList.add(new TargetExpr(entry.getValue(), entry.getKey()));
    }
    return targetExprList;
  }

  public void switchTarget(String name, EvalNode evalNode) {
    nameToEvalMap.put(name, evalNode);
//    Expr expr = nameToExprMap.remove(name);
//    exprToNameMap.remove(expr);
    resolvedFlags.put(name, true);
  }

  public Target getTarget(String name) {
    if (resolvedFlags.containsKey(name)) {
      return new Target(new FieldEval(name, nameToEvalMap.get(name).getValueType()));
    } else {
      if (nameToEvalMap.containsKey(name)) {
        return new Target(nameToEvalMap.get(name), name);
      } else {
        return null;
      }
    }
  }

  public Target getTarget(Expr expr) {
    if (exprToNameMap.containsKey(expr)) {
      String name = exprToNameMap.get(expr);
      if (nameToEvalMap.containsKey(name)) {
        EvalNode evalNode = nameToEvalMap.get(name);
        return new Target(evalNode, name);
      } else {
        return null;
      }
    } else {
      return null;
    }
  }

  public TargetExpr getRawTarget(String name) {
    return new TargetExpr(nameToExprMap.get(name), name);
  }

  public String toString() {
    return "rawTargets=" + nameToExprMap.size() + ", targets=" + nameToEvalMap.size();
  }
}
