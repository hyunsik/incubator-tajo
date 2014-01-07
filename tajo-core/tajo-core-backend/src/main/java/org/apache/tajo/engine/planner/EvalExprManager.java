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
import org.apache.tajo.annotation.Nullable;
import org.apache.tajo.engine.eval.EvalNode;
import org.apache.tajo.engine.eval.FieldEval;

import java.util.*;

/**
 * It manages a list of targets.
 */
public class EvalExprManager {
  private Map<String, EvalNode> nameToEvalMap = new LinkedHashMap<String, EvalNode>();
  private LinkedHashMap<String, Expr> nameToExprMap = new LinkedHashMap<String, Expr>();
  private LinkedHashMap<Expr, String> exprToNameMap = new LinkedHashMap<Expr, String>();
  private LinkedHashMap<String, Boolean> resolvedFlags = new LinkedHashMap<String, Boolean>();

  private LogicalPlan plan;
  private LogicalPlanner planner;

  public EvalExprManager(LogicalPlan plan, LogicalPlanner planner, LogicalPlan.QueryBlock block) {
    this.plan = plan;
    this.planner = planner;
  }

  public boolean isResolved(String name) {
    String normalized = name.toLowerCase();
    return resolvedFlags.containsKey(normalized) && resolvedFlags.get(normalized);
  }

  public String addExpr(String alias, Expr expr) {
    if (exprToNameMap.containsKey(expr)) {
      return exprToNameMap.get(expr);
    } else {
      String normalized = alias.toLowerCase();
      nameToExprMap.put(normalized, expr);
      exprToNameMap.put(expr, normalized);
      resolvedFlags.put(normalized, false);
      return normalized;
    }
  }

  public String addExpr(Expr expr) {
    String name;

    // all columns are projected automatically. BTW, should we add column reference to this list?
    if (expr.getType() == OpType.Column) {
      name = ((ColumnReferenceExpr)expr).getCanonicalName();
      if (nameToExprMap.containsKey(name)) { // if it is column and another one already exists, skip.
        return name;
      }
    } else {
      name = plan.newNonameColumnName(expr.getType().name());
    }
    return addExpr(name, expr);
  }

  public String [] addExprArray(Expr[] exprs) {
    String [] names = new String[exprs.length];
    for (int i = 0; i < exprs.length; i++) {
      Expr expr = exprs[i];
      if (expr.getType() == OpType.Column) {
        String referenceName = ((ColumnReferenceExpr)expr).getCanonicalName();
        if (nameToExprMap.containsKey(referenceName)) {
          names[i] = referenceName;
        } else {
          names[i] = addExpr(exprs[i]);
        }
      } else {
        names[i] = addExpr(exprs[i]);
      }
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

  public String [] addTargetExprArray(@Nullable Collection<TargetExpr> targets) {
    if (targets != null || targets.size() > 0) {
      String [] names = new String[targets.size()];
      int i = 0;
      for (TargetExpr target : targets) {
        names[i++] = addTargetExpr(target);
      }
      return names;
    } else {
      return null;
    }
  }

  public Collection<TargetExpr> getRawTargets() {
    List<TargetExpr> targetExprList = new ArrayList<TargetExpr>();
    for (Map.Entry<String, Expr> entry: nameToExprMap.entrySet()) {
      targetExprList.add(new TargetExpr(entry.getValue(), entry.getKey()));
    }
    return targetExprList;
  }

  public void switchTarget(String name, EvalNode evalNode) {
    String normalized = name.toLowerCase();
    nameToEvalMap.put(normalized, evalNode);
    resolvedFlags.put(normalized, true);
  }

  public Target getTarget(String name) {
    String normalized = name;
    if (resolvedFlags.containsKey(normalized) && resolvedFlags.get(normalized)) {
      return new Target(new FieldEval(normalized, nameToEvalMap.get(normalized).getValueType()));
    } else {
      if (nameToEvalMap.containsKey(normalized)) {
        return new Target(nameToEvalMap.get(normalized), name);
      } else {
        return null;
      }
    }
  }

  public Target getTarget(Expr expr) {
    if (exprToNameMap.containsKey(expr)) {
      String name = exprToNameMap.get(expr);
      return getTarget(name);
    } else {
      return null;
    }
  }

  public String [] getTargetNames() {
    return nameToEvalMap.keySet().toArray(new String[nameToEvalMap.size()]);
  }

  public TargetExpr getRawTarget(String name) {
    String normalized = name.toLowerCase();
    return new TargetExpr(nameToExprMap.get(normalized), normalized);
  }

  public String toString() {
    return "rawTargets=" + nameToExprMap.size() + ", targets=" + nameToEvalMap.size();
  }
}
