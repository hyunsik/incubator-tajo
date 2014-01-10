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

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import org.apache.tajo.algebra.ColumnReferenceExpr;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.algebra.NamedExpr;
import org.apache.tajo.algebra.OpType;
import org.apache.tajo.annotation.Nullable;
import org.apache.tajo.engine.eval.EvalNode;
import org.apache.tajo.engine.eval.FieldEval;

import java.util.*;

/**
 * NamedExprsManager manages an expressions to be evaluated in a query block.
 * NamedExprsManager uses a reference name to identify one expression or one
 * EvalNode (annotated expression).
 */
public class NamedExprsManager {
  /** Map; Reference name -> EvalNode */
  private Map<String, EvalNode> nameToEvalMap = new LinkedHashMap<String, EvalNode>();
  /** Map; Reference name -> Expr */
  private LinkedHashMap<String, Expr> nameToExprMap = new LinkedHashMap<String, Expr>();
  /** Map; Expr -> Reference Name */
  private LinkedHashMap<Expr, String> exprToNameMap = new LinkedHashMap<Expr, String>();
  /** Transitive Closer Map: Name -> Name */
  private BiMap<String, String> nameToNameMap = HashBiMap.create();
  /** Map; Reference Name -> Boolean (if it is resolved or not) */
  private LinkedHashMap<String, Boolean> resolvedFlags = new LinkedHashMap<String, Boolean>();

  private LogicalPlan plan;

  public NamedExprsManager(LogicalPlan plan) {
    this.plan = plan;
  }

  /**
   * Check whether the expression corresponding to a given name was resolved.
   *
   * @param name The name of a certain expression to be checked
   * @return true if resolved. Otherwise, false.
   */
  public boolean isResolved(String name) {
    String normalized = name.toLowerCase();
    return resolvedFlags.containsKey(normalized) && resolvedFlags.get(normalized);
  }

  public boolean contains(String name) {
    return nameToExprMap.containsKey(name);
  }

  public boolean contains(Expr expr) {
    return exprToNameMap.containsKey(expr);
  }

  public String getName(Expr expr) {
    return exprToNameMap.get(expr);
  }

  public Expr getExpr(String name) {
    return nameToExprMap.get(name);
  }

  public NamedExpr getNamedExpr(String name) {
    String normalized = name.toLowerCase();
    return new NamedExpr(nameToExprMap.get(normalized), normalized);
  }

  public void transite(String from, String to) {
    nameToNameMap.put(from, to);
  }

  public boolean hasTransition(String from) {
    return nameToNameMap.containsKey(from);
  }

  public String getTransittedName(String from) {
    return nameToNameMap.get(from);
  }

  public String addExpr(Expr expr, String alias) {
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
      name = plan.newGeneratedFieldName(expr);
    }
    return addExpr(expr, name);
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

  public String addNamedExpr(NamedExpr namedExpr) {
    if (namedExpr.hasAlias()) {
      return addExpr(namedExpr.getExpr(), namedExpr.getAlias());
    } else {
      return addExpr(namedExpr.getExpr());
    }
  }

  public String [] addNamedExprArray(@Nullable Collection<NamedExpr> targets) {
    if (targets != null || targets.size() > 0) {
      String [] names = new String[targets.size()];
      int i = 0;
      for (NamedExpr target : targets) {
        names[i++] = addNamedExpr(target);
      }
      return names;
    } else {
      return null;
    }
  }

  public Collection<NamedExpr> getAllNamedExprs() {
    List<NamedExpr> namedExprList = new ArrayList<NamedExpr>();
    for (Map.Entry<String, Expr> entry: nameToExprMap.entrySet()) {
      namedExprList.add(new NamedExpr(entry.getValue(), entry.getKey()));
    }
    return namedExprList;
  }

  public void resolveExpr(String name, EvalNode evalNode) {
    String normalized = name.toLowerCase();
    nameToEvalMap.put(normalized, evalNode);
    resolvedFlags.put(normalized, true);
  }

  public Target getTarget(Expr expr, boolean unresolved) {
    String name = exprToNameMap.get(expr);
    return getTarget(name, unresolved);
  }

  public Target getTarget(String name) {
    return getTarget(name, false);
  }

  public Target getTarget(String name, boolean unresolved) {
    String normalized = name;
    if (!unresolved && resolvedFlags.containsKey(normalized) && resolvedFlags.get(normalized)) {
      return new Target(new FieldEval(normalized, nameToEvalMap.get(normalized).getValueType()));
    } else {
      if (nameToEvalMap.containsKey(normalized)) {
        return new Target(nameToEvalMap.get(normalized), name);
      } else {
        return null;
      }
    }
  }

  public String toString() {
    return "raw=" + nameToExprMap.size() + ", resolved=" + nameToEvalMap.size();
  }
}
