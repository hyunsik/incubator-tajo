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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.tajo.algebra.ColumnReferenceExpr;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.algebra.NamedExpr;
import org.apache.tajo.algebra.OpType;
import org.apache.tajo.annotation.Nullable;
import org.apache.tajo.engine.eval.EvalNode;
import org.apache.tajo.engine.eval.EvalType;
import org.apache.tajo.engine.eval.FieldEval;
import org.apache.tajo.util.TUtil;

import java.util.*;

/**
 * NamedExprsManager manages an expressions to be evaluated in a query block.
 * NamedExprsManager uses a reference name to identify one expression or one
 * EvalNode (annotated expression).
 *
 * It keeps a map from an unique integer id to both unique Expr and EvalNode.
 */
public class NamedExprsManager {
  /** a sequence id */
  private int sequenceId = 0;

  /** Map: Name -> ID. Two or more different names can indicates the same id. */
  private LinkedHashMap<String, Integer> nameToIdMap = Maps.newLinkedHashMap();

  /** Map; ID <-> EvalNode */
  private BiMap<Integer, EvalNode> idToEvalMap = HashBiMap.create();

  /** Map: ID -> Names */
  private LinkedHashMap<Integer, List<String>> idToNamesMap = Maps.newLinkedHashMap();

  /** Map: ID -> Expr */
  private BiMap<Integer, Expr> idToExprBiMap = HashBiMap.create();

  /** Map; Name -> Boolean (if it is resolved or not) */
  private LinkedHashMap<Integer, Boolean> evaluationStateMap = Maps.newLinkedHashMap();

  /** Map: Alias Name <-> Original Name */
  private BiMap<String, String> aliasedColumnMap = HashBiMap.create();

  private LogicalPlan plan;

  public NamedExprsManager(LogicalPlan plan) {
    this.plan = plan;
  }

  private int getNextId() {
    return sequenceId++;
  }

  private static String normalizeName(String name) {
    return name.toLowerCase();
  }

  /**
   * Check whether the expression corresponding to a given name was resolved.
   *
   * @param name The name of a certain expression to be checked
   * @return true if resolved. Otherwise, false.
   */
  public boolean isResolved(String name) {
    String normalized = normalizeName(name);
    if (nameToIdMap.containsKey(normalized)) {
      int refId = nameToIdMap.get(normalized);
      return evaluationStateMap.containsKey(refId) && evaluationStateMap.get(refId);
    } else {
      return false;
    }
  }

  /**
   * Check whether the expression corresponding to a given name was resolved.
   *
   * @param evalNode The EvalNode of a certain expression to be checked
   * @return true if resolved. Otherwise, false.
   */
  public boolean isResolved(EvalNode evalNode) {
    if (idToEvalMap.inverse().containsKey(evalNode)) {
      int refId = idToEvalMap.inverse().get(evalNode);
      return evaluationStateMap.containsKey(refId) && evaluationStateMap.get(refId);
    } else {
      return false;
    }
  }

  public boolean contains(String name) {
    return nameToIdMap.containsKey(name);
  }

  public boolean contains(Expr expr) {
    return idToExprBiMap.inverse().containsKey(expr);
  }

  private Expr getExpr(String name) {
    return idToExprBiMap.get(nameToIdMap.get(name));
  }

  public NamedExpr getNamedExpr(String name) {
    String normalized = name.toLowerCase();
    return new NamedExpr(getExpr(name), normalized);
  }

  public boolean isAliased(String name) {
    return aliasedColumnMap.containsKey(name);
  }

  public String getAlias(String originalName) {
    return aliasedColumnMap.get(originalName);
  }

  public boolean isAliasedName(String aliasName) {
    return aliasedColumnMap.inverse().containsKey(aliasName);
  }

  public String getOriginalName(String aliasName) {
    return aliasedColumnMap.inverse().get(aliasName);
  }

  public String addExpr(Expr expr) throws PlanningException {
    if (idToExprBiMap.inverse().containsKey(expr)) {
      int refId = idToExprBiMap.inverse().get(expr);
      return idToNamesMap.get(refId).get(0);
    }

    String generatedName = plan.newGeneratedFieldName(expr);
    return addExpr(expr, generatedName);
  }

  public String addExpr(Expr expr, String specifiedName) throws PlanningException {
    String normalized = normalizeName(specifiedName);

    // if this name already exists, just returns the name.
    if (nameToIdMap.containsKey(normalized)) {
      return normalized;
    }

    // if the name is first
    int refId;
    if (idToExprBiMap.inverse().containsKey(expr)) {
      refId = idToExprBiMap.inverse().get(expr);
    } else {
      refId = getNextId();
      idToExprBiMap.put(refId, expr);
    }

    nameToIdMap.put(normalized, refId);
    TUtil.putToNestedList(idToNamesMap, refId, normalized);
    evaluationStateMap.put(refId, false);

    return normalized;
  }

  public String [] addReferences(Expr expr) throws PlanningException {
    Set<ColumnReferenceExpr> foundSet = ExprFinder.finds(expr, OpType.Column);
    String [] names = new String[foundSet.size()];
    int i = 0;
    for (ColumnReferenceExpr column : foundSet) {
      addExpr(column);
      names[i++] = column.getCanonicalName();
    }
    return names;
  }

  public String addNamedExpr(NamedExpr namedExpr) throws PlanningException {
    if (namedExpr.hasAlias()) {
      return addExpr(namedExpr.getExpr(), namedExpr.getAlias());
    } else {
      return addExpr(namedExpr.getExpr());
    }
  }

  public String [] addNamedExprArray(@Nullable Collection<NamedExpr> targets) throws PlanningException {
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
    List<NamedExpr> namedExprList = Lists.newArrayList();
    for (Map.Entry<Integer, Expr> entry: idToExprBiMap.entrySet()) {
      namedExprList.add(new NamedExpr(entry.getValue(), idToNamesMap.get(entry.getKey()).get(0)));
    }
    return namedExprList;
  }

  public void resolveExpr(String name, EvalNode evalNode) throws PlanningException {
    String normalized = name.toLowerCase();

    int refId = nameToIdMap.get(normalized);
    evaluationStateMap.put(refId, true);
    idToEvalMap.put(refId, evalNode);

    String originalName = checkAndGetIfAliasedColumn(normalized);
    if (originalName != null) {
      aliasedColumnMap.put(originalName, normalized);
    }
  }

  /**
   * It returns an original column name if it is aliased column reference.
   * Otherwise, it will return NULL.
   */
  public String checkAndGetIfAliasedColumn(String name) {
    Expr expr = getExpr(name);
    if (expr != null && expr.getType() == OpType.Column) {
      ColumnReferenceExpr column = (ColumnReferenceExpr) expr;
      if (!column.getCanonicalName().equals(name)) {
        return column.getCanonicalName();
      }
    }
    return null;
  }

  public Target getTarget(String name) {
    return getTarget(name, false);
  }

  private boolean isFirstName(int id, String name) {
    return idToNamesMap.get(id).get(0).equals(name);
  }

  private String getFirstName(int id) {
    return idToNamesMap.get(id).get(0);
  }

  public Target getTarget(String name, boolean raw) {
    String normalized = name;
    int refId = nameToIdMap.get(normalized);

    if (!raw && evaluationStateMap.containsKey(refId) && evaluationStateMap.get(refId)) {
      EvalNode evalNode = idToEvalMap.get(refId);

      if (!isFirstName(refId, name) && isResolved(normalized)) {
        return new Target(new FieldEval(getFirstName(refId),evalNode.getValueType()), name);
      }

      EvalNode referredEval;
      if (evalNode.getType() == EvalType.CONST) {
        referredEval = evalNode;
      } else {
        referredEval = new FieldEval(idToNamesMap.get(refId).get(0), evalNode.getValueType());
      }
      return new Target(referredEval, name);

    } else {
      if (idToEvalMap.containsKey(refId)) {
        return new Target(idToEvalMap.get(refId), name);
      } else {
        return null;
      }
    }
  }

  public String toString() {
    return "unresolved=" + nameToIdMap.size() + ", resolved=" + idToEvalMap.size()
        + ", renamed=" + aliasedColumnMap.size();
  }

  /**
   * It returns an iterator for unresolved NamedExprs.
   */
  public Iterator<NamedExpr> getUnresolvedExprs() {
    return new UnresolvedIterator();
  }

  public class UnresolvedIterator implements Iterator<NamedExpr> {
    private final Iterator<NamedExpr> iterator;

    public UnresolvedIterator() {
      List<NamedExpr> unresolvedList = TUtil.newList();
      for (Integer refId: idToNamesMap.keySet()) {
        String name = idToNamesMap.get(refId).get(0);
        if (!isResolved(name)) {
          Expr expr = idToExprBiMap.get(refId);
          unresolvedList.add(new NamedExpr(expr, name));
        }
      }
      if (unresolvedList.size() == 0) {
        iterator = null;
      } else {
        iterator = unresolvedList.iterator();
      }
    }

    @Override
    public boolean hasNext() {
      return iterator != null && iterator.hasNext();
    }

    @Override
    public NamedExpr next() {
      return iterator.next();
    }

    @Override
    public void remove() {
    }
  }
}
