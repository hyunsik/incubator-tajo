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

import com.google.common.collect.Maps;
import org.apache.tajo.algebra.Projection;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.engine.eval.EvalNode;
import org.apache.tajo.engine.eval.EvalTreeUtil;
import org.apache.tajo.engine.eval.EvalType;
import org.apache.tajo.engine.eval.FieldEval;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * It manages a list of targets.
 */
public class TargetListManager {
  private LogicalPlan plan;
  private boolean [] resolvedFlags;
  private Projection projection;
  private Target[] targets;
  private Target[] unresolvedTargets;
  private Map<Column, Integer> targetColumnToId = Maps.newHashMap();

  public TargetListManager(LogicalPlan plan, Projection projection) {
    this.plan = plan;
    int targetNum = projection.size();
    if (projection.size() == 0) {
      resolvedFlags = new boolean[0];
    } else {
      resolvedFlags = new boolean[targetNum];
    }
    this.targets = new Target[targetNum];
    this.unresolvedTargets = new Target[targetNum];
  }

  public TargetListManager(LogicalPlan plan, Target[] unresolvedTargets) {
    this.plan = plan;

    this.targets = new Target[unresolvedTargets.length];
    this.unresolvedTargets = new Target[unresolvedTargets.length];
    for (int i = 0; i < unresolvedTargets.length; i++) {
      try {
        this.targets[i] = (Target) unresolvedTargets[i].clone();
        this.unresolvedTargets[i] = (Target) unresolvedTargets[i].clone();
      } catch (CloneNotSupportedException e) {
        e.printStackTrace();
      }
    }
    resolvedFlags = new boolean[unresolvedTargets.length];
  }

  public TargetListManager(LogicalPlan plan, String blockName) {
    //this(plan, plan.getBlock(blockName).getTargetListManager().getUnresolvedTargets());
  }

  public Target getTarget(int id) {
    return targets[id];
  }

  public Target[] getTargets() {
    return this.targets;
  }

  public Target[] getUnresolvedTargets() {
    return this.unresolvedTargets;
  }

  public void fill(int id, Target target) {
    this.targets[id] = target;
    this.unresolvedTargets[id] = target;

    EvalNode evalNode = target.getEvalTree();
    if (evalNode.getType() == EvalType.FIELD) {
      if (target.hasAlias()) {
        FieldEval fieldEval = (FieldEval) evalNode;
        targetColumnToId.put(fieldEval.getColumnRef(), id);
      }
    }
    targetColumnToId.put(target.getColumnSchema(), id);
  }

  public int size() {
    return targets.length;
  }

  public void resolve(int id) {
    resolvedFlags[id] = true;
  }

  public void resolveAll() {
    for (int i = 0; i < resolvedFlags.length; i++) {
      resolvedFlags[i] = true;
    }
  }

  public boolean isResolved(int id) {
    return resolvedFlags[id];
  }

  /**
   * It checks whether only column reference is resolve or not.
   * Note that this method doesn't know if an expression is resolved.
   *
   * @param targetColumn A column to be checked
   * @return True if a column reference belong to a target list and is already resolved. Otherwise, false.
   */
  public boolean isResolve(Column targetColumn) throws PlanningException {
    Integer targetId = targetColumnToId.get(targetColumn);
    if (targetId == null) {
      return false;
    }
    return resolvedFlags[targetId];
  }

  public Column getResolvedColumn(Column targetColumn) throws PlanningException {
    Integer targetId = targetColumnToId.get(targetColumn);
    if (targetId == null) {
      throw new PlanningException("Unknown target column: " + targetColumn);
    }
    return getResolvedTargetToColumn(targetId);
  }

  public Target [] getUpdatedTarget(Set<Integer> exclude) throws PlanningException {
    Target [] updated = new Target[targets.length];

    for (int i = 0; i < targets.length; i++) {
      if (targets[i] == null) { // if it is not created
        continue;
      }

      if (!exclude.contains(i) && resolvedFlags[i]) { // if this target was evaluated, it becomes a column target.
        Column col = getResolvedTargetToColumn(i);
        updated[i] = new Target(new FieldEval(col));
      } else {
        try {
          updated[i] = (Target) targets[i].clone();
        } catch (CloneNotSupportedException e) {
          throw new PlanningException(e);
        }
      }
    }
    return updated;
  }

  public Target [] getUpdatedTarget() throws PlanningException {
    Target [] updated = new Target[targets.length];

    for (int i = 0; i < targets.length; i++) {
      if (targets[i] == null) { // if it is not created
        continue;
      }

      if (resolvedFlags[i]) { // if this target was evaluated, it becomes a column target.
        Column col = getResolvedTargetToColumn(i);
        updated[i] = new Target(new FieldEval(col));
      } else {
        try {
          updated[i] = (Target) targets[i].clone();
        } catch (CloneNotSupportedException e) {
          throw new PlanningException(e);
        }
      }
    }
    targets = updated;
    return updated;
  }

  public Schema getUpdatedSchema() {
    Schema schema = new Schema();
    for (int i = 0; i < resolvedFlags.length; i++) {
      if (resolvedFlags[i]) {
        Column col = getResolvedTargetToColumn(i);
        if (!schema.containsByQualifiedName(col.getQualifiedName()))
        schema.addColumn(col);
      } else {
        Collection<Column> cols = getColumnRefs(i);
        for (Column col : cols) {
          if (!schema.containsByQualifiedName(col.getQualifiedName())) {
            schema.addColumn(col);
          }
        }
      }
    }
    return schema;
  }

  public Collection<Column> getColumnRefs(int id) {
    return EvalTreeUtil.findDistinctRefColumns(targets[id].getEvalTree());
  }

  public Column getResolvedTargetToColumn(int id) {
    Target t = targets[id];
    String name;
    if (t.hasAlias() || t.getEvalTree().getType() == EvalType.FIELD) {
      name = t.getCanonicalName();
    } else { // if alias name is not given or target is an expression
      t.setAlias(plan.newNonameColumnName(t.getEvalTree().getName()));
      name = t.getCanonicalName();
    }
    return new Column(name, t.getEvalTree().getValueType());
  }

  public boolean isAllResolved() {
    for (boolean resolved : resolvedFlags) {
      if (!resolved) {
        return false;
      }
    }

    return true;
  }

}
