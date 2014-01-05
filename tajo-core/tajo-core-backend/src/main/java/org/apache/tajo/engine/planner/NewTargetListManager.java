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
import com.google.common.collect.ImmutableSet;
import org.apache.tajo.algebra.ColumnReferenceExpr;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.algebra.OpType;
import org.apache.tajo.algebra.TargetExpr;
import org.apache.tajo.engine.eval.FieldEval;

import java.util.*;

/**
 * It manages a list of targets.
 */
public class NewTargetListManager {
  private LinkedHashMap<String, Target> targets = new LinkedHashMap<String, Target>();
  private LinkedHashMap<String, Boolean> resolvedFlags = new LinkedHashMap<String, Boolean>();

  private Set<TargetExpr> rawTargets = new LinkedHashSet<TargetExpr>();
  private BiMap<String, TargetExpr> nameToRawTargets = HashBiMap.create();
  private Map<TargetExpr, Target> rawsToTargets = new LinkedHashMap<TargetExpr, Target>();

  private LogicalPlan plan;

  public NewTargetListManager(LogicalPlan plan, LogicalPlanner planner, LogicalPlan.QueryBlock block) {
    this.plan = plan;
  }

  public String addExpr(Expr expr) {
    String name = plan.newNonameColumnName(expr.getType().name());
    return addRawTarget(new TargetExpr(expr, name));
  }

  public String [] addExprArray(Expr[] exprs) {
    String [] names = new String[exprs.length];
    for (int i = 0; i < exprs.length; i++) {
      names[i] = addExpr(exprs[i]);
    }
    return names;
  }

  public String addRawTarget(TargetExpr targetExpr) {
    if (nameToRawTargets.containsValue(targetExpr)) {
      return nameToRawTargets.inverse().get(targetExpr);
    } else {
      String name;
      if (targetExpr.hasAlias()) {
        name = targetExpr.getAlias();
      } else {
        if (targetExpr.getExpr().getType() == OpType.Column) {
          name = ((ColumnReferenceExpr)targetExpr.getExpr()).getCanonicalName();
        } else {
          name = plan.newNonameColumnName(targetExpr.getExpr().getType().name());
        }
      }
      targetExpr.setAlias(name);
      rawTargets.add(targetExpr);
      nameToRawTargets.put(targetExpr.getAlias(), targetExpr);
      return targetExpr.getAlias();
    }
  }

  public Target [] getTargets() {
    return targets.values().toArray(new Target[targets.size()]);
  }

  public String [] addRawTargetArray(TargetExpr[] targets) {
    String [] names = new String[targets.length];
    for (int i = 0; i < targets.length; i++) {
      names[i] = addRawTarget(targets[i]);
    }
    return names;
  }

  public Collection<TargetExpr> getRawTargets() {
    return ImmutableSet.copyOf(rawTargets);
  }


  public void switchTarget(String name, Target target) {
    switchTarget(nameToRawTargets.get(name), target);
  }

  public void switchTarget(TargetExpr rawTarget, Target target) {
    rawsToTargets.put(rawTarget, target);
    targets.put(target.getCanonicalName(), target);

    rawTargets.remove(rawTarget);
    nameToRawTargets.remove(rawTarget.getAlias());

    resolvedFlags.put(rawTarget.getAlias(), true);
  }

  public Target getTarget(String name) {
//    if (targets.containsKey(name)) {
      return targets.get(name);
//    } else {
//      if (nameToRawTargets.containsKey(name)) {
//        try {
//          EvalNode evalNode = planner.createEvalTree(plan, block, nameToRawTargets.get(name));
//          Target target = new Target(evalNode, name);
//          targets.put(name, target);
//          nameToRawTargets.remove(name);
//        } catch (PlanningException e) {
//          e.printStackTrace();
//        }
//      }
//    }
//    return null;
  }

  public Target getTarget(TargetExpr target) {
    if (resolvedFlags.containsKey(target.getAlias()) && resolvedFlags.get(target.getAlias())) {
      return new Target(new FieldEval(targets.get(target.getAlias()).getColumnSchema()), target.getAlias());
    } else {
      return rawsToTargets.get(target);
    }
  }

  public String toString() {
    return "rawTargets=" + nameToRawTargets.size() + ", targets=" + targets.size();
  }
}
