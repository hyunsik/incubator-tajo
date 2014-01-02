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
import org.apache.tajo.algebra.TargetExpr;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.engine.eval.EvalNode;
import org.apache.tajo.engine.eval.EvalTreeUtil;
import org.apache.tajo.engine.eval.EvalType;
import org.apache.tajo.engine.eval.FieldEval;

import java.util.*;

/**
 * It manages a list of targets.
 */
public class NewTargetListManager {
  private LinkedHashMap<String, Target> targets = new LinkedHashMap<String, Target>();
  private LinkedHashMap<String, Boolean> resolvedFlags = new LinkedHashMap<String, Boolean>();
  private Set<TargetExpr> targetExprs = new HashSet<TargetExpr>();

  public NewTargetListManager() {
  }

  public void addRawTargets(TargetExpr[] targets) {
    for (TargetExpr expr : targets) {
      targetExprs.add(expr);
    }
  }

  public Set<TargetExpr> getRawTargets() {
    return targetExprs;
  }

  public void addTarget(Target target) {
    targets.put(target.getCanonicalName(), target);
  }
}
