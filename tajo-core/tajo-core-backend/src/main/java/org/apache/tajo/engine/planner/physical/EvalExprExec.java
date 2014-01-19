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

package org.apache.tajo.engine.planner.physical;

import org.apache.tajo.worker.TaskAttemptContext;
import org.apache.tajo.engine.eval.EvalContext;
import org.apache.tajo.engine.planner.Target;
import org.apache.tajo.engine.planner.logical.EvalExprNode;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;

import java.io.IOException;

public class EvalExprExec extends PhysicalExec {
  private final EvalExprNode plan;
  private final EvalContext[] evalContexts;

  public EvalExprExec(final TaskAttemptContext context, final EvalExprNode plan) {
    super(context, plan.getInSchema(), plan.getOutSchema());
    this.plan = plan;

    evalContexts = new EvalContext[plan.getTargets().length];
    for (int i = 0; i < plan.getTargets().length; i++) {
      evalContexts[i] = plan.getTargets()[i].getEvalTree().newContext();
    }
  }

  @Override
  public void init() throws IOException {
  }

  /* (non-Javadoc)
  * @see PhysicalExec#next()
  */
  @Override
  public Tuple next() throws IOException {    
    Target [] targets = plan.getTargets();
    Tuple t = new VTuple(targets.length);
    for (int i = 0; i < targets.length; i++) {
      targets[i].getEvalTree().eval(evalContexts[i], inSchema, null);
      t.put(i, targets[i].getEvalTree().terminate(evalContexts[i]));
    }
    return t;
  }

  @Override
  public void rescan() throws IOException {    
  }

  @Override
  public void close() throws IOException {
  }
}
