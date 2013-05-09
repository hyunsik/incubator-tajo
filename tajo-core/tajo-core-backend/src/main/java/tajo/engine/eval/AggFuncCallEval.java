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

package tajo.engine.eval;

import com.google.gson.annotations.Expose;
import tajo.catalog.FunctionDesc;
import tajo.catalog.Schema;
import tajo.catalog.function.AggFunction;
import tajo.catalog.function.FunctionContext;
import tajo.common.TajoDataTypes.DataType;
import tajo.datum.Datum;
import tajo.engine.json.GsonCreator;
import tajo.storage.Tuple;
import tajo.storage.VTuple;

public class AggFuncCallEval extends FuncEval implements Cloneable {
  @Expose protected AggFunction instance;
  @Expose boolean firstPhase = false;
  private Tuple params;

  public AggFuncCallEval(FunctionDesc desc, AggFunction instance, EvalNode[] givenArgs) {
    super(Type.AGG_FUNCTION, desc, givenArgs);
    this.instance = instance;
  }

  @Override
  public EvalContext newContext() {
    AggFunctionCtx newCtx = new AggFunctionCtx(argEvals, instance.newContext());

    return newCtx;
  }

  @Override
  public void eval(EvalContext ctx, Schema schema, Tuple tuple) {
    AggFunctionCtx localCtx = (AggFunctionCtx) ctx;
    if (params == null) {
      this.params = new VTuple(argEvals.length);
    }

    if (argEvals != null) {
      params.clear();

      for (int i = 0; i < argEvals.length; i++) {
        argEvals[i].eval(localCtx.argCtxs[i], schema, tuple);
        params.put(i, argEvals[i].terminate(localCtx.argCtxs[i]));
      }
    }

    if (firstPhase) {
      instance.eval(localCtx.funcCtx, params);
    } else {
      instance.merge(localCtx.funcCtx, params);
    }
  }

  @Override
  public Datum terminate(EvalContext ctx) {
    if (firstPhase) {
      return instance.getPartialResult(((AggFunctionCtx)ctx).funcCtx);
    } else {
      return instance.terminate(((AggFunctionCtx)ctx).funcCtx);
    }
  }

  @Override
  public DataType[] getValueType() {
    if (firstPhase) {
      return instance.getPartialResultType();
    } else {
      return funcDesc.getReturnType();
    }
  }

  public String toJSON() {
	  return GsonCreator.getInstance().toJson(this, EvalNode.class);
  }

  public Object clone() throws CloneNotSupportedException {
    AggFuncCallEval agg = (AggFuncCallEval) super.clone();
    return agg;
  }

  public void setFirstPhase() {
    this.firstPhase = true;
  }

  protected class AggFunctionCtx extends FuncCallCtx {
    FunctionContext funcCtx;

    AggFunctionCtx(EvalNode [] argEvals, FunctionContext funcCtx) {
      super(argEvals);
      this.funcCtx = funcCtx;
    }
  }
}
