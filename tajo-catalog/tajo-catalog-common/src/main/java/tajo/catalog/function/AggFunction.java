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

package tajo.catalog.function;

import com.google.gson.Gson;
import tajo.catalog.Column;
import tajo.catalog.json.GsonCreator;
import tajo.common.TajoDataTypes.DataType;
import tajo.datum.Datum;
import tajo.storage.Tuple;

public abstract class AggFunction<T extends Datum> extends Function<T> {

  public AggFunction(Column[] definedArgs) {
    super(definedArgs);
  }

  public abstract FunctionContext newContext();

  public abstract void eval(FunctionContext ctx, Tuple params);

  public void merge(FunctionContext ctx, Tuple part) {
    eval(ctx, part);
  }

  public abstract Datum getPartialResult(FunctionContext ctx);

  public abstract DataType [] getPartialResultType();

  public abstract T terminate(FunctionContext ctx);

  public String toJSON() {
    Gson gson = GsonCreator.getInstance();
    return gson.toJson(this, AggFunction.class);
  }
}
