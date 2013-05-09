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

package tajo.engine.function.builtin;

import tajo.catalog.CatalogUtil;
import tajo.catalog.Column;
import tajo.catalog.function.AggFunction;
import tajo.catalog.function.FunctionContext;
import tajo.common.TajoDataTypes.DataType;
import tajo.common.TajoDataTypes.Type;
import tajo.datum.ArrayDatum;
import tajo.datum.Datum;
import tajo.datum.DatumFactory;
import tajo.storage.Tuple;

public class AvgDouble extends AggFunction {
  public AvgDouble() {
    super(new Column[] {
        new Column("val", Type.FLOAT8)
    });
  }

  public AvgContext newContext() {
    return new AvgContext();
  }

  public void init() {
  }

  @Override
  public void eval(FunctionContext ctx, Tuple params) {
    AvgContext avgCtx = (AvgContext) ctx;
    avgCtx.sum += params.get(0).asFloat8();
    avgCtx.count++;
  }

  @Override
  public void merge(FunctionContext ctx, Tuple part) {
    AvgContext avgCtx = (AvgContext) ctx;
    ArrayDatum array = (ArrayDatum) part.get(0);
    avgCtx.sum += array.get(0).asFloat8();
    avgCtx.count += array.get(1).asInt8();
  }

  @Override
  public Datum getPartialResult(FunctionContext ctx) {
    AvgContext avgCtx = (AvgContext) ctx;
    ArrayDatum part = new ArrayDatum(2);
    part.put(0, DatumFactory.createFloat8(avgCtx.sum));
    part.put(1, DatumFactory.createInt8(avgCtx.count));

    return part;
  }

  @Override
  public DataType[] getPartialResultType() {
    return CatalogUtil.newDataTypesWithoutLen(Type.FLOAT8, Type.INT8);
  }

  @Override
  public Datum terminate(FunctionContext ctx) {
    AvgContext avgCtx = (AvgContext) ctx;
    return DatumFactory.createFloat8(avgCtx.sum / avgCtx.count);
  }

  private class AvgContext implements FunctionContext {
    double sum;
    long count;
  }
}
