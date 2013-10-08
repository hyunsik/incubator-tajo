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

package org.apache.tajo.engine.function.datetime;

import org.apache.tajo.catalog.Column;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.engine.eval.FunctionEval;
import org.apache.tajo.engine.function.GeneralFunction;
import org.apache.tajo.storage.Tuple;

import java.text.SimpleDateFormat;

import static org.apache.tajo.common.TajoDataTypes.Type.INT8;
import static org.apache.tajo.common.TajoDataTypes.Type.TEXT;

public class ToTimestamp extends GeneralFunction {
  private final static String DEFAULT_FORMAT = "yyyy-MM-dd HH:mm:ss";
  private SimpleDateFormat dateFormat;
  private boolean hasDateFormat = false;

  public ToTimestamp() {
    super(new Column[] {new Column("epoch", INT8), new Column("format", TEXT)});
  }

  public void init(FunctionEval.ParamType[] paramTypes) {
    if (paramTypes.length == 2) {
      hasDateFormat = true;
    }
  }

  @Override
  public Datum eval(Tuple params) {
    if (dateFormat == null) {
      if (hasDateFormat) {
        dateFormat = new SimpleDateFormat(params.get(1).asChars());
      } else {
        dateFormat = new SimpleDateFormat(DEFAULT_FORMAT);
      }
    }

    Datum datum = params.get(0);

    if(datum instanceof NullDatum) {
      return NullDatum.get();
    }
    return DatumFactory.createText(dateFormat.format(new java.util.Date(datum.asInt8())));
  }
}
