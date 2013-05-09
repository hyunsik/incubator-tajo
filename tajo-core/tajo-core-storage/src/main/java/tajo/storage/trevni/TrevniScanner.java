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

package tajo.storage.trevni;

import org.apache.hadoop.conf.Configuration;
import org.apache.trevni.ColumnFileReader;
import org.apache.trevni.ColumnValues;
import org.apache.trevni.avro.HadoopInput;
import tajo.catalog.Column;
import tajo.catalog.TableMeta;
import tajo.datum.BlobDatum;
import tajo.datum.DatumFactory;
import tajo.storage.FileScanner;
import tajo.storage.Fragment;
import tajo.storage.Tuple;
import tajo.storage.VTuple;

import java.io.IOException;
import java.nio.ByteBuffer;

public class TrevniScanner extends FileScanner {
  private boolean inited = false;
  private ColumnFileReader reader;
  private int [] projectionMap;
  private ColumnValues [] columns;

  public TrevniScanner(Configuration conf, TableMeta meta, Fragment fragment) throws IOException {
    super(conf, meta, fragment);
    reader = new ColumnFileReader(new HadoopInput(fragment.getPath(), conf));
  }

  @Override
  public void init() throws IOException {
    if (targets == null) {
      targets = schema.toArray();
    }

    prepareProjection(targets);

    columns = new ColumnValues[projectionMap.length];

    for (int i = 0; i < projectionMap.length; i++) {
      columns[i] = reader.getValues(projectionMap[i]);
    }

    super.init();
  }

  private void prepareProjection(Column [] targets) {
    projectionMap = new int[targets.length];
    int tid;
    for (int i = 0; i < targets.length; i++) {
      tid = schema.getColumnIdByName(targets[i].getColumnName());
      projectionMap[i] = tid;
    }
  }

  @Override
  public Tuple next() throws IOException {
    Tuple tuple = new VTuple(schema.getColumnNum());

    if (!columns[0].hasNext()) {
      return null;
    }

    int tid; // column id of the original input schema
    for (int i = 0; i < projectionMap.length; i++) {
      tid = projectionMap[i];
      columns[i].startRow();
      switch (schema.getColumn(tid).getDataType().getType()) {
        case BOOLEAN:
          tuple.put(tid,
              DatumFactory.createBool(((Integer)columns[i].nextValue()).byteValue()));
          break;
        case BIT:
          tuple.put(tid,
              DatumFactory.createBit(((Integer) columns[i].nextValue()).byteValue()));
          break;
        case CHAR:
          tuple.put(tid,
              DatumFactory.createChar(((Integer)columns[i].nextValue()).byteValue()));
          break;

        case INT2:
          tuple.put(tid,
              DatumFactory.createInt2(((Integer) columns[i].nextValue()).shortValue()));
          break;
        case INT4:
          tuple.put(tid,
              DatumFactory.createInt4((Integer) columns[i].nextValue()));
          break;

        case INT8:
          tuple.put(tid,
              DatumFactory.createInt8((Long) columns[i].nextValue()));
          break;

        case FLOAT4:
          tuple.put(tid,
              DatumFactory.createFloat4((Float) columns[i].nextValue()));
          break;

        case FLOAT8:
          tuple.put(tid,
              DatumFactory.createFloat8((Double) columns[i].nextValue()));
          break;

        case INET4:
          tuple.put(tid,
              DatumFactory.createInet4(((ByteBuffer) columns[i].nextValue()).array()));
          break;

//        case TEXT:
//          tuple.put(tid,
//              DatumFactory.createText((String) columns[i].nextValue()));
//          break;

        case TEXT:
          tuple.put(tid,
              DatumFactory.createText((String) columns[i].nextValue()));
          break;

        case BLOB:
          tuple.put(tid,
              new BlobDatum(((ByteBuffer) columns[i].nextValue())));
          break;

        default:
          throw new IOException("Unsupport data type");
      }
    }

    return tuple;
  }

  @Override
  public void reset() throws IOException {
    for (int i = 0; i < projectionMap.length; i++) {
      columns[i] = reader.getValues(projectionMap[i]);
    }
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }

  @Override
  public boolean isProjectable() {
    return true;
  }


  @Override
  public boolean isSelectable() {
    return false;
  }
}
