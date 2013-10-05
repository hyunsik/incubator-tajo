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

package org.apache.tajo.storage;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.statistics.ColumnStat;
import org.apache.tajo.catalog.statistics.TableStat;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.NullDatum;

/**
 * This class is not thread-safe.
 */
public class TableStatistics {
  private final static int HISTOGRAM_BASE_GRANULARITY = 100;
  private final static int HISTOGRAM_MAX_SIZE = 50000;

  private Schema schema;
  private Tuple minValues;
  private Tuple maxValues;
  private long [] numNulls;
  private long numRows = 0;
  private long numBytes = 0;

  private boolean [] comparable;

  private List<Integer> joinKeys;

  private Map<Integer, Long> histogram;

  private long tupleSize = 0;

  private Tuple keyTuple;

  private int keyTupleIdx = 0;


  public TableStatistics(Schema schema) {
    this.schema = schema;
    minValues = new VTuple(schema.getColumnNum());
    maxValues = new VTuple(schema.getColumnNum());

    numNulls = new long[schema.getColumnNum()];
    comparable = new boolean[schema.getColumnNum()];

    DataType type;
    for (int i = 0; i < schema.getColumnNum(); i++) {
      type = schema.getColumn(i).getDataType();
      if (type.getType() == Type.PROTOBUF) {
        comparable[i] = false;
      } else {
        comparable[i] = true;
      }
    }
  }

  public Schema getSchema() {
    return this.schema;
  }

  public void incrementRow() {
    numRows++;
  }

  public long getNumRows() {
    return this.numRows;
  }

  public void setNumBytes(long bytes) {
    this.numBytes = bytes;
  }

  public long getNumBytes() {
    return this.numBytes;
  }

  public void analyzeField(int idx, Datum datum) {
    if (datum instanceof NullDatum) {
      numNulls[idx]++;
      return;
    }

    if (comparable[idx]) {
      if (!maxValues.contains(idx) ||
          maxValues.get(idx).compareTo(datum) < 0) {
        maxValues.put(idx, datum);
      }
      if (!minValues.contains(idx) ||
          minValues.get(idx).compareTo(datum) > 0) {
        minValues.put(idx, datum);
      }
    }

    tupleSize += datum.size();
    if (joinKeys != null) {
      if (joinKeys.contains(idx)) {
        keyTuple.put(keyTupleIdx++, datum);
      }

      if (idx == schema.getColumnNum() - 1) {
        int key = keyTuple.hashCode() + HISTOGRAM_BASE_GRANULARITY - (keyTuple.hashCode() % HISTOGRAM_BASE_GRANULARITY);

        Long accumulated = histogram.get(key);
        if (accumulated != null) {
          histogram.put(key, accumulated + tupleSize);
        } else {
          histogram.put(key, tupleSize);
        }

        keyTupleIdx = 0;
        keyTuple.clear();
        tupleSize = 0;
      }
    }
  }

  public TableStat getTableStat() {
    TableStat stat = new TableStat();

    ColumnStat columnStat;
    for (int i = 0; i < schema.getColumnNum(); i++) {
      columnStat = new ColumnStat(schema.getColumn(i));
      columnStat.setNumNulls(numNulls[i]);
      columnStat.setMinValue(minValues.get(i));
      columnStat.setMaxValue(maxValues.get(i));
      stat.addColumnStat(columnStat);
    }

    stat.setNumRows(this.numRows);
    stat.setNumBytes(this.numBytes);

    if (histogram != null) {
      int granularity = HISTOGRAM_BASE_GRANULARITY;

      // decrease granularity of histogram if its size is above a given
      // threshold
      while (histogram.size() > HISTOGRAM_MAX_SIZE) {
        double multiplier = (double) histogram.size() / (double) HISTOGRAM_MAX_SIZE;
        granularity = (int) Math.ceil(((double) granularity * multiplier));

        Map<Integer, Long> newHistogram = new TreeMap<Integer, Long>();

        for (Integer originalKey : histogram.keySet()) {
          long value = histogram.get(originalKey);
          int key = originalKey + granularity - (originalKey % granularity);

          Long accumulated = newHistogram.get(key);
          if (accumulated != null) {
            newHistogram.put(key, accumulated + value);
          } else {
            newHistogram.put(key, value);
          }
        }
        histogram = newHistogram;
      }
      stat.setHistogram(histogram);
    }

    return stat;
  }

  public void setJoinKeys(List<Integer> joinKeys) {
    this.joinKeys = joinKeys;
    keyTuple = new VTuple(joinKeys.size());
    histogram = new TreeMap<Integer, Long>();
  }
}
