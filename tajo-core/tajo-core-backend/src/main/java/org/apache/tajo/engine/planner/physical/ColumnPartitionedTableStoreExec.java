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

/**
 *
 */
package org.apache.tajo.engine.planner.physical;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.partition.PartitionMethodDesc;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.statistics.StatisticsUtil;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.engine.planner.logical.StoreTableNode;
import org.apache.tajo.engine.planner.PlannerUtil;
import org.apache.tajo.storage.*;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;
import java.util.*;


/**
 * This class is a physical operator to store at column partitioned table.
 */
public class ColumnPartitionedTableStoreExec extends UnaryPhysicalExec {
  private static Log LOG = LogFactory.getLog(ColumnPartitionedTableStoreExec.class);

  private final TableMeta meta;
  private final StoreTableNode plan;
  private Tuple tuple;
  private Path storeTablePath;
  private final Map<String, Appender> appenderMap = new HashMap<String, Appender>();
  private List<Column> columns;
  private List<Integer> columnIndices;
  private List<Integer> partitionColumnIndices;

  public ColumnPartitionedTableStoreExec(TaskAttemptContext context, StoreTableNode plan, PhysicalExec child)
      throws IOException {
    super(context, plan.getInSchema(), plan.getOutSchema(), child);
    this.plan = plan;

    // set table meta
    if (this.plan.hasOptions()) {
      meta = CatalogUtil.newTableMeta(plan.getStorageType(), plan.getOptions());
    } else {
      meta = CatalogUtil.newTableMeta(plan.getStorageType());
    }

    // Rewrite a output schema because we don't have to store field values
    // corresponding to partition key columns.
    if (plan.getPartitionMethod() != null &&
        plan.getPartitionMethod().getPartitionType() == CatalogProtos.PartitionType.COLUMN) {
      rewriteColumnPartitionedTableSchema();
    }

    // Find column index to name subpartition directory path
    if (this.plan.getPartitionMethod() != null) {
      Schema partSchema = plan.getPartitionMethod().getExpressionSchema();
      if (partSchema.getColumns() != null) {
        partitionColumnIndices = new ArrayList<Integer>();
        columnIndices = new ArrayList<Integer>();
        Set<String> partColSet = new HashSet<String>();
        for (Column partCol : partSchema.getColumns()) {
          partColSet.add(partCol.getColumnName());
        }

        columns = plan.getInSchema().getColumns();
        for (int j = 0; j < columns.size(); j++) {
          String inputColumnName = columns.get(j).getColumnName();
          if(partColSet.contains(inputColumnName)) {
            partitionColumnIndices.add(j);
          } else {
            columnIndices.add(j);
          }
        }
      }
    }
  }

  /**
   * This method rewrites an input schema of column-partitioned table because
   * there are no actual field values in data file in a column-partitioned table.
   * So, this method removes partition key columns from the input schema.
   */
  private void rewriteColumnPartitionedTableSchema() {
    PartitionMethodDesc partitionDesc = plan.getPartitionMethod();
    Schema columnPartitionSchema = (Schema) partitionDesc.getExpressionSchema().clone();
    columnPartitionSchema.setQualifier(plan.getTableName());
    String qualifier = plan.getTableName();
    outSchema = PlannerUtil.rewriteColumnPartitionedTableSchema(
                                             partitionDesc,
                                             columnPartitionSchema,
                                             outSchema,
                                             qualifier);
  }

  public void init() throws IOException {
    super.init();

    storeTablePath = context.getOutputPath();
    FileSystem fs = storeTablePath.getFileSystem(context.getConf());
    if (!fs.exists(storeTablePath.getParent())) {
      fs.mkdirs(storeTablePath.getParent());
    }
  }

  private Appender getAppender(String partition) throws IOException {
    Appender appender = appenderMap.get(partition);

    if (appender == null) {
      Path dataFile = getDataFile(partition);
      FileSystem fs = dataFile.getFileSystem(context.getConf());

      if (fs.exists(dataFile.getParent())) {
        LOG.info("Path " + dataFile.getParent() + " already exists!");
      } else {
        fs.mkdirs(dataFile.getParent());
        LOG.info("Add subpartition path directory :" + dataFile.getParent());
      }

      if (fs.exists(dataFile)) {
        LOG.info("File " + dataFile + " already exists!");
        FileStatus status = fs.getFileStatus(dataFile);
        LOG.info("File size: " + status.getLen());
      }

      appender = StorageManagerFactory.getStorageManager(context.getConf()).getAppender(meta, outSchema, dataFile);
      appender.enableStats();
      appender.init();
      appenderMap.put(partition, appender);
    } else {
      appender = appenderMap.get(partition);
    }
    return appender;
  }

  private Path getDataFile(String partition) {
    return StorageUtil.concatPath(storeTablePath.getParent(), partition, storeTablePath.getName());
  }

  /* (non-Javadoc)
   * @see PhysicalExec#next()
   */
  @Override
  public Tuple next() throws IOException {
    StringBuilder sb = new StringBuilder();
    while((tuple = child.next()) != null) {
      // set subpartition directory name
      sb.delete(0, sb.length());
      for (int partIndex : partitionColumnIndices) {
        Datum datum = tuple.get(partIndex);
        sb.append(columns.get(partIndex).getColumnName()).append("=");
        sb.append(datum.asChars()).append('/');
      }
      sb.deleteCharAt(sb.length() - 1); // remove the last '/'

      // add tuple
      Appender appender = getAppender(sb.toString());
      Tuple newTuple = new VTuple(columnIndices.size());
      for(int i = 0; i < columnIndices.size(); i++) {
        int index = columnIndices.get(i);
        newTuple.put(i, tuple.get(index));
      }
      appender.addTuple(newTuple);
    }

    List<TableStats> statSet = new ArrayList<TableStats>();
    for (Map.Entry<String, Appender> entry : appenderMap.entrySet()) {
      Appender app = entry.getValue();
      app.flush();
      app.close();
      statSet.add(app.getStats());
    }

    // Collect and aggregated statistics data
    TableStats aggregated = StatisticsUtil.aggregateTableStat(statSet);
    context.setResultStats(aggregated);

    return null;
  }

  @Override
  public void rescan() throws IOException {
    // nothing to do
  }

}
