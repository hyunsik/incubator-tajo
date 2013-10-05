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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.tajo.TaskAttemptContext;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.engine.planner.logical.StoreTableNode;
import org.apache.tajo.storage.*;

import java.io.IOException;

/**
 * This physical operator stores a relation into a table.
 */
public class StoreTableExec extends UnaryPhysicalExec {
  private final StoreTableNode plan;
  private Appender appender;
  private Tuple tuple;
  
  /**
   * @throws java.io.IOException
   *
   */
  public StoreTableExec(TaskAttemptContext context, StoreTableNode plan, PhysicalExec child) throws IOException {
    super(context, plan.getInSchema(), plan.getOutSchema(), child);
    this.plan = plan;
  }

  public void init() throws IOException {
    super.init();

    TableMeta meta;
    if (plan.hasOptions()) {
      meta = CatalogUtil.newTableMeta(outSchema, plan.getStorageType(), plan.getOptions());
    } else {
      meta = CatalogUtil.newTableMeta(outSchema, plan.getStorageType());
    }

    if (context.isInterQuery()) {
      Path storeTablePath = new Path(context.getWorkDir(), "out");
      FileSystem fs = new RawLocalFileSystem();
      fs.mkdirs(storeTablePath);
      appender = StorageManagerFactory.getStorageManager(context.getConf()).getAppender(meta,
          StorageUtil.concatPath(storeTablePath, "0"));
    } else {
      appender = StorageManagerFactory.getStorageManager(context.getConf()).getAppender(meta, context.getOutputPath());
    }
    appender.enableStats();
    appender.setJoinKeys(context.getJoinKeys());
    appender.init();
  }

  /* (non-Javadoc)
   * @see PhysicalExec#next()
   */
  @Override
  public Tuple next() throws IOException {
    while((tuple = child.next()) != null) {
      appender.addTuple(tuple);
    }
        
    return null;
  }

  @Override
  public void rescan() throws IOException {
    // nothing to do
  }

  public void close() throws IOException {
    super.close();

    appender.flush();
    appender.close();

    // Collect statistics data
//    ctx.addStatSet(annotation.getType().toString(), appender.getStats());
    context.setResultStats(appender.getStats());
    context.addRepartition(0, context.getTaskId().toString());
  }
}
