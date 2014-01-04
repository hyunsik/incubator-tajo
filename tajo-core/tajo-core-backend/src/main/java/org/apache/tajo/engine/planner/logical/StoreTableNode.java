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

package org.apache.tajo.engine.planner.logical;

import com.google.gson.annotations.Expose;
import org.apache.tajo.catalog.Options;
import org.apache.tajo.catalog.partition.PartitionDesc;
import org.apache.tajo.engine.planner.PlanString;
import org.apache.tajo.util.TUtil;

import static org.apache.tajo.catalog.proto.CatalogProtos.StoreType;

public class StoreTableNode extends PersistentStoreNode implements Cloneable {
  @Expose private boolean isCreatedTable = false;
  @Expose private boolean isOverwritten = false;
  @Expose private PartitionDesc partitionDesc;

  public StoreTableNode(int pid, String tableName) {
    super(pid, tableName);
  }

  public StoreTableNode(int pid, String tableName, PartitionDesc partitionDesc) {
    super(pid, tableName);
    this.partitionDesc = partitionDesc;
  }

  public void setStorageType(StoreType storageType) {
    this.storageType = storageType;
  }

  public StoreType getStorageType() {
    return this.storageType;
  }

  public boolean hasOptions() {
    return this.options != null;
  }

  public void setOptions(Options options) {
    this.options = options;
  }

  public Options getOptions() {
    return this.options;
  }

  public PartitionDesc getPartitions() {
    return partitionDesc;
  }

  public void setPartitions(PartitionDesc partitionDesc) {
    this.partitionDesc = partitionDesc;
  }

  @Override
  public PlanString getPlanString() {
    PlanString planStr = new PlanString("Store");
    planStr.appendTitle(" into ").appendTitle(tableName);
    planStr.addExplan("Store type: " + storageType);

    return planStr;
  }

  public boolean isCreatedTable() {
    return isCreatedTable;
  }

  public void setCreateTable() {
    isCreatedTable = true;
  }
  
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof StoreTableNode) {
      StoreTableNode other = (StoreTableNode) obj;
      boolean eq = super.equals(other);
      eq = eq && isCreatedTable == other.isCreatedTable;
      eq = eq && isOverwritten == other.isOverwritten;
      eq = eq && TUtil.checkEquals(partitionDesc, other.partitionDesc);
      return eq;
    } else {
      return false;
    }
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    StoreTableNode store = (StoreTableNode) super.clone();
    store.isCreatedTable = isCreatedTable;
    store.isOverwritten = isOverwritten;
    store.partitionDesc = partitionDesc;
    return store;
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("\"Store\": {\"table\": \""+tableName);
    if (storageType != null) {
      sb.append(", storage: "+ storageType.name());
    }
    
    sb.append("\n  \"out schema\": ").append(getOutSchema()).append(",")
    .append("\n  \"in schema\": ").append(getInSchema());

    if(partitionDesc != null) {
      sb.append(partitionDesc.toString());
    }

    sb.append("}");
    
    return sb.toString() + "\n"
        + getChild().toString();
  }
}