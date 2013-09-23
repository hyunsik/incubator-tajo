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

package org.apache.tajo;

import com.google.common.base.Preconditions;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.master.ExecutionBlock;

import static org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import static org.apache.tajo.ipc.TajoWorkerProtocol.*;

public class DataChannel {
  private ExecutionBlockId srcId;
  private ExecutionBlockId targetId;
  private TransmitType transmitType = TransmitType.PULL_TRANSMIT;
  private PartitionType partitionType;
  private Integer partitionNum = 1;
  private Column[] key;

  private Schema schema;

  private StoreType storeType = StoreType.CSV;

  public DataChannel(ExecutionBlockId srcId, ExecutionBlockId targetId) {
    this.srcId = srcId;
    this.targetId = targetId;
  }

  public DataChannel(ExecutionBlockId srcId, ExecutionBlockId targetId, PartitionType partitionType) {
    this(srcId, targetId);
    this.partitionType = partitionType;
  }

  public DataChannel(ExecutionBlock src, ExecutionBlock target, PartitionType partitionType, int partNum) {
    this(src.getId(), target.getId(), partitionType, partNum);
    setSchema(src.getPlan().getOutSchema());
  }

  public DataChannel(ExecutionBlockId srcId, ExecutionBlockId targetId, PartitionType partitionType, int partNum) {
    this(srcId, targetId, partitionType);
    this.partitionNum = partNum;
  }

  public DataChannel(DataChannelProto proto) {
    this.srcId = new ExecutionBlockId(proto.getSrcId());
    this.targetId = new ExecutionBlockId(proto.getTargetId());
    this.transmitType = proto.getTransmitType();
    this.partitionType = proto.getPartitionType();
    if (proto.hasSchema()) {
      this.setSchema(new Schema(proto.getSchema()));
    }
    if (proto.getPartitionKeyCount() > 0) {
      key = new Column[proto.getPartitionKeyCount()];
      for (int i = 0; i < proto.getPartitionKeyCount(); i++) {
        key[i] = new Column(proto.getPartitionKey(i));
      }
    } else {
      key = new Column[] {};
    }
    if (proto.hasPartitionNum()) {
      this.partitionNum = proto.getPartitionNum();
    }
  }

  public ExecutionBlockId getSrcId() {
    return srcId;
  }

  public ExecutionBlockId getTargetId() {
    return targetId;
  }

  public PartitionType getPartitionType() {
    return partitionType;
  }

  public TransmitType getTransmitType() {
    return this.transmitType;
  }

  public void setTransmitType(TransmitType transmitType) {
    this.transmitType = transmitType;
  }

  public void setPartition(PartitionType partitionType, Column [] keys, int numPartitions) {
    Preconditions.checkArgument(keys.length >= 0, "At least one partition key must be specified.");
    Preconditions.checkArgument(numPartitions > 0, "The number of partitions must be positive: %s", numPartitions);

    this.partitionType = partitionType;
    this.key = keys;
    this.partitionNum = numPartitions;
  }

  public void setPartitionType(PartitionType partitionType) {
    this.partitionType = partitionType;
  }

  public boolean hasPartitionKey() {
    return key != null;
  }

  public void setPartitionKey(Column [] key) {
    this.key = key;
  }

  public Column [] getPartitionKey() {
    return this.key;
  }

  public void setPartitionNum(int partNum) {
    this.partitionNum = partNum;
  }

  public int getPartitionNum() {
    return partitionNum;
  }

  public boolean hasStoreType() {
    return this.storeType != null;
  }

  public void setStoreType(StoreType storeType) {
    this.storeType = storeType;
  }

  public StoreType getStoreType() {
    return storeType;
  }

  public DataChannelProto getProto() {
    DataChannelProto.Builder builder = DataChannelProto.newBuilder();
    builder.setSrcId(srcId.getProto());
    builder.setTargetId(targetId.getProto());
    if (transmitType != null) {
      builder.setTransmitType(transmitType);
    }
    builder.setPartitionType(partitionType);
    if (schema != null) {
      builder.setSchema(schema.getProto());
    }
    if (key != null) {
      for (Column column : key) {
        builder.addPartitionKey(column.getProto());
      }
    }
    if (partitionNum != null) {
      builder.setPartitionNum(partitionNum);
    }
    return builder.build();
  }

  public void setSchema(Schema schema) {
    this.schema = (Schema) schema.clone();
  }

  public Schema getSchema() {
    return schema;
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("[").append(srcId.getQueryId()).append("] ");
    sb.append(srcId.getId()).append(" => ").append(targetId.getId());
    sb.append(" (type=").append(partitionType);
    if (hasPartitionKey()) {
      sb.append(", key=");
      boolean first = true;
      for (Column column : getPartitionKey()) {
        if (first) {
          first = false;
        } else {
          sb.append(",");
        }
        sb.append(column.getColumnName());
      }
      sb.append(", num=").append(partitionNum);
    }
    sb.append(")");
    return sb.toString();
  }
}
