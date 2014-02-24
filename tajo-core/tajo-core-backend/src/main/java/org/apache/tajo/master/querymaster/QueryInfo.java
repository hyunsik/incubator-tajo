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

package org.apache.tajo.master.querymaster;


import org.apache.tajo.QueryId;
import org.apache.tajo.TajoProtos;
import org.apache.tajo.master.rm.Worker;
import org.apache.tajo.master.rm.WorkerResource;

public class QueryInfo {
  private QueryId queryId;
  private String sql;
  private TajoProtos.QueryState queryState;
  private float progress;
  private long startTime;
  private long finishTime;
  private String lastMessage;
  private Worker queryMaster;

  public QueryInfo(QueryId queryId) {
    this(queryId, null);
  }

  public QueryInfo(QueryId queryId, String sql) {
    this.queryId = queryId;
    this.sql = sql;
    this.queryState = TajoProtos.QueryState.QUERY_MASTER_INIT;
  }

  public QueryId getQueryId() {
    return queryId;
  }

  public String getSql() {
    return sql;
  }

  public String getQueryMasterHost() {
    if(queryMaster == null) {
      return null;
    }
    return queryMaster.getAllocatedHost();
  }

  public void setQueryMasterResource(Worker worker) {
    this.queryMaster = worker;
  }

  public int getQueryMasterPort() {
    if(queryMaster == null) {
      return 0;
    }
    return queryMaster.getQueryMasterPort();
  }

  public int getQueryMasterClientPort() {
    if(queryMaster == null) {
      return 0;
    }
    return queryMaster.getClientPort();
  }

  public TajoProtos.QueryState getQueryState() {
    return queryState;
  }

  public void setQueryState(TajoProtos.QueryState queryState) {
    this.queryState = queryState;
  }

  public long getStartTime() {
    return startTime;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  public long getFinishTime() {
    return finishTime;
  }

  public void setFinishTime(long finishTime) {
    this.finishTime = finishTime;
  }

  public String getLastMessage() {
    return lastMessage;
  }

  public void setLastMessage(String lastMessage) {
    this.lastMessage = lastMessage;
  }

  public Worker getQueryMasterResource() {
    return queryMaster;
  }

  public float getProgress() {
    return progress;
  }

  public void setProgress(float progress) {
    this.progress = progress;
  }

  @Override
  public String toString() {
    return queryId.toString() + "state=" + queryState +",progress=" + progress + ", queryMaster=" + queryMaster;
  }
}
