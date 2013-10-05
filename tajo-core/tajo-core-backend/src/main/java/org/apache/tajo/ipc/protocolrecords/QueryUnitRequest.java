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
package org.apache.tajo.ipc.protocolrecords;

import org.apache.tajo.DataChannel;
import org.apache.tajo.QueryUnitAttemptId;
import org.apache.tajo.common.ProtoObject;
import org.apache.tajo.engine.planner.enforce.Enforcer;
import org.apache.tajo.ipc.TajoWorkerProtocol;
import org.apache.tajo.master.QueryContext;
import org.apache.tajo.storage.Fragment;

import java.net.URI;
import java.util.List;
import java.util.Map;

public interface QueryUnitRequest extends ProtoObject<TajoWorkerProtocol.QueryUnitRequestProto> {

	public QueryUnitAttemptId getId();
	public List<Fragment> getFragments();
	public String getOutputTableId();
	public boolean isClusteredOutput();
	public String getSerializedData();
	public boolean isInterQuery();
	public void setInterQuery();
	public void addFetch(String name, URI uri);
	public List<TajoWorkerProtocol.Fetch> getFetches();
  public boolean shouldDie();
  public void setShouldDie();
  public List<Integer> getJoinKeys();
  public void setJoinKeys(List<Integer> joinKeys);
  public void setHistogram(Map<Integer, Long> histogram);
  public Map<Integer, Long> getHistogram();
  public QueryContext getQueryContext();
  public DataChannel getDataChannel();
  public Enforcer getEnforcer();
}
