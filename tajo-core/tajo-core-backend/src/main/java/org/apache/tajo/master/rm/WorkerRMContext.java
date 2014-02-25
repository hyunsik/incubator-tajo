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

package org.apache.tajo.master.rm;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.tajo.ipc.TajoMasterProtocol;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

public class WorkerRMContext {

  final Dispatcher rmDispatcher;

  private final ConcurrentMap<String, Worker> workers = Maps.newConcurrentMap();

  private final ConcurrentMap<String, Worker> inactiveWorkers = Maps.newConcurrentMap();

  private final Set<String> liveQueryMasterWorkerResources = Sets.newConcurrentHashSet();

  public WorkerRMContext(Dispatcher dispatcher) {
    this.rmDispatcher = dispatcher;
  }

  public Dispatcher getDispatcher() {
    return rmDispatcher;
  }

  public ConcurrentMap<String, Worker> getWorkers() {
    return workers;
  }

  public ConcurrentMap<String, Worker> getInactiveWorkers() {
    return inactiveWorkers;
  }

  public Set<String> getQueryMasterWorker() {
    return liveQueryMasterWorkerResources;
  }
}
