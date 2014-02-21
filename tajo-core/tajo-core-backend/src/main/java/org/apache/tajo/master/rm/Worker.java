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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.state.InvalidStateTransitonException;
import org.apache.hadoop.yarn.state.StateMachine;

import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Worker implements EventHandler<WorkerEvent> {

  private static final Log LOG = LogFactory.getLog(Worker.class);

  private final ReentrantReadWriteLock.ReadLock readLock;
  private final ReentrantReadWriteLock.WriteLock writeLock;

  private NodeId nodeId;

  private WorkerResource resource;

  private final StateMachine<WorkerState, WorkerEventType, WorkerEvent> stateMachine = null;

  public Worker(WorkerResource resource) {

    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    this.readLock = lock.readLock();
    this.writeLock = lock.writeLock();

    this.resource = resource;
  }

  public NodeId getNodeId() {
    return null;
  }

  public WorkerState getState() {
    return null;
  }

  public WorkerResource getResource() {
    return this.resource;
  }

  @Override
  public void handle(WorkerEvent event) {
    LOG.debug("Processing " + event.getNodeId() + " of type " + event.getType());
    try {
      writeLock.lock();
      WorkerState oldState = getState();
      try {
        stateMachine.doTransition(event.getType(), event);
      } catch (InvalidStateTransitonException e) {
        LOG.error("Can't handle this event at current state", e);
        LOG.error("Invalid event " + event.getType() + " on Node  " + this.nodeId);
      }
      if (oldState != getState()) {
        LOG.info(nodeId + " Node Transitioned from " + oldState + " to " + getState());
      }
    }

    finally {
      writeLock.unlock();
    }
  }
}
