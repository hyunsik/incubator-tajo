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
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.state.InvalidStateTransitonException;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;

import java.util.EnumSet;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Worker implements EventHandler<WorkerEvent> {

  private static final Log LOG = LogFactory.getLog(Worker.class);

  private final ReentrantReadWriteLock.ReadLock readLock;
  private final ReentrantReadWriteLock.WriteLock writeLock;

  private final String workerId;

  private WorkerResource resource;

  private static final StateMachineFactory<Worker,
      WorkerState,
      WorkerEventType,
      WorkerEvent> stateMachineFactory
      = new StateMachineFactory<Worker,
      WorkerState,
      WorkerEventType,
      WorkerEvent>(WorkerState.NEW)

      // Transition from NEW
      .addTransition(WorkerState.NEW, WorkerState.RUNNING,
      WorkerEventType.STARTED, new AddNodeTransition())

      // Transition from RUNNING
      .addTransition(WorkerState.RUNNING, EnumSet.of(WorkerState.RUNNING, WorkerState.UNHEALTHY),
          WorkerEventType.STATE_UPDATE, null)
      .addTransition(WorkerState.RUNNING, WorkerState.LOST,
          WorkerEventType.EXPIRE, new DeactivateNodeTransition(WorkerState.LOST))
      .addTransition(WorkerState.RUNNING, WorkerState.RUNNING,
          WorkerEventType.RECONNECTED, null)

      // Transitions from UNHEALTHY state
      .addTransition(WorkerState.UNHEALTHY, EnumSet.of(WorkerState.RUNNING, WorkerState.UNHEALTHY),
          WorkerEventType.STATE_UPDATE, null)
      .addTransition(WorkerState.UNHEALTHY, WorkerState.LOST,
          WorkerEventType.EXPIRE, new DeactivateNodeTransition(WorkerState.LOST))
      .addTransition(WorkerState.UNHEALTHY, WorkerState.UNHEALTHY,
          WorkerEventType.RECONNECTED, null);

  private final StateMachine<WorkerState, WorkerEventType, WorkerEvent> stateMachine =
      stateMachineFactory.make(this, WorkerState.NEW);

  private final WorkerRMContext rmContext;

  public Worker(String workerId, WorkerRMContext rmContext, WorkerResource resource) {
    this.workerId = workerId;
    this.rmContext = rmContext;

    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    this.readLock = lock.readLock();
    this.writeLock = lock.writeLock();

    this.resource = resource;
  }

  public String getWorkerId() {
    return workerId;
  }

  public WorkerState getState() {
    this.readLock.lock();

    try {
      return this.stateMachine.getCurrentState();
    } finally {
      this.readLock.unlock();
    }
  }

  public WorkerResource getResource() {
    return this.resource;
  }

  public static class AddNodeTransition implements SingleArcTransition<Worker, WorkerEvent> {
    @Override
    public void transition(Worker worker, WorkerEvent workerEvent) {

      if(worker.getResource().isQueryMasterMode()) {
        worker.rmContext.getQueryMasterWorker().add(worker.getWorkerId());
      }
      LOG.info("Worker with " + worker.getResource() + " is joined to Tajo cluster");
    }
  }

  public static class DeactivateNodeTransition implements SingleArcTransition<Worker, WorkerEvent> {
    private final WorkerState finalState;

    public DeactivateNodeTransition(WorkerState finalState) {
      this.finalState = finalState;
    }

    @Override
    public void transition(Worker worker, WorkerEvent workerEvent) {

      worker.rmContext.getWorkers().remove(worker);
      LOG.info("Deactivating Node " + worker.getWorkerId() + " as it is now " + finalState);
      worker.rmContext.getInactiveWorkers().putIfAbsent(worker.getWorkerId(), worker);
    }
  }

  public static class ReconnectedNodeTransition implements SingleArcTransition<Worker, WorkerEvent> {

    @Override
    public void transition(Worker worker, ReconnectedWorkerEvent workerEvent) {

      worker.rmContext.getWorkers().remove(worker.getWorkerId());
      worker.rmContext.getWorkers().put(worker.getWorkerId(), )
      LOG.info("Deactivating Node " + worker.getWorkerId() + " as it is now " + finalState);
      worker.rmContext.getInactiveWorkers().putIfAbsent(worker.getWorkerId(), worker);
    }
  }

  @Override
  public void handle(WorkerEvent event) {
    LOG.debug("Processing " + event.getWorkerId() + " of type " + event.getType());
    try {
      writeLock.lock();
      WorkerState oldState = getState();
      try {
        stateMachine.doTransition(event.getType(), event);
      } catch (InvalidStateTransitonException e) {
        LOG.error("Can't handle this event at current state", e);
        LOG.error("Invalid event " + event.getType() + " on Worker  " + this.workerId);
      }
      if (oldState != getState()) {
        LOG.info(workerId + " Node Transitioned from " + oldState + " to " + getState());
      }
    }

    finally {
      writeLock.unlock();
    }
  }
}
