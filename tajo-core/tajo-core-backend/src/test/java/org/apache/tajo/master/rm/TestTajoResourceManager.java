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

import com.google.protobuf.RpcCallback;
import org.apache.hadoop.yarn.proto.YarnProtos;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.QueryId;
import org.apache.tajo.QueryIdFactory;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.ipc.NodeResourceTracker;
import org.apache.tajo.ipc.TajoMasterProtocol.*;
import org.apache.tajo.rpc.NullCallback;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestTajoResourceManager {
  private final PrimitiveProtos.BoolProto BOOL_TRUE = PrimitiveProtos.BoolProto.newBuilder().setValue(true).build();
  private final PrimitiveProtos.BoolProto BOOL_FALSE = PrimitiveProtos.BoolProto.newBuilder().setValue(false).build();

  TajoConf tajoConf;

  long queryIdTime = System.currentTimeMillis();
  int numWorkers = 5;
  float workerDiskSlots = 5.0f;
  int workerMemoryMB = 512 * 10;
  WorkerResourceAllocationResponse response;

  private TajoWorkerResourceManager initResourceManager(boolean queryMasterMode) throws Exception {
    tajoConf = new org.apache.tajo.conf.TajoConf();

    tajoConf.setFloatVar(TajoConf.ConfVars.TAJO_QUERYMASTER_DISK_SLOT, 0.0f);
    tajoConf.setIntVar(TajoConf.ConfVars.TAJO_QUERYMASTER_MEMORY_MB, 512);

    TajoWorkerResourceManager tajoWorkerResourceManager = new TajoWorkerResourceManager(tajoConf);
    tajoWorkerResourceManager.init(tajoConf);
    tajoWorkerResourceManager.start();

    for(int i = 0; i < numWorkers; i++) {
      ServerStatusProto.System system = ServerStatusProto.System.newBuilder()
          .setAvailableProcessors(1)
          .setFreeMemoryMB(workerMemoryMB)
          .setMaxMemoryMB(workerMemoryMB)
          .setTotalMemoryMB(workerMemoryMB)
          .build();

      ServerStatusProto.JvmHeap jvmHeap = ServerStatusProto.JvmHeap.newBuilder()
          .setFreeHeap(workerMemoryMB)
          .setMaxHeap(workerMemoryMB)
          .setTotalHeap(workerMemoryMB)
          .build();

      ServerStatusProto.Disk disk = ServerStatusProto.Disk.newBuilder()
          .setAbsolutePath("/")
          .setFreeSpace(0)
          .setTotalSpace(0)
          .setUsableSpace(0)
          .build();

      List<ServerStatusProto.Disk> disks = new ArrayList<ServerStatusProto.Disk>();

      disks.add(disk);

      ServerStatusProto serverStatus = ServerStatusProto.newBuilder()
          .setQueryMasterMode(queryMasterMode ? BOOL_TRUE : BOOL_FALSE)
          .setTaskRunnerMode(BOOL_TRUE)
          .setDiskSlots(workerDiskSlots)
          .setMemoryResourceMB(workerMemoryMB)
          .setJvmHeap(jvmHeap)
          .setSystem(system)
          .addAllDisk(disks)
          .setRunningTaskNum(0)
          .build();

      NodeResourceTracker.NodeHeartbeat tajoHeartbeat = NodeResourceTracker.NodeHeartbeat.newBuilder()
          .setTajoWorkerHost("host" + (i + 1))
          .setTajoQueryMasterPort(21000)
          .setTajoWorkerHttpPort(28080 + i)
          .setPeerRpcPort(12345)
          .setServerStatus(serverStatus)
          .build();

      tajoWorkerResourceManager.getWorkerTrackerService().heartbeat(null, tajoHeartbeat, NullCallback.get());
    }

    return tajoWorkerResourceManager;
  }


  @Test
  public void testHeartbeat() throws Exception {
    TajoWorkerResourceManager tajoWorkerResourceManager = null;
    try {
      tajoWorkerResourceManager = initResourceManager(false);
      assertEquals(numWorkers, tajoWorkerResourceManager.getWorkers().size());
      for(Worker worker: tajoWorkerResourceManager.getWorkers().values()) {
        WorkerResource resource = worker.getResource();
        assertEquals(workerMemoryMB, resource.getAvailableMemoryMB());
        assertEquals(workerDiskSlots, resource.getAvailableDiskSlots(), 0);
      }
    } finally {
      if (tajoWorkerResourceManager != null) {
        tajoWorkerResourceManager.stop();
      }
    }
  }

  @Test
  public void testMemoryResource() throws Exception {
    TajoWorkerResourceManager tajoWorkerResourceManager = null;
    try {
      tajoWorkerResourceManager = initResourceManager(false);

      final int minMemory = 256;
      final int maxMemory = 512;
      float diskSlots = 1.0f;

      QueryId queryId = QueryIdFactory.newQueryId(queryIdTime, 1);
      ExecutionBlockId ebId = QueryIdFactory.newExecutionBlockId(queryId);

      WorkerResourceAllocationRequest request = WorkerResourceAllocationRequest.newBuilder()
          .setResourceRequestPriority(ResourceRequestPriority.MEMORY)
          .setNumContainers(60)
          .setExecutionBlockId(ebId.getProto())
          .setMaxDiskSlotPerContainer(diskSlots)
          .setMinDiskSlotPerContainer(diskSlots)
          .setMinMemoryMBPerContainer(minMemory)
          .setMaxMemoryMBPerContainer(maxMemory)
          .build();

      final Object monitor = new Object();
      final List<YarnProtos.ContainerIdProto> containerIds = new ArrayList<YarnProtos.ContainerIdProto>();


      RpcCallback<WorkerResourceAllocationResponse> callBack = new RpcCallback<WorkerResourceAllocationResponse>() {

        @Override
        public void run(WorkerResourceAllocationResponse response) {
          TestTajoResourceManager.this.response = response;
          synchronized(monitor) {
            monitor.notifyAll();
          }
        }
      };

      tajoWorkerResourceManager.allocateWorkerResources(request, callBack);
      synchronized(monitor) {
        monitor.wait();
      }


      // assert after callback
      int totalUsedMemory = 0;
      int totalUsedDisks = 0;

      for(Worker worker: tajoWorkerResourceManager.getWorkers().values()) {
        WorkerResource resource = worker.getResource();
        assertEquals(0, resource.getAvailableMemoryMB());
        assertEquals(0, resource.getAvailableDiskSlots(), 0);
        assertEquals(5.0f, resource.getUsedDiskSlots(), 0);

        totalUsedMemory += resource.getUsedMemoryMB();
        totalUsedDisks += resource.getUsedDiskSlots();
      }

      assertEquals(workerMemoryMB * numWorkers, totalUsedMemory);
      assertEquals(workerDiskSlots * numWorkers, totalUsedDisks, 0);

      assertEquals(numWorkers * 10, response.getWorkerAllocatedResourceList().size());

      for(WorkerAllocatedResource eachResource: response.getWorkerAllocatedResourceList()) {
        assertTrue(
            eachResource.getAllocatedMemoryMB() >= minMemory &&  eachResource.getAllocatedMemoryMB() <= maxMemory);
        containerIds.add(eachResource.getContainerId());
      }

      for(YarnProtos.ContainerIdProto eachContainerId: containerIds) {
        tajoWorkerResourceManager.releaseWorkerResource(ebId, eachContainerId);
      }

      for(Worker worker: tajoWorkerResourceManager.getWorkers().values()) {
        WorkerResource resource = worker.getResource();
        assertEquals(workerMemoryMB, resource.getAvailableMemoryMB());
        assertEquals(0, resource.getUsedMemoryMB());

        assertEquals(workerDiskSlots, resource.getAvailableDiskSlots(), 0);
        assertEquals(0.0f, resource.getUsedDiskSlots(), 0);
      }
    } finally {
      if (tajoWorkerResourceManager != null) {
        tajoWorkerResourceManager.stop();
      }
    }
  }

  @Test
  public void testMemoryNotCommensurable() throws Exception {
    TajoWorkerResourceManager tajoWorkerResourceManager = null;

    try {
      tajoWorkerResourceManager = initResourceManager(false);

      final int minMemory = 200;
      final int maxMemory = 500;
      float diskSlots = 1.0f;

      QueryId queryId = QueryIdFactory.newQueryId(queryIdTime, 2);
      ExecutionBlockId ebId = QueryIdFactory.newExecutionBlockId(queryId);

      int requiredContainers = 60;

      int numAllocatedContainers = 0;

      int loopCount = 0;
      while(true) {
        WorkerResourceAllocationRequest request = WorkerResourceAllocationRequest.newBuilder()
            .setResourceRequestPriority(ResourceRequestPriority.MEMORY)
            .setNumContainers(requiredContainers - numAllocatedContainers)
            .setExecutionBlockId(ebId.getProto())
            .setMaxDiskSlotPerContainer(diskSlots)
            .setMinDiskSlotPerContainer(diskSlots)
            .setMinMemoryMBPerContainer(minMemory)
            .setMaxMemoryMBPerContainer(maxMemory)
            .build();

        final Object monitor = new Object();

        RpcCallback<WorkerResourceAllocationResponse> callBack = new RpcCallback<WorkerResourceAllocationResponse>() {
          @Override
          public void run(WorkerResourceAllocationResponse response) {
            TestTajoResourceManager.this.response = response;
            synchronized(monitor) {
              monitor.notifyAll();
            }
          }
        };

        tajoWorkerResourceManager.allocateWorkerResources(request, callBack);
        synchronized(monitor) {
          monitor.wait();
        }

        numAllocatedContainers += TestTajoResourceManager.this.response.getWorkerAllocatedResourceList().size();

        //release resource
        for(WorkerAllocatedResource eachResource: TestTajoResourceManager.this.response.getWorkerAllocatedResourceList()) {
          assertTrue(
              eachResource.getAllocatedMemoryMB() >= minMemory &&  eachResource.getAllocatedMemoryMB() <= maxMemory);
          tajoWorkerResourceManager.releaseWorkerResource(ebId, eachResource.getContainerId());
        }

        for(Worker worker: tajoWorkerResourceManager.getWorkers().values()) {
          WorkerResource resource = worker.getResource();
          assertEquals(0, resource.getUsedMemoryMB());
          assertEquals(workerMemoryMB, resource.getAvailableMemoryMB());

          assertEquals(0.0f, resource.getUsedDiskSlots(), 0);
          assertEquals(workerDiskSlots, resource.getAvailableDiskSlots(), 0);
        }

        loopCount++;

        if(loopCount == 2) {
          assertEquals(requiredContainers, numAllocatedContainers);
          break;
        }
      }

      for(Worker worker: tajoWorkerResourceManager.getWorkers().values()) {
        WorkerResource resource = worker.getResource();
        assertEquals(0, resource.getUsedMemoryMB());
        assertEquals(workerMemoryMB, resource.getAvailableMemoryMB());

        assertEquals(0.0f, resource.getUsedDiskSlots(), 0);
        assertEquals(workerDiskSlots, resource.getAvailableDiskSlots(), 0);
      }
    } finally {
      if (tajoWorkerResourceManager != null) {
        tajoWorkerResourceManager.stop();
      }
    }
  }

  @Test
  public void testDiskResource() throws Exception {
    TajoWorkerResourceManager tajoWorkerResourceManager = null;

    try {
      tajoWorkerResourceManager = initResourceManager(false);

      final float minDiskSlots = 1.0f;
      final float maxDiskSlots = 2.0f;
      int memoryMB = 256;

      QueryId queryId = QueryIdFactory.newQueryId(queryIdTime, 3);
      ExecutionBlockId ebId = QueryIdFactory.newExecutionBlockId(queryId);

      WorkerResourceAllocationRequest request = WorkerResourceAllocationRequest.newBuilder()
          .setResourceRequestPriority(ResourceRequestPriority.DISK)
          .setNumContainers(60)
          .setExecutionBlockId(ebId.getProto())
          .setMaxDiskSlotPerContainer(maxDiskSlots)
          .setMinDiskSlotPerContainer(minDiskSlots)
          .setMinMemoryMBPerContainer(memoryMB)
          .setMaxMemoryMBPerContainer(memoryMB)
          .build();

      final Object monitor = new Object();
      final List<YarnProtos.ContainerIdProto> containerIds = new ArrayList<YarnProtos.ContainerIdProto>();


      RpcCallback<WorkerResourceAllocationResponse> callBack = new RpcCallback<WorkerResourceAllocationResponse>() {

        @Override
        public void run(WorkerResourceAllocationResponse response) {
          TestTajoResourceManager.this.response = response;
          synchronized(monitor) {
            monitor.notifyAll();
          }
        }
      };

      tajoWorkerResourceManager.allocateWorkerResources(request, callBack);
      synchronized(monitor) {
        monitor.wait();
      }
      for(WorkerAllocatedResource eachResource: response.getWorkerAllocatedResourceList()) {
        assertTrue("AllocatedDiskSlot:" + eachResource.getAllocatedDiskSlots(),
            eachResource.getAllocatedDiskSlots() >= minDiskSlots &&
                eachResource.getAllocatedDiskSlots() <= maxDiskSlots);
        containerIds.add(eachResource.getContainerId());
      }

      // assert after callback
      int totalUsedDisks = 0;
      for(Worker worker: tajoWorkerResourceManager.getWorkers().values()) {
        WorkerResource resource = worker.getResource();
        //each worker allocated 3 container (2 disk slot = 2, 1 disk slot = 1)
        assertEquals(0, resource.getAvailableDiskSlots(), 0);
        assertEquals(5.0f, resource.getUsedDiskSlots(), 0);
        assertEquals(256 * 3, resource.getUsedMemoryMB());

        totalUsedDisks += resource.getUsedDiskSlots();
      }

      assertEquals(workerDiskSlots * numWorkers, totalUsedDisks, 0);

      assertEquals(numWorkers * 3, response.getWorkerAllocatedResourceList().size());

      for(YarnProtos.ContainerIdProto eachContainerId: containerIds) {
        tajoWorkerResourceManager.releaseWorkerResource(ebId, eachContainerId);
      }

      for(Worker worker: tajoWorkerResourceManager.getWorkers().values()) {
        WorkerResource resource = worker.getResource();
        assertEquals(workerMemoryMB, resource.getAvailableMemoryMB());
        assertEquals(0, resource.getUsedMemoryMB());

        assertEquals(workerDiskSlots, resource.getAvailableDiskSlots(), 0);
        assertEquals(0.0f, resource.getUsedDiskSlots(), 0);
      }
    } finally {
      if (tajoWorkerResourceManager != null) {
        tajoWorkerResourceManager.stop();
      }
    }
  }

  @Test
  public void testQueryMasterResource() throws Exception {
    TajoWorkerResourceManager tajoWorkerResourceManager = null;

    try {
      tajoWorkerResourceManager = initResourceManager(true);

      int qmDefaultMemoryMB = tajoConf.getIntVar(TajoConf.ConfVars.TAJO_QUERYMASTER_MEMORY_MB);
      float qmDefaultDiskSlots = tajoConf.getFloatVar(TajoConf.ConfVars.TAJO_QUERYMASTER_DISK_SLOT);

      QueryId queryId = QueryIdFactory.newQueryId(queryIdTime, 4);

      tajoWorkerResourceManager.allocateQueryMaster(queryId);

      // assert after callback
      int totalUsedMemory = 0;
      int totalUsedDisks = 0;
      for(Worker worker: tajoWorkerResourceManager.getWorkers().values()) {
        WorkerResource resource = worker.getResource();
        if(resource.getUsedMemoryMB() > 0) {
          //worker which allocated querymaster
          assertEquals(qmDefaultMemoryMB, resource.getUsedMemoryMB());
          assertEquals(qmDefaultDiskSlots, resource.getUsedDiskSlots(), 0);
        } else {
          assertEquals(0, resource.getUsedMemoryMB());
          assertEquals(0, resource.getUsedDiskSlots(), 0);
        }

        totalUsedMemory += resource.getUsedMemoryMB();
        totalUsedDisks += resource.getUsedDiskSlots();
      }

      assertEquals(qmDefaultMemoryMB, totalUsedMemory);
      assertEquals(qmDefaultDiskSlots, totalUsedDisks, 0);

      //release
      tajoWorkerResourceManager.stopQueryMaster(queryId);
      for(Worker worker: tajoWorkerResourceManager.getWorkers().values()) {
        WorkerResource resource = worker.getResource();
        assertEquals(0, resource.getUsedMemoryMB());
        assertEquals(0, resource.getUsedDiskSlots(), 0);
        totalUsedMemory += resource.getUsedMemoryMB();
        totalUsedDisks += resource.getUsedDiskSlots();
      }

    } finally {
      if (tajoWorkerResourceManager != null) {
        tajoWorkerResourceManager.stop();
      }
    }
  }
}
