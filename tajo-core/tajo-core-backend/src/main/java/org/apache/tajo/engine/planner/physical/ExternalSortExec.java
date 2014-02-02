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

package org.apache.tajo.engine.planner.physical;

import com.sun.org.apache.commons.logging.Log;
import com.sun.org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.engine.planner.logical.SortNode;
import org.apache.tajo.storage.*;
import org.apache.tajo.storage.Scanner;
import org.apache.tajo.util.TUtil;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;
import java.util.*;

import static org.apache.tajo.storage.RawFile.RawFileAppender;
import static org.apache.tajo.storage.RawFile.RawFileScanner;

public class ExternalSortExec extends SortExec {
  private static final Log LOG = LogFactory.getLog(ExternalSortExec.class);

  private final SortNode plan;
  private final TableMeta meta;
  /** the fanout of external sort */
  private final int fanout;
  /** It's the size of in-memory table. If memory consumption exceeds it, store the memory table into a disk. */
  private final int bufferBytesSize;
  private final FileSystem localFS;
  private final Path sortTmpDir;

  private final List<Tuple> inMemoryTable;
  /** already sorted or not */
  private boolean sorted = false;
  /** a flag to point whether memory exceeded or not */
  private boolean memoryExceeded = false;
  /** the final result */
  private Scanner result;

  public ExternalSortExec(final TaskAttemptContext context,
                          final AbstractStorageManager sm, final SortNode plan, final PhysicalExec child)
      throws IOException {
    super(context, plan.getInSchema(), plan.getOutSchema(), child, plan.getSortKeys());
    this.plan = plan;

    this.fanout = context.getConf().getIntVar(ConfVars.EXECUTOR_EXTERNAL_SORT_FANOUT);
    this.bufferBytesSize = context.getConf().getIntVar(ConfVars.EXECUTOR_EXTERNAL_SORT_BUFFER_SIZE);
    this.inMemoryTable = new ArrayList<Tuple>(1000000);

    this.sortTmpDir = new Path(context.getWorkDir(), UUID.randomUUID().toString());
    this.localFS = FileSystem.getLocal(context.getConf());
    meta = CatalogUtil.newTableMeta(StoreType.ROWFILE);
  }

  public void init() throws IOException {
    super.init();
    localFS.mkdirs(sortTmpDir);
  }

  public SortNode getPlan() {
    return this.plan;
  }

  private void sortAndStoreChunk(int chunkId, List<Tuple> tupleSlots)
      throws IOException {
    TableMeta meta = CatalogUtil.newTableMeta(StoreType.RAW);
    Collections.sort(tupleSlots, getComparator());
    // TODO - RawFile requires the local file path.
    // So, I add the scheme 'file:/' to path. But, it should be improved.
    Path localPath = new Path(sortTmpDir + "/0_" + chunkId);

    final RawFileAppender appender = new RawFileAppender(context.getConf(), inSchema, meta, localPath);
    appender.init();

    for (Tuple t : tupleSlots) {
      appender.addTuple(t);
    }
    appender.close();
    tupleSlots.clear();
  }

  /**
   * It divides all tuples into a number of chunks, then sort for each chunk.
   * @return the number of stored chunks
   * @throws java.io.IOException
   */
  private int sortAndStoreAllChunks() throws IOException {
    int chunkId = 0;

    Tuple tuple;
    int memoryConsumption = 0;

    long runStartTime = System.currentTimeMillis();
    while ((tuple = child.next()) != null) { // partition sort start
      VTuple vtuple = new VTuple(tuple);
      inMemoryTable.add(vtuple);
      memoryConsumption += MemoryUtil.calculateMemorySize(vtuple);

      if (memoryConsumption > bufferBytesSize) {
        memoryExceeded = true;
        long runEndTime = System.currentTimeMillis();
        LOG.info(chunkId + " run loading time: " + (runEndTime - runStartTime) + " msec");
        runStartTime = runEndTime;

        long start = System.currentTimeMillis();
        LOG.info("Memory Consumption exceeds " + bufferBytesSize + " bytes");
        int rowNum = inMemoryTable.size();
        sortAndStoreChunk(chunkId, inMemoryTable);
        long end = System.currentTimeMillis();
        LOG.info("Chunk #" + chunkId + " " + rowNum + " rows - sorted and written (" + (end - start) + " msec)");
        memoryConsumption = 0;
        chunkId++;
      }
    }

    if (inMemoryTable.size() > 0) {
      if (memoryExceeded) {
        if (inMemoryTable.size() > 0) {
          long start = System.currentTimeMillis();
          int rowNum = inMemoryTable.size();
          sortAndStoreChunk(chunkId, inMemoryTable);
          long end = System.currentTimeMillis();
          LOG.info("Last Chunk #" + chunkId + " " + rowNum + " rows written (" + (end - start) + " msec)");
        }
      }

      // if at least one or more tuples
      chunkId++;
    }

    return chunkId;
  }

  private Path getChunkPath(int level, int chunkId) {
    return StorageUtil.concatPath(sortTmpDir, "" + level + "_" + chunkId);
  }

  @Override
  public Tuple next() throws IOException {
    if (!sorted) {

      // the total number of chunks for zero level
      long startSplitRuns = System.currentTimeMillis();
      int totalChunkNumForLevel = sortAndStoreAllChunks();
      long endSplitRuns = System.currentTimeMillis();
      LOG.info("Run creation time: " + (endSplitRuns - startSplitRuns) + " msec");

      // if there are no chunk
      if (totalChunkNumForLevel == 0) {
        return null;
      }

      if (memoryExceeded) {

        int level = 0;
        int chunkId = 0;

        long mergeStart = System.currentTimeMillis();
        // continue until the chunk remains only one
        while (totalChunkNumForLevel > fanout) {

          while (chunkId < totalChunkNumForLevel) {

            Path nextChunk = getChunkPath(level + 1, chunkId / fanout);

            // if number of chunkId is odd just copy it.
            if (chunkId + 1 >= totalChunkNumForLevel) {

              Path chunk = getChunkPath(level, chunkId);
              localFS.moveFromLocalFile(chunk, nextChunk);

            } else {

              final RawFileAppender appender = new RawFileAppender(context.getConf(), inSchema, meta, nextChunk);
              appender.init();

              // we use choose the minimum k-ways between the remain chunks and fanout
              int kway = Math.min((totalChunkNumForLevel - chunkId), fanout);
              Scanner merger = createKWayMerger(level, chunkId, kway);
              merger.init();
              Tuple mergeTuple;
              while((mergeTuple = merger.next()) != null) {
                appender.addTuple(mergeTuple);
              }
              merger.close();

              appender.flush();
              appender.close();
              LOG.info(nextChunk + " is written");
            }

            chunkId += fanout;
          }

          // increase the level
          level++;

          // init chunkId for each level
          chunkId = 0;

          // calculate the total number of chunks for next level
          totalChunkNumForLevel = (int) Math.ceil((float)totalChunkNumForLevel / fanout);
        }

        if (totalChunkNumForLevel == 1) {
          Path result = getChunkPath(level, 0);
          this.result = new RawFileScanner(context.getConf(), plan.getInSchema(), meta, result);
        } else {
          this.result = createKWayMerger(level, chunkId, totalChunkNumForLevel);
        }
        long mergeEnd = System.currentTimeMillis();
        LOG.info("Total merge time: " + (mergeEnd - mergeStart) + " msec");
      } else {
        this.result = new MemScanner();
      }
      this.result.init();
      sorted = true;
    }
    return result.next();
  }

  private Scanner getFileScanner(Path path) throws IOException {
    return new RawFileScanner(context.getConf(), plan.getInSchema(), meta, path);
  }

  private Scanner createKWayMerger(int level, int startChunkId, int num) throws IOException {
    List<Scanner> sources = TUtil.newList();
    for (int i = 0; i < num; i++) {
      sources.add(getFileScanner(getChunkPath(level, startChunkId + i)));
    }

    return createKWayMergerInternal(sources, 0, num);
  }

  private Scanner createKWayMergerInternal(List<Scanner> sources, int startId, int num) throws IOException {
    if (num > 1) {
      int mid = (int) Math.ceil((float)num / 2);
      return new PairWiseMerger(
          createKWayMergerInternal(sources, startId, mid),
          createKWayMergerInternal(sources, startId + mid, num - mid));
    } else {
      return sources.get(startId);
    }
  }

  private class MemScanner implements Scanner {
    Iterator<Tuple> iterator;

    @Override
    public void init() throws IOException {
      iterator = inMemoryTable.iterator();
    }

    @Override
    public Tuple next() throws IOException {
      if (iterator.hasNext()) {
        return iterator.next();
      } else {
        return null;
      }
    }

    @Override
    public void reset() throws IOException {
      init();
    }

    @Override
    public void close() throws IOException {
      iterator = null;
    }

    @Override
    public boolean isProjectable() {
      return false;
    }

    @Override
    public void setTarget(Column[] targets) {
    }

    @Override
    public boolean isSelectable() {
      return false;
    }

    @Override
    public void setSearchCondition(Object expr) {
    }

    @Override
    public boolean isSplittable() {
      return false;
    }

    @Override
    public Schema getSchema() {
      return null;
    }
  }

  private class PairWiseMerger implements Scanner {
    private final Scanner leftScan;
    private final Scanner rightScan;

    private Tuple leftTuple;
    private Tuple rightTuple;

    private final Comparator<Tuple> comparator = getComparator();

    public PairWiseMerger(Scanner leftScanner, Scanner rightScanner) throws IOException {
      this.leftScan = leftScanner;
      this.rightScan = rightScanner;
    }

    @Override
    public void init() throws IOException {
      leftScan.init();
      rightScan.init();

      leftTuple = leftScan.next();
      rightTuple = rightScan.next();
    }

    public Tuple next() throws IOException {
      Tuple outTuple = null;
      if (leftTuple != null && rightTuple != null) {
        if (comparator.compare(leftTuple, rightTuple) < 0) {
          outTuple = leftTuple;
          leftTuple = leftScan.next();
        } else {
          outTuple = rightTuple;
          rightTuple = rightScan.next();
        }
        return outTuple;
      }

      if (leftTuple == null) {
        outTuple = rightTuple;
        rightTuple = rightScan.next();
      } else {
        outTuple = leftTuple;
        leftTuple = leftScan.next();
      }
      return outTuple;
    }

    @Override
    public void reset() throws IOException {
      leftScan.reset();
      rightScan.reset();
      init();
    }

    public void close() throws IOException {
      leftScan.close();
      rightScan.close();
    }

    @Override
    public boolean isProjectable() {
      return false;
    }

    @Override
    public void setTarget(Column[] targets) {
    }

    @Override
    public boolean isSelectable() {
      return false;
    }

    @Override
    public void setSearchCondition(Object expr) {
    }

    @Override
    public boolean isSplittable() {
      return false;
    }

    @Override
    public Schema getSchema() {
      return inSchema;
    }
  }

  @Override
  public void close() throws IOException {
    result.close();
    inMemoryTable.clear();
  }

  @Override
  public void rescan() throws IOException {
    if (result != null) {
      result.reset();
    }
  }
}
