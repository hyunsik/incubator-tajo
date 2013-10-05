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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.TaskAttemptContext;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.engine.eval.EvalContext;
import org.apache.tajo.engine.eval.EvalNode;
import org.apache.tajo.engine.planner.PlannerUtil;
import org.apache.tajo.engine.planner.Projector;
import org.apache.tajo.engine.planner.logical.JoinNode;
import org.apache.tajo.engine.utils.SchemaUtil;
import org.apache.tajo.storage.Appender;
import org.apache.tajo.storage.FrameTuple;
import org.apache.tajo.storage.Scanner;
import org.apache.tajo.storage.StorageManagerFactory;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;

/**
 * This physical operator implements the hybrid hash join algorithm.
 */
public class HybridHashJoinExec extends BinaryPhysicalExec {
  private static Log LOG = LogFactory.getLog(HybridHashJoinExec.class);

  private long workingMemory;

  private EvalNode joinQual;
  private FrameTuple frameTuple;
  private Tuple outTuple;

  private List<Column[]> joinKeyPairs;

  private EvalContext qualCtx;

  private Map<Tuple, List<Tuple>> tupleSlots;

  private int[] outerKeyList;
  private int[] innerKeyList;

  private Tuple outerTuple;
  private VTuple outerKeyTuple;

  private Iterator<Tuple> iterator;
  private boolean foundMatch = false;

  // projection
  private final Projector projector;
  private final EvalContext[] evalContexts;

  private int step = 1;

  private Map<Integer, List<Bucket>> bucketsMap = new TreeMap<Integer, List<Bucket>>();

  private Iterator<Integer> bucketsMapIterator;

  private boolean hasTuples = false;

  private boolean hasBuckets = false;

  private int bucketId = 0;

  private TableMeta innerTableMeta;
  private TableMeta outerTableMeta;

  private Iterator<Bucket> bucketsIterator;

  private Tuple nextOuterTuple;

  private Scanner outerScanner;

  public HybridHashJoinExec(TaskAttemptContext context, JoinNode plan, PhysicalExec outer, PhysicalExec inner) {
    super(context, SchemaUtil.merge(outer.getSchema(), inner.getSchema()), plan.getOutSchema(), outer, inner);

    this.workingMemory = context.getConf().getIntVar(ConfVars.HYBRID_HASH_JOIN_MEMORY) * 1024 * 1024;

    this.joinQual = plan.getJoinQual();
    this.qualCtx = joinQual.newContext();
    this.tupleSlots = new HashMap<Tuple, List<Tuple>>(10000);

    // contains the pairs of columns to join
    this.joinKeyPairs = PlannerUtil.getJoinKeyPairs(joinQual, outer.getSchema(), inner.getSchema());

    outerKeyList = new int[joinKeyPairs.size()];
    innerKeyList = new int[joinKeyPairs.size()];

    for (int i = 0; i < joinKeyPairs.size(); i++) {
      outerKeyList[i] = outer.getSchema().getColumnId(joinKeyPairs.get(i)[0].getQualifiedName());
    }

    for (int i = 0; i < joinKeyPairs.size(); i++) {
      innerKeyList[i] = inner.getSchema().getColumnId(joinKeyPairs.get(i)[1].getQualifiedName());
    }

    // for projection
    this.projector = new Projector(inSchema, outSchema, plan.getTargets());
    this.evalContexts = projector.renew();

    // for join
    frameTuple = new FrameTuple();
    outTuple = new VTuple(outSchema.getColumnNum());
    outerKeyTuple = new VTuple(outerKeyList.length);

    innerTableMeta = CatalogUtil.newTableMeta(rightChild.outSchema, StoreType.CSV);
    outerTableMeta = CatalogUtil.newTableMeta(leftChild.outSchema, StoreType.CSV);
  }

  private void partitionHistogram() throws IOException {
    Map<Integer, Long> histogram = context.getHistogram();
    if (histogram == null || histogram.size() == 0) {
      LOG.info("No histogram provided.");

      histogram = new TreeMap<Integer, Long>();
      histogram.put(Integer.MAX_VALUE, workingMemory);
    } else {
      LOG.info("Histogram size: " + histogram.size());
    }

    int lastKey = -1;
    long accumulated = 0;
    List<Bucket> buckets;
    boolean isFirst = true;

    for (int key : histogram.keySet()) {
      long value = histogram.get(key);

      if (accumulated + value > workingMemory) {

        if (accumulated > 0) {
          buckets = new ArrayList<Bucket>();
          buckets.add(new Bucket(isFirst));
          isFirst = false;
          bucketsMap.put(lastKey, buckets);
          accumulated = value;
        }

        if (value > workingMemory) {
          // handle bucket overflow
          buckets = new ArrayList<Bucket>();

          Path outerPath = new Path(context.getWorkDir(), "outerBucket" + bucketId);
          Appender outerAppender = null;
          outerAppender = StorageManagerFactory.getStorageManager(context.getConf()).getAppender(outerTableMeta,
              outerPath);
          outerAppender.init();

          long i = value / workingMemory;
          while (i-- > 0) {
            buckets.add(new Bucket(outerPath, outerAppender));
          }
          bucketsMap.put(key, buckets);
          accumulated = 0;
        }
      } else {
        accumulated += value;
      }

      lastKey = key;
    }
    if (accumulated > 0) {
      buckets = new ArrayList<Bucket>();
      buckets.add(new Bucket(isFirst));
      bucketsMap.put(lastKey, buckets);
    }
  }

  @Override
  public Tuple next() throws IOException {
    if (step == 1) {
      partitionHistogram();
      bucketInnerRelation();
      step++;
    }

    if (step == 2) {
      if (bucketOuterRelation() == null) {

        // close all appenders and open scanners
        for (List<Bucket> buckets : bucketsMap.values()) {
          for (Bucket bucket : buckets) {
            bucket.closeAppendersAndOpenScanners();
          }
        }
        bucketsMapIterator = bucketsMap.keySet().iterator();
        step++;
      }
    }

    if (step == 3) {

      Scanner innerScanner = null;
      List<Bucket> buckets;
      Tuple tuple;
      List<Tuple> tuples;
      Tuple innerKeyTuple;

      while (true) {
        if (!hasTuples && !hasBuckets) {
          if (!bucketsMapIterator.hasNext()) {
            return null;
          }
          buckets = bucketsMap.get(bucketsMapIterator.next());
          bucketsIterator = buckets.iterator();
        }

        while (true) {
          if (!hasTuples) {
            if (!bucketsIterator.hasNext()) {
              break;
            }
            Bucket bucket = bucketsIterator.next();

            // load inner bucket
            innerScanner = bucket.getInnerScanner();
            tupleSlots.clear();

            while ((tuple = innerScanner.next()) != null) {
              innerKeyTuple = getKeyTuple(tuple, true);

              tuples = tupleSlots.get(innerKeyTuple);
              if (tuples == null) {
                tuples = new ArrayList<Tuple>();
              }
              tuples.add(tuple);
              tupleSlots.put(innerKeyTuple, tuples);
            }

            outerScanner = bucket.getOuterScanner();
            nextOuterTuple = outerScanner.next();
            hasTuples = nextOuterTuple != null;
            hasBuckets = bucketsIterator.hasNext();
          }

          // probe outer bucket
          while (!foundMatch && hasTuples) {

            outerTuple = nextOuterTuple;
            getKeyTuple(outerTuple, false);

            tuples = tupleSlots.get(outerKeyTuple);
            if (tuples != null) {
              foundMatch = true;
              iterator = tuples.iterator();
            }
            nextOuterTuple = outerScanner.next();
            hasTuples = nextOuterTuple != null;
          }

          if (foundMatch) {
            Tuple innerTuple = iterator.next();
            frameTuple.set(outerTuple, innerTuple);
            joinQual.eval(qualCtx, inSchema, frameTuple);
            if (joinQual.terminate(qualCtx).asBool()) {
              projector.eval(evalContexts, frameTuple);
              projector.terminate(evalContexts, outTuple);
            }
            foundMatch = iterator.hasNext();
            return outTuple;
          }
        }
      }
    }
    return outTuple;
  }

  private void bucketInnerRelation() throws IOException {
    Tuple tuple;
    List<Bucket> buckets;
    Bucket bucket = null;
    Tuple innerKeyTuple;

    while ((tuple = rightChild.next()) != null) {
      innerKeyTuple = getKeyTuple(tuple, true);
      buckets = getBuckets(innerKeyTuple.hashCode());
      if (buckets.size() == 1) {
        bucket = buckets.get(0);
      } else {
        long tupleSize = getTupleSize(tuple);
        for (Bucket b : buckets) {
          long estimatedSize = b.getSize() + tupleSize;
          if (estimatedSize < workingMemory) {
            b.setSize(estimatedSize);
            bucket = b;
            break;
          }
        }
      }

      if (bucket.isBucketZero()) {
        List<Tuple> tuples = tupleSlots.get(innerKeyTuple);
        if (tuples == null) {
          tuples = new ArrayList<Tuple>();
        }
        tuples.add(tuple);
        tupleSlots.put(innerKeyTuple, tuples);
      } else {
        // write tuple out to disk
        Appender appender = bucket.getInnerAppender();
        appender.addTuple(tuple);
      }
    }
  }

  private long getTupleSize(Tuple tuple) {
    long size = 0;
    for (int i = 0; i < tuple.size(); i++) {
      size += tuple.get(i).size();
    }
    return size;
  }

  private Tuple bucketOuterRelation() throws IOException {
    Tuple innerTuple;
    List<Tuple> tuples;
    List<Bucket> buckets;
    Bucket bucket;

    while (!foundMatch) {
      // getting new outer tuple
      outerTuple = leftChild.next();
      if (outerTuple == null) {
        return null;
      }

      getKeyTuple(outerTuple, false);
      buckets = getBuckets(outerKeyTuple.hashCode());
      if (buckets == null) {
        continue;
      }
      bucket = buckets.get(0);

      if (!bucket.isBucketZero() || buckets.size() > 1) {
        // write tuple out to disk
        Appender appender = bucket.getOuterAppender();
        appender.addTuple(outerTuple);
      }
      if (bucket.isBucketZero()) {

        // probe directly
        tuples = tupleSlots.get(outerKeyTuple);
        if (tuples != null) {
          iterator = tuples.iterator();
          break;
        }
      }
    }

    innerTuple = iterator.next();
    frameTuple.set(outerTuple, innerTuple);
    joinQual.eval(qualCtx, inSchema, frameTuple);
    if (joinQual.terminate(qualCtx).asBool()) {
      projector.eval(evalContexts, frameTuple);
      projector.terminate(evalContexts, outTuple);
    }
    foundMatch = iterator.hasNext();

    return outTuple;
  }

  private List<Bucket> getBuckets(int hashCode) throws IOException {
    for (int key : bucketsMap.keySet()) {
      if (hashCode < key) {
        return bucketsMap.get(key);
      }
    }
    throw new IOException("The histogram provided is corrupted or does not belong to the current inner relation.");
  }

  private Tuple getKeyTuple(Tuple tuple, boolean isInner) {
    int[] keyList = isInner ? innerKeyList : outerKeyList;
    Tuple keyTuple = isInner ? new VTuple(innerKeyList.length) : outerKeyTuple;
    for (int i = 0; i < keyList.length; i++) {
      keyTuple.put(i, tuple.get(keyList[i]));
    }
    return keyTuple;
  }

  // for testing purposes
  public void setWorkingMemory(long workingMemory) {
    this.workingMemory = workingMemory;
  }

  private class Bucket {

    private Path innerPath;

    private Path outerPath;

    private Appender innerAppender;

    private Appender outerAppender;

    private Scanner innerScanner;

    private Scanner outerScanner;

    private long size = 0;

    private boolean bucketZero = false;

    public Bucket(boolean bucketZero) {
      this.bucketZero = bucketZero;
      try {
        innerPath = new Path(context.getWorkDir(), "innerBucket" + bucketId);
        this.innerAppender = StorageManagerFactory.getStorageManager(context.getConf()).getAppender(innerTableMeta,
            innerPath);
        this.innerAppender.init();

        this.outerPath = new Path(context.getWorkDir(), "outerBucket" + bucketId++);
        this.outerAppender = StorageManagerFactory.getStorageManager(context.getConf()).getAppender(outerTableMeta,
            outerPath);
        this.outerAppender.init();
      } catch (IOException e) {
        LOG.error(e);
      }
    }

    public Bucket(Path outerPath, Appender outerAppender) {
      try {
        innerPath = new Path(context.getWorkDir(), "innerBucket" + bucketId++);
        this.innerAppender = StorageManagerFactory.getStorageManager(context.getConf()).getAppender(innerTableMeta,
            innerPath);
        this.innerAppender.init();

        this.outerPath = outerPath;
        this.outerAppender = outerAppender;
      } catch (IOException e) {
        LOG.error(e);
      }
    }

    public void closeAppendersAndOpenScanners() throws IOException {
      innerAppender.close();
      outerAppender.close();
      innerScanner = StorageManagerFactory.getStorageManager(context.getConf()).getScanner(innerTableMeta, innerPath);
      innerScanner.init();
      outerScanner = StorageManagerFactory.getStorageManager(context.getConf()).getScanner(outerTableMeta, outerPath);
      outerScanner.init();
    }

    public long getSize() {
      return size;
    }

    public void setSize(long size) {
      this.size = size;
    }

    public boolean isBucketZero() {
      return bucketZero;
    }

    public Appender getInnerAppender() {
      return innerAppender;
    }

    public Appender getOuterAppender() {
      return outerAppender;
    }

    public Scanner getInnerScanner() {
      return innerScanner;
    }

    public Scanner getOuterScanner() {
      return outerScanner;
    }

  }
}
