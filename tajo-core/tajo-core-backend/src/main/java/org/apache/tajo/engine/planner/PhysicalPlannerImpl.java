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
package org.apache.tajo.engine.planner;

import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.TaskAttemptContext;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.planner.logical.*;
import org.apache.tajo.engine.planner.physical.*;
import org.apache.tajo.exception.InternalException;
import org.apache.tajo.storage.AbstractStorageManager;
import org.apache.tajo.storage.Fragment;
import org.apache.tajo.storage.TupleComparator;
import org.apache.tajo.util.IndexUtil;

import java.io.IOException;

public class PhysicalPlannerImpl implements PhysicalPlanner {
  private static final Log LOG = LogFactory.getLog(PhysicalPlannerImpl.class);
  protected final TajoConf conf;
  protected final AbstractStorageManager sm;

  public PhysicalPlannerImpl(final TajoConf conf, final AbstractStorageManager sm) {
    this.conf = conf;
    this.sm = sm;
  }

  public PhysicalExec createPlan(final TaskAttemptContext context,
      final LogicalNode logicalPlan) throws InternalException {

    PhysicalExec plan;

    try {
      plan = createPlanRecursive(context, logicalPlan);

    } catch (IOException ioe) {
      throw new InternalException(ioe);
    }

    return plan;
  }

  private PhysicalExec createPlanRecursive(TaskAttemptContext ctx, LogicalNode logicalNode) throws IOException {
    PhysicalExec outer;
    PhysicalExec inner;

    switch (logicalNode.getType()) {

      case ROOT:
        LogicalRootNode rootNode = (LogicalRootNode) logicalNode;
        return createPlanRecursive(ctx, rootNode.getChild());

      case EXPRS:
        EvalExprNode evalExpr = (EvalExprNode) logicalNode;
        return new EvalExprExec(ctx, evalExpr);

      case STORE:
        StoreTableNode storeNode = (StoreTableNode) logicalNode;
        outer = createPlanRecursive(ctx, storeNode.getChild());
        return createStorePlan(ctx, storeNode, outer);

      case SELECTION:
        SelectionNode selNode = (SelectionNode) logicalNode;
        outer = createPlanRecursive(ctx, selNode.getChild());
        return new SelectionExec(ctx, selNode, outer);

      case PROJECTION:
        ProjectionNode prjNode = (ProjectionNode) logicalNode;
        outer = createPlanRecursive(ctx, prjNode.getChild());
        return new ProjectionExec(ctx, prjNode, outer);

      case SCAN:
        outer = createScanPlan(ctx, (ScanNode) logicalNode);
        return outer;

      case GROUP_BY:
        GroupbyNode grpNode = (GroupbyNode) logicalNode;
        outer = createPlanRecursive(ctx, grpNode.getChild());
        return createGroupByPlan(ctx, grpNode, outer);

      case SORT:
        SortNode sortNode = (SortNode) logicalNode;
        outer = createPlanRecursive(ctx, sortNode.getChild());
        return createSortPlan(ctx, sortNode, outer);

      case JOIN:
        JoinNode joinNode = (JoinNode) logicalNode;
        outer = createPlanRecursive(ctx, joinNode.getLeftChild());
        inner = createPlanRecursive(ctx, joinNode.getRightChild());
        return createJoinPlan(ctx, joinNode, outer, inner);

      case UNION:
        UnionNode unionNode = (UnionNode) logicalNode;
        outer = createPlanRecursive(ctx, unionNode.getLeftChild());
        inner = createPlanRecursive(ctx, unionNode.getRightChild());
        return new UnionExec(ctx, outer, inner);

      case LIMIT:
        LimitNode limitNode = (LimitNode) logicalNode;
        outer = createPlanRecursive(ctx, limitNode.getChild());
        return new LimitExec(ctx, limitNode.getInSchema(),
            limitNode.getOutSchema(), outer, limitNode);

      case BST_INDEX_SCAN:
        IndexScanNode indexScanNode = (IndexScanNode) logicalNode;
        outer = createIndexScanExec(ctx, indexScanNode);
        return outer;

      default:
        return null;
    }
  }

  private long estimateSizeRecursive(TaskAttemptContext ctx, String [] tableIds) {
    long size = 0;
    for (String tableId : tableIds) {
      Fragment[] fragments = ctx.getTables(tableId);
      for (Fragment frag : fragments) {
        size += frag.getLength();
      }
    }
    return size;
  }

  public PhysicalExec createJoinPlan(TaskAttemptContext ctx, JoinNode joinNode,
                                     PhysicalExec outer, PhysicalExec inner)
      throws IOException {
    switch (joinNode.getJoinType()) {
      case CROSS:
        LOG.info("The planner chooses [Nested Loop Join]");
        return new NLJoinExec(ctx, joinNode, outer, inner);

      case INNER:
        String [] outerLineage = PlannerUtil.getLineage(joinNode.getLeftChild());
        String [] innerLineage = PlannerUtil.getLineage(joinNode.getRightChild());
        long outerSize = estimateSizeRecursive(ctx, outerLineage);
        long innerSize = estimateSizeRecursive(ctx, innerLineage);

        final long threshold = 1048576 * 128; // 64MB

        boolean hashJoin = false;
        if (outerSize < threshold || innerSize < threshold) {
          hashJoin = true;
        }

        if (hashJoin) {
          PhysicalExec selectedOuter;
          PhysicalExec selectedInner;

          // HashJoinExec loads the inner relation to memory.
          if (outerSize <= innerSize) {
            selectedInner = outer;
            selectedOuter = inner;
          } else {
            selectedInner = inner;
            selectedOuter = outer;
          }

          LOG.info("The planner chooses [InMemory Hash Join]");
          return new HashJoinExec(ctx, joinNode, selectedOuter, selectedInner);
        }

      default:
        SortSpec[][] sortSpecs = PlannerUtil.getSortKeysFromJoinQual(
            joinNode.getJoinQual(), outer.getSchema(), inner.getSchema());
        ExternalSortExec outerSort = new ExternalSortExec(ctx, sm,
            new SortNode(sortSpecs[0], outer.getSchema(), outer.getSchema()),
            outer);
        ExternalSortExec innerSort = new ExternalSortExec(ctx, sm,
            new SortNode(sortSpecs[1], inner.getSchema(), inner.getSchema()),
            inner);

        LOG.info("The planner chooses [Merge Join]");
        return new MergeJoinExec(ctx, joinNode, outerSort, innerSort,
            sortSpecs[0], sortSpecs[1]);
    }
  }

  public PhysicalExec createStorePlan(TaskAttemptContext ctx,
                                      StoreTableNode plan, PhysicalExec subOp) throws IOException {
    if (plan.hasPartitionKey()) {
      switch (plan.getPartitionType()) {
        case HASH:
          return new PartitionedStoreExec(ctx, sm, plan, subOp);

        case RANGE:
          SortSpec [] sortSpecs = null;
          if (subOp instanceof SortExec) {
            sortSpecs = ((SortExec)subOp).getSortSpecs();
          } else {
            Column[] columns = plan.getPartitionKeys();
            SortSpec specs[] = new SortSpec[columns.length];
            for (int i = 0; i < columns.length; i++) {
              specs[i] = new SortSpec(columns[i]);
            }
          }

          return new IndexedStoreExec(ctx, sm, subOp,
              plan.getInSchema(), plan.getInSchema(), sortSpecs);
      }
    }
    if (plan instanceof StoreIndexNode) {
      return new TunnelExec(ctx, plan.getOutSchema(), subOp);
    }

    return new StoreTableExec(ctx, plan, subOp);
  }

  public PhysicalExec createScanPlan(TaskAttemptContext ctx, ScanNode scanNode)
      throws IOException {
    Preconditions.checkNotNull(ctx.getTable(scanNode.getTableId()),
        "Error: There is no table matched to %s", scanNode.getTableId());

    Fragment[] fragments = ctx.getTables(scanNode.getTableId());
    return new SeqScanExec(ctx, sm, scanNode, fragments);
  }

  public PhysicalExec createGroupByPlan(TaskAttemptContext ctx,
                                        GroupbyNode groupbyNode, PhysicalExec subOp) throws IOException {
    Column[] grpColumns = groupbyNode.getGroupingColumns();
    if (grpColumns.length == 0) {
      LOG.info("The planner chooses [Hash Aggregation]");
      return new HashAggregateExec(ctx, groupbyNode, subOp);
    } else {
      String [] outerLineage = PlannerUtil.getLineage(groupbyNode.getChild());
      long estimatedSize = estimateSizeRecursive(ctx, outerLineage);
      final long threshold = conf.getLongVar(TajoConf.ConfVars.HASH_AGGREGATION_THRESHOLD);

      // if the relation size is less than the threshold,
      // the hash aggregation will be used.
      if (estimatedSize <= threshold) {
        LOG.info("The planner chooses [Hash Aggregation]");
        return new HashAggregateExec(ctx, groupbyNode, subOp);
      } else {
        SortSpec[] specs = new SortSpec[grpColumns.length];
        for (int i = 0; i < grpColumns.length; i++) {
          specs[i] = new SortSpec(grpColumns[i], true, false);
        }
        SortNode sortNode = new SortNode(specs);
        sortNode.setInSchema(subOp.getSchema());
        sortNode.setOutSchema(subOp.getSchema());
        // SortExec sortExec = new SortExec(sortNode, child);
        ExternalSortExec sortExec = new ExternalSortExec(ctx, sm, sortNode,
            subOp);
        LOG.info("The planner chooses [Sort Aggregation]");
        return new SortAggregateExec(ctx, groupbyNode, sortExec);
      }
    }
  }

  public PhysicalExec createSortPlan(TaskAttemptContext ctx, SortNode sortNode,
                                     PhysicalExec subOp) throws IOException {
    return new ExternalSortExec(ctx, sm, sortNode, subOp);
  }

  public PhysicalExec createIndexScanExec(TaskAttemptContext ctx,
                                          IndexScanNode annotation)
      throws IOException {
    //TODO-general Type Index
    Preconditions.checkNotNull(ctx.getTable(annotation.getTableId()),
        "Error: There is no table matched to %s", annotation.getTableId());

    Fragment[] fragments = ctx.getTables(annotation.getTableId());

    String indexName = IndexUtil.getIndexNameOfFrag(fragments[0],
        annotation.getSortKeys());
    Path indexPath = new Path(sm.getTablePath(annotation.getTableId()), "index");

    TupleComparator comp = new TupleComparator(annotation.getKeySchema(),
        annotation.getSortKeys());
    return new BSTIndexScanExec(ctx, sm, annotation, fragments[0], new Path(
        indexPath, indexName), annotation.getKeySchema(), comp,
        annotation.getDatum());

  }
}
