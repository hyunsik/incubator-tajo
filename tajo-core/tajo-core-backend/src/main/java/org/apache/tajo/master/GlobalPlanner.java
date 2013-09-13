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

package org.apache.tajo.master;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.QueryId;
import org.apache.tajo.QueryIdFactory;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.planner.FromTable;
import org.apache.tajo.engine.planner.PlannerUtil;
import org.apache.tajo.engine.planner.global.MasterPlan;
import org.apache.tajo.engine.planner.logical.*;
import org.apache.tajo.master.ExecutionBlock.PartitionType;
import org.apache.tajo.storage.AbstractStorageManager;
import org.apache.tajo.util.TajoIdUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.tajo.conf.TajoConf.ConfVars;

public class GlobalPlanner {
  private static Log LOG = LogFactory.getLog(GlobalPlanner.class);

  private TajoConf conf;
  private AbstractStorageManager sm;
  private QueryId queryId;

  public GlobalPlanner(final TajoConf conf,
                       final AbstractStorageManager sm,
                       final EventHandler eventHandler)
      throws IOException {
    this.conf = conf;
    this.sm = sm;
  }

  /**
   * Builds a master plan from the given logical plan.
   * @param queryId
   * @param rootNode
   * @return
   * @throws IOException
   */
  public MasterPlan build(QueryId queryId, LogicalRootNode rootNode)
      throws IOException {
    this.queryId = queryId;

    String outputTableName = null;
    if (rootNode.getChild().getType() == NodeType.STORE) {
      // create table queries are executed by the master
      StoreTableNode storeTableNode = (StoreTableNode) rootNode.getChild();
      outputTableName = storeTableNode.getTableName();
    }

    // insert store at the subnode of the root
    UnaryNode root = rootNode;
    if (root.getChild().getType() != NodeType.STORE) {
      ExecutionBlockId executionBlockId = QueryIdFactory.newExecutionBlockId(this.queryId);
      outputTableName = executionBlockId.toString();
      insertStore(executionBlockId.toString(),root).setLocal(false);
    }
    
    // convert 2-phase plan
    LogicalNode twoPhased = convertTo2Phase(rootNode);

    // make query graph
    MasterPlan globalPlan = convertToGlobalPlan(twoPhased);
    globalPlan.setOutputTableName(outputTableName);

    return globalPlan;
  }
  
  private StoreTableNode insertStore(String tableId, LogicalNode parent) {
    StoreTableNode store = new StoreTableNode(tableId);
    store.setLocal(true);
    PlannerUtil.insertNode(parent, store);
    return store;
  }
  
  /**
   * Transforms a logical plan to a two-phase plan. 
   * Store nodes are inserted for every logical nodes except store and scan nodes
   */
  private class GlobalPlanBuilder implements LogicalNodeVisitor {
    @Override
    public void visit(LogicalNode node) {
      String tableId;
      StoreTableNode store;
      if (node.getType() == NodeType.GROUP_BY) {
        // transform group by to two-phase plan 
        GroupbyNode groupby = (GroupbyNode) node;
        // insert a store for the child of first group by
        if (groupby.getChild().getType() != NodeType.UNION &&
            groupby.getChild().getType() != NodeType.STORE &&
            groupby.getChild().getType() != NodeType.SCAN) {
          tableId = QueryIdFactory.newExecutionBlockId(queryId).toString();
          insertStore(tableId, groupby);
        }
        tableId = QueryIdFactory.newExecutionBlockId(queryId).toString();
        // insert (a store for the first group by) and (a second group by)
        PlannerUtil.transformGroupbyTo2PWithStore((GroupbyNode)node, tableId);
      } else if (node.getType() == NodeType.SORT) {
        // transform sort to two-phase plan 
        SortNode sort = (SortNode) node;
        // insert a store for the child of first sort
        if (sort.getChild().getType() != NodeType.UNION &&
            sort.getChild().getType() != NodeType.STORE &&
            sort.getChild().getType() != NodeType.SCAN) {
          tableId = QueryIdFactory.newExecutionBlockId(queryId).toString();
          insertStore(tableId, sort);
        }
        tableId = QueryIdFactory.newExecutionBlockId(queryId).toString();
        // insert (a store for the first sort) and (a second sort)
        PlannerUtil.transformSortTo2PWithStore((SortNode)node, tableId);
      } else if (node.getType() == NodeType.JOIN) {
        // transform join to two-phase plan 
        // the first phase of two-phase join can be any logical nodes
        JoinNode join = (JoinNode) node;

        if (join.getRightChild().getType() == NodeType.SCAN &&
            join.getLeftChild().getType() == NodeType.SCAN) {
          ScanNode outerScan = (ScanNode) join.getRightChild();
          ScanNode innerScan = (ScanNode) join.getLeftChild();


          TableMeta outerMeta = outerScan.getFromTable().getTableDesc().getMeta();
          TableMeta innerMeta = innerScan.getFromTable().getTableDesc().getMeta();
          long threshold = conf.getLongVar(ConfVars.BROADCAST_JOIN_THRESHOLD);


          // if the broadcast join is available
          boolean outerSmall = false;
          boolean innerSmall = false;
          if (!outerScan.isLocal() && outerMeta.getStat() != null &&
              outerMeta.getStat().getNumBytes() <= threshold) {
            outerSmall = true;
            LOG.info("The relation (" + outerScan.getTableId() +
                ") is less than " + threshold);
          }
          if (!innerScan.isLocal() && innerMeta.getStat() != null &&
              innerMeta.getStat().getNumBytes() <= threshold) {
            innerSmall = true;
            LOG.info("The relation (" + innerScan.getTableId() +
                ") is less than " + threshold);
          }

          if (outerSmall && innerSmall) {
            if (outerMeta.getStat().getNumBytes() <=
                innerMeta.getStat().getNumBytes()) {
              outerScan.setBroadcast();
              LOG.info("The relation " + outerScan.getTableId()
                  + " is broadcasted");
            } else {
              innerScan.setBroadcast();
              LOG.info("The relation " + innerScan.getTableId()
                  + " is broadcasted");
            }
          } else {
            if (outerSmall) {
              outerScan.setBroadcast();
              LOG.info("The relation (" + outerScan.getTableId()
                  + ") is broadcasted");
            } else if (innerSmall) {
              innerScan.setBroadcast();
              LOG.info("The relation (" + innerScan.getTableId()
                  + ") is broadcasted");
            }
          }

          if (outerScan.isBroadcast() || innerScan.isBroadcast()) {
            return;
          }
        }

        // insert stores for the first phase
        if (join.getLeftChild().getType() != NodeType.UNION &&
            join.getLeftChild().getType() != NodeType.STORE) {
          tableId = QueryIdFactory.newExecutionBlockId(queryId).toString();
          store = new StoreTableNode(tableId);
          store.setLocal(true);
          PlannerUtil.insertOuterNode(node, store);
        }
        if (join.getRightChild().getType() != NodeType.UNION &&
            join.getRightChild().getType() != NodeType.STORE) {
          tableId = QueryIdFactory.newExecutionBlockId(queryId).toString();
          store = new StoreTableNode(tableId);
          store.setLocal(true);
          PlannerUtil.insertInnerNode(node, store);
        }
      } else if (node.getType() == NodeType.UNION) {
        // not two-phase transform
        UnionNode union = (UnionNode) node;
        // insert stores
        if (union.getLeftChild().getType() != NodeType.UNION &&
            union.getLeftChild().getType() != NodeType.STORE) {
          tableId = QueryIdFactory.newExecutionBlockId(queryId).toString();
          store = new StoreTableNode(tableId);
          if(union.getLeftChild().getType() == NodeType.GROUP_BY) {
            /*This case is for cube by operator
             * TODO : more complicated conidtion*/
            store.setLocal(true);
          } else {
            /* This case is for union query*/
            store.setLocal(false);
          }
          PlannerUtil.insertOuterNode(node, store);
        }
        if (union.getRightChild().getType() != NodeType.UNION &&
            union.getRightChild().getType() != NodeType.STORE) {
          tableId = QueryIdFactory.newExecutionBlockId(queryId).toString();
          store = new StoreTableNode(tableId);
          if(union.getRightChild().getType() == NodeType.GROUP_BY) {
            /*This case is for cube by operator
             * TODO : more complicated conidtion*/
            store.setLocal(true);
          }else {
            /* This case is for union query*/
            store.setLocal(false);
          }
          PlannerUtil.insertInnerNode(node, store);
        }
      } else if (node instanceof UnaryNode) {
        UnaryNode unary = (UnaryNode)node;
        if (unary.getType() != NodeType.STORE &&
            unary.getChild().getType() != NodeType.STORE) {
          tableId = QueryIdFactory.newExecutionBlockId(queryId).toString();
          insertStore(tableId, unary);
        }
      }
    }
  }

  /**
   * Convert the logical plan to a two-phase plan by the post-order traverse.
   * 
   * @param logicalPlan
   * @return
   */
  private LogicalNode convertTo2Phase(LogicalNode logicalPlan) {
    LogicalRootNode root = (LogicalRootNode) logicalPlan;
    root.postOrder(new GlobalPlanBuilder());
    return logicalPlan;
  }
  
  private Map<StoreTableNode, ExecutionBlock> convertMap =
      new HashMap<StoreTableNode, ExecutionBlock>();
  
  /**
   * Logical plan을 후위 탐색하면서 SubQuery 생성
   * 
   * @param node 현재 방문 중인 노드
   * @throws IOException
   */
  private void recursiveBuildSubQuery(LogicalNode node)
      throws IOException {
    ExecutionBlock subQuery;
    StoreTableNode store;
    if (node instanceof UnaryNode) {
      recursiveBuildSubQuery(((UnaryNode) node).getChild());
      
      if (node.getType() == NodeType.STORE) {
        store = (StoreTableNode) node;
        ExecutionBlockId id;
        if (store.getTableName().startsWith(ExecutionBlockId.EB_ID_PREFIX)) {
          id = TajoIdUtils.createExecutionBlockId(store.getTableName());
        } else {
          id = QueryIdFactory.newExecutionBlockId(queryId);
        }
        subQuery = new ExecutionBlock(id);

        switch (store.getChild().getType()) {
        case BST_INDEX_SCAN:
        case SCAN:  // store - scan
          subQuery = makeScanSubQuery(subQuery);
          subQuery.setPlan(node);
          break;
        case SELECTION:
        case PROJECTION:
        case LIMIT:
          subQuery = makeUnarySubQuery(store, node, subQuery);
          subQuery.setPlan(node);
          break;
        case GROUP_BY:
          subQuery = makeGroupbySubQuery(store, node, subQuery);
          subQuery.setPlan(node);
          break;
        case SORT:
          subQuery = makeSortSubQuery(store, node, subQuery);
          subQuery.setPlan(node);
          break;
        case JOIN:  // store - join
          subQuery = makeJoinSubQuery(store, node, subQuery);
          subQuery.setPlan(node);
          break;
        case UNION:
          subQuery = makeUnionSubQuery(store, node, subQuery);
          subQuery.setPlan(node);
          break;
        default:
          subQuery = null;
          break;
        }

        convertMap.put(store, subQuery);
      }
    } else if (node instanceof BinaryNode) {
      recursiveBuildSubQuery(((BinaryNode) node).getLeftChild());
      recursiveBuildSubQuery(((BinaryNode) node).getRightChild());
    } else if (node instanceof ScanNode) {

    } else {

    }
  }
  
  private ExecutionBlock makeScanSubQuery(ExecutionBlock block) {
    block.setPartitionType(PartitionType.LIST);
    return block;
  }
  
  /**
   * Unifiable node(selection, projection)을 자식 플랜과 같은 SubQuery로 생성
   * 
   * @param rootStore 생성할 SubQuery의 store
   * @param plan logical plan
   * @param unit 생성할 SubQuery
   * @return
   * @throws IOException
   */
  private ExecutionBlock makeUnarySubQuery(StoreTableNode rootStore,
                                     LogicalNode plan, ExecutionBlock unit) throws IOException {
    ScanNode newScan;
    ExecutionBlock prev;
    UnaryNode unary = (UnaryNode) plan;
    UnaryNode child = (UnaryNode) unary.getChild();
    StoreTableNode prevStore = (StoreTableNode)child.getChild();

    // add scan
    newScan = GlobalPlannerUtils.newScanPlan(prevStore.getOutSchema(),
        prevStore.getTableName(), sm.getTablePath(prevStore.getTableName()));
    newScan.setLocal(true);
    child.setChild(newScan);
    prev = convertMap.get(prevStore);

    if (prev != null) {
      prev.setParentBlock(unit);
      unit.addChildBlock(newScan, prev);
      prev.setPartitionType(PartitionType.LIST);
    }

    unit.setPartitionType(PartitionType.LIST);

    return unit;
  }
  
  /**
   * Two-phase SubQuery 생성.
   * 
   * @param rootStore 생성할 SubQuery의 store
   * @param plan logical plan
   * @param unit 생성할 SubQuery
   * @return
   * @throws IOException
   */
  private ExecutionBlock makeGroupbySubQuery(StoreTableNode rootStore,
                                       LogicalNode plan, ExecutionBlock unit) throws IOException {
    UnaryNode unary = (UnaryNode) plan;
    UnaryNode unaryChild;
    StoreTableNode prevStore;
    ScanNode newScan;
    ExecutionBlock prev;
    unaryChild = (UnaryNode) unary.getChild();  // groupby
    NodeType curType = unaryChild.getType();
    if (unaryChild.getChild().getType() == NodeType.STORE) {
      // store - groupby - store
      unaryChild = (UnaryNode) unaryChild.getChild(); // store
      prevStore = (StoreTableNode) unaryChild;
      newScan = GlobalPlannerUtils.newScanPlan(prevStore.getOutSchema(),
          prevStore.getTableName(),
          sm.getTablePath(prevStore.getTableName()));
      newScan.setLocal(true);
      ((UnaryNode) unary.getChild()).setChild(newScan);
      prev = convertMap.get(prevStore);
      if (prev != null) {
        prev.setParentBlock(unit);
        unit.addChildBlock(newScan, prev);
      }

      if (unaryChild.getChild().getType() == curType) {
        // the second phase
        unit.setPartitionType(PartitionType.LIST);
        if (prev != null) {
          prev.setPartitionType(PartitionType.HASH);
        }
      } else {
        // the first phase
        unit.setPartitionType(PartitionType.HASH);
        if (prev != null) {
          prev.setPartitionType(PartitionType.LIST);
        }
      }
    } else if (unaryChild.getChild().getType() == NodeType.SCAN) {
      // the first phase
      // store - groupby - scan
      unit.setPartitionType(PartitionType.HASH);
    } else if (unaryChild.getChild().getType() == NodeType.UNION) {
      _handleUnionNode(rootStore, (UnionNode)unaryChild.getChild(), unit,
          null, PartitionType.LIST);
    } else {
      // error
    }
    return unit;
  }
  
  /**
   *
   *
   * @param rootStore 생성할 SubQuery의 store
   * @param plan logical plan
   * @param unit 생성할 SubQuery
   * @return
   * @throws IOException
   */
  private ExecutionBlock makeUnionSubQuery(StoreTableNode rootStore,
                                     LogicalNode plan, ExecutionBlock unit) throws IOException {
    UnaryNode unary = (UnaryNode) plan;
    StoreTableNode outerStore, innerStore;
    ExecutionBlock prev;
    UnionNode union = (UnionNode) unary.getChild();
    unit.setPartitionType(PartitionType.LIST);
    
    if (union.getLeftChild().getType() == NodeType.STORE) {
      outerStore = (StoreTableNode) union.getLeftChild();
      TableMeta outerMeta = CatalogUtil.newTableMeta(outerStore.getOutSchema(),
          StoreType.CSV);
      insertOuterScan(union, outerStore.getTableName(), outerMeta);
      prev = convertMap.get(outerStore);
      if (prev != null) {
        prev.getStoreTableNode().setTableName(rootStore.getTableName());
        prev.setPartitionType(PartitionType.LIST);
        prev.setParentBlock(unit);
        unit.addChildBlock((ScanNode) union.getLeftChild(), prev);
      }
    } else if (union.getLeftChild().getType() == NodeType.UNION) {
      _handleUnionNode(rootStore, union, unit, null, PartitionType.LIST);
    }
    
    if (union.getRightChild().getType() == NodeType.STORE) {
      innerStore = (StoreTableNode) union.getRightChild();
      TableMeta innerMeta = CatalogUtil.newTableMeta(innerStore.getOutSchema(),
          StoreType.CSV);
      insertInnerScan(union, innerStore.getTableName(), innerMeta);
      prev = convertMap.get(innerStore);
      if (prev != null) {
        prev.getStoreTableNode().setTableName(rootStore.getTableName());
        prev.setPartitionType(PartitionType.LIST);
        prev.setParentBlock(unit);
        unit.addChildBlock((ScanNode) union.getRightChild(), prev);
      }
    } else if (union.getRightChild().getType() == NodeType.UNION) {
      _handleUnionNode(rootStore, union, unit, null, PartitionType.LIST);
    }

    return unit;
  }

  private ExecutionBlock makeSortSubQuery(StoreTableNode rootStore,
                                    LogicalNode plan, ExecutionBlock unit) throws IOException {

    UnaryNode unary = (UnaryNode) plan;
    UnaryNode unaryChild;
    StoreTableNode prevStore;
    ScanNode newScan;
    ExecutionBlock prev;
    unaryChild = (UnaryNode) unary.getChild();  // groupby
    NodeType curType = unaryChild.getType();
    if (unaryChild.getChild().getType() == NodeType.STORE) {
      // store - groupby - store
      unaryChild = (UnaryNode) unaryChild.getChild(); // store
      prevStore = (StoreTableNode) unaryChild;
      newScan = GlobalPlannerUtils.newScanPlan(prevStore.getOutSchema(),
          prevStore.getTableName(), sm.getTablePath(prevStore.getTableName()));
      newScan.setLocal(true);
      ((UnaryNode) unary.getChild()).setChild(newScan);
      prev = convertMap.get(prevStore);
      if (prev != null) {
        prev.setParentBlock(unit);
        unit.addChildBlock(newScan, prev);
        if (unaryChild.getChild().getType() == curType) {
          // TODO - this is duplicated code
          prev.setPartitionType(PartitionType.RANGE);
        } else {
          prev.setPartitionType(PartitionType.LIST);
        }
      }
      if (unaryChild.getChild().getType() == curType) {
        // the second phase
        unit.setPartitionType(PartitionType.LIST);
      } else {
        // the first phase
        unit.setPartitionType(PartitionType.HASH);
      }
    } else if (unaryChild.getChild().getType() == NodeType.SCAN) {
      // the first phase
      // store - sort - scan
      unit.setPartitionType(PartitionType.RANGE);
    } else if (unaryChild.getChild().getType() == NodeType.UNION) {
      _handleUnionNode(rootStore, (UnionNode)unaryChild.getChild(), unit,
          null, PartitionType.LIST);
    } else {
      // error
    }
    return unit;
  }
  
  private ExecutionBlock makeJoinSubQuery(StoreTableNode rootStore,
                                    LogicalNode plan, ExecutionBlock unit) throws IOException {
    UnaryNode unary = (UnaryNode)plan;
    StoreTableNode outerStore, innerStore;
    ExecutionBlock prev;
    JoinNode join = (JoinNode) unary.getChild();
    Schema outerSchema = join.getLeftChild().getOutSchema();
    Schema innerSchema = join.getRightChild().getOutSchema();
    unit.setPartitionType(PartitionType.LIST);

    List<Column> outerCollist = new ArrayList<Column>();
    List<Column> innerCollist = new ArrayList<Column>();
    
    // TODO: set partition for store nodes
    if (join.hasJoinQual()) {
      // getting repartition keys
      List<Column[]> cols = PlannerUtil.getJoinKeyPairs(join.getJoinQual(), outerSchema, innerSchema);
      for (Column [] pair : cols) {
        outerCollist.add(pair[0]);
        innerCollist.add(pair[1]);
      }
    } else {
      // broadcast
    }
    
    Column[] outerCols = new Column[outerCollist.size()];
    Column[] innerCols = new Column[innerCollist.size()];
    outerCols = outerCollist.toArray(outerCols);
    innerCols = innerCollist.toArray(innerCols);
    
    // outer
    if (join.getLeftChild().getType() == NodeType.STORE) {
      outerStore = (StoreTableNode) join.getLeftChild();
      TableMeta outerMeta = CatalogUtil.newTableMeta(outerStore.getOutSchema(),
          StoreType.CSV);
      insertOuterScan(join, outerStore.getTableName(), outerMeta);
      prev = convertMap.get(outerStore);
      if (prev != null) {
        prev.setPartitionType(PartitionType.HASH);
        prev.setParentBlock(unit);
        unit.addChildBlock((ScanNode) join.getLeftChild(), prev);
      }
      outerStore.setPartitions(PartitionType.HASH, outerCols, 32);
    } else if (join.getLeftChild().getType() == NodeType.UNION) {
      _handleUnionNode(rootStore, (UnionNode)join.getLeftChild(), unit,
          outerCols, PartitionType.HASH);
    } else {

    }
    
    // inner
    if (join.getRightChild().getType() == NodeType.STORE) {
      innerStore = (StoreTableNode) join.getRightChild();
      TableMeta innerMeta = CatalogUtil.newTableMeta(innerStore.getOutSchema(),
          StoreType.CSV);
      insertInnerScan(join, innerStore.getTableName(), innerMeta);
      prev = convertMap.get(innerStore);
      if (prev != null) {
        prev.setPartitionType(PartitionType.HASH);
        prev.setParentBlock(unit);
        unit.addChildBlock((ScanNode) join.getRightChild(), prev);
      }
      innerStore.setPartitions(PartitionType.HASH, innerCols, 32);
    } else if (join.getRightChild().getType() == NodeType.UNION) {
      _handleUnionNode(rootStore, (UnionNode)join.getRightChild(), unit,
          innerCols, PartitionType.HASH);
    }
    
    return unit;
  }
  
  /**
   * Recursive하게 union의 자식 plan들을 설정
   * 
   * @param rootStore 생성할 SubQuery의 store
   * @param union union을 root로 하는 logical plan
   * @param cur 생성할 SubQuery
   * @param cols partition 정보를 설정하기 위한 column array
   * @param prevOutputType 자식 SubQuery의 partition type
   * @throws IOException
   */
  private void _handleUnionNode(StoreTableNode rootStore, UnionNode union, 
      ExecutionBlock cur, Column[] cols, PartitionType prevOutputType)
          throws IOException {
    StoreTableNode store;
    TableMeta meta;
    ExecutionBlock prev;
    
    if (union.getLeftChild().getType() == NodeType.STORE) {
      store = (StoreTableNode) union.getLeftChild();
      meta = CatalogUtil.newTableMeta(store.getOutSchema(), StoreType.CSV);
      insertOuterScan(union, store.getTableName(), meta);
      prev = convertMap.get(store);
      if (prev != null) {
        prev.getStoreTableNode().setTableName(rootStore.getTableName());
        prev.setPartitionType(prevOutputType);
        prev.setParentBlock(cur);
        cur.addChildBlock((ScanNode) union.getLeftChild(), prev);
      }
      if (cols != null) {
        store.setPartitions(PartitionType.LIST, cols, 32);
      }
    } else if (union.getLeftChild().getType() == NodeType.UNION) {
      _handleUnionNode(rootStore, (UnionNode)union.getLeftChild(), cur, cols,
          prevOutputType);
    }
    
    if (union.getRightChild().getType() == NodeType.STORE) {
      store = (StoreTableNode) union.getRightChild();
      meta = CatalogUtil.newTableMeta(store.getOutSchema(), StoreType.CSV);
      insertInnerScan(union, store.getTableName(), meta);
      prev = convertMap.get(store);
      if (prev != null) {
        prev.getStoreTableNode().setTableName(rootStore.getTableName());
        prev.setPartitionType(prevOutputType);
        prev.setParentBlock(cur);
        cur.addChildBlock((ScanNode) union.getRightChild(), prev);
      }
      if (cols != null) {
        store.setPartitions(PartitionType.LIST, cols, 32);
      }
    } else if (union.getRightChild().getType() == NodeType.UNION) {
      _handleUnionNode(rootStore, (UnionNode)union.getRightChild(), cur, cols,
          prevOutputType);
    }
  }
  
  private LogicalNode insertOuterScan(BinaryNode parent, String tableId,
      TableMeta meta) throws IOException {
    TableDesc desc = CatalogUtil.newTableDesc(tableId, meta, sm.getTablePath(tableId));
    ScanNode scan = new ScanNode(new FromTable(desc));
    scan.setLocal(true);
    scan.setInSchema(meta.getSchema());
    scan.setOutSchema(meta.getSchema());
    parent.setLeftChild(scan);
    return parent;
  }
  
  private LogicalNode insertInnerScan(BinaryNode parent, String tableId, 
      TableMeta meta) throws IOException {
    TableDesc desc = CatalogUtil.newTableDesc(tableId, meta, sm.getTablePath(tableId));
    ScanNode scan = new ScanNode(new FromTable(desc));
    scan.setLocal(true);
    scan.setInSchema(meta.getSchema());
    scan.setOutSchema(meta.getSchema());
    parent.setRightChild(scan);
    return parent;
  }
  
  private MasterPlan convertToGlobalPlan(LogicalNode logicalPlan) throws IOException {
    recursiveBuildSubQuery(logicalPlan);
    ExecutionBlock root;

    root = convertMap.get(((LogicalRootNode)logicalPlan).getChild());
    root.getStoreTableNode().setLocal(false);

    return new MasterPlan(root);
  }
}
