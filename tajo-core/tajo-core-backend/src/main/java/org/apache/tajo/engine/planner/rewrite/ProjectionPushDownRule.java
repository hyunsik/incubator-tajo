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

package org.apache.tajo.engine.planner.rewrite;

import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.engine.eval.EvalNode;
import org.apache.tajo.engine.eval.EvalTreeUtil;
import org.apache.tajo.engine.eval.EvalType;
import org.apache.tajo.engine.eval.FieldEval;
import org.apache.tajo.engine.exception.InvalidQueryException;
import org.apache.tajo.engine.planner.*;
import org.apache.tajo.engine.planner.logical.*;
import org.apache.tajo.engine.utils.SchemaUtil;
import org.apache.tajo.util.TUtil;

import java.util.*;

public class ProjectionPushDownRule extends
    BasicLogicalPlanVisitor<ProjectionPushDownRule.Context, LogicalNode> implements RewriteRule {
  /** Class Logger */
  private final Log LOG = LogFactory.getLog(ProjectionPushDownRule.class);
  private static final String name = "ProjectionPushDown";

  @Override
  public String getName() {
    return name;
  }

  @Override
  public boolean isEligible(LogicalPlan plan) {
    LogicalNode toBeOptimized = plan.getRootBlock().getRoot();

    if (PlannerUtil.checkIfDDLPlan(toBeOptimized) || !plan.getRootBlock().hasTableExpression()) {
      LOG.info("This query skips the logical optimization step.");
      return false;
    }

    return true;
  }

  @Override
  public LogicalPlan rewrite(LogicalPlan plan) throws PlanningException {
    LogicalPlan.QueryBlock rootBlock = plan.getRootBlock();

    LogicalPlan.QueryBlock topmostBlock;

    // skip a non-table-expression block.
    if (plan.getRootBlock().getRootType() == NodeType.INSERT) {
      topmostBlock = plan.getChildBlocks(rootBlock).get(0);
    } else {
      topmostBlock = rootBlock;
    }

    Stack<LogicalNode> stack = new Stack<LogicalNode>();
    Context context = new Context(plan);
    visit(context, plan, topmostBlock, topmostBlock.getRoot(), stack);

    return plan;
  }

  public static class TargetListManager {
    private LinkedHashMap<String, EvalNode> nameToEvalMap;
    private LinkedHashMap<EvalNode, String> evalToNameMap;
    private LinkedHashMap<String, Boolean> resolvedFlags;
    private LogicalPlan plan;

    public TargetListManager(LogicalPlan plan) {
      this.plan = plan;
      nameToEvalMap = new LinkedHashMap<String, EvalNode>();
      evalToNameMap = new LinkedHashMap<EvalNode, String>();
      resolvedFlags = new LinkedHashMap<String, Boolean>();
    }

    public TargetListManager(TargetListManager targetListMgr) {
      this.plan = targetListMgr.plan;
      nameToEvalMap = new LinkedHashMap<String, EvalNode>(targetListMgr.nameToEvalMap);
      evalToNameMap = new LinkedHashMap<EvalNode, String>(targetListMgr.evalToNameMap);
      resolvedFlags = new LinkedHashMap<String, Boolean>(targetListMgr.resolvedFlags);
    }

    private String add(String name, EvalNode evalNode) throws PlanningException {
      if (evalToNameMap.containsKey(evalNode)) {
        name = evalToNameMap.get(evalNode);
      } else {

        // Name can be conflicts between a column reference and an aliased EvalNode.
        // Example, a SQL statement 'select l_orderkey + l_partkey as total ...' leads to
        // two EvalNodes: a column reference total and an EvalNode (+, l_orderkey, l_partkey)
        // If they are inserted into here, their names are conflict to each other, and one of them is removed.
        // In this case, we just keep an original eval node instead of a column reference.
        // This is because a column reference that points to an aliased EvalNode can be restored from the given alias.
        if (nameToEvalMap.containsKey(name)) {
          EvalNode storedEvalNode = nameToEvalMap.get(name);
          if (!storedEvalNode.equals(evalNode)) {
            if (storedEvalNode.getType() != EvalType.FIELD && evalNode.getType() != EvalType.FIELD) {
              throw new PlanningException("Duplicate alias: " + evalNode);
            }
            if (storedEvalNode.getType() == EvalType.FIELD) {
              nameToEvalMap.put(name, evalNode);
            }
          }
        } else {
          nameToEvalMap.put(name, evalNode);
        }

        evalToNameMap.put(evalNode, name);
        resolvedFlags.put(name, false);

        for (Column column : EvalTreeUtil.findDistinctRefColumns(evalNode)) {
          add(new FieldEval(column));
        }
      }
      return name;
    }

    public String add(Target target) throws PlanningException {
      return add(target.getCanonicalName(), target.getEvalTree());
    }

    public String add(EvalNode evalNode) throws PlanningException {
      String name;
      if (evalToNameMap.containsKey(evalNode)) {
        name = evalToNameMap.get(evalNode);
      } else {
        if (evalNode.getType() == EvalType.FIELD) {
          FieldEval fieldEval = (FieldEval) evalNode;
          name = fieldEval.getName();
        } else {
          name = plan.newGeneratedFieldName(evalNode);
        }
        add(name, evalNode);
      }
      return name;
    }

    public Target getTarget(String name) {
      if (!nameToEvalMap.containsKey(name)) {
        throw new RuntimeException("No Such target name: " + name);
      }
      EvalNode evalNode = nameToEvalMap.get(name);
      if (evalNode.getType() == EvalType.FIELD) {
        return new Target((FieldEval)evalNode);
      } else {
        return new Target(evalNode, name);
      }
    }

    public boolean isResolved(EvalNode evalNode) {
      if (!evalToNameMap.containsKey(evalNode)) {
        throw new RuntimeException("No such eval: " + evalNode);
      }
      return resolvedFlags.get(name);
    }

    public boolean isResolved(String name) {
      if (!nameToEvalMap.containsKey(name)) {
        throw new RuntimeException("No Such target name: " + name);
      }

      return resolvedFlags.get(name);
    }

    public void resolve(Target target) {
      resolve(target.getEvalTree());
    }

    public void resolve(EvalNode evalNode) {
      if (!evalToNameMap.containsKey(evalNode)) {
        throw new RuntimeException("No such eval: " + evalNode);
      }
      String name = evalToNameMap.get(evalNode);
      resolvedFlags.put(name, true);
    }

    public Iterator<Target> getFilteredTarget(Set<String> required) {
      return new FilteredTargetIterator(required);
    }

    class FilteredTargetIterator implements Iterator<Target> {
      List<Target> filtered = TUtil.newList();

      public FilteredTargetIterator(Set<String> required) {
        for (Map.Entry<String,EvalNode> entry : nameToEvalMap.entrySet()) {
          if (required.contains(entry.getKey())) {
            filtered.add(getTarget(entry.getKey()));
          }
        }
      }

      @Override
      public boolean hasNext() {
        return false;
      }

      @Override
      public Target next() {
        return null;
      }

      @Override
      public void remove() {
      }
    }

    public String toString() {
      int resolved = 0;
      for (Boolean flag: resolvedFlags.values()) {
        if (flag) {
          resolved++;
        }
      }
      return "eval=" + resolvedFlags.size() + ", resolved=" + resolved;
    }
  }

  static class Context {
    TargetListManager targetListMgr;
    Set<String> requiredSet;

    public Context(LogicalPlan plan) {
      targetListMgr = new TargetListManager(plan);
      requiredSet = new HashSet<String>();
    }

    public Context(Context upperContext) {
      requiredSet = new HashSet<String>(upperContext.requiredSet);
      targetListMgr = upperContext.targetListMgr;
    }

    public String addExpr(Target target) throws PlanningException {
      String reference = targetListMgr.add(target);
      addNecessaryReferences(target.getEvalTree());
      return reference;
    }

    public String addExpr(EvalNode evalNode) throws PlanningException {
      String reference = targetListMgr.add(evalNode);
      addNecessaryReferences(evalNode);
      return reference;
    }

    private void addNecessaryReferences(EvalNode evalNode) {
      for (Column column : EvalTreeUtil.findDistinctRefColumns(evalNode)) {
        requiredSet.add(column.getQualifiedName());
      }
    }

    @Override
    public String toString() {
      return "required=" + requiredSet.size() + "," + targetListMgr.toString();
    }
  }

  @Override
  public LogicalNode visitProjection(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                     ProjectionNode node, Stack<LogicalNode> stack) throws PlanningException {
    Context newContext = new Context(context);
    String [] referenceNames = new String[node.getTargets().length];
    for (int i = 0; i < node.getTargets().length; i++) {
      referenceNames[i] = newContext.addExpr(node.getTargets()[i]);
    }

    LogicalNode child = super.visitProjection(newContext, plan, block, node, stack);

    List<Target> finalTargets = TUtil.newList();
    for (String referenceName : referenceNames) {
      Target target = context.targetListMgr.getTarget(referenceName);

      if (context.targetListMgr.isResolved(referenceName)) {
        finalTargets.add(new Target(new FieldEval(target.getNamedColumn())));
      } else if (checkIfBeEvaluate(target, node)) {
        finalTargets.add(target);
        context.targetListMgr.resolve(target);
      }
    }
    node.setInSchema(child.getOutSchema());
    node.setTargets(finalTargets.toArray(new Target[finalTargets.size()]));

    return node;
  }

  public LogicalNode visitLimit(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block, LimitNode node,
                           Stack<LogicalNode> stack) throws PlanningException {
    LogicalNode child = super.visitLimit(context, plan, block, node, stack);
    return node;
  }

  @Override
  public LogicalNode visitSort(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                               SortNode node, Stack<LogicalNode> stack) throws PlanningException {
    Context newContext = new Context(context);

    final int sortKeyNum = node.getSortKeys().length;
    String [] keyNames = new String[sortKeyNum];
    for (int i = 0; i < sortKeyNum; i++) {
      SortSpec sortSpec = node.getSortKeys()[i];
      keyNames[i] = context.addExpr(new FieldEval(sortSpec.getSortKey()));
    }

    LogicalNode child = super.visitSort(newContext, plan, block, node, stack);
    return node;
  }

  @Override
  public LogicalNode visitHaving(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block, HavingNode node,
                            Stack<LogicalNode> stack) throws PlanningException {
    Context newContext = new Context(context);
    String referenceName = newContext.targetListMgr.add(node.getQual());
    newContext.addNecessaryReferences(node.getQual());

    LogicalNode child = super.visitHaving(newContext, plan, block, node, stack);
    return node;
  }

  public LogicalNode visitGroupBy(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block, GroupbyNode node,
                             Stack<LogicalNode> stack) throws PlanningException {
    Context newContext = new Context(context);

    final int targetNum = node.getTargets().length;
    final String [] referenceNames = new String[targetNum];
    for (int i = 0; i < targetNum; i++) {
      Target target = node.getTargets()[i];
      referenceNames[i] = newContext.addExpr(target);
    }

    LogicalNode child = super.visitGroupBy(newContext, plan, block, node, stack);

    List<Target> projectedTargets = TUtil.newList();
    for (Iterator<String> it = getFilteredReferences(referenceNames, context.requiredSet); it.hasNext();) {
      String referenceName = it.next();
      Target target = context.targetListMgr.getTarget(referenceName);

      if (context.targetListMgr.isResolved(referenceName)) {
        projectedTargets.add(new Target(new FieldEval(target.getNamedColumn())));
      } else if (checkIfBeEvaluatedForGroupBy(target, node)) {
        projectedTargets.add(target);
        context.targetListMgr.resolve(target);
      }
    }
    node.setInSchema(child.getOutSchema());
    node.setTargets(projectedTargets.toArray(new Target[projectedTargets.size()]));

    node.setInSchema(child.getOutSchema());
    return node;
  }

  public LogicalNode visitFilter(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block,
                                 SelectionNode node, Stack<LogicalNode> stack) throws PlanningException {
    Context newContext = new Context(context);
    newContext.addExpr(node.getQual());

    LogicalNode child = super.visitFilter(newContext, plan, block, node, stack);
    return node;
  }

  public LogicalNode visitJoin(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block, JoinNode node,
                          Stack<LogicalNode> stack) throws PlanningException {
    Context newContext = new Context(context);

    String joinQualReference = null;
    if (node.hasJoinQual()) {
      joinQualReference = newContext.addExpr(node.getJoinQual());
    }

    String [] referenceNames = null;
    if (node.hasTargets()) {
      referenceNames = new String[node.getTargets().length];
      int i = 0;
      for (Iterator<Target> it = getFilteredTarget(node.getTargets(), context.requiredSet);
           it.hasNext();) {
        referenceNames[i++] = newContext.addExpr(it.next());
      }
    }

    stack.push(node);
    LogicalNode left = visit(newContext, plan, block, node.getLeftChild(), stack);
    LogicalNode right = visit(newContext, plan, block, node.getRightChild(), stack);
    stack.pop();

    Schema merged = SchemaUtil.merge(left.getOutSchema(), right.getOutSchema());
    List<Target> projectedTargets = TUtil.newList();
    for (Iterator<String> it = getFilteredReferences(referenceNames, context.requiredSet); it.hasNext();) {
      String referenceName = it.next();
      Target target = context.targetListMgr.getTarget(referenceName);

      if (context.targetListMgr.isResolved(referenceName)) {
        projectedTargets.add(new Target(new FieldEval(target.getNamedColumn())));
      } else if (checkIfBeEvaluatedForJoin(target, node)) {
        projectedTargets.add(target);
        context.targetListMgr.resolve(target);
      }
    }
    node.setInSchema(merged);
    node.setTargets(projectedTargets.toArray(new Target[projectedTargets.size()]));
    return node;
  }

  static Iterator<String> getFilteredReferences(String [] referenceNames, Set<String> required) {
    return new FilteredStringsIterator(referenceNames, required);
  }

  static class FilteredStringsIterator implements Iterator<String> {
    Iterator<String> iterator;

    FilteredStringsIterator(String [] referenceNames, Set<String> required) {
      List<String> filtered = TUtil.newList();
      for (String name : referenceNames) {
        if (required.contains(name)) {
          filtered.add(name);
        }
      }

      iterator = filtered.iterator();
    }
    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public String next() {
      return iterator.next();
    }

    @Override
    public void remove() {
    }
  }

  static Iterator<Target> getFilteredTarget(Target[] targets, Set<String> required) {
    return new FilteredIterator(targets, required);
  }

  static class FilteredIterator implements Iterator<Target> {
    Iterator<Target> iterator;

    FilteredIterator(Target [] targets, Set<String> required) {
      List<Target> filtered = TUtil.newList();
      for (Target target : targets) {
        String name = target.getCanonicalName();
        EvalNode evalNode = target.getEvalTree();

        if (required.contains(name)) {
          filtered.add(target);
        }
      }

      iterator = filtered.iterator();
    }
    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public Target next() {
      return iterator.next();
    }

    @Override
    public void remove() {
    }
  }

  public static boolean checkIfBeEvaluatedForScan(Target target, ScanNode node) {
    Set<Column> columnRefs = EvalTreeUtil.findDistinctRefColumns(target.getEvalTree());

    if (EvalTreeUtil.findDistinctAggFunction(target.getEvalTree()).size() > 0) {
      return false;
    }

    if (node.getInSchema().containsAll(columnRefs)) {
      return true;
    } else {
      return false;
    }
  }

  public static boolean checkIfBeEvaluate(Target target, LogicalNode node) {
    Set<Column> columnRefs = EvalTreeUtil.findDistinctRefColumns(target.getEvalTree());
    if (!node.getInSchema().containsAll(columnRefs)) {
      return false;
    }
    return true;
  }

  public static boolean checkIfBeEvaluatedForGroupBy(Target target, GroupbyNode groupbyNode) {
    Set<Column> columnRefs = EvalTreeUtil.findDistinctRefColumns(target.getEvalTree());

    if (!groupbyNode.getInSchema().containsAll(columnRefs)) {
      return false;
    }

    Set<String> tableIds = Sets.newHashSet();
    // getting distinct table references
    for (Column col : columnRefs) {
      if (!tableIds.contains(col.getQualifier())) {
        tableIds.add(col.getQualifier());
      }
    }

    if (tableIds.size() > 1) {
      return false;
    }

    return true;
  }

  public static boolean checkIfBeEvaluatedForJoin(Target target, JoinNode joinNode) {
    Set<Column> columnRefs = EvalTreeUtil.findDistinctRefColumns(target.getEvalTree());

    if (EvalTreeUtil.findDistinctAggFunction(target.getEvalTree()).size() > 0) {
      return false;
    }

    Schema merged = SchemaUtil.merge(joinNode.getLeftChild().getOutSchema(), joinNode.getRightChild().getOutSchema());
    if (!merged.containsAll(columnRefs)) {
      return false;
    }

    if (columnRefs.size() == 1) {
      return true;
    }

    Set<String> tableIds = Sets.newHashSet();
    // getting distinct table references
    for (Column col : columnRefs) {
      if (!tableIds.contains(col.getQualifier())) {
        tableIds.add(col.getQualifier());
      }
    }

    if (tableIds.size() > 0) {
      return true;
    }

    String [] outer = PlannerUtil.getRelationLineage(joinNode.getLeftChild());
    String [] inner = PlannerUtil.getRelationLineage(joinNode.getRightChild());

    Set<String> o = Sets.newHashSet(outer);
    Set<String> i = Sets.newHashSet(inner);
    if (outer == null || inner == null) {
      throw new InvalidQueryException("ERROR: Unexpected logical plan");
    }
    Iterator<String> it = tableIds.iterator();
    if (o.contains(it.next()) && i.contains(it.next())) {
      return true;
    }

    it = tableIds.iterator();

    return i.contains(it.next()) && o.contains(it.next());
  }

  @Override
  public LogicalNode visitUnion(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block, UnionNode node,
                           Stack<LogicalNode> stack) throws PlanningException {
    Context leftContext = new Context(context);
    Context rightContext = new Context(context);

    LogicalPlan.QueryBlock leftBlock = plan.getBlock(node.getLeftChild());
    LogicalPlan.QueryBlock rightBlock = plan.getBlock(node.getRightChild());

    stack.push(node);
    LogicalNode leftChild = visit(leftContext, plan, leftBlock, node.getLeftChild(), stack);
    LogicalNode rightChild = visit(rightContext, plan, rightBlock, node.getRightChild(), stack);
    stack.pop();
    return node;
  }

  public LogicalNode visitScan(Context context, LogicalPlan plan, LogicalPlan.QueryBlock block, ScanNode node,
                          Stack<LogicalNode> stack) throws PlanningException {

    Context newContext = new Context(context);

    Target [] targets;
    if (node.hasTargets()) {
      targets = node.getTargets();
    } else {
      targets = PlannerUtil.schemaToTargets(node.getOutSchema());
    }

    List<Target> projectedTargets = TUtil.newList();
    for (Iterator<Target> it = getFilteredTarget(targets, newContext.requiredSet); it.hasNext();) {
      Target target = it.next();
      newContext.addExpr(target);
    }

    for (Iterator<Target> it = getFilteredTarget(targets, context.requiredSet); it.hasNext();) {
      Target target = it.next();

      if (checkIfBeEvaluatedForScan(target, node)) {
        projectedTargets.add(target);
        newContext.targetListMgr.resolve(target);
      }
    }

    node.setTargets(projectedTargets.toArray(new Target[projectedTargets.size()]));
    return node;
  }
}
