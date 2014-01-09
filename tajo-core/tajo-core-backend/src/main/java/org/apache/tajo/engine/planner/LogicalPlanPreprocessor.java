package org.apache.tajo.engine.planner;

import org.apache.tajo.algebra.*;
import org.apache.tajo.catalog.CatalogService;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.engine.eval.EvalNode;
import org.apache.tajo.engine.eval.EvalType;
import org.apache.tajo.engine.eval.FieldEval;
import org.apache.tajo.engine.planner.logical.*;
import org.apache.tajo.engine.utils.SchemaUtil;

import java.util.Stack;

class LogicalPlanPreprocessor extends BaseAlgebraVisitor<LogicalPlanPreprocessor.PreprocessContext, LogicalNode> {
  private ExprAnnotator annotator;

  static class PreprocessContext {
    LogicalPlan plan;
    LogicalPlan.QueryBlock currentBlock;

    public PreprocessContext(LogicalPlan plan, LogicalPlan.QueryBlock currentBlock) {
      this.plan = plan;
      this.currentBlock = currentBlock;
    }

    public PreprocessContext(PreprocessContext context, LogicalPlan.QueryBlock currentBlock) {
      this.plan = context.plan;
      this.currentBlock = currentBlock;
    }
  }

  private CatalogService catalog;

  LogicalPlanPreprocessor(CatalogService catalog, ExprAnnotator annotator) {
    this.catalog = catalog;
    this.annotator = annotator;
  }

  @Override
  public void preHook(PreprocessContext ctx, Stack<Expr> stack, Expr expr) throws PlanningException {
    ctx.currentBlock.setAlgebraicExpr(expr);
    ctx.plan.mapExprToBlock(expr, ctx.currentBlock.getName());
  }

  @Override
  public LogicalNode postHook(PreprocessContext ctx, Stack<Expr> stack, Expr expr, LogicalNode result) throws PlanningException {
    ctx.currentBlock.setNode(result);
    ctx.currentBlock.mapExprToLogicalNode(expr, result);
    return result;
  }

  @Override
  public LogicalNode visitProjection(PreprocessContext ctx, Stack<Expr> stack, Projection expr) throws PlanningException {
    stack.push(expr); // <--- push
    LogicalNode child = visit(ctx, stack, expr.getChild());

    Target [] targets;
    if (expr.isAllProjected()) {
      targets = PlannerUtil.schemaToTargets(child.getOutSchema());
    } else {
      targets = new Target[expr.getTargets().length];

      for (int i = 0; i < expr.getTargets().length; i++) {
        TargetExpr targetExpr = expr.getTargets()[i];
        EvalNode evalNode = annotator.createEvalNode(ctx.plan, ctx.currentBlock, targetExpr.getExpr());

        if (targetExpr.hasAlias()) {
          targets[i] = new Target(evalNode, targetExpr.getAlias());
        } else if (evalNode.getType() == EvalType.FIELD) {
          targets[i] = new Target(evalNode, ((FieldEval)evalNode).getColumnRef().getQualifiedName());
        } else {
          targets[i] = new Target(evalNode, "$name_" + i);
        }
      }
    }
    stack.pop(); // <--- Pop

    ProjectionNode projectionNode = new ProjectionNode(ctx.plan.newPID());
    projectionNode.setInSchema(child.getOutSchema());
    projectionNode.setOutSchema(PlannerUtil.targetToSchema(targets));
    return projectionNode;
  }

  @Override
  public LogicalNode visitLimit(PreprocessContext ctx, Stack<Expr> stack, Limit expr) throws PlanningException {
    stack.push(expr);
    LogicalNode child = visit(ctx, stack, expr.getChild());
    stack.pop();

    LimitNode limitNode = new LimitNode(ctx.plan.newPID());
    limitNode.setInSchema(child.getOutSchema());
    limitNode.setOutSchema(child.getOutSchema());
    return limitNode;
  }

  @Override
  public LogicalNode visitSort(PreprocessContext ctx, Stack<Expr> stack, Sort expr) throws PlanningException {
    stack.push(expr);
    LogicalNode child = visit(ctx, stack, expr.getChild());
    stack.pop();

    SortNode sortNode = new SortNode(ctx.plan.newPID());
    sortNode.setInSchema(child.getOutSchema());
    sortNode.setOutSchema(child.getOutSchema());
    return sortNode;
  }

  @Override
  public LogicalNode visitGroupBy(PreprocessContext ctx, Stack<Expr> stack, Aggregation expr) throws PlanningException {
    stack.push(expr); // <--- push
    LogicalNode child = visit(ctx, stack, expr.getChild());

    Target [] targets = new Target[expr.getTargets().length];
    int i = 0;
    for (TargetExpr targetExpr : expr.getTargets()) {
      EvalNode evalNode = annotator.createEvalNode(ctx.plan, ctx.currentBlock, targetExpr.getExpr());

      if (targetExpr.hasAlias()) {
        targets[i] = new Target(evalNode, targetExpr.getAlias());
      } else {
        targets[i] = new Target(evalNode, "$name_" + i);
      }
    }
    stack.pop();

    GroupbyNode groupByNode = new GroupbyNode(ctx.plan.newPID());
    groupByNode.setInSchema(child.getOutSchema());
    groupByNode.setOutSchema(PlannerUtil.targetToSchema(targets));
    return groupByNode;
  }

  public LogicalNode visitFilter(PreprocessContext ctx, Stack<Expr> stack, Selection expr) throws PlanningException {
    stack.push(expr);
    LogicalNode child = visit(ctx, stack, expr.getChild());
    stack.pop();

    SelectionNode selectionNode = new SelectionNode(ctx.plan.newPID());
    selectionNode.setInSchema(child.getOutSchema());
    selectionNode.setOutSchema(child.getOutSchema());
    return selectionNode;
  }

  @Override
  public LogicalNode visitJoin(PreprocessContext ctx, Stack<Expr> stack, Join expr) throws PlanningException {
    stack.push(expr);
    LogicalNode left = visit(ctx, stack, expr.getLeft());
    LogicalNode right = visit(ctx, stack, expr.getRight());
    stack.pop();
    JoinNode joinNode = new JoinNode(ctx.plan.newPID());
    Schema merged = SchemaUtil.merge(left.getOutSchema(), right.getOutSchema());
    joinNode.setInSchema(merged);
    joinNode.setOutSchema(merged);
    return joinNode;
  }

  @Override
  public LogicalNode visitRelation(PreprocessContext ctx, Stack<Expr> stack, Relation expr)
      throws PlanningException {

    Relation relation = expr;
    TableDesc desc = catalog.getTableDesc(relation.getName());

    ScanNode scanNode;
    if (relation.hasAlias()) {
      scanNode = new ScanNode(ctx.plan.newPID(), desc, relation.getAlias());
    } else {
      scanNode = new ScanNode(ctx.plan.newPID(), desc);
    }
    ctx.currentBlock.addRelation(scanNode);

    return scanNode;
  }

  @Override
  public LogicalNode visitTableSubQuery(PreprocessContext ctx, Stack<Expr> stack, TablePrimarySubQuery expr)
      throws PlanningException {

    PreprocessContext newContext;
    if (expr.hasAlias()) {
      newContext = new PreprocessContext(ctx, ctx.plan.newAndGetBlock(expr.getAlias()));
    } else {
      newContext = new PreprocessContext(ctx, ctx.plan.newQueryBlock());
    }
    LogicalNode child = super.visitTableSubQuery(newContext, stack, expr);

    // a table subquery is regarded as a relation.
    TableSubQueryNode node = new TableSubQueryNode(ctx.plan.newPID(), newContext.currentBlock.getName(), child);
    ctx.currentBlock.addRelation(node);
    return node;
  }
}
