/*
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

package org.apache.tajo.engine.planner.logical;

import org.apache.tajo.engine.planner.PlanString;

public class CreateDatabaseNode extends LogicalNode {
  private String databaseName;

  public CreateDatabaseNode(int pid) {
    super(pid, NodeType.CREATE_DATABASE);
  }

  public void init(String databaseName) {
    this.databaseName = databaseName;
  }

  public String getDatabaseName() {
    return this.databaseName;
  }

  @Override
  public PlanString getPlanString() {
    return new PlanString(this).appendTitle(databaseName);
  }

  public boolean equals(Object obj) {
    if (obj instanceof CreateDatabaseNode) {
      CreateDatabaseNode other = (CreateDatabaseNode) obj;
      return super.equals(other) && this.databaseName.equals(other.databaseName);
    } else {
      return false;
    }
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    CreateDatabaseNode newNode = (CreateDatabaseNode) super.clone();
    newNode.databaseName = databaseName;
    return newNode;
  }

  @Override
  public String toString() {
    return "CREATE DATABASE " + databaseName;
  }

  @Override
  public void preOrder(LogicalNodeVisitor visitor) {
    visitor.visit(this);
  }

  @Override
  public void postOrder(LogicalNodeVisitor visitor) {
    visitor.visit(this);
  }
}