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

package org.apache.tajo.algebra;

import org.apache.tajo.util.TUtil;

public class Target extends Expr {
  private Expr expr;
  private String alias;

  public Target(Expr expr) {
    super(OpType.Target);
    this.expr = expr;
  }

  public Target(Expr expr, String alias) {
    this(expr);
    setAlias(alias);
  }

  public Expr getExpr() {
    return expr;
  }

  public boolean hasAlias() {
    return this.alias != null;
  }

  public String getAlias() {
    return this.alias;
  }

  public void setAlias(String alias) {
    this.alias = alias;
  }

  @Override
  public boolean equalsTo(Expr obj) {
    if (obj instanceof Target) {
      Target another = (Target) obj;
      return expr.equals(another.expr) && TUtil.checkEquals(alias, another.alias);
    }

    return false;
  }

  @Override
  public String toJson() {
    return JsonHelper.toJson(this);
  }
}
