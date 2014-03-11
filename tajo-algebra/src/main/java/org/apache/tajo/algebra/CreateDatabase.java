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

import com.google.common.base.Objects;
import org.apache.tajo.util.TUtil;

public class CreateDatabase extends Expr {
  private String databaseName;
  private String tablespaceName;

  public CreateDatabase(final String databaseName, final String tablespaceName) {
    super(OpType.CreateDatabase);
    this.databaseName = databaseName;
    this.tablespaceName = tablespaceName;
  }

  public String getDatabaseName() {
    return this.databaseName;
  }

  public boolean hasTablespaceName() {
    return tablespaceName != null;
  }

  public String getTablespaceName() {
    return tablespaceName;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(databaseName, tablespaceName);
  }

  @Override
  boolean equalsTo(Expr expr) {
    CreateDatabase another = (CreateDatabase) expr;
    return databaseName.equals(another.databaseName) && TUtil.checkEquals(tablespaceName, another.tablespaceName);

  }
}
