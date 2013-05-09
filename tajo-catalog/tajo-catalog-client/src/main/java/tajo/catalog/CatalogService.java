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

package tajo.catalog;

import tajo.common.TajoDataTypes.DataType;

import java.util.Collection;

public interface CatalogService {

  /**
   * Get a table description by name
   * @param name table name
   * @return a table description
   * @see TableDescImpl
   * @throws Throwable
   */
  TableDesc getTableDesc(String name);

  /**
   *
   * @return
   * @throws tajo.catalog.exception.CatalogException
   */
  Collection<String> getAllTableNames();

  /**
   *
   * @return
   * @throws tajo.catalog.exception.CatalogException
   */
  Collection<FunctionDesc> getFunctions();

  /**
   * Add a table via table description
   * @see TableDescImpl
   * @throws Throwable
   */
  boolean addTable(TableDesc desc);

  /**
   * Drop a table by name
   *
   * @param name table name
   * @throws Throwable
   */
  boolean deleteTable(String name);

  boolean existsTable(String tableId);

  boolean addIndex(IndexDesc index);

  boolean existIndex(String indexName);

  boolean existIndex(String tableName, String columnName);

  IndexDesc getIndex(String indexName);

  IndexDesc getIndex(String tableName, String columnName);

  boolean deleteIndex(String indexName);

  boolean registerFunction(FunctionDesc funcDesc);

  boolean unregisterFunction(String signature, DataType... paramTypes);

  /**
   *
   * @param signature
   * @return
   */
  FunctionDesc getFunction(String signature, DataType... paramTypes);

  /**
   *
   * @param signature
   * @return
   */
  boolean containFunction(String signature, DataType... paramTypes);
}