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

package org.apache.tajo.catalog;

import org.apache.tajo.annotation.Nullable;
import org.apache.tajo.catalog.partition.PartitionMethodDesc;
import org.apache.tajo.common.TajoDataTypes.DataType;

import java.util.Collection;

import static org.apache.tajo.catalog.proto.CatalogProtos.FunctionType;

public interface CatalogService {

  /**
   *
   * @param databaseName Database name to be created
   * @return True if database is created successfully. Otherwise, it will return FALSE.
   */
  Boolean createDatabase(String databaseName);

  /**
   *
   * @param databaseName Database name to be dropped
   * @return True if database is dropped sucessfully. Otherwise, it will return FALSE.
   */
  Boolean dropDatabase(String databaseName);

  /**
   *
   * @param databaseName Database name to be checked
   * @return True if database exists. Otherwise, it will return FALSE.
   */
  Boolean existDatabase(String databaseName);

  /**
   *
   * @return All database names
   */
  Collection<String> getAllDatabaseNames();

  /**
   * Get a table description by name
   * @param tableName table name
   * @return a table description
   * @see TableDesc
   * @throws Throwable
   */
  TableDesc getTableDesc(String databaseName, @Nullable String namespace, String tableName);

  /**
   *
   * @return All table names which belong to a given database.
   */
  Collection<String> getAllTableNames(String databaseName, @Nullable String namespace);

  /**
   *
   * @return All FunctionDescs
   */
  Collection<FunctionDesc> getFunctions();

  /**
   * Add a table via table description
   * @see TableDesc
   * @throws Throwable
   */
  boolean createTable(TableDesc desc);


  /**
   * Drop a table by name
   *
   * @param tableName table name
   * @throws Throwable
   */
  boolean dropTable(String databaseName, @Nullable String schemaName, String tableName);

  boolean existsTable(String databaseName, @Nullable String schemaName, String tableName);

  PartitionMethodDesc getPartitionMethod(String databaseName, @Nullable String schemaName, String tableName);

  boolean existPartitionMethod(String databaseName, @Nullable String schemaName, String tableId);

  boolean createIndex(IndexDesc index);

  boolean existIndexByName(String databaseName, @Nullable String namespace, String indexName);

  boolean existIndexByColumn(String databaseName, @Nullable String namespace, String tableName, String columnName);

  IndexDesc getIndexByName(String databaseName, @Nullable String namespace, String indexName);

  IndexDesc getIndexByColumn(String databaseName, @Nullable String namespace, String tableName, String columnName);

  boolean dropIndex(String databaseName, String namespace, String indexName);

  boolean createFunction(FunctionDesc funcDesc);

  boolean dropFunction(String signature);

  FunctionDesc getFunction(String signature, DataType... paramTypes);

  FunctionDesc getFunction(String signature, FunctionType funcType, DataType... paramTypes);

  boolean containFunction(String signature, DataType... paramTypes);

  boolean containFunction(String signature, FunctionType funcType, DataType... paramTypes);
}