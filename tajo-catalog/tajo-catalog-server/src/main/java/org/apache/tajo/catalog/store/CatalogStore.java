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

package org.apache.tajo.catalog.store;

import org.apache.tajo.catalog.FunctionDesc;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.proto.CatalogProtos.IndexDescProto;

import java.io.Closeable;
import org.apache.tajo.catalog.exception.CatalogException;

import java.util.Collection;
import java.util.List;

import static org.apache.tajo.catalog.proto.CatalogProtos.PartitionMethodProto;

public interface CatalogStore extends Closeable {
  /*************************** Database ******************************/
  void createDatabase(String name) throws CatalogException;

  boolean existDatabase(String name) throws CatalogException;

  void dropDatabase(String name) throws CatalogException;

  Collection<String> getAllDatabaseNames() throws CatalogException;

  /*************************** TABLE ******************************/
  void addTable(CatalogProtos.TableDescProto desc) throws CatalogException;
  
  boolean existTable(String databaseName, String namespace, String tableName) throws CatalogException;
  
  void dropTable(String databaseName, String namespace, String tableName) throws CatalogException;
  
  CatalogProtos.TableDescProto getTable(String databaseName, String namespace, String name) throws CatalogException;
  
  List<String> getAllTableNames(String databaseName, String namespace) throws CatalogException;


  /************************ PARTITION METHOD **************************/
  void addPartitionMethod(PartitionMethodProto partitionMethodProto) throws CatalogException;

  PartitionMethodProto getPartitionMethod(String databaseName, String namespace, String tableName)
      throws CatalogException;

  boolean existPartitionMethod(String dbName, String namespace, String tableName) throws CatalogException;

  void dropPartitionMethod(String dbName, String tableName) throws CatalogException;


  /************************** PARTITIONS *****************************/
  void addPartitions(CatalogProtos.PartitionsProto partitionsProto) throws CatalogException;

  void addPartition(CatalogProtos.PartitionDescProto partitionDescProto) throws CatalogException;

  /**
   * Get all partitions of a table
   * @param tableName the table name
   * @return
   * @throws CatalogException
   */
  CatalogProtos.PartitionsProto getPartitions(String tableName) throws CatalogException;

  CatalogProtos.PartitionDescProto getPartition(String partitionName) throws CatalogException;

  void delPartition(String partitionName) throws CatalogException;

  void delPartitions(String tableName) throws CatalogException;

  /**************************** INDEX *******************************/
  void addIndex(IndexDescProto proto) throws CatalogException;
  
  void delIndex(String indexName) throws CatalogException;
  
  IndexDescProto getIndex(String indexName) throws CatalogException;
  
  IndexDescProto getIndex(String tableName, String columnName) throws CatalogException;
  
  boolean existIndex(String indexName) throws CatalogException;
  
  boolean existIndex(String tableName, String columnName) throws CatalogException;

  /************************** FUNCTION *****************************/
  IndexDescProto [] getIndexes(String tableName) throws CatalogException;
  
  void addFunction(FunctionDesc func) throws CatalogException;
  
  void deleteFunction(FunctionDesc func) throws CatalogException;
  
  void existFunction(FunctionDesc func) throws CatalogException;
  
  List<String> getAllFunctionNames() throws CatalogException;
}
