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
package org.apache.tajo.catalog.store;

import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.tajo.catalog.CatalogConstants;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.FunctionDesc;
import org.apache.tajo.catalog.exception.*;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.proto.CatalogProtos.IndexDescProto;

import java.io.IOException;
import java.util.*;

/**
 * CatalogServer guarantees that all operations are thread-safe.
 * So, we don't need to consider concurrency problem here.
 */
public class MemStore implements CatalogStore {
  private final Map<String, Map<String, CatalogProtos.TableDescProto>> databases = Maps.newHashMap();
  private final Map<String, CatalogProtos.FunctionDescProto> functions = Maps.newHashMap();
  private final Map<String, Map<String, IndexDescProto>> indexes = Maps.newHashMap();
  private final Map<String, Map<String, IndexDescProto>> indexesByColumn = Maps.newHashMap();
  
  public MemStore(Configuration conf) {
    createDatabase(CatalogConstants.DEFAULT_DATABASE_NAME);
  }

  @Override
  public void close() throws IOException {
    databases.clear();
    functions.clear();
    indexes.clear();
  }

  @Override
  public void createDatabase(String dbName) throws CatalogException {
    if (databases.containsKey(dbName)) {
      throw new AlreadyExistsDatabaseException(dbName);
    }

    databases.put(dbName, new HashMap<String, CatalogProtos.TableDescProto>());
  }

  @Override
  public boolean existDatabase(String dbName) throws CatalogException {
    return databases.containsKey(dbName);
  }

  @Override
  public void dropDatabase(String dbName) throws CatalogException {
    if (!databases.containsKey(dbName)) {
      throw new NoSuchDatabaseException(dbName);
    }
    databases.remove(dbName);
  }

  @Override
  public Collection<String> getAllDatabaseNames() throws CatalogException {
    return databases.keySet();
  }

  /**
   * Get a database namespace from a Map instance.
   */
  private <T> Map<String, T> checkAndGetDatabaseNS(final Map<String, Map<String, T>> databaseMap,
                                                   String databaseName) {
    if (databaseMap.containsKey(databaseName)) {
      return databaseMap.get(databaseName);
    } else {
      throw new NoSuchDatabaseException(databaseName);
    }
  }

  @Override
  public void createTable(CatalogProtos.TableDescProto desc) throws CatalogException {
    String dbName = CatalogUtil.normalizeIdentifier(desc.getDatabaseName());
    Map<String, CatalogProtos.TableDescProto> database = checkAndGetDatabaseNS(databases, dbName);

    String tbName = CatalogUtil.normalizeIdentifier(desc.getTableName());
    if (database.containsKey(tbName)) {
      throw new AlreadyExistsTableException(tbName);
    }
    database.put(tbName, desc);
  }

  @Override
  public boolean existTable(String dbName, String namespace, String tbName) throws CatalogException {
    Map<String, CatalogProtos.TableDescProto> database = checkAndGetDatabaseNS(databases, dbName);

    return database.containsKey(tbName);
  }

  @Override
  public void dropTable(String dbName, String namespace, String tbName) throws CatalogException {
    Map<String, CatalogProtos.TableDescProto> database = checkAndGetDatabaseNS(databases, dbName);

    if (database.containsKey(tbName)) {
      database.remove(tbName);
    } else {
      throw new NoSuchTableException(tbName);
    }
  }

  /* (non-Javadoc)
   * @see CatalogStore#getTable(java.lang.String)
   */
  @Override
  public CatalogProtos.TableDescProto getTable(String databaseName, String namespace, String tableName)
      throws CatalogException {
    Map<String, CatalogProtos.TableDescProto> database = checkAndGetDatabaseNS(databases, databaseName);

    if (database.containsKey(tableName)) {
      CatalogProtos.TableDescProto unqualified = database.get(tableName);
      CatalogProtos.TableDescProto.Builder builder = CatalogProtos.TableDescProto.newBuilder();
      CatalogProtos.SchemaProto schemaProto = CatalogUtil.getQualfiedSchema(tableName, unqualified.getSchema());
      builder.mergeFrom(unqualified);
      builder.setSchema(schemaProto);
      return builder.build();
    } else {
      throw new NoSuchTableException(tableName);
    }
  }

  /* (non-Javadoc)
   * @see CatalogStore#getAllTableNames()
   */
  @Override
  public List<String> getAllTableNames(String databaseName, String namespace) throws CatalogException {
    Map<String, CatalogProtos.TableDescProto> database = checkAndGetDatabaseNS(databases, databaseName);
    return new ArrayList<String>(database.keySet());
  }

  @Override
  public void addPartitionMethod(CatalogProtos.PartitionMethodProto partitionMethodProto) throws CatalogException {
    throw new RuntimeException("not supported!");
  }

  @Override
  public CatalogProtos.PartitionMethodProto getPartitionMethod(String databaseName, String namespace, String tableName)
      throws CatalogException {
    Map<String, CatalogProtos.TableDescProto> database = checkAndGetDatabaseNS(databases, databaseName);

    if (database.containsKey(tableName)) {
      CatalogProtos.TableDescProto table = database.get(tableName);
      return table.hasPartition() ? table.getPartition() : null;
    } else {
      throw new NoSuchTableException(tableName);
    }
  }

  @Override
  public boolean existPartitionMethod(String databaseName, String namespace, String tableName)
      throws CatalogException {
    Map<String, CatalogProtos.TableDescProto> database = checkAndGetDatabaseNS(databases, databaseName);

    if (database.containsKey(tableName)) {
      CatalogProtos.TableDescProto table = database.get(tableName);
      return table.hasPartition();
    } else {
      throw new NoSuchTableException(tableName);
    }
  }

  @Override
  public void dropPartitionMethod(String dbName, String tableName) throws CatalogException {
    throw new RuntimeException("not supported!");
  }

  @Override
  public void addPartitions(CatalogProtos.PartitionsProto partitionDescList) throws CatalogException {
    throw new RuntimeException("not supported!");
  }

  @Override
  public void addPartition(String databaseName, String namespace, String tableName, CatalogProtos.PartitionDescProto
      partitionDescProto) throws CatalogException {
    throw new RuntimeException("not supported!");
  }

  @Override
  public CatalogProtos.PartitionsProto getPartitions(String tableName) throws CatalogException {
    throw new RuntimeException("not supported!");
  }

  @Override
  public CatalogProtos.PartitionDescProto getPartition(String partitionName) throws CatalogException {
    throw new RuntimeException("not supported!");
  }

  @Override
  public void delPartition(String partitionName) throws CatalogException {
    throw new RuntimeException("not supported!");
  }

  @Override
  public void dropPartitions(String tableName) throws CatalogException {
    throw new RuntimeException("not supported!");
  }

  /* (non-Javadoc)
   * @see CatalogStore#createIndex(nta.catalog.proto.CatalogProtos.IndexDescProto)
   */
  @Override
  public void createIndex(IndexDescProto proto) throws CatalogException {
    final String databaseName = proto.getTableIdentifier().getDatabaseName();

    Map<String, IndexDescProto> index = checkAndGetDatabaseNS(indexes, databaseName);
    Map<String, IndexDescProto> indexByColumn = checkAndGetDatabaseNS(indexesByColumn, databaseName);

    if (index.containsKey(proto.getName())) {
      throw new AlreadyExistsIndexException(proto.getName());
    }

    index.put(proto.getName(), proto);
    indexByColumn.put(proto.getTableIdentifier().getTableName() + "."
        + CatalogUtil.extractSimpleName(proto.getColumn().getName()), proto);
  }

  /* (non-Javadoc)
   * @see CatalogStore#dropIndex(java.lang.String)
   */
  @Override
  public void dropIndex(String databaseName, String namespace, String indexName) throws CatalogException {
    Map<String, IndexDescProto> index = checkAndGetDatabaseNS(indexes, databaseName);
    if (!index.containsKey(indexName)) {
      throw new NoSuchIndexException(indexName);
    }
    index.remove(indexName);
  }

  /* (non-Javadoc)
   * @see CatalogStore#getIndexByName(java.lang.String)
   */
  @Override
  public IndexDescProto getIndexByName(String databaseName, String namespace, String indexName) throws CatalogException {
    Map<String, IndexDescProto> index = checkAndGetDatabaseNS(indexes, databaseName);
    if (!index.containsKey(indexName)) {
      throw new NoSuchIndexException(indexName);
    }

    return index.get(indexName);
  }

  /* (non-Javadoc)
   * @see CatalogStore#getIndexByName(java.lang.String, java.lang.String)
   */
  @Override
  public IndexDescProto getIndexByColumn(String databaseName, String namespace, String tableName, String columnName)
      throws CatalogException {

    Map<String, IndexDescProto> indexByColumn = checkAndGetDatabaseNS(indexesByColumn, databaseName);
    if (!indexByColumn.containsKey(columnName)) {
      throw new NoSuchIndexException(columnName);
    }

    return indexByColumn.get(columnName);
  }

  @Override
  public boolean existIndexByName(String databaseName, String namespace, String indexName) throws CatalogException {
    Map<String, IndexDescProto> index = checkAndGetDatabaseNS(indexes, databaseName);
    return index.containsKey(indexName);
  }

  @Override
  public boolean existIndexByColumn(String databaseName, String namespace, String tableName, String columnName)
      throws CatalogException {
    Map<String, IndexDescProto> indexByColumn = checkAndGetDatabaseNS(indexesByColumn, databaseName);
    return indexByColumn.containsKey(columnName);
  }

  @Override
  public IndexDescProto[] getIndexes(String databaseName, String namespace, String tableName) throws CatalogException {
    List<IndexDescProto> protos = new ArrayList<IndexDescProto>();
    Map<String, IndexDescProto> indexByColumn = checkAndGetDatabaseNS(indexesByColumn, databaseName);
    for (IndexDescProto proto : indexByColumn.values()) {
      if (proto.equals(tableName)) {
        protos.add(proto);
      }
    }

    return protos.toArray(new IndexDescProto[protos.size()]);
  }

  @Override
  public void addFunction(FunctionDesc func) throws CatalogException {
    // to be implemented
  }

  @Override
  public void deleteFunction(FunctionDesc func) throws CatalogException {
    // to be implemented
  }

  @Override
  public void existFunction(FunctionDesc func) throws CatalogException {
    // to be implemented
  }

  @Override
  public List<String> getAllFunctionNames() throws CatalogException {
    // to be implemented
    return null;
  }

}
