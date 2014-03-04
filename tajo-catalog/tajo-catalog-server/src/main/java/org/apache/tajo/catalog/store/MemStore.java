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

public class MemStore implements CatalogStore {
  private final Map<String, Map<String, CatalogProtos.TableDescProto>> databases = Maps.newConcurrentMap();
  private final Map<String, CatalogProtos.FunctionDescProto> functions = Maps.newHashMap();
  private final Map<String, IndexDescProto> indexes = Maps.newHashMap();
  private final Map<String, IndexDescProto> indexesByColumn = Maps.newHashMap();
  
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

  private Map<String, CatalogProtos.TableDescProto> checkAndGetDatabase(String dbName) {
    if (databases.containsKey(dbName)) {
      return databases.get(dbName);
    } else {
      throw new NoSuchDatabaseException(dbName);
    }
  }

  /* (non-Javadoc)
     * @see CatalogStore#addTable(TableDesc)
     */
  @Override
  public void addTable(CatalogProtos.TableDescProto desc) throws CatalogException {
    String dbName = CatalogUtil.normalizeIdentifier(desc.getDatabaseName());
    Map<String, CatalogProtos.TableDescProto> database = checkAndGetDatabase(dbName);
    synchronized(database) {
      String tbName = CatalogUtil.normalizeIdentifier(desc.getTableName());
      if (database.containsKey(tbName)) {
        throw new AlreadyExistsTableException(tbName);
      }
      database.put(tbName, desc);
    }
  }

  @Override
  public boolean existTable(String dbName, String namespace, String tbName) throws CatalogException {
    Map<String, CatalogProtos.TableDescProto> database = checkAndGetDatabase(dbName);
    synchronized(database) {
      return database.containsKey(tbName);
    }
  }

  @Override
  public void deleteTable(String dbName, String namespace, String tbName) throws CatalogException {
    Map<String, CatalogProtos.TableDescProto> database = checkAndGetDatabase(dbName);
    synchronized(database) {
      if (database.containsKey(tbName)) {
        database.remove(tbName);
      } else {
        throw new NoSuchTableException(tbName);
      }
    }
  }

  /* (non-Javadoc)
   * @see CatalogStore#getTable(java.lang.String)
   */
  @Override
  public CatalogProtos.TableDescProto getTable(String databaseName, String namespace, String tableName) throws CatalogException {
    Map<String, CatalogProtos.TableDescProto> database = checkAndGetDatabase(databaseName);
    synchronized(database) {
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
  }

  /* (non-Javadoc)
   * @see CatalogStore#getAllTableNames()
   */
  @Override
  public List<String> getAllTableNames(String databaseName, String namespace) throws CatalogException {
    Map<String, CatalogProtos.TableDescProto> database = checkAndGetDatabase(databaseName);
    synchronized(database) {
      return new ArrayList<String>(database.keySet());
    }
  }

  @Override
  public void addPartitionMethod(CatalogProtos.PartitionMethodProto partitionMethodProto) throws CatalogException {
    throw new RuntimeException("not supported!");
  }

  @Override
  public CatalogProtos.PartitionMethodProto getPartitionMethod(String databaseName, String namespace, String tableName)
      throws CatalogException {
    Map<String, CatalogProtos.TableDescProto> database = checkAndGetDatabase(databaseName);
    synchronized(database) {
      if (database.containsKey(tableName)) {
        CatalogProtos.TableDescProto table = database.get(tableName);
        return table.hasPartition() ? table.getPartition() : null;
      } else {
        throw new NoSuchTableException(tableName);
      }
    }
  }

  @Override
  public boolean existPartitionMethod(String databaseName, String namespace, String tableName)
      throws CatalogException {
    Map<String, CatalogProtos.TableDescProto> database = checkAndGetDatabase(databaseName);
    synchronized(database) {
      if (database.containsKey(tableName)) {
        CatalogProtos.TableDescProto table = database.get(tableName);
        return table.hasPartition();
      } else {
        throw new NoSuchTableException(tableName);
      }
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
  public void addPartition(CatalogProtos.PartitionDescProto partitionDesc) throws CatalogException {
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
  public void delPartitions(String tableName) throws CatalogException {
    throw new RuntimeException("not supported!");
  }

  /* (non-Javadoc)
   * @see CatalogStore#addIndex(nta.catalog.proto.CatalogProtos.IndexDescProto)
   */
  @Override
  public void addIndex(IndexDescProto proto) throws CatalogException {
    synchronized(indexes) {
      indexes.put(proto.getName(), proto);
      indexesByColumn.put(proto.getTableId() + "." 
          + CatalogUtil.extractSimpleName(proto.getColumn().getName()), proto);
    }
  }

  /* (non-Javadoc)
   * @see CatalogStore#delIndex(java.lang.String)
   */
  @Override
  public void delIndex(String indexName) throws CatalogException {
    synchronized(indexes) {
      indexes.remove(indexName);
    }
  }

  /* (non-Javadoc)
   * @see CatalogStore#getIndex(java.lang.String)
   */
  @Override
  public IndexDescProto getIndex(String indexName) throws CatalogException {
    return indexes.get(indexName);
  }

  /* (non-Javadoc)
   * @see CatalogStore#getIndex(java.lang.String, java.lang.String)
   */
  @Override
  public IndexDescProto getIndex(String tableName, String columnName) throws CatalogException {
    return indexesByColumn.get(tableName+"."+columnName);
  }

  /* (non-Javadoc)
   * @see CatalogStore#existIndex(java.lang.String)
   */
  @Override
  public boolean existIndex(String indexName) throws CatalogException {
    return indexes.containsKey(indexName);
  }

  /* (non-Javadoc)
   * @see CatalogStore#existIndex(java.lang.String, java.lang.String)
   */
  @Override
  public boolean existIndex(String tableName, String columnName) throws CatalogException {
    return indexesByColumn.containsKey(tableName + "." + columnName);
  }

  /* (non-Javadoc)
   * @see CatalogStore#getIndexes(java.lang.String)
   */
  @Override
  public IndexDescProto[] getIndexes(String tableName) throws CatalogException {
    List<IndexDescProto> protos = new ArrayList<IndexDescProto>();
    for (IndexDescProto proto : indexesByColumn.values()) {
      if (proto.getTableId().equals(tableName)) {
        protos.add(proto);
      }
    }
    return protos.toArray(new IndexDescProto[protos.size()]);
  }

  /* (non-Javadoc)
   * @see CatalogStore#addFunction(FunctionDesc)
   */
  @Override
  public void addFunction(FunctionDesc func) throws CatalogException {
    // to be implemented
  }

  /* (non-Javadoc)
   * @see CatalogStore#deleteFunction(FunctionDesc)
   */
  @Override
  public void deleteFunction(FunctionDesc func) throws CatalogException {
    // to be implemented
  }

  /* (non-Javadoc)
   * @see CatalogStore#existFunction(FunctionDesc)
   */
  @Override
  public void existFunction(FunctionDesc func) throws CatalogException {
    // to be implemented
  }

  /* (non-Javadoc)
   * @see CatalogStore#getAllFunctionNames()
   */
  @Override
  public List<String> getAllFunctionNames() throws CatalogException {
    // to be implemented
    return null;
  }

}
