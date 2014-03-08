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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hcatalog.common.HCatUtil;
import org.apache.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hcatalog.data.schema.HCatSchema;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.exception.CatalogException;
import org.apache.tajo.catalog.partition.PartitionMethodDesc;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.exception.InternalException;
import org.apache.tajo.exception.UnimplementedException;

import java.io.IOException;
import java.util.*;

import static org.apache.tajo.catalog.proto.CatalogProtos.PartitionType;

public class HCatalogStore extends CatalogConstants implements CatalogStore {
  public static final String CVSFILE_DELIMITER = "csvfile.delimiter";

  protected final Log LOG = LogFactory.getLog(getClass());
  protected Configuration conf;
  private static final int CLIENT_POOL_SIZE = 2;
  private final HCatalogStoreClientPool clientPool = new HCatalogStoreClientPool(0);

  public HCatalogStore(final Configuration conf)
      throws InternalException {
    this.conf = conf;
    try {
      clientPool.addClients(CLIENT_POOL_SIZE);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public boolean existTable(String databaseName, String namespace, final String tableName) throws CatalogException {
    boolean exist = false;

    org.apache.hadoop.hive.ql.metadata.Table table = null;
    HCatalogStoreClientPool.HCatalogStoreClient client = null;

    // get table
    try {
      try {
        client = clientPool.getClient();
        table = HCatUtil.getTable(client.getHiveClient(), databaseName, tableName);
        if (table != null) {
          exist = true;
        }
      } catch (NoSuchObjectException nsoe) {
        exist = false;
      } catch (Exception e) {
        throw new CatalogException(e);
      }
    } finally {
      client.release();
    }

    return exist;
  }

  @Override
  public final CatalogProtos.TableDescProto getTable(final String databaseName, String namespace, final String tableName)
      throws CatalogException {

    org.apache.hadoop.hive.ql.metadata.Table table = null;
    HCatalogStoreClientPool.HCatalogStoreClient client = null;
    Path path = null;
    CatalogProtos.StoreType storeType = null;
    org.apache.tajo.catalog.Schema schema = null;
    Options options = null;
    TableStats stats = null;
    PartitionMethodDesc partitions = null;

    //////////////////////////////////
    // set tajo table schema.
    //////////////////////////////////
    try {
      // get hive table schema
      try {
        client = clientPool.getClient();
        table = HCatUtil.getTable(client.getHiveClient(), databaseName, tableName);
        path = table.getPath();
      } catch (NoSuchObjectException nsoe) {
        throw new CatalogException("Table not found. - tableName:" + tableName, nsoe);
      } catch (Exception e) {
        throw new CatalogException(e);
      }

      // convert hcatalog field schema into tajo field schema.
      schema = new org.apache.tajo.catalog.Schema();
      HCatSchema tableSchema = null;

      try {
        tableSchema = HCatUtil.getTableSchemaWithPtnCols(table);
      } catch (IOException ioe) {
        throw new CatalogException("Fail to get table schema. - tableName:" + tableName, ioe);
      }
      List<HCatFieldSchema> fieldSchemaList = tableSchema.getFields();
      boolean isPartitionKey = false;
      for (HCatFieldSchema eachField : fieldSchemaList) {
        isPartitionKey = false;

        if (table.getPartitionKeys() != null) {
          for(FieldSchema partitionKey: table.getPartitionKeys()) {
            if (partitionKey.getName().equals(eachField.getName())) {
              isPartitionKey = true;
            }
          }
        }

        if (!isPartitionKey) {
          String fieldName = databaseName + "." + tableName + "." + eachField.getName();
          TajoDataTypes.Type dataType = HCatalogUtil.getTajoFieldType(eachField.getType().toString());
          schema.addColumn(fieldName, dataType);
        }
      }

      // validate field schema.
      try {
        HCatalogUtil.validateHCatTableAndTajoSchema(tableSchema);
      } catch (Exception e) {
        throw new CatalogException("HCatalog cannot support schema. - schema:" + tableSchema.toString(), e);
      }

      stats = new TableStats();
      options = Options.create();
      Properties properties = table.getMetadata();
      if (properties != null) {
        // set field delimiter
        String fieldDelimiter = "", fileOutputformat = "";
        if (properties.getProperty(serdeConstants.FIELD_DELIM) != null) {
          fieldDelimiter = properties.getProperty(serdeConstants.FIELD_DELIM);
        } else {
          // if hive table used default row format delimiter, Properties doesn't have it.
          // So, Tajo must set as follows:
          fieldDelimiter = "\\001";
        }

        // set file output format
        fileOutputformat = properties.getProperty("file.outputformat");
        storeType = CatalogUtil.getStoreType(HCatalogUtil.getStoreType(fileOutputformat));

        if (storeType.equals(CatalogProtos.StoreType.CSV) ) {
          options.put(CVSFILE_DELIMITER, fieldDelimiter);
        }

        // set data size
        long totalSize = 0;
        if(properties.getProperty("totalSize") != null) {
          totalSize = new Long(properties.getProperty("totalSize"));
        } else {
          try {
            FileSystem fs = path.getFileSystem(conf);
            if (fs.exists(path)) {
              totalSize = fs.getContentSummary(path).getLength();
            }
          } catch (IOException ioe) {
            throw new CatalogException("Fail to get path. - path:" + path.toString(), ioe);
          }
        }
        stats.setNumBytes(totalSize);
      }

      // set partition keys
      if (table.getPartitionKeys() != null) {
        Schema expressionSchema = new Schema();
        StringBuilder sb = new StringBuilder();
        if (table.getPartitionKeys().size() > 0) {
          List<FieldSchema> partitionKeys = table.getPartitionKeys();
          for(int i = 0; i < partitionKeys.size(); i++) {
            FieldSchema fieldSchema = partitionKeys.get(i);
            TajoDataTypes.Type dataType = HCatalogUtil.getTajoFieldType(fieldSchema.getType().toString());
            expressionSchema.addColumn(
                new Column(databaseName + "." + tableName + "." + fieldSchema.getName(), dataType));
            if (i > 0) {
              sb.append(",");
            }
            sb.append(fieldSchema.getName());
          }
          partitions = new PartitionMethodDesc(
              databaseName,
              namespace,
              tableName,
              PartitionType.COLUMN,
              sb.toString(),
              expressionSchema);
        }
      }
    } finally {
      client.release();
    }
    TableMeta meta = new TableMeta(storeType, options);

    TableDesc tableDesc = new TableDesc(databaseName + "." + tableName, schema, meta, path);
    if (stats != null) {
      tableDesc.setStats(stats);
    }
    if (partitions != null) {
      tableDesc.setPartitionMethod(partitions);
    }
    return tableDesc.getProto();
  }


  private TajoDataTypes.Type getDataType(final String typeStr) {
    try {
      return Enum.valueOf(TajoDataTypes.Type.class, typeStr);
    } catch (IllegalArgumentException iae) {
      LOG.error("Cannot find a matched type aginst from '" + typeStr + "'");
      return null;
    }
  }

  @Override
  public final List<String> getAllTableNames(String databaseName, String namespace) throws CatalogException {
    List<String> dbs = null;
    List<String> tables = null;
    List<String> allTables = new ArrayList<String>();
    HCatalogStoreClientPool.HCatalogStoreClient client = null;

    try {
      client = clientPool.getClient();
      tables = client.getHiveClient().getAllTables(databaseName);
      for(String eachTable: tables) {
        allTables.add(databaseName + "." + eachTable);
      }
    } catch (MetaException e) {
      throw new CatalogException(e);
    } finally {
      client.release();
    }
    return allTables;
  }

  @Override
  public void createDatabase(String name) throws CatalogException {
    throw new UnimplementedException("createDatabase is not implemented yet");
  }

  @Override
  public boolean existDatabase(String name) throws CatalogException {
    throw new UnimplementedException("existDatabase() is not implemented yet");
  }

  @Override
  public void dropDatabase(String name) throws CatalogException {
    throw new UnimplementedException("dropDatabase() is not implemented yet");
  }

  @Override
  public Collection<String> getAllDatabaseNames() throws CatalogException {
    return null;
  }

  @Override
  public final void createTable(final CatalogProtos.TableDescProto tableDesc) throws CatalogException {
    HCatalogStoreClientPool.HCatalogStoreClient client = null;

    String databaseName = CatalogUtil.normalizeIdentifier(tableDesc.getDatabaseName());
    String tableName = CatalogUtil.normalizeIdentifier(tableDesc.getTableName());

    try {
      client = clientPool.getClient();

      org.apache.hadoop.hive.metastore.api.Table table = new org.apache.hadoop.hive.metastore.api.Table();

      table.setDbName(databaseName);
      table.setTableName(tableName);
      // TODO: set owner
      //table.setOwner();

      StorageDescriptor sd = new StorageDescriptor();

      // if tajo set location method, thrift client make exception as follows:
      // Caused by: MetaException(message:java.lang.NullPointerException)
      // If you want to modify table path, you have to modify on Hive cli.
      //sd.setLocation(tableDesc.getPath().toString());

      // set column information
      ArrayList<FieldSchema> cols = new ArrayList<FieldSchema>(tableDesc.getSchema().getFieldsCount());
      for (CatalogProtos.ColumnProto col : tableDesc.getSchema().getFieldsList()) {
        cols.add(new FieldSchema(
            col.getName(),
            HCatalogUtil.getHiveFieldType(col.getDataType().getType().name()),
            ""));
      }
      sd.setCols(cols);

      sd.setCompressed(false);
      if (tableDesc.getMeta().hasParams()) {
        for (CatalogProtos.KeyValueProto entry: tableDesc.getMeta().getParams().getKeyvalList()) {
          if (entry.getKey().equals("compression.codec")) {
            sd.setCompressed(true);
          } else if (entry.getKey().equals(CVSFILE_DELIMITER)) {
            sd.getSerdeInfo().getParameters().put(serdeConstants.FIELD_DELIM, entry.getValue());
          }
        }
      }

      sd.setParameters(new HashMap<String, String>());
      sd.setSerdeInfo(new SerDeInfo());
      sd.getSerdeInfo().setName(table.getTableName());

      if(tableDesc.getMeta().getStoreType().equals(CatalogProtos.StoreType.RCFILE)) {
        sd.getSerdeInfo().setSerializationLib(org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe.class.getName());
      } else {
        sd.getSerdeInfo().setSerializationLib(org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe.class.getName());
      }

      sd.getSerdeInfo().setParameters(new HashMap<String, String>());
//      sd.getSerdeInfo().getParameters().put(serdeConstants.SERIALIZATION_FORMAT, "1");

      if(tableDesc.getMeta().getStoreType().equals(CatalogProtos.StoreType.RCFILE)) {
        sd.setInputFormat(org.apache.hadoop.hive.ql.io.RCFileInputFormat.class.getName());
      } else {
        sd.setInputFormat(org.apache.hadoop.mapred.TextInputFormat.class.getName());
      }

      if(tableDesc.getMeta().getStoreType().equals(CatalogProtos.StoreType.RCFILE)) {
        sd.setOutputFormat(org.apache.hadoop.hive.ql.io.RCFileOutputFormat.class.getName());
      } else {
        sd.setOutputFormat(org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat.class.getName());
      }

      sd.setSortCols(new ArrayList<Order>());

      table.setSd(sd);
      client.getHiveClient().createTable(table);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new CatalogException(e);
    } finally {
      client.release();
    }
  }

  @Override
  public final void dropTable(String databaseName, String namespace, final String tableName) throws CatalogException {
    HCatalogStoreClientPool.HCatalogStoreClient client = null;

    try {
      client = clientPool.getClient();
      client.getHiveClient().dropTable(databaseName, tableName, false, false);
    } catch (NoSuchObjectException nsoe) {
    } catch (Exception e) {
      throw new CatalogException(e);
    } finally {
      client.release();
    }
  }

  @Override
  public void addPartitionMethod(CatalogProtos.PartitionMethodProto partitionMethodProto) throws CatalogException {
    // TODO - not implemented yet
  }

  @Override
  public CatalogProtos.PartitionMethodProto getPartitionMethod(String databaseName, String namespace, String tableName) throws CatalogException {
    return null;
  }

  @Override
  public boolean existPartitionMethod(String dbName, String namespace, String tableName) throws CatalogException {
    return false;
  }

  @Override
  public void dropPartitionMethod(String dbName, String tableName) throws CatalogException {

  }

  @Override
  public void addPartitions(CatalogProtos.PartitionsProto partitionsProto) throws CatalogException {
    // TODO - not implemented yet
  }

  @Override
  public void addPartition(String databaseName, String namespace, String tableName, CatalogProtos.PartitionDescProto partitionDescProto) throws CatalogException {

  }

  @Override
  public CatalogProtos.PartitionsProto getPartitions(String tableName) throws CatalogException {
    return null; // TODO - not implemented yet
  }

  @Override
  public CatalogProtos.PartitionDescProto getPartition(String partitionName) throws CatalogException {
    return null; // TODO - not implemented yet
  }

  @Override
  public void delPartition(String partitionName) throws CatalogException {
    // TODO - not implemented yet
  }

  @Override
  public void dropPartitions(String tableName) throws CatalogException {
    // TODO - not implemented yet
  }

  @Override
  public final void addFunction(final FunctionDesc func) throws CatalogException {
    // TODO - not implemented yet
  }

  @Override
  public final void deleteFunction(final FunctionDesc func) throws CatalogException {
    // TODO - not implemented yet
  }

  @Override
  public final void existFunction(final FunctionDesc func) throws CatalogException {
    // TODO - not implemented yet
  }

  @Override
  public final List<String> getAllFunctionNames() throws CatalogException {
    // TODO - not implemented yet
    return null;
  }

  @Override
  public void createIndex(CatalogProtos.IndexDescProto proto) throws CatalogException {
    // TODO - not implemented yet
  }

  @Override
  public void dropIndex(String databaseName, String namespace, String indexName) throws CatalogException {

  }

  @Override
  public CatalogProtos.IndexDescProto getIndexByName(String databaseName, String namespace, String indexName) throws CatalogException {
    return null;
  }

  @Override
  public CatalogProtos.IndexDescProto getIndexByColumn(String databaseName, String namespace, String tableName,
                                                       String columnName) throws CatalogException {
    return null;
  }

  @Override
  public boolean existIndexByName(String databaseName, String namespace, String indexName) throws CatalogException {
    return false;
  }

  @Override
  public boolean existIndexByColumn(String databaseName, String namespace, String tableName, String columnName) throws CatalogException {
    return false;
  }

  @Override
  public CatalogProtos.IndexDescProto[] getIndexes(String databaseName, String namespace, String tableName) throws CatalogException {
    return new CatalogProtos.IndexDescProto[0];
  }


  @Override
  public final void close() {
    clientPool.close();
  }
}
