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

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.tajo.catalog.CatalogConstants;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.FunctionDesc;
import org.apache.tajo.catalog.exception.CatalogException;
import org.apache.tajo.catalog.exception.NoSuchDatabaseException;
import org.apache.tajo.catalog.exception.NoSuchTableException;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.proto.CatalogProtos.*;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.exception.InternalException;
import org.apache.tajo.exception.UnimplementedException;
import org.apache.tajo.util.FileUtil;

import java.io.File;
import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public abstract class AbstractDBStore extends CatalogConstants implements CatalogStore {
  protected final Log LOG = LogFactory.getLog(getClass());

  protected Configuration conf;
  protected String connectionId;
  protected String connectionPassword;
  protected String catalogUri;

  private Connection conn;

  protected Map<String, Boolean> baseTableMaps = new HashMap<String, Boolean>();

  protected abstract String getCatalogDriverName();

  protected abstract Connection createConnection(final Configuration conf) throws SQLException;

  protected abstract boolean isInitialized() throws CatalogException;

  protected abstract void createBaseTable() throws CatalogException;

  protected abstract void dropBaseTable() throws CatalogException;

  public AbstractDBStore(Configuration conf) throws InternalException {

    this.conf = conf;

    if(conf.get(CatalogConstants.DEPRECATED_CATALOG_URI) != null) {
      LOG.warn("Configuration parameter " + CatalogConstants.DEPRECATED_CATALOG_URI + " " +
          "is deprecated. Use " + CatalogConstants.CATALOG_URI + " instead.");
      this.catalogUri = conf.get(CatalogConstants.DEPRECATED_CATALOG_URI);
    } else {
      this.catalogUri = conf.get(CatalogConstants.CATALOG_URI);
    }

    if(conf.get(CatalogConstants.DEPRECATED_CONNECTION_ID) != null) {
      LOG.warn("Configuration parameter " + CatalogConstants.DEPRECATED_CONNECTION_ID + " " +
          "is deprecated. Use " + CatalogConstants.CONNECTION_ID + " instead.");
      this.connectionId = conf.get(CatalogConstants.DEPRECATED_CONNECTION_ID);
    } else {
      this.connectionId = conf.get(CatalogConstants.CONNECTION_ID);
    }

    if(conf.get(CatalogConstants.DEPRECATED_CONNECTION_PASSWORD) != null) {
      LOG.warn("Configuration parameter " + CatalogConstants.DEPRECATED_CONNECTION_PASSWORD + " " +
          "is deprecated. Use " + CatalogConstants.CONNECTION_PASSWORD + " instead.");
      this.connectionPassword = conf.get(CatalogConstants.DEPRECATED_CONNECTION_PASSWORD);
    } else {
      this.connectionPassword = conf.get(CatalogConstants.CONNECTION_PASSWORD);
    }

    String catalogDriver = getCatalogDriverName();
    try {
      Class.forName(getCatalogDriverName()).newInstance();
      LOG.info("Loaded the Catalog driver (" + catalogDriver + ")");
    } catch (Exception e) {
      throw new CatalogException("Cannot load Catalog driver " + catalogDriver, e);
    }

    try {
      LOG.info("Trying to connect database (" + catalogUri + ")");
      conn = createConnection(conf);
      LOG.info("Connected to database (" + catalogUri + ")");
    } catch (SQLException e) {
      throw new CatalogException("Cannot connect to database (" + catalogUri
          + ")", e);
    }

    try {
      if (isInitialized()) {
        LOG.info("The base tables of CatalogServer already is initialized.");
        verifySchemaVersion();
      } else {
        try {
          createBaseTable();
          LOG.info("The base tables of CatalogServer are created.");
        } catch (CatalogException ce) {
          try {
            dropBaseTable();
          } catch (Throwable t) {
            LOG.error(t);
          }
          throw ce;
        }
      }
    } catch (Exception se) {
      throw new CatalogException("Cannot initialize the persistent storage of Catalog", se);
    }
  }

  public abstract int getDriverVersion();

  public String readSchemaFile(String path) throws CatalogException {
    URL schema = ClassLoader.getSystemResource("schemas/" + path);

    if (schema == null) {
      throw new CatalogException(String.format("No such a schema file \'%s'", path));
    }

    String sql;
    try {
      sql = FileUtil.readTextFile(new File(schema.toURI()));
    } catch (Exception e) {
      throw new CatalogException(e);
    }
    return sql;
  }

  protected String getCatalogUri(){
    return catalogUri;
  }

  public Connection getConnection() {
    try {
      boolean isValid = conn.isValid(100);
      if (!isValid) {
        CatalogUtil.closeQuietly(conn);
        conn = createConnection(conf);
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return conn;
  }

  private void verifySchemaVersion() throws CatalogException {
    Connection conn = null;
    PreparedStatement pstmt = null;
    ResultSet result = null;

    try {
      String sql = readSchemaFile("common/get_version");
      if (LOG.isDebugEnabled()) {
        LOG.debug(sql.toString());
      }

      conn = getConnection();
      pstmt = conn.prepareStatement(sql);
      result = pstmt.executeQuery();

      if (!result.next()) {
        throw new CatalogException("No Version in meta table");
      }

      int schemaVersion =  result.getInt(1);
      if (schemaVersion != getDriverVersion()) {
        LOG.error(String.format("Schema version is %d, but the driver version is %d.",
            schemaVersion, getDriverVersion()));

        if (schemaVersion > getDriverVersion()) {
          throw new CatalogException("You might downgrade the catalog. " +
              "Downgrading can be available in some versions." +
              "In order to know how to migration Tajo catalog, " +
              "please refer http://tajo.incubator.apache.org/docs/0.8.0/backup_and_restore/catalog.html");
        } else if (schemaVersion < getDriverVersion()) {
          throw new CatalogException(
              "You need to upgrade the catalog." +
                  "In order to know how to upgrade the catalog, " +
                  "please refer http://tajo.incubator.apache.org/docs/0.8.0/backup_and_restore/catalog.html");
        }
      }
    } catch (SQLException e) {
      throw new CatalogException(e);
    } finally {
      CatalogUtil.closeQuietly(conn, pstmt, result);
    }

    LOG.info(String.format("The compatibility of the catalog schema (version: %d) has been verified."));
  }

  /**
   * Insert the version of the current catalog schema
   */
  protected void insertSchemaVersion() throws CatalogException {
    Connection conn = null;
    PreparedStatement pstmt = null;
    try {
      conn = getConnection();
      pstmt = conn.prepareStatement("INSERT INTO meta VALUES (?)");
      pstmt.setInt(1, getDriverVersion());
      pstmt.executeUpdate();
    } catch (SQLException se) {
      throw new CatalogException("cannot insert catalog schema version");
    } finally {
      CatalogUtil.closeQuietly(conn, pstmt);
    }
  }

  @Override
  public void createDatabase(String databaseName) throws CatalogException {
    Connection conn = null;
    PreparedStatement pstmt = null;
    ResultSet res = null;

    try {
      conn = getConnection();
      conn.setAutoCommit(false);
      String sql = String.format("INSERT INTO %s (DB_NAME) VALUES (?)", TB_DATABASES);

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }

      pstmt = conn.prepareStatement(sql);
      pstmt.setString(1, databaseName);
      pstmt.executeUpdate();
      pstmt.close();
    } catch (SQLException se) {
      try {
        // If there is any error, rollback the changes.
        conn.rollback();
      } catch (SQLException se2) {
        LOG.error(se2);
      }
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(conn, pstmt, res);
    }
  }

  @Override
  public boolean existDatabase(String databaseName) throws CatalogException {
    Connection conn = null;
    PreparedStatement pstmt = null;
    ResultSet res = null;
    boolean exist = false;

    try {
      StringBuilder sql = new StringBuilder();
      sql.append("SELECT DB_NAME FROM DATABASES WHERE DB_NAME = ?");
      if (LOG.isDebugEnabled()) {
        LOG.debug(sql.toString());
      }

      conn = getConnection();
      pstmt = conn.prepareStatement(sql.toString());
      pstmt.setString(1, databaseName);
      res = pstmt.executeQuery();
      exist = res.next();
    } catch (SQLException se) {
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(conn, pstmt, res);
    }

    return exist;
  }

  @Override
  public void dropDatabase(String databaseName) throws CatalogException {
    Collection<String> tableNames = getAllTableNames(databaseName, DEFAULT_NAMESPACE);

    Connection conn = null;
    PreparedStatement pstmt = null;
    try {
      conn = getConnection();
      conn.setAutoCommit(false);

      for (String tableName : tableNames) {
        dropTableInternal(conn, databaseName, DEFAULT_NAMESPACE, tableName);
      }

      String sql = "DELETE FROM DATABASES WHERE DB_NAME = ?";
      pstmt = conn.prepareStatement(sql);
      pstmt.setString(1, databaseName);
      pstmt.executeUpdate();
      conn.commit();
    } catch (SQLException se) {
      try {
        conn.rollback();
      } catch (SQLException e) {
        LOG.error(e);
      }
      throw new CatalogException(String.format("Failed to drop database \"%s\"", databaseName), se);
    } finally {
      CatalogUtil.closeQuietly(conn, pstmt);
    }
  }

  @Override
  public Collection<String> getAllDatabaseNames() throws CatalogException {
    Connection conn = null;
    PreparedStatement pstmt = null;
    ResultSet resultSet = null;

    List<String> databaseNames = new ArrayList<String>();

    try {
      String sql = "SELECT DB_NAME FROM " + TB_DATABASES;

      conn = getConnection();
      pstmt = conn.prepareStatement(sql);
      resultSet = pstmt.executeQuery();
      while(resultSet.next()) {
        databaseNames.add(resultSet.getString(1));
      }
    } catch (SQLException se) {
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(conn, pstmt, resultSet);
    }

    return databaseNames;
  }

  private int getTableId(int databaseId, String databaseName, String namespace, String tableName) {
    Connection conn = null;
    PreparedStatement pstmt = null;
    ResultSet res = null;

    try {
      String tidSql = "SELECT TID from TABLES WHERE db_id = ? AND TABLE_ID = ?";
      conn = getConnection();
      pstmt = conn.prepareStatement(tidSql);
      pstmt.setInt(1, databaseId);
      pstmt.setString(2, tableName);
      res = pstmt.executeQuery();
      if (!res.next()) {
        throw new CatalogException("ERROR: there is no tid matched to " + tableName);
      }
      return res.getInt(1);
    } catch (SQLException se) {
      throw new NoSuchTableException(databaseName, namespace, tableName);
    } finally {
      CatalogUtil.closeQuietly(conn, pstmt, res);
    }
  }

  @Override
  public void createTable(final CatalogProtos.TableDescProto table) throws CatalogException {
    Connection conn = null;
    PreparedStatement pstmt = null;
    ResultSet res = null;

    try {
      conn = getConnection();
      conn.setAutoCommit(false);

      String databaseName = CatalogUtil.normalizeIdentifier(table.getDatabaseName());
      String tableName = CatalogUtil.normalizeIdentifier(table.getTableName());

      int dbid = getDatabaseId(databaseName);
      String sql = "INSERT INTO TABLES (db_id, TABLE_ID, path, store_type) VALUES(?, ?, ?, ?) ";

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }

      pstmt = conn.prepareStatement(sql);
      pstmt.setInt(1, dbid);
      pstmt.setString(2, tableName);
      pstmt.setString(3, table.getPath());
      pstmt.setString(4, table.getMeta().getStoreType().name());
      pstmt.executeUpdate();
      pstmt.close();

      String tidSql = "SELECT TID from TABLES WHERE db_id = ? AND TABLE_ID = ?";
      pstmt = conn.prepareStatement(tidSql);
      pstmt.setInt(1, dbid);
      pstmt.setString(2, tableName);
      res = pstmt.executeQuery();

      if (!res.next()) {
        throw new CatalogException("ERROR: there is no tid matched to " + table.getTableName());
      }

      int tableId = res.getInt("TID");
      res.close();
      pstmt.close();

      String colSql =
          "INSERT INTO " + TB_COLUMNS + " (TID, COLUMN_NAME, COLUMN_ID, data_type, type_length) VALUES(?, ?, ?, ?, ?) ";

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }

      pstmt = conn.prepareStatement(colSql);
      for(int i = 0; i < table.getSchema().getFieldsCount(); i++) {
        ColumnProto col = table.getSchema().getFields(i);
        pstmt.setInt(1, tableId);
        pstmt.setString(2, CatalogUtil.extractSimpleName(col.getName()));
        pstmt.setInt(3, i);
        pstmt.setString(4, col.getDataType().getType().name());
        pstmt.setInt(5, (col.getDataType().hasLength() ? col.getDataType().getLength() : 0));
        pstmt.addBatch();
        pstmt.clearParameters();
      }
      pstmt.executeBatch();
      pstmt.close();

      if(table.getMeta().hasParams()) {
        String propSQL = "INSERT INTO " + TB_OPTIONS + "(TID, key_, value_) VALUES(?, ?, ?)";

        if (LOG.isDebugEnabled()) {
          LOG.debug(propSQL);
        }

        pstmt = conn.prepareStatement(propSQL);
        for (CatalogProtos.KeyValueProto entry : table.getMeta().getParams().getKeyvalList()) {
          pstmt.setInt(1, tableId);
          pstmt.setString(2, entry.getKey());
          pstmt.setString(3, entry.getValue());
          pstmt.addBatch();
          pstmt.clearParameters();
        }
        pstmt.executeBatch();
        pstmt.close();
      }

      if (table.hasStats()) {

        String statSql = "INSERT INTO " + TB_STATISTICS + " (TID, num_rows, num_bytes) VALUES(?, ?, ?)";

        if (LOG.isDebugEnabled()) {
          LOG.debug(statSql);
        }

        pstmt = conn.prepareStatement(statSql);
        pstmt.setInt(1, tableId);
        pstmt.setLong(2, table.getStats().getNumRows());
        pstmt.setLong(3, table.getStats().getNumBytes());
        pstmt.executeUpdate();
        pstmt.close();
      }

      if(table.hasPartition()) {
        String partSql =
            "INSERT INTO PARTITION_METHODS (TID, PARTITION_TYPE, EXPRESSION, EXPRESSION_SCHEMA) VALUES(?, ?, ?, ?)";

        if (LOG.isDebugEnabled()) {
          LOG.debug(partSql);
        }

        pstmt = conn.prepareStatement(partSql);
        pstmt.setInt(1, tableId);
        pstmt.setString(2, table.getPartition().getPartitionType().name());
        pstmt.setString(3, table.getPartition().getExpression());
        pstmt.setBytes(4, table.getPartition().getExpressionSchema().toByteArray());
        pstmt.executeUpdate();
      }

      // If there is no error, commit the changes.
      conn.commit();
    } catch (SQLException se) {
      try {
        // If there is any error, rollback the changes.
        conn.rollback();
      } catch (SQLException se2) {
      }
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(conn, pstmt, res);
    }
  }

  private int getDatabaseId(String databaseName) throws SQLException {
    String sql = String.format("SELECT db_id from %s WHERE db_name = ?", TB_DATABASES);

    if (LOG.isDebugEnabled()) {
      LOG.debug(sql);
    }

    Connection conn = null;
    PreparedStatement pstmt = null;
    ResultSet res = null;

    try {
      conn = getConnection();
      pstmt = conn.prepareStatement(sql);
      pstmt.setString(1, databaseName);
      res = pstmt.executeQuery();
      if (!res.next()) {
        throw new NoSuchDatabaseException(databaseName);
      }

      return res.getInt("db_id");
    } finally {
      CatalogUtil.closeQuietly(conn, pstmt, res);
    }
  }

  @Override
  public boolean existTable(String databaseName, String namespace, final String tableName) throws CatalogException {
    Connection conn = null;
    PreparedStatement pstmt = null;
    ResultSet res = null;
    boolean exist = false;

    try {
      int dbid = getDatabaseId(databaseName);

      String sql = "SELECT TID FROM TABLES WHERE db_id = ? AND TABLE_ID = ?";

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql.toString());
      }

      conn = getConnection();
      pstmt = conn.prepareStatement(sql.toString());

      pstmt.setInt(1, dbid);
      pstmt.setString(2, tableName);
      res = pstmt.executeQuery();
      exist = res.next();
    } catch (SQLException se) {
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(conn, pstmt, res);
    }

    return exist;
  }

  public void dropTableInternal(Connection conn, String databaseName, String namespace, final String tableName)
      throws SQLException {

    PreparedStatement pstmt = null;

    try {
      int databaseId = getDatabaseId(databaseName);
      int tableId = getTableId(databaseId, databaseName, namespace, tableName);

      String sql = "DELETE FROM " + TB_COLUMNS + " WHERE " + COL_TABLES_PK + " = ?";

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }

      pstmt = conn.prepareStatement(sql);
      pstmt.setInt(1, tableId);
      pstmt.executeUpdate();
      pstmt.close();


      sql = "DELETE FROM " + TB_OPTIONS + " WHERE " + COL_TABLES_PK + " = ? ";

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }

      pstmt = conn.prepareStatement(sql);
      pstmt.setInt(1, tableId);
      pstmt.executeUpdate();
      pstmt.close();


      sql = "DELETE FROM " + TB_STATISTICS + " WHERE " + COL_TABLES_PK + " = ? ";

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }

      pstmt = conn.prepareStatement(sql);
      pstmt.setInt(1, tableId);
      pstmt.executeUpdate();
      pstmt.close();

      sql = "DELETE FROM " + TB_PARTTIONS + " WHERE " + COL_TABLES_PK + " = ? ";

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }

      pstmt = conn.prepareStatement(sql);
      pstmt.setInt(1, tableId);
      pstmt.executeUpdate();
      pstmt.close();

      sql = "DELETE FROM " + TB_PARTITION_METHODS + " WHERE " + COL_TABLES_PK + " = ? ";

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }

      pstmt = conn.prepareStatement(sql);
      pstmt.setInt(1, tableId);
      pstmt.executeUpdate();
      pstmt.close();

      sql = "DELETE FROM TABLES WHERE db_id = ? AND " + COL_TABLES_PK + " = ?";

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }

      pstmt = conn.prepareStatement(sql);
      pstmt.setInt(1, databaseId);
      pstmt.setInt(2, tableId);
      pstmt.executeUpdate();

    } finally {
      CatalogUtil.closeQuietly(conn, pstmt);
    }
  }

  @Override
  public void dropTable(String databaseName, String namespace, final String tableName) throws CatalogException {
    Connection conn = null;
    try {
      conn = getConnection();
      conn.setAutoCommit(false);
      dropTableInternal(conn, databaseName, namespace, tableName);
      conn.commit();
    } catch (SQLException se) {
      try {
        conn.rollback();
      } catch (SQLException e) {
        LOG.error(e);
      }
    } finally {
      CatalogUtil.closeQuietly(conn);
    }
  }

  @Override
  public CatalogProtos.TableDescProto getTable(String databaseName, String namespace, String tableName)
      throws CatalogException {
    Connection conn = null;
    ResultSet res = null;
    PreparedStatement pstmt = null;

    CatalogProtos.TableDescProto.Builder tableBuilder = null;
    StoreType storeType;

    try {
      tableBuilder = CatalogProtos.TableDescProto.newBuilder();

      int dbid = getDatabaseId(databaseName);
      tableBuilder.setDatabaseName(databaseName);

      //////////////////////////////////////////
      // Geting Table Description
      //////////////////////////////////////////
      String sql = "SELECT TID, TABLE_ID, path, store_type FROM TABLES WHERE db_id = ? AND TABLE_ID = ?";


      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }

      conn = getConnection();
      pstmt = conn.prepareStatement(sql);
      pstmt.setInt(1, dbid);
      pstmt.setString(2, tableName);
      res = pstmt.executeQuery();

      if (!res.next()) { // there is no table of the given name.
        return null;
      }

      int tableId = res.getInt("TID");
      tableBuilder.setTableName(res.getString(COL_TABLES_NAME).trim());
      tableBuilder.setPath(res.getString("path").trim());
      storeType = CatalogUtil.getStoreType(res.getString("store_type").trim());

      res.close();
      pstmt.close();

      //////////////////////////////////////////
      // Geting Column Descriptions
      //////////////////////////////////////////
      CatalogProtos.SchemaProto.Builder schemaBuilder = CatalogProtos.SchemaProto.newBuilder();
      sql = "SELECT COLUMN_NAME, DATA_TYPE, TYPE_LENGTH from " + TB_COLUMNS +
          " WHERE " + COL_TABLES_PK + " = ? ORDER BY COLUMN_ID ASC";

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }

      pstmt = conn.prepareStatement(sql);
      pstmt.setInt(1, tableId);
      res = pstmt.executeQuery();

      while (res.next()) {
        schemaBuilder.addFields(resultToColumnProto(res));
      }

      tableBuilder.setSchema(CatalogUtil.getQualfiedSchema(tableName, schemaBuilder.build()));

      res.close();
      pstmt.close();

      //////////////////////////////////////////
      // Geting Table Properties
      //////////////////////////////////////////
      CatalogProtos.TableProto.Builder metaBuilder = CatalogProtos.TableProto.newBuilder();
      metaBuilder.setStoreType(storeType);
      sql = "SELECT key_, value_ FROM " + TB_OPTIONS +" WHERE " + COL_TABLES_PK + " = ?";

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }

      pstmt = conn.prepareStatement(sql);
      pstmt.setInt(1, tableId);
      res = pstmt.executeQuery();
      metaBuilder.setParams(resultToKeyValueSetProto(res));
      tableBuilder.setMeta(metaBuilder);

      res.close();
      pstmt.close();

      //////////////////////////////////////////
      // Geting Table Stats
      //////////////////////////////////////////
      sql = "SELECT num_rows, num_bytes FROM " + TB_STATISTICS + " WHERE " + COL_TABLES_PK + " = ?";
      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }
      pstmt = conn.prepareStatement(sql);
      pstmt.setInt(1, tableId);
      res = pstmt.executeQuery();

      if (res.next()) {
        TableStatsProto.Builder statBuilder = TableStatsProto.newBuilder();
        statBuilder.setNumRows(res.getLong("num_rows"));
        statBuilder.setNumBytes(res.getLong("num_bytes"));
        tableBuilder.setStats(statBuilder);
      }
      res.close();
      pstmt.close();


      //////////////////////////////////////////
      // Getting Table Partition Method
      //////////////////////////////////////////
      sql = " SELECT partition_type, expression, expression_schema FROM " + TB_PARTITION_METHODS +
          " WHERE " + COL_TABLES_PK + " = ?";

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }

      pstmt = conn.prepareStatement(sql);
      pstmt.setInt(1, tableId);
      res = pstmt.executeQuery();

      if (res.next()) {
        tableBuilder.setPartition(resultToPartitionMethodProto(databaseName, namespace, tableName, res));
      }
    } catch (InvalidProtocolBufferException e) {
      throw new CatalogException(e);
    } catch (SQLException se) {
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(conn, pstmt, res);
    }

    return tableBuilder.build();
  }

  private Type getDataType(final String typeStr) {
    try {
      return Enum.valueOf(Type.class, typeStr);
    } catch (IllegalArgumentException iae) {
      LOG.error("Cannot find a matched type aginst from '" + typeStr + "'");
      return null;
    }
  }

  @Override
  public List<String> getAllTableNames(String databaseName, String namespace) throws CatalogException {
    Connection conn = null;
    PreparedStatement pstmt = null;
    ResultSet res = null;

    List<String> tables = new ArrayList<String>();

    try {

      int dbid = getDatabaseId(databaseName);

      String sql = "SELECT TABLE_ID FROM TABLES WHERE db_id = ?";

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }

      conn = getConnection();
      pstmt = conn.prepareStatement(sql);
      pstmt.setInt(1, dbid);
      res = pstmt.executeQuery();
      while (res.next()) {
        tables.add(res.getString(COL_TABLES_NAME).trim());
      }
    } catch (SQLException se) {
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(conn, pstmt, res);
    }
    return tables;
  }

  private static final String ADD_PARTITION_SQL =
      "INSERT INTO " + TB_PARTTIONS + " (TID, PARTITION_NAME, ORDINAL_POSITION, PATH) VALUES (?,?,?,?)";


  @Override
  public void addPartitions(CatalogProtos.PartitionsProto partitionsProto) throws CatalogException {
    Connection conn = null;
    PreparedStatement pstmt = null;

    try {
      if (LOG.isDebugEnabled()) {
        LOG.debug(ADD_PARTITION_SQL);
      }

      String databaseName = CatalogUtil.normalizeIdentifier(partitionsProto.getTableIdentifier().getDatabaseName());
      String namespace = CatalogUtil.normalizeIdentifier(partitionsProto.getTableIdentifier().getNamespace());
      String tableName = CatalogUtil.normalizeIdentifier(partitionsProto.getTableIdentifier().getTableName());

      int databaseId = getDatabaseId(databaseName);
      int tableId = getTableId(databaseId, databaseName, namespace, tableName);

      conn = getConnection();
      pstmt = conn.prepareStatement(ADD_PARTITION_SQL);

      for (CatalogProtos.PartitionDescProto partition : partitionsProto.getPartitionList()) {
        addPartitionInternal(pstmt, tableId, partition);
      }
      pstmt.executeBatch();
      conn.commit();
    } catch (SQLException se) {
      try {
        conn.rollback();
      } catch (SQLException e) {
        LOG.error(e);
      }
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(conn, pstmt);
    }
  }

  private static void addPartitionInternal(PreparedStatement pstmt, int tableId, PartitionDescProto partition) throws
      SQLException {
      pstmt.setInt(1, tableId);
      pstmt.setString(2, partition.getPartitionName());
      pstmt.setInt(3, partition.getOrdinalPosition());
      pstmt.setString(4, partition.getPath());
      pstmt.addBatch();
      pstmt.clearParameters();
  }

  @Override
  public void addPartitionMethod(CatalogProtos.PartitionMethodProto proto) throws CatalogException {
    Connection conn = null;
    PreparedStatement pstmt = null;

    try {
      String sql = "INSERT INTO " + TB_PARTITION_METHODS + " (TID, PARTITION_TYPE,  EXPRESSION, EXPRESSION_SCHEMA) " +
              "VALUES (?,?,?,?)";

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }

      String databaseName = CatalogUtil.normalizeIdentifier(proto.getTableIdentifier().getDatabaseName());
      String namespace = CatalogUtil.normalizeIdentifier(proto.getTableIdentifier().getNamespace());
      String tableName = CatalogUtil.normalizeIdentifier(proto.getTableIdentifier().getTableName());

      int databaseId = getDatabaseId(databaseName);
      int tableId = getTableId(databaseId, databaseName, namespace, tableName);

      conn = getConnection();
      pstmt = conn.prepareStatement(sql);
      pstmt.setInt(1, tableId);
      pstmt.setString(2, proto.getPartitionType().name());
      pstmt.setString(3, proto.getExpression());
      pstmt.setBytes(4, proto.getExpressionSchema().toByteArray());
      pstmt.executeUpdate();
    } catch (SQLException se) {
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(conn, pstmt);
    }
  }

  @Override
  public void dropPartitionMethod(String databaseName, String tableName) throws CatalogException {
    Connection conn = null;
    PreparedStatement pstmt = null;

    try {
      String sql = "DELETE FROM " + TB_PARTITION_METHODS + " WHERE " + COL_TABLES_NAME + " = ? ";

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }

      conn = getConnection();
      pstmt = conn.prepareStatement(sql);
      pstmt.setString(1, tableName);
      pstmt.executeUpdate();
    } catch (SQLException se) {
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(conn, pstmt);
    }
  }

  @Override
  public CatalogProtos.PartitionMethodProto getPartitionMethod(String databaseName, String namespace, String tableName)
      throws CatalogException {
    Connection conn = null;
    ResultSet res = null;
    PreparedStatement pstmt = null;

    try {
      String sql = "SELECT partition_type, expression, expression_schema FROM " + TB_PARTITION_METHODS +
      " WHERE " + COL_TABLES_NAME + " = ? ";

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }

      conn = getConnection();
      pstmt = conn.prepareStatement(sql);
      pstmt.setString(1, tableName);
      res = pstmt.executeQuery();

      if (res.next()) {
        return resultToPartitionMethodProto(databaseName, namespace, tableName, res);
      }
    } catch (InvalidProtocolBufferException e) {
      throw new CatalogException(e);
    } catch (SQLException se) {
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(conn, pstmt, res);
    }
    return null;
  }

  @Override
  public boolean existPartitionMethod(String databaseName, String namespace, String tableName) throws CatalogException {
    Connection conn = null;
    ResultSet res = null;
    PreparedStatement pstmt = null;
    boolean exist = false;

    try {
      String sql = "SELECT partition_type, expression, expression_schema FROM " + TB_PARTITION_METHODS +
      " WHERE " + COL_TABLES_NAME + "= ?";

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }

      conn = getConnection();
      pstmt = conn.prepareStatement(sql);
      pstmt.setString(1, tableName);
      res = pstmt.executeQuery();

      exist = res.next();
    } catch (SQLException se) {
      throw new CatalogException(se);
    } finally {                           
      CatalogUtil.closeQuietly(conn, pstmt, res);
    }
    return exist;
  }

  @Override
  public void addPartition(String databaseName, String namespace, String tableName,
                           CatalogProtos.PartitionDescProto partition) throws CatalogException {
    Connection conn = null;
    PreparedStatement pstmt = null;

    try {
      if (LOG.isDebugEnabled()) {
        LOG.debug(ADD_PARTITION_SQL);
      }

      int databaseId = getDatabaseId(databaseName);
      int tableId = getTableId(databaseId, databaseName, namespace, tableName);

      conn = getConnection();
      pstmt = conn.prepareStatement(ADD_PARTITION_SQL);
      addPartitionInternal(pstmt, tableId, partition);
      pstmt.executeUpdate();
    } catch (SQLException se) {
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(conn, pstmt);
    }
  }

  @Override
  public CatalogProtos.PartitionDescProto getPartition(String partitionName) throws CatalogException {
    // TODO
    throw new UnimplementedException("getPartition is not implemented");
  }


  @Override
  public CatalogProtos.PartitionsProto getPartitions(String tableName) throws CatalogException {
    // TODO
    throw new UnimplementedException("getPartitions is not implemented");
  }


  @Override
  public void delPartition(String partitionName) throws CatalogException {
    Connection conn = null;
    PreparedStatement pstmt = null;

    try {
      String sql = "DELETE FROM " + TB_PARTTIONS + " WHERE PARTITION_NAME = ? ";

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }

      conn = getConnection();
      pstmt = conn.prepareStatement(sql);
      pstmt.setString(1, partitionName);
      pstmt.executeUpdate();
    } catch (SQLException se) {
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(conn, pstmt);
    }
  }

  @Override
  public void dropPartitions(String tableName) throws CatalogException {
    Connection conn = null;
    PreparedStatement pstmt = null;

    try {
      String sql = "DELETE FROM " + TB_PARTTIONS + " WHERE " + COL_TABLES_NAME + "= ? ";

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }

      conn = getConnection();
      pstmt = conn.prepareStatement(sql);
      pstmt.setString(1, tableName);
      pstmt.executeUpdate();
    } catch (SQLException se) {
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(conn, pstmt);
    }
  }


  @Override
  public void createIndex(final IndexDescProto proto) throws CatalogException {
    Connection conn = null;
    PreparedStatement pstmt = null;

    String databaseName = CatalogUtil.normalizeIdentifier(proto.getTableIdentifier().getDatabaseName());
    String namespace = CatalogUtil.normalizeIdentifier(proto.getTableIdentifier().getNamespace());
    String tableName = CatalogUtil.normalizeIdentifier(proto.getTableIdentifier().getTableName());
    String columnName = CatalogUtil.extractSimpleName(CatalogUtil.normalizeIdentifier(proto.getColumn().getName()));

    try {
      int databaseId = getDatabaseId(databaseName);
      int tableId = getTableId(databaseId, databaseName, namespace, tableName);

      String sql = "INSERT INTO " + TB_INDEXES +
        " (" + COL_DATABASES_PK + ", " + COL_TABLES_PK + ", INDEX_NAME, " +
          "COLUMN_NAME, DATA_TYPE, INDEX_TYPE, IS_UNIQUE, IS_CLUSTERED, IS_ASCENDING) VALUES (?,?,?,?,?,?,?,?,?)";

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }

      conn = getConnection();
      pstmt = conn.prepareStatement(sql);

      pstmt.setInt(1, databaseId);
      pstmt.setInt(2, tableId);
      pstmt.setString(3, proto.getName());
      pstmt.setString(4, columnName);
      pstmt.setString(5, proto.getColumn().getDataType().getType().name());
      pstmt.setString(6, proto.getIndexMethod().toString());
      pstmt.setBoolean(7, proto.hasIsUnique() && proto.getIsUnique());
      pstmt.setBoolean(8, proto.hasIsClustered() && proto.getIsClustered());
      pstmt.setBoolean(9, proto.hasIsAscending() && proto.getIsAscending());
      pstmt.executeUpdate();

    } catch (SQLException se) {
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(conn, pstmt);
    }
  }

  @Override
  public void dropIndex(String databaseName, String namespace, final String indexName) throws CatalogException {
    Connection conn = null;
    PreparedStatement pstmt = null;

    try {
      int databaseId = getDatabaseId(databaseName);
      String sql = "DELETE FROM " + TB_INDEXES + " WHERE " + COL_DATABASES_PK + "=? AND index_name=?";

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }

      conn = getConnection();
      pstmt = conn.prepareStatement(sql);
      pstmt.setInt(1, databaseId);
      pstmt.setString(2, indexName);
      pstmt.executeUpdate();
    } catch (SQLException se) {
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(conn, pstmt);
    }
  }

  public static String getTableName(Connection conn, int tableId) throws SQLException {
    ResultSet res = null;
    PreparedStatement pstmt = null;

    try {
      pstmt =
          conn.prepareStatement("SELECT " + COL_TABLES_NAME + " FROM " + TB_TABLES + " WHERE " + COL_TABLES_PK +"=?");
      pstmt.setInt(1, tableId);
      res = pstmt.executeQuery();
      if (!res.next()) {
        throw new CatalogException("Cannot get any table name from TID");
      }
      return res.getString(1);
    } finally {
      CatalogUtil.closeQuietly(null, pstmt, res);
    }
  }

  final static String GET_INDEXES_SQL =
      "SELECT " + COL_TABLES_PK + ", INDEX_NAME, COLUMN_NAME, DATA_TYPE, INDEX_TYPE, IS_UNIQUE, "+
          "IS_CLUSTERED, IS_ASCENDING FROM " + TB_INDEXES;

  @Override
  public IndexDescProto getIndexByName(String databaseName, String namespace, final String indexName)
      throws CatalogException {
    Connection conn = null;
    ResultSet res = null;
    PreparedStatement pstmt = null;
    IndexDescProto proto = null;

    try {
      int databaseId = getDatabaseId(databaseName);

      String sql = GET_INDEXES_SQL + " WHERE " + COL_DATABASES_PK + "=? AND INDEX_NAME=?";

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }

      conn = getConnection();
      pstmt = conn.prepareStatement(sql);
      pstmt.setInt(1, databaseId);
      pstmt.setString(2, indexName);
      res = pstmt.executeQuery();
      if (!res.next()) {
        throw new CatalogException("ERROR: there is no index matched to " + indexName);
      }
      IndexDescProto.Builder builder = IndexDescProto.newBuilder();
      resultToIndexDescProtoBuilder(builder, res);
      String tableName = getTableName(conn, res.getInt(COL_TABLES_PK));
      builder.setTableIdentifier(CatalogUtil.buildTableIdentifier(databaseName, namespace, tableName));
      proto = builder.build();
    } catch (SQLException se) {
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(conn, pstmt, res);
    }

    return proto;
  }

  @Override
  public IndexDescProto getIndexByColumn(final String databaseName,
                                         final String namespace,
                                         final String tableName,
                                         final String columnName) throws CatalogException {
    Connection conn = null;
    ResultSet res = null;
    PreparedStatement pstmt = null;
    IndexDescProto proto = null;

    try {
      int databaseId = getDatabaseId(databaseName);

      String sql = GET_INDEXES_SQL + " WHERE " + COL_DATABASES_PK + "=? AND COLUMN_NAME=?";

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }

      conn = getConnection();
      pstmt = conn.prepareStatement(sql);
      pstmt.setInt(1, databaseId);;
      pstmt.setString(2, columnName);
      res = pstmt.executeQuery();
      if (!res.next()) {
        throw new CatalogException("ERROR: there is no index matched to " + columnName);
      }
      IndexDescProto.Builder builder = IndexDescProto.newBuilder();
      resultToIndexDescProtoBuilder(builder, res);
      builder.setTableIdentifier(CatalogUtil.buildTableIdentifier(databaseName, namespace, tableName));
      proto = builder.build();
    } catch (SQLException se) {
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(conn, pstmt, res);
    }

    return proto;
  }

  @Override
  public boolean existIndexByName(String databaseName, String namespace, final String indexName) throws CatalogException {
    Connection conn = null;
    ResultSet res = null;
    PreparedStatement pstmt = null;

    boolean exist = false;

    try {
      int databaseId = getDatabaseId(databaseName);

      String sql =
          "SELECT INDEX_NAME FROM " + TB_INDEXES + " WHERE " + COL_DATABASES_PK + "=? AND INDEX_NAME=?";

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }

      conn = getConnection();
      pstmt = conn.prepareStatement(sql);
      pstmt.setInt(1, databaseId);
      pstmt.setString(2, indexName);
      res = pstmt.executeQuery();
      exist = res.next();
    } catch (SQLException se) {
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(conn, pstmt, res);
    }

    return exist;
  }

  @Override
  public boolean existIndexByColumn(String databaseName, String namespace, String tableName, String columnName)
      throws CatalogException {
    Connection conn = null;
    ResultSet res = null;
    PreparedStatement pstmt = null;

    boolean exist = false;

    try {
      int databaseId = getDatabaseId(databaseName);

      String sql =
          "SELECT INDEX_NAME FROM " + TB_INDEXES + " WHERE " + COL_DATABASES_PK + "=? AND COLUMN_NAME=?";

      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }

      conn = getConnection();
      pstmt = conn.prepareStatement(sql);
      pstmt.setInt(1, databaseId);
      pstmt.setString(2, columnName);
      res = pstmt.executeQuery();
      exist = res.next();
    } catch (SQLException se) {
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(conn, pstmt, res);
    }
    return exist;
  }

  @Override
  public IndexDescProto[] getIndexes(String databaseName, String namespace, final String tableName)
      throws CatalogException {
    Connection conn = null;
    ResultSet res = null;
    PreparedStatement pstmt = null;
    final List<IndexDescProto> protos = new ArrayList<IndexDescProto>();

    try {
      final int databaseId = getDatabaseId(databaseName);
      final int tableId = getTableId(databaseId, databaseName, namespace, tableName);
      final TableIdentifierProto tableIdentifier = CatalogUtil.buildTableIdentifier(databaseName, namespace, tableName);


      String sql = GET_INDEXES_SQL + " WHERE " + COL_DATABASES_PK + "=? AND " + COL_TABLES_PK + "=?";


      if (LOG.isDebugEnabled()) {
        LOG.debug(sql);
      }

      conn = getConnection();
      pstmt = conn.prepareStatement(sql);
      pstmt.setInt(1, databaseId);
      pstmt.setInt(2, tableId);
      res = pstmt.executeQuery();

      while (res.next()) {
        IndexDescProto.Builder builder = IndexDescProto.newBuilder();
        resultToIndexDescProtoBuilder(builder, res);
        builder.setTableIdentifier(tableIdentifier);
        protos.add(builder.build());
      }
    } catch (SQLException se) {
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(conn, pstmt, res);
    }

    return protos.toArray(new IndexDescProto[protos.size()]);
  }

  private void resultToIndexDescProtoBuilder(IndexDescProto.Builder builder,
                                                final ResultSet res) throws SQLException {
    builder.setName(res.getString("index_name"));
    builder.setColumn(indexResultToColumnProto(res));
    builder.setIndexMethod(getIndexMethod(res.getString("index_type").trim()));
    builder.setIsUnique(res.getBoolean("is_unique"));
    builder.setIsClustered(res.getBoolean("is_clustered"));
    builder.setIsAscending(res.getBoolean("is_ascending"));
  }

  /**
   * INDEXS table doesn't store type_length, so we need another resultToColumnProto method
   */
  private ColumnProto indexResultToColumnProto(final ResultSet res) throws SQLException {
    ColumnProto.Builder builder = ColumnProto.newBuilder();
    builder.setName(res.getString("column_name").trim());

    Type type = getDataType(res.getString("data_type").trim());
    builder.setDataType(CatalogUtil.newSimpleDataType(type));

    return builder.build();
  }

  private ColumnProto resultToColumnProto(final ResultSet res) throws SQLException {
    ColumnProto.Builder builder = ColumnProto.newBuilder();
    builder.setName(res.getString("column_name").trim());

    Type type = getDataType(res.getString("data_type").trim());
    int typeLength = res.getInt("type_length");
    if(typeLength > 0 ) {
      builder.setDataType(CatalogUtil.newDataTypeWithLen(type, typeLength));
    } else {
      builder.setDataType(CatalogUtil.newSimpleDataType(type));
    }

    return builder.build();
  }

  private CatalogProtos.KeyValueSetProto resultToKeyValueSetProto(final ResultSet res) throws SQLException {
    CatalogProtos.KeyValueSetProto.Builder setBuilder = CatalogProtos.KeyValueSetProto.newBuilder();
    CatalogProtos.KeyValueProto.Builder builder = CatalogProtos.KeyValueProto.newBuilder();
    while (res.next()) {
      builder.setKey(res.getString("key_"));
      builder.setValue(res.getString("value_"));
      setBuilder.addKeyval(builder.build());
    }
    return setBuilder.build();
  }

  private IndexMethod getIndexMethod(final String typeStr) {
    if (typeStr.equals(IndexMethod.TWO_LEVEL_BIN_TREE.toString())) {
      return IndexMethod.TWO_LEVEL_BIN_TREE;
    } else {
      LOG.error("Cannot find a matched type aginst from '"
          + typeStr + "'");
      // TODO - needs exception handling
      return null;
    }
  }

  private CatalogProtos.PartitionMethodProto resultToPartitionMethodProto(final String databaseName,
                                                                          final String namespace,
                                                                          final String tableName,
                                                                          final ResultSet res)
      throws SQLException, InvalidProtocolBufferException {
    CatalogProtos.PartitionMethodProto.Builder partBuilder = CatalogProtos.PartitionMethodProto.newBuilder();
    partBuilder.setTableIdentifier(CatalogUtil.buildTableIdentifier(databaseName, namespace, tableName));
    partBuilder.setPartitionType(CatalogProtos.PartitionType.valueOf(res.getString("partition_type")));
    partBuilder.setExpression(res.getString("expression"));
    partBuilder.setExpressionSchema(SchemaProto.parseFrom(res.getBytes("expression_schema")));
    return partBuilder.build();
  }

  @Override
  public void close() {
    CatalogUtil.closeQuietly(conn);
    LOG.info("Shutdown database (" + catalogUri + ")");
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
}
