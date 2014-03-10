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

import org.apache.hadoop.conf.Configuration;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.exception.CatalogException;
import org.apache.tajo.exception.InternalException;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

public class MySQLStore extends AbstractDBStore  {
  /** 2014-03-07: First Versioning */
  private static final int MYSQL_CATALOG_STORE_VERSION = 1;

  private static final String CATALOG_DRIVER = "com.mysql.jdbc.Driver";
  protected String getCatalogDriverName(){
    return CATALOG_DRIVER;
  }

  public MySQLStore(final Configuration conf) throws InternalException {
    super(conf);
  }

  @Override
  public int getDriverVersion() {
    return MYSQL_CATALOG_STORE_VERSION;
  }

  protected Connection createConnection(Configuration conf) throws SQLException {
    Connection con = DriverManager.getConnection(getCatalogUri(), this.connectionId,
        this.connectionPassword);
    //TODO con.setAutoCommit(false);
    return con;
  }

  // TODO - DDL and index statements should be renamed
  @Override
  protected void createBaseTable() throws CatalogException {
    Statement stmt = null;
    Connection conn = null;

    try {
      conn = getConnection();
      stmt = conn.createStatement();


      // META
      if (!baseTableMaps.get(TB_META)) {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE ").append(TB_META).append(" (version int NOT NULL)");

        if (LOG.isDebugEnabled()) {
          LOG.debug(sql.toString());
        }

        stmt.executeUpdate(sql.toString());
        LOG.info("Table '" + TB_META + " is created.");
        baseTableMaps.put(TB_META, true);
      }

      // DATABASES
      if (!baseTableMaps.get(TB_DATABASES)) {
        String sql = readSchemaFile("mysql/databases.sql");
        if (LOG.isDebugEnabled()) {
          LOG.debug(sql);
        }
        stmt.executeUpdate(sql);
      }

      // TABLES
      if (!baseTableMaps.get(TB_TABLES)) {
        StringBuilder sql = new StringBuilder();
        sql.delete(0, sql.length());
        sql.append("CREATE TABLE ").append(TB_TABLES).append("(");
        sql.append("TID int NOT NULL AUTO_INCREMENT PRIMARY KEY, ");
        sql.append(COL_TABLES_NAME).append(" VARCHAR(255) NOT NULL UNIQUE, ");
        sql.append("path TEXT, ").append("store_type CHAR(16)").append(")");

        if (LOG.isDebugEnabled()) {
          LOG.debug(sql.toString());
        }

        stmt.executeUpdate(sql.toString());
        LOG.info("Table '" + TB_TABLES + "' is created.");
        baseTableMaps.put(TB_TABLES, true);
      }

      // COLUMNS
      if (!baseTableMaps.get(TB_COLUMNS)) {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE ").append(TB_COLUMNS).append("(");
        sql.append("TID INT NOT NULL, ");
        sql.append(COL_TABLES_NAME).append(" VARCHAR(255) NOT NULL,");
        sql.append("column_id INT NOT NULL,");
        sql.append("column_name VARCHAR(255) NOT NULL, ");
        sql.append("data_type CHAR(16), ");
        sql.append("type_length INTEGER, ");
        sql.append("UNIQUE KEY(").append(COL_TABLES_NAME).append(", column_name),");
        sql.append("FOREIGN KEY(TID) REFERENCES ").append(TB_TABLES).append("(TID) ON DELETE CASCADE,");
        sql.append("FOREIGN KEY(").append(COL_TABLES_NAME).append(") REFERENCES ");
        sql.append(TB_TABLES).append("(").append(COL_TABLES_NAME).append(") ON DELETE CASCADE)");

        if (LOG.isDebugEnabled()) {
          LOG.debug(sql.toString());
        }

        stmt.executeUpdate(sql.toString());
        LOG.info("Table '" + TB_COLUMNS + " is created.");
        baseTableMaps.put(TB_COLUMNS, true);
      }

      // OPTIONS
      if (!baseTableMaps.get(TB_OPTIONS)) {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE ").append(TB_OPTIONS).append("(");
        sql.append(COL_TABLES_NAME).append( " VARCHAR(255) NOT NULL,");
        sql.append("key_ VARCHAR(255) NOT NULL, value_ VARCHAR(255) NOT NULL,");
        sql.append("INDEX(").append(COL_TABLES_NAME).append(", key_),");
        sql.append("FOREIGN KEY(").append(COL_TABLES_NAME);
        sql.append(") REFERENCES ").append(TB_TABLES);
        sql.append("(").append(COL_TABLES_NAME).append(") ON DELETE CASCADE)");

        if (LOG.isDebugEnabled()) {
          LOG.debug(sql.toString());
        }

        stmt.executeUpdate(sql.toString());
        LOG.info("Table '" + TB_OPTIONS + " is created.");
        baseTableMaps.put(TB_OPTIONS, true);
      }

      // INDEXES
      if (!baseTableMaps.get(TB_INDEXES)) {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE ").append(TB_INDEXES).append("(");
        sql.append("index_name VARCHAR(255) NOT NULL PRIMARY KEY, ");
        sql.append(COL_TABLES_NAME).append(" VARCHAR(255) NOT NULL,");
        sql.append("column_name VARCHAR(255) NOT NULL, ");
        sql.append("data_type VARCHAR(255) NOT NULL, ");
        sql.append("index_type CHAR(32) NOT NULL, ");
        sql.append("is_unique BOOLEAN NOT NULL, ");
        sql.append("is_clustered BOOLEAN NOT NULL, ");
        sql.append("is_ascending BOOLEAN NOT NULL,");
        sql.append("INDEX(").append(COL_TABLES_NAME).append(", column_name),");
        sql.append("FOREIGN KEY(").append(COL_TABLES_NAME);
        sql.append(") REFERENCES ").append(TB_TABLES);
        sql.append("(").append(COL_TABLES_NAME).append(") ON DELETE CASCADE)");

        if (LOG.isDebugEnabled()) {
          LOG.debug(sql.toString());
        }

        stmt.executeUpdate(sql.toString());
        LOG.info("Table '" + TB_INDEXES + "' is created.");
        baseTableMaps.put(TB_INDEXES, true);
      }

      if (!baseTableMaps.get(TB_STATISTICS)) {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE ").append(TB_STATISTICS).append("(");
        sql.append(COL_TABLES_NAME).append(" VARCHAR(255) NOT NULL,");
        sql.append("num_rows BIGINT, ");
        sql.append("num_bytes BIGINT,");
        sql.append("INDEX(").append(COL_TABLES_NAME).append("),");
        sql.append("FOREIGN KEY(").append(COL_TABLES_NAME);
        sql.append(") REFERENCES ").append(TB_TABLES);
        sql.append("(").append(COL_TABLES_NAME).append(") ON DELETE CASCADE)");

        if (LOG.isDebugEnabled()) {
          LOG.debug(sql.toString());
        }

        stmt.executeUpdate(sql.toString());
        LOG.info("Table '" + TB_STATISTICS + "' is created.");
        baseTableMaps.put(TB_STATISTICS, true);
      }

      // PARTITION_METHODS
      if (!baseTableMaps.get(TB_PARTITION_METHODS)) {
        String sql = readSchemaFile("partition_methods.sql");

        if (LOG.isDebugEnabled()) {
          LOG.debug(sql);
        }

        stmt.executeUpdate(sql);
        LOG.info("Table '" + TB_PARTITION_METHODS + "' is created.");
        baseTableMaps.put(TB_PARTITION_METHODS, true);
      }

      // PARTITIONS
      if (!baseTableMaps.get(TB_PARTTIONS)) {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE ").append(TB_PARTTIONS).append("(");
        sql.append("PID int NOT NULL AUTO_INCREMENT PRIMARY KEY, ");
        sql.append(COL_TABLES_NAME).append( " VARCHAR(255) NOT NULL,");
        sql.append("partition_name VARCHAR(255), ");
        sql.append("ordinal_position INT NOT NULL,");
        sql.append("partition_value TEXT,");
        sql.append("path TEXT,");
        sql.append("cache_nodes VARCHAR(255), ");
        sql.append("UNIQUE KEY(").append(COL_TABLES_NAME).append(", partition_name),");
        sql.append("FOREIGN KEY(").append(COL_TABLES_NAME);
        sql.append(") REFERENCES ").append(TB_TABLES);
        sql.append("(").append(COL_TABLES_NAME).append(") ON DELETE CASCADE)");

        if (LOG.isDebugEnabled()) {
          LOG.debug(sql.toString());
        }

        stmt.executeUpdate(sql.toString());
        LOG.info("Table '" + TB_PARTTIONS + "' is created.");
        baseTableMaps.put(TB_PARTTIONS, true);
      }

      insertSchemaVersion();

    } catch (SQLException se) {
      throw new CatalogException("failed to create base tables for MySQL catalog store", se);
    } finally {
      CatalogUtil.closeQuietly(conn, stmt);
    }
  }

  @Override
  protected void dropBaseTable() throws CatalogException {
    Connection conn = null;
    Statement stmt = null;
    Map<String, Boolean> droppedTable = new HashMap<String, Boolean>();

    try {
      conn = getConnection();
      stmt = conn.createStatement();
      StringBuilder sql = new StringBuilder();

      for(Map.Entry<String, Boolean> entry : baseTableMaps.entrySet()) {
        if(entry.getValue() && !entry.getKey().equals(TB_TABLES)) {
          sql.delete(0, sql.length());
          sql.append("DROP TABLE ").append(entry.getKey());
          stmt.addBatch(sql.toString());
          droppedTable.put(entry.getKey(), true);
        }
      }
      if(baseTableMaps.get(TB_TABLES)) {
        sql.delete(0, sql.length());
        sql.append("DROP TABLE ").append(TB_TABLES);
        stmt.addBatch(sql.toString());
        droppedTable.put(TB_TABLES, true);
      }
      stmt.executeBatch();

      for(String tableName : droppedTable.keySet()) {
        LOG.info("Table '" + tableName + "' is dropped");
      }
    } catch (SQLException se) {
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(conn, stmt);
    }
  }

  @Override
  protected boolean isInitialized() throws CatalogException {
    Connection conn = null;
    ResultSet res = null;

    try {
      conn = getConnection();
      res = conn.getMetaData().getTables(null, null, null,
          new String[]{"TABLE"});

      baseTableMaps.put(TB_META, false);
      baseTableMaps.put(TB_TABLES, false);
      baseTableMaps.put(TB_COLUMNS, false);
      baseTableMaps.put(TB_OPTIONS, false);
      baseTableMaps.put(TB_STATISTICS, false);
      baseTableMaps.put(TB_INDEXES, false);
      baseTableMaps.put(TB_PARTITION_METHODS, false);
      baseTableMaps.put(TB_PARTTIONS, false);

      if (res.wasNull())
        return false;

      while (res.next()) {
        // if my.cnf has lower_case_table_names = 1,
        // TABLE_NAME returns lower case even it created by upper case.
        baseTableMaps.put(res.getString("TABLE_NAME").toUpperCase(), true);
      }

      for(Map.Entry<String, Boolean> entry : baseTableMaps.entrySet()) {
        if (!entry.getValue()) {
          return false;
        }
      }

    } catch(SQLException se) {
      throw new CatalogException(se);
    } finally {
      CatalogUtil.closeQuietly(conn, res);
    }

    return  true;
  }
}
