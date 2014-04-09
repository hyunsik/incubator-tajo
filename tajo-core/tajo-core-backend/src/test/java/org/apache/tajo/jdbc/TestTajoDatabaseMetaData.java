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

package org.apache.tajo.jdbc;

import org.apache.tajo.QueryTestCaseBase;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.util.TUtil;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestTajoDatabaseMetaData extends QueryTestCaseBase {
  private static InetSocketAddress tajoMasterAddress;

  @BeforeClass
  public static void setUp() throws Exception {
    tajoMasterAddress = testingCluster.getMaster().getTajoMasterClientService().getBindAddress();
    Class.forName("org.apache.tajo.jdbc.TajoDriver").newInstance();
  }

  public static List<String> getListFromResultSet(ResultSet resultSet, String columnName) throws SQLException {
    List<String> list = new ArrayList<String>();
    while(resultSet.next()) {
      list.add(resultSet.getString(columnName));
    }
    return list;
  }

  @Test
  public void testSetAndGetCatalog() throws Exception {
    String connUri = TestTajoJdbc.buildConnectionUri(tajoMasterAddress.getHostName(), tajoMasterAddress.getPort(),
        TajoConstants.DEFAULT_DATABASE_NAME);
    Connection conn = DriverManager.getConnection(connUri);

    assertDatabaseNotExists("jdbc_test1");
    PreparedStatement pstmt = conn.prepareStatement("CREATE DATABASE jdbc_test1");
    pstmt.executeUpdate();
    assertDatabaseExists("jdbc_test1");
    pstmt.close();

    assertDatabaseNotExists("Jdbc_Test2");
    pstmt = conn.prepareStatement("CREATE DATABASE \"Jdbc_Test2\"");
    pstmt.executeUpdate();
    assertDatabaseExists("Jdbc_Test2");
    pstmt.close();

    conn.setCatalog("jdbc_test1");
    assertEquals("jdbc_test1", conn.getCatalog());
    conn.setCatalog("Jdbc_Test2");
    assertEquals("Jdbc_Test2", conn.getCatalog());
    conn.setCatalog("jdbc_test1");
    assertEquals("jdbc_test1", conn.getCatalog());

    conn.setCatalog(TajoConstants.DEFAULT_DATABASE_NAME);
    pstmt = conn.prepareStatement("DROP DATABASE jdbc_test1");
    pstmt.executeUpdate();
    pstmt.close();
    pstmt = conn.prepareStatement("DROP DATABASE \"Jdbc_Test2\"");
    pstmt.executeUpdate();
    pstmt.close();

    conn.close();
  }

  @Test
  public void testGetCatalogsAndTables() throws Exception {
    String connUri = TestTajoJdbc.buildConnectionUri(tajoMasterAddress.getHostName(), tajoMasterAddress.getPort(),
        TajoConstants.DEFAULT_DATABASE_NAME);
    Connection defaultConnect = DriverManager.getConnection(connUri);

    DatabaseMetaData dbmd = defaultConnect.getMetaData();
    List<String> existingDatabases = getListFromResultSet(dbmd.getCatalogs(), "TABLE_CAT");

    // create database "jdbc_test1" and its tables
    assertDatabaseNotExists("jdbc_test1");
    PreparedStatement pstmt = defaultConnect.prepareStatement("CREATE DATABASE jdbc_test1");
    pstmt.executeUpdate();
    assertDatabaseExists("jdbc_test1");
    pstmt.close();
    pstmt = defaultConnect.prepareStatement("CREATE TABLE jdbc_test1.table1 (age int)");
    pstmt.executeUpdate();
    pstmt.close();
    pstmt = defaultConnect.prepareStatement("CREATE TABLE jdbc_test1.table2 (age int)");
    pstmt.executeUpdate();
    pstmt.close();

    // create database "jdbc_test2" and its tables
    assertDatabaseNotExists("Jdbc_Test2");
    pstmt = defaultConnect.prepareStatement("CREATE DATABASE \"Jdbc_Test2\"");
    pstmt.executeUpdate();
    assertDatabaseExists("Jdbc_Test2");
    pstmt.close();

    pstmt = defaultConnect.prepareStatement("CREATE TABLE \"Jdbc_Test2\".table3 (age int)");
    pstmt.executeUpdate();
    pstmt.close();
    pstmt = defaultConnect.prepareStatement("CREATE TABLE \"Jdbc_Test2\".table4 (age int)");
    pstmt.executeUpdate();
    pstmt.close();

    // verify getCatalogs()
    dbmd = defaultConnect.getMetaData();
    List<String> newDatabases = getListFromResultSet(dbmd.getCatalogs(), "TABLE_CAT");

    newDatabases.removeAll(existingDatabases);
    assertEquals(2, newDatabases.size());
    assertTrue(newDatabases.contains("jdbc_test1"));
    assertTrue(newDatabases.contains("Jdbc_Test2"));

    // verify getTables()
    ResultSet res = defaultConnect.getMetaData().getTables("jdbc_test1", null, null, null);
    assertResultSet(res, "getTables1.result");
    res.close();
    res = defaultConnect.getMetaData().getTables("Jdbc_Test2", null, null, null);
    assertResultSet(res, "getTables2.result");
    res.close();

    defaultConnect.close();

    // jdbc1_test database connection test
    String jdbcTest1ConnUri =
        TestTajoJdbc.buildConnectionUri(tajoMasterAddress.getHostName(), tajoMasterAddress.getPort(), "jdbc_test1");
    Connection jdbcTest1Conn = DriverManager.getConnection(jdbcTest1ConnUri);
    assertEquals("jdbc_test1", jdbcTest1Conn.getCatalog());
    jdbcTest1Conn.close();

    String jdbcTest2ConnUri =
        TestTajoJdbc.buildConnectionUri(tajoMasterAddress.getHostName(), tajoMasterAddress.getPort(), "Jdbc_Test2");
    Connection jdbcTest2Conn = DriverManager.getConnection(jdbcTest2ConnUri);
    assertEquals("Jdbc_Test2", jdbcTest2Conn.getCatalog());
    jdbcTest2Conn.close();

    executeString("DROP DATABASE jdbc_test1");
    executeString("DROP DATABASE \"Jdbc_Test2\"");
  }

  @Test
  public void testGetTablesWithPattern() throws Exception {
    String connUri = TestTajoJdbc.buildConnectionUri(tajoMasterAddress.getHostName(), tajoMasterAddress.getPort(),
        TajoConstants.DEFAULT_DATABASE_NAME);
    Connection conn = DriverManager.getConnection(connUri);

    String databaseName = getCurrentDatabase();

    assertDatabaseNotExists("db_1");
    executeString("CREATE DATABASE db_1");
    assertDatabaseExists("db_1");
    for (int i = 0; i < 3; i++) {
      executeString("CREATE TABLE tb_" + i + " (age int)");
    }
    for (int i = 0; i < 3; i++) {
      executeString("CREATE TABLE table_" + i + "_ptn (age int)");
    }
  }
}
