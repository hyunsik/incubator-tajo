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

package org.apache.tajo;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.client.ResultSetUtil;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.storage.StorageUtil;
import org.apache.tajo.util.FileUtil;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class QueryTestCaseBase {

  protected static final TpchTestBase testingCluster;
  protected static final URL queryBaseURL;
  protected static final Path queryBasePath;
  protected static final URL resultBaseURL;
  protected static final Path resultBasePath;

  static {
    testingCluster = TpchTestBase.getInstance();
    queryBaseURL = ClassLoader.getSystemResource("queries");
    queryBasePath = new Path(queryBaseURL.toString());
    resultBaseURL = ClassLoader.getSystemResource("results");
    resultBasePath = new Path(resultBaseURL.toString());
  }

  TajoConf conf;

  // Test Class Information
  private String className;
  @Rule public TestName name= new TestName();

  @Rule
  public TestRule watcher = new TestWatcher() {
    protected void starting(Description description) {
      if (className == null) {
        String qualifiedClassName = description.getClassName();
        className = qualifiedClassName.substring(qualifiedClassName.lastIndexOf('.') + 1);
      }
    }
  };

  // Test Directory
  private Path currentQueryPath;
  private Path currentResultPath;

  public QueryTestCaseBase() {
    conf = testingCluster.getTestingCluster().getConfiguration();
  }

  public final ResultSet executeQuery() throws Exception {
    if (currentQueryPath == null) {
      currentQueryPath = new Path(queryBasePath, className);
    }
    if (currentResultPath == null) {
      currentResultPath = new Path(resultBasePath, className);
    }

    FileSystem fs = currentQueryPath.getFileSystem(testingCluster.getTestingCluster().getConfiguration());

    Path queryFile = getQueryFile(name.getMethodName());
    assertTrue(queryFile.toString() + " existence check", fs.exists(queryFile));


    ResultSet result = testingCluster.execute(FileUtil.readTextFile(new File(queryFile.toUri())));
    assertNotNull("Query succeeded test", result);
    return result;
  }

  public final void verifyQuery(ResultSet result) throws IOException {
    FileSystem fs = currentQueryPath.getFileSystem(testingCluster.getTestingCluster().getConfiguration());
    Path resultFile = getResultFile(name.getMethodName());
    assertTrue(resultFile.toString() + " existence check", fs.exists(resultFile));
    try {
      assertResult(result, resultFile);
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }

  public final void cleanupQuery(ResultSet resultSet) throws IOException {
    try {
      resultSet.close();
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }

  public String resultSetToString(ResultSet resultSet) throws SQLException {
    StringBuilder sb = new StringBuilder();
    ResultSetMetaData rsmd = resultSet.getMetaData();
    int numOfColumns = rsmd.getColumnCount();

    for (int i = 1; i <= numOfColumns; i++) {
      if (i > 1) sb.append(",");
      String columnName = rsmd.getColumnName(i);
      sb.append(columnName);
    }
    sb.append("\n-------------------------------\n");

    while (resultSet.next()) {
      for (int i = 1; i <= numOfColumns; i++) {
        if (i > 1) sb.append(",");
        String columnValue = resultSet.getObject(i).toString();
        sb.append(columnValue);
      }
      sb.append("\n");
    }
    return sb.toString();
  }

  private void assertResult(ResultSet res, Path resultFile) throws SQLException, IOException {
    String actualResult = resultSetToString(res);
    String expectedResult = FileUtil.readTextFile(new File(resultFile.toUri()));
    assertEquals("Result Verification", expectedResult, actualResult);
  }

  private Path getQueryFile(String methodName) {
    return StorageUtil.concatPath(currentQueryPath, methodName + ".sql");
  }

  private Path getResultFile(String methodName) {
    return StorageUtil.concatPath(currentResultPath, methodName + ".result");
  }
}
