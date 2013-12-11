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

package org.apache.tajo.engine.query;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.DeflateCodec;
import org.apache.tajo.IntegrationTest;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.TpchTestBase;
import org.apache.tajo.catalog.CatalogService;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.util.FileUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;

import static org.junit.Assert.*;

@Category(IntegrationTest.class)
public class TestBDW {
  private static TpchTestBase tpch;
  public TestBDW() throws IOException {
    super();
  }

  @BeforeClass
  public static void setUp() throws Exception {
    tpch = TpchTestBase.getInstance();
  }

  @Test
  public final void testRFQ3() throws Exception {
    ResultSet res = tpch.execute(FileUtil.readTextFile(new File("src/test/queries/rf-q3_schema.sql")));
    res.close();
    res = tpch.execute(FileUtil.readTextFile(new File("src/test/queries/rf-q3.sql")));
    res.close();
  }

  @Test
  public final void testRFQ4() throws Exception {
    ResultSet res = tpch.execute(FileUtil.readTextFile(new File("src/test/queries/rf-q4_schema.sql")));
    res.close();
    res = tpch.execute(FileUtil.readTextFile(new File("src/test/queries/rf-q4.sql")));
    res.close();
  }
}
