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

package org.apache.tajo.cli;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestSimpleScanner {

  @Test
  public final void testMetaCommands() throws InvalidStatement {
    List<String> res1 = SimpleScanner.scanStatements("\\d");
    assertEquals(1, res1.size());
    assertEquals("\\d", res1.get(0));

    List<String> res2 = SimpleScanner.scanStatements("\\d;\\c;\\f");
    assertEquals(3, res2.size());
    assertEquals("\\d", res2.get(0));
    assertEquals("\\c", res2.get(1));
    assertEquals("\\f", res2.get(2));

    List<String> res3 = SimpleScanner.scanStatements("\n\t\t  \\d;\n\\c;\t\t\\f  ;");
    assertEquals(3, res3.size());
    assertEquals("\\d", res3.get(0));
    assertEquals("\\c", res3.get(1));
    assertEquals("\\f", res3.get(2));
  }

  @Test
  public final void testStatements() throws InvalidStatement {
    List<String> res1 = SimpleScanner.scanStatements("select * from test");
    assertEquals(1, res1.size());
    assertEquals("select * from test", res1.get(0));

    List<String> res2 = SimpleScanner.scanStatements("select * from test;");
    assertEquals(1, res2.size());
    assertEquals("select * from test", res2.get(0));

    List<String> res3 = SimpleScanner.scanStatements("select * from test1;select * from test2;");
    assertEquals(2, res3.size());
    assertEquals("select * from test1", res3.get(0));
    assertEquals("select * from test2", res3.get(1));

    List<String> res4 = SimpleScanner.scanStatements("\t\t\n\rselect * from \ntest1;select * from test2\n;");
    assertEquals(2, res4.size());
    assertEquals("select * from \ntest1", res4.get(0));
    assertEquals("select * from test2", res4.get(1));

    List<String> res5 =
        SimpleScanner.scanStatements("\t\t\n\rselect * from \ntest1;\\d test;select * from test2;\n\nselect 1;");
    assertEquals(4, res5.size());
    assertEquals("select * from \ntest1", res5.get(0));
    assertEquals("\\d test", res5.get(1));
    assertEquals("select * from test2", res5.get(2));
    assertEquals("select 1", res5.get(3));
  }

  @Test
  public final void testQuoted() throws InvalidStatement {
    List<String> res1 = SimpleScanner.scanStatements("select '\n;' from test");
    assertEquals(1, res1.size());
    assertEquals("select '\n;' from test", res1.get(0));

    List<String> res2 = SimpleScanner.scanStatements("select 'abc\nbbc\nddf' from test;");
    assertEquals(1, res2.size());
    assertEquals("select 'abc\nbbc\nddf' from test", res2.get(0));

    boolean exception = false;
    try {
      SimpleScanner.scanStatements("select 'abc");
    } catch (InvalidStatement is) {
      System.out.println(is.getMessage());
      exception = true;
    }
    assertTrue(exception);
  }
}
