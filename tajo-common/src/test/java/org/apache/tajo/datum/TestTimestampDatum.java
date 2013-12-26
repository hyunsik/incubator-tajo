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

package org.apache.tajo.datum;

import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.datum.exception.InvalidCastException;
import org.apache.tajo.json.CommonGsonHelper;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class TestTimestampDatum {
  public static TimestampDatum d =
      new TimestampDatum(2013, 12, 25, 13, 32, 36, 999999);

	@Test
	public final void testType() {
    assertEquals(Type.TIMESTAMP, d.type());
	}
	
	@Test(expected = InvalidCastException.class)
	public final void testAsInt4() {
    d.asInt4();
	}

  @Test
	public final void testAsInt8() {
    System.out.println(d.asInt8());
	}

  @Test(expected = InvalidCastException.class)
	public final void testAsFloat4() {
    d.asFloat4();
	}

  @Test(expected = InvalidCastException.class)
	public final void testAsFloat8() {
    d.asFloat8();
	}

	@Test
	public final void testAsText() {
    assertEquals("2013-12-25 13:32:36.999999", d.asChars());
    Datum copy = DatumFactory.createTimeStamp(d.asChars());
    assertEquals(d, copy);
	}

  @Test
  public final void testAsByteArray() {
    Datum copy = new TimestampDatum(d.asByteArray());
    assertEquals(d, copy);
  }

	@Test
  public final void testSize() {
    assertEquals(TimestampDatum.SIZE, d.asByteArray().length);
  }

  @Test
  public final void testAsTextBytes() {
    Datum d = DatumFactory.createTimeStamp("1980-04-01 01:50:01");
    assertArrayEquals(d.toString().getBytes(), d.asTextBytes());
  }

  @Test
  public final void testToJson() {
    Datum copy = CommonGsonHelper.fromJson(d.toJson(), Datum.class);
    assertEquals(d, copy);
  }

  @Test
  public final void testGetUnixtime() {
    TimestampDatum d = new TimestampDatum(2013, 12, 25, 13, 32, 36, 999999);
    System.out.println(d.getUnixTimestamp());
  }

  @Test
  public final void testGetFields() {
    TimestampDatum d = new TimestampDatum(2013, 12, 25, 13, 32, 36, 999999);
    assertEquals(2013, d.getYear());
    assertEquals(12, d.getMonthOfYear());
    assertEquals(25, d.getDayOfMonth());
    assertEquals(13, d.getHourOfDay());
    assertEquals(32, d.getMinuteOfHour());
    assertEquals(36, d.getSecondOfMinute());
    assertEquals(999999, d.getMillisOfSecond());
  }
}
