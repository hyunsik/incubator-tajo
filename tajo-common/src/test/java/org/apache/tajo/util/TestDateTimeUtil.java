package org.apache.tajo.util;

import org.apache.tajo.datum.DateDatum;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestDateTimeUtil {
  @Test
  public void testDateToJulian() throws Exception {
    System.out.println(DateTimeUtil.dateToJulian(2013, 12, 25));
  }

  @Test
  public void testDecodeDateString() {
    System.out.println(new DateDatum("1980-04-04"));
    int julian = DateTimeUtil.decodeDate("2013-04-25");
    assertEquals(julian, DateTimeUtil.decodeDate("2013-4-25"));
    assertEquals(julian, DateTimeUtil.decodeDate("2013.4.25"));
  }
}
