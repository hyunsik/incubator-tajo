package org.apache.tajo.util;

import org.apache.tajo.datum.DateDatum;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestDateTimeUtil {
  @Test
  public void testDateToJulian() throws Exception {
    System.out.println(DateTimeUtil.toJulianDate(2013, 12, 25));
  }

  @Test
  public void testDecodeDateString() {
    System.out.println(new DateDatum("1980-04-04"));
    int julian = DateTimeUtil.decodeDate("2013-04-25");
    assertEquals(julian, DateTimeUtil.decodeDate("2013-4-25"));
    assertEquals(julian, DateTimeUtil.decodeDate("2013.4.25"));
  }

  @Test
  public void testParseDateTimeString() {
    DateTimeUtil.parseDateTime("2013-04-25 23:11:50.1234", 2);
  }

  @Test
  public void testEncodeDateTime() throws Exception {
    //DateTimeUtil.encodeDateTime()

  }

  @Test
  public void testAppendSeconds() throws Exception {
    StringBuilder sb1 = new StringBuilder("13:52:");
    DateTimeUtil.appendSeconds(sb1, 23, 99999999, 6, false);
    assertEquals("13:52:23.999999", sb1.toString());

    StringBuilder sb2 = new StringBuilder("13:52:");
    DateTimeUtil.appendSeconds(sb2, 23, 99999999, 5, false);
    assertEquals("13:52:23.99999", sb2.toString());

    StringBuilder sb3 = new StringBuilder("13:52:");
    DateTimeUtil.appendSeconds(sb3, 23, 99999999, 4, false);
    assertEquals("13:52:23.9999", sb3.toString());

    StringBuilder sb4 = new StringBuilder("13:52:");
    DateTimeUtil.appendSeconds(sb4, 23, 99999999, 3, false);
    assertEquals("13:52:23.999", sb4.toString());

    StringBuilder sb5 = new StringBuilder("13:52:");
    DateTimeUtil.appendSeconds(sb5, 23, 99999999, 2, false);
    assertEquals("13:52:23.99", sb5.toString());

    StringBuilder sb6 = new StringBuilder("13:52:");
    DateTimeUtil.appendSeconds(sb6, 23, 99999999, 1, false);
    assertEquals("13:52:23.9", sb6.toString());

    StringBuilder sb7 = new StringBuilder("13:52:");
    DateTimeUtil.appendSeconds(sb7, 23, 99999999, 0, false);
    assertEquals("13:52:23", sb7.toString());
  }

  @Test
  public void testTrimTrailingZeros() throws Exception {
    StringBuilder sb1 = new StringBuilder("1.1200");
    DateTimeUtil.trimTrailingZeros(sb1);
    assertEquals("1.12", sb1.toString());

    StringBuilder sb2 = new StringBuilder("1.12000120");
    DateTimeUtil.trimTrailingZeros(sb2);
    assertEquals("1.1200012", sb2.toString());

    StringBuilder sb3 = new StringBuilder(".12000120");
    DateTimeUtil.trimTrailingZeros(sb3);
    assertEquals(".1200012", sb3.toString());
  }
}
