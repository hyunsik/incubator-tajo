package org.apache.tajo.util;

import org.junit.Test;

public class TestDateTimeUtil {
  @Test
  public void testDateToJulian() throws Exception {
    System.out.println(DateTimeUtil.dateToJulian(2013, 12, 25));
  }

  @Test
  public void testDecodeDateString() {
    DateTimeUtil.decodeDate("");
  }
}
