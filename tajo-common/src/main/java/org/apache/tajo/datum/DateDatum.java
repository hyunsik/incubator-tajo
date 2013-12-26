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

import com.google.gson.annotations.Expose;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.exception.InvalidOperationException;
import org.apache.tajo.util.Bytes;
import org.apache.tajo.util.DateTimeUtil;

public class DateDatum extends Datum {
  public static final int SIZE = 4;
  /** Julian date */
  @Expose private int julianDates;

  public DateDatum(int julian) {
    super(TajoDataTypes.Type.DATE);
    this.julianDates = julian;
  }

  public DateDatum(int year, int monthOfYear, int dayOfMonth) {
    super(TajoDataTypes.Type.DATE);
    julianDates = DateTimeUtil.toJulianDate(year, monthOfYear, dayOfMonth);
  }

  public DateDatum(String dateStr) {
    super(TajoDataTypes.Type.DATE);
    julianDates = DateTimeUtil.decodeDate(dateStr);
  }

  public DateDatum(byte [] bytes) {
    this(Bytes.toInt(bytes));
  }

  public int getYear() {
    long julian = julianDates;
    julian += 32044;
    long quad = julian / 146097;
    long extra = (julian - quad * 146097) * 4 + 3;
    julian += 60 + quad * 3 + extra / 146097;
    quad = julian / 1461;
    julian -= quad * 1461;
    long y = julian * 4 / 1461;
    y += quad * 4;

    return (int)(y - 4800);
  }

  public int getMonthOfYear() {
    long julian = julianDates;
    julian += 32044;
    long quad = julian / 146097;
    long extra = (julian - quad * 146097) * 4 + 3;
    julian += 60 + quad * 3 + extra / 146097;
    quad = julian / 1461;
    julian -= quad * 1461;
    long y = julian * 4 / 1461;
    julian = ((y != 0) ? ((julian + 305) % 365) : ((julian + 306) % 366)) + 123;
    y += quad * 4;

    quad = julian * 2141 / 65536;
    int month = (int) ((quad + 10) % DateTimeUtil.MONTHS_PER_YEAR + 1);

    return month;
  }

  /**
   * Get day-of-week (0..6 == Sun..Sat)
   */
  public int getDayOfWeek() {
    long day;

    day = julianDates;

    day += 1;
    day %= 7;

    return (int) day;
  }

  public int getDayOfMonth() {

    long julian = julianDates;
    julian += 32044;
    long quad = julian / 146097;
    long extra = (julian - quad * 146097) * 4 + 3;
    julian += 60 + quad * 3 + extra / 146097;
    quad = julian / 1461;
    julian -= quad * 1461;
    long y = julian * 4 / 1461;
    julian = ((y != 0) ? ((julian + 305) % 365) : ((julian + 306) % 366))
        + 123;
    y += quad * 4;

    quad = julian * 2141 / 65536;
    return (int)(julian - 7834 * quad / 256);
  }

  public String toString() {
    return asChars();
  }

  @Override
  public int asInt4() {
    return julianDates;
  }

  @Override
  public long asInt8() {
    return julianDates;
  }

  @Override
  public float asFloat4() {
    return julianDates;
  }

  @Override
  public double asFloat8() {
    return julianDates;
  }

  @Override
  public String asChars() {
    DateTimeUtil.TimeMeta tm = new DateTimeUtil.TimeMeta();
    DateTimeUtil.julianToDate(julianDates, tm);
    return String.format("%04d-%02d-%02d", tm.years, tm.monthOfYear, tm.dayOfMonth);
  }

  @Override
  public int size() {
    return SIZE;
  }

  @Override
  public byte [] asByteArray() {
    return Bytes.toBytes(julianDates);
  }

  @Override
  public Datum equalsTo(Datum datum) {
    if (datum.type() == TajoDataTypes.Type.DATE) {
      return DatumFactory.createBool(julianDates == datum.asInt4());
    } else if (datum.isNull()) {
      return datum;
    } else {
      throw new InvalidOperationException();
    }
  }

  @Override
  public int compareTo(Datum datum) {
    if (datum.type() == TajoDataTypes.Type.DATE) {
      DateDatum another = (DateDatum) datum;
      return (julianDates < another.julianDates) ? -1 : ((julianDates > another.julianDates) ? 1 : 0);
    } else if (datum.type() == TajoDataTypes.Type.NULL_TYPE) {
      return -1;
    } else {
      throw new InvalidOperationException();
    }
  }

  public boolean equals(Object obj) {
    if (obj instanceof DateDatum) {
      DateDatum another = (DateDatum) obj;
      return julianDates == another.julianDates;
    } else {
      throw new InvalidOperationException();
    }
  }

  @Override
  public int hashCode() {
    return julianDates;
  }

  public static DateDatum newInstance(int year, int monthOfYear, int dayOfMonth) {
    return new DateDatum(year, monthOfYear, dayOfMonth);
  }
}
