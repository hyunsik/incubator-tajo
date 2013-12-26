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

import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.exception.InvalidOperationException;
import org.apache.tajo.datum.exception.ValueOutOfRangeException;
import org.apache.tajo.util.Bytes;
import org.apache.tajo.util.DateTimeUtil;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;

public class TimestampDatum extends Datum {
  public static final int SIZE = 8;
  private long timestamp;

  public TimestampDatum(int timestamp) {
    super(TajoDataTypes.Type.TIMESTAMP);

  }

  public TimestampDatum(DateTime dateTime) {
    super(TajoDataTypes.Type.TIMESTAMP);

  }

  TimestampDatum(byte [] bytes) {
    super(TajoDataTypes.Type.TIMESTAMP);
    this.timestamp = Bytes.toLong(bytes);
  }

  public TimestampDatum(String datetime) {
    super(TajoDataTypes.Type.TIMESTAMP);
    this.timestamp = DateTimeUtil.parseDateTime();
  }

  public TimestampDatum(int years, int months, int days, int hours, int minutes, int seconds, int fraction) {
    super(TajoDataTypes.Type.TIMESTAMP);

    /* Julian day routines are not correct for negative Julian days */
    if (!DateTimeUtil.isValidJulianDate(years, months, days)) {
      throw new ValueOutOfRangeException("Out of Range Julian days");
    }

    long date = DateTimeUtil.toJulianDate(years, months, days) - DateTimeUtil.POSTGRES_EPOCH_JDATE;
    long time = DateTimeUtil.toTime(hours, minutes, seconds, fraction);

    timestamp = date * DateTimeUtil.USECS_PER_DAY + time;
	  /* check for major overflow */
    if ((timestamp - time) / DateTimeUtil.USECS_PER_DAY != date) {
      throw new ValueOutOfRangeException("Out of Range of Time");
    }
	  /* check for just-barely overflow (okay except time-of-day wraps) */
	  /* caution: we want to allow 1999-12-31 24:00:00 */
    if ((timestamp < 0 && date > 0) || (timestamp > 0 && date < -1)) {
      throw new ValueOutOfRangeException("Out of Range of Date");
    }

    /* TODO - time zone
    if (tzp != NULL) {
      timestamp = dt2local(*result, -(*tzp));
    }
    */
  }

  /**
   * It's the same value to asInt8().
   * @return The Timestamp
   */
  public long getTimestamp() {
    return timestamp;
  }

  public int getUnixTimestamp() {
    return DateTimeUtil.timestampToUnixtime(timestamp);
  }

  public int getYear() {
    DateTimeUtil.TimeMeta tm = new DateTimeUtil.TimeMeta();
    DateTimeUtil.timestampToTimeMeta(timestamp, tm);
    return tm.years;
  }

  public int getMonthOfYear() {
    DateTimeUtil.TimeMeta tm = new DateTimeUtil.TimeMeta();
    DateTimeUtil.timestampToTimeMeta(timestamp, tm);
    return tm.monthOfYear;
  }

  public int getDayOfWeek() {
    return 0;
  }

  public int getDayOfMonth() {
    DateTimeUtil.TimeMeta tm = new DateTimeUtil.TimeMeta();
    DateTimeUtil.timestampToTimeMeta(timestamp, tm);
    return tm.dayOfMonth;
  }

  public int getHourOfDay() {
    DateTimeUtil.TimeMeta tm = new DateTimeUtil.TimeMeta();
    DateTimeUtil.timestampToTimeMeta(timestamp, tm);
    return tm.hours;
  }

  public int getMinuteOfHour() {
    DateTimeUtil.TimeMeta tm = new DateTimeUtil.TimeMeta();
    DateTimeUtil.timestampToTimeMeta(timestamp, tm);
    return tm.minutes;
  }

  public int getSecondOfMinute() {
    DateTimeUtil.TimeMeta tm = new DateTimeUtil.TimeMeta();
    DateTimeUtil.timestampToTimeMeta(timestamp, tm);
    return tm.secs;
  }

  public int getMillisOfSecond() {
    DateTimeUtil.TimeMeta tm = new DateTimeUtil.TimeMeta();
    DateTimeUtil.timestampToTimeMeta(timestamp, tm);
    return tm.fsecs;
  }

  public String toString() {
    return asChars();
  }

  @Override
  public long asInt8() {
    return timestamp;
  }

  @Override
  public String asChars() {
    DateTimeUtil.TimeMeta tm = new DateTimeUtil.TimeMeta();
    DateTimeUtil.timestampToTimeMeta(timestamp, tm);
    return DateTimeUtil.encodeDateTime(tm, DateTimeUtil.DateStyle.ISO_DATES);
  }

  public String toChars(DateTimeFormatter format) {
    return "";
  }

  @Override
  public int size() {
    return SIZE;
  }

  @Override
  public byte [] asByteArray() {
    return Bytes.toBytes(timestamp);
  }

  @Override
  public Datum equalsTo(Datum datum) {
    if (datum.type() == TajoDataTypes.Type.TIME) {
      return timestamp == datum.asInt8() ? BooleanDatum.TRUE : BooleanDatum.FALSE;
    } else if (datum.isNull()) {
      return datum;
    } else {
      throw new InvalidOperationException();
    }
  }

  @Override
  public int compareTo(Datum datum) {
    if (datum.type() == TajoDataTypes.Type.TIMESTAMP) {
      TimestampDatum another = (TimestampDatum) datum;
      return (timestamp < another.timestamp) ? -1 : ((timestamp > another.timestamp) ? 1 : 0);
    } else if (datum.isNull()) {
      return -1;
    } else {
      throw new InvalidOperationException();
    }
  }

  public boolean equals(Object obj) {
    if (obj instanceof TimestampDatum) {
      TimestampDatum another = (TimestampDatum) obj;
      return timestamp == another.timestamp;
    } else {
      throw new InvalidOperationException();
    }
  }
}
