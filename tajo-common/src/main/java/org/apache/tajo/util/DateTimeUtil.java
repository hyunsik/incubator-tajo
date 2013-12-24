package org.apache.tajo.util;

import org.apache.commons.lang.*;
import org.apache.tajo.datum.DateDatum;

public class DateTimeUtil {

  /* Assorted constants for datetime-related calculations */

  /** assumes leap year every four years */
  public static final double DAYS_PER_YEAR = 365.25;
  public static final int MONTHS_PER_YEAR = 12;

  /*
   *	DAYS_PER_MONTH is very imprecise.  The more accurate value is
   *	365.2425/12 = 30.436875, or '30 days 10:29:06'.  Right now we only
   *	return an integral number of days, but someday perhaps we should
   *	also return a 'time' value to be used as well.	ISO 8601 suggests
   *	30 days.
   */

  /** assumes exactly 30 days per month */
  public static final int DAYS_PER_MONTH	= 30;
  /** assume no daylight savings time changes */
  public static final int HOURS_PER_DAY = 24;

  /*
  *	This doesn't adjust for uneven daylight savings time intervals or leap
  *	seconds, and it crudely estimates leap years.  A more accurate value
  *	for days per years is 365.2422.
  */

  /** avoid floating-point computation */
  public static final int SECS_PER_YEAR	= 36525 * 864;
  public static final int SECS_PER_DAY = 86400;
  public static final int SECS_PER_HOUR	= 3600;
  public static final int SECS_PER_MINUTE = 60;
  public static final int MINS_PER_HOUR	= 60;

  public static final long USECS_PER_DAY = 86400000000L;
  public static final long USECS_PER_HOUR	= 3600000000L;
  public static final long USECS_PER_MINUTE = 60000000L;
  public static final long USECS_PER_SEC = 1000000L;

  /**
   * Calendar time to Julian date conversions.
   * Julian date is commonly used in astronomical applications,
   *	since it is numerically accurate and computationally simple.
   * The algorithms here will accurately convert between Julian day
   *	and calendar date for all non-negative Julian days
   *	(i.e. from Nov 24, -4713 on).
   *
   * These routines will be used by other date/time packages
   * - thomas 97/02/25
   *
   * Rewritten to eliminate overflow problems. This now allows the
   * routines to work correctly for all Julian day counts from
   * 0 to 2147483647	(Nov 24, -4713 to Jun 3, 5874898) assuming
   * a 32-bit integer. Longer types should also work to the limits
   * of their precision.
   */
  public static final int dateToJulian(int year, int month, int day) {
    int			julian;
    int			century;

    if (month > 2)
    {
      month += 1;
      year += 4800;
    }
    else
    {
      month += 13;
      year += 4799;
    }

    century = year / 100;
    julian = year * 365 - 32167;
    julian += year / 4 - century + century / 4;
    julian += 7834 * month / 256 + day;

    return julian;
  }

  public static final int [] julianToDate(int jd) {
    long julian;
    long quad;
    long extra;
    long y;

    julian = jd;
    julian += 32044;
    quad = julian / 146097;
    extra = (julian - quad * 146097) * 4 + 3;
    julian += 60 + quad * 3 + extra / 146097;
    quad = julian / 1461;
    julian -= quad * 1461;
    y = julian * 4 / 1461;
    julian = ((y != 0) ? ((julian + 305) % 365) : ((julian + 306) % 366))
        + 123;
    y += quad * 4;


    int year = (int)(y - 4800);
    quad = julian * 2141 / 65536;
    int day = (int)(julian - 7834 * quad / 256);
    int month = (int) ((quad + 10) % MONTHS_PER_YEAR + 1);

    return new int[] {year, month, day};
  }


  /*
   * j2day - convert Julian date to day-of-week (0..6 == Sun..Sat)
   *
   * Note: various places use the locution j2day(date - 1) to produce a
   * result according to the convention 0..6 = Mon..Sun.	This is a bit of
   * a crock, but will work as long as the computation here is just a modulo.
   */
  public static final int j2day(int date) {
    long day;

    day = date;

    day += 1;
    day %= 7;

    return (int) day;
  }

  /** maximum possible number of fields in a date * string */
  private static final int MAXDATEFIELDS = 25;

  public static int decodeDate(String parse) {

    int idx = 0;
    int nf = 0;
    int length = parse.length();
    char [] dateStr = parse.toCharArray();
    String [] fields = new String[MAXDATEFIELDS];

    while(idx < length && nf < MAXDATEFIELDS) {

      /* skip field separators */
      while (idx < length && !Character.isLetterOrDigit(dateStr[idx])) {
       idx++;
      }

      if (idx == length) {
        return -1;	/* end of string after separator */
      }

      int fieldStartIdx = idx;
      int fieldLength = idx;
      if (Character.isDigit(dateStr[idx])) {
        while (idx < length && Character.isDigit(dateStr[idx])) {
          idx++;
        }
        fieldLength = idx;
      } else if (Character.isLetterOrDigit(dateStr[idx])) {
        while (idx < length && Character.isLetterOrDigit(dateStr[idx])) {
          idx++;
        }
        fieldLength = idx;
      }

      fields[nf] = parse.substring(fieldStartIdx, fieldLength);
      nf++;
    }

    int [] numFields = new int[3];

    /* look first for text fields, since that will be unambiguous month */
    for (int i = 0; i < nf; i++) {
      if (Character.isLetter(fields[i].charAt(0))) {
        //
      }
    }

    /* now pick up remaining numeric fields */
    for (int i = 0; i < nf && i < 3; i++) {
      if (Character.isDigit(fields[i].charAt(0))) {
        numFields[i] = Integer.valueOf(fields[i]);
      }
    }

    return dateToJulian(numFields[0], numFields[1], numFields[2]);
  }
}
