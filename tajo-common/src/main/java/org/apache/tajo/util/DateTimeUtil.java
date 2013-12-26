package org.apache.tajo.util;

import org.apache.tajo.datum.exception.ValueOutOfRangeException;

public class DateTimeUtil {

  // Assorted constants for datetime-related calculations

  /** assumes leap year every four years */
  public static final double DAYS_PER_YEAR = 365.25;
  public static final int MONTHS_PER_YEAR = 12;


  // DAYS_PER_MONTH is very imprecise.  The more accurate value is
  // 365.2425/12 = 30.436875, or '30 days 10:29:06'.  Right now we only
  // return an integral number of days, but someday perhaps we should
  // also return a 'time' value to be used as well.	ISO 8601 suggests
  // 0 days.

  /** assumes exactly 30 days per month */
  public static final int DAYS_PER_MONTH	= 30;
  /** assume no daylight savings time changes */
  public static final int HOURS_PER_DAY = 24;

  // This doesn't adjust for uneven daylight savings time intervals or leap
  // seconds, and it crudely estimates leap years.  A more accurate value
  // for days per years is 365.2422.

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

  public static final int JULIAN_MINYEAR = -4713;
  public static final int JULIAN_MINMONTH = 11;
  public static final int JULIAN_MINDAY = 24;
  public static final int JULIAN_MAXYEAR = 5874898;

  public static class TimeMeta {
    public int      fsecs;
    public int			secs;
    public int			minutes;
    public int			hours;
    public int			dayOfMonth;
    public int			monthOfYear; // origin 0, not 1
    public int			years;		   // relative to 1900
    public int			isDST;       // daylight savings time
  };

  /**
   * Julian date support.
   *
   * isValidJulianDate checks the minimum date exactly, but is a bit sloppy
   * about the maximum, since it's far enough out to not be especially
   * interesting.
   */
  public static final boolean isValidJulianDate(int years, int months, int days) {
    return years > JULIAN_MINYEAR || years == JULIAN_MINYEAR &&
        months > JULIAN_MINMONTH || months == JULIAN_MINMONTH &&
        days >= JULIAN_MINDAY && years < JULIAN_MAXYEAR;
  }

  /** == DateTimeUtil.toJulianDate(JULIAN_MAXYEAR, 1, 1) */
  public static final int JULIAN_MAX = 2147483494;

  // Julian-date equivalents of Day 0 in Unix and Postgres reckoning
  /** == DateTimeUtil.toJulianDate(1970, 1, 1) */
  public static final int UNIX_EPOCH_JDATE =     2440588;
  /** == DateTimeUtil.toJulianDate(2000, 1, 1) */
  public static final int POSTGRES_EPOCH_JDATE = 2451545;
  /** == (POSTGRES_EPOCH_JDATE * SECS_PER_DAY) - (UNIX_EPOCH_JDATE * SECS_PER_DAY); */
  public static final long SECS_DIFFERENCE_BETWEEN_JULIAN_AND_UNIXTIME = 946684800;

  public static final int timestampToUnixtime(long timestamp) {
    long time = timestamp / DateTimeUtil.USECS_PER_SEC;
    return (int) (time - SECS_DIFFERENCE_BETWEEN_JULIAN_AND_UNIXTIME);
  }

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
  public static final int toJulianDate(int year, int month, int day) {
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
    julian = (year * 365) - 32167;
    julian += (((year / 4) - century) + (century / 4));
    julian += ((7834 * month) / 256) + day;

    return julian;
  }

  public static final void julianToDate(int julianDate, TimeMeta tm) {
    long julian;
    long quad;
    long extra;
    long y;

    julian = julianDate;
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


    tm.years = (int)(y - 4800);
    quad = julian * 2141 / 65536;
    tm.dayOfMonth = (int)(julian - 7834 * quad / 256);
    tm.monthOfYear = (int) ((quad + 10) % MONTHS_PER_YEAR + 1);
  }


  /**
   * This method is originated from j2date in datetime.c of PostgreSQL.
   *
   * julianToDay - convert Julian date to day-of-week (0..6 == Sun..Sat)
   *
   * Note: various places use the locution julianToDay(date - 1) to produce a
   * result according to the convention 0..6 = Mon..Sun.	This is a bit of
   * a crock, but will work as long as the computation here is just a modulo.
   */
  public static final int julianToDay(int julianDate) {
    long day;

    day = julianDate;

    day += 1;
    day %= 7;

    return (int) day;
  }

  public static final long toTime(int hour, int min, int sec, int fsec) {
    return (((((hour * MINS_PER_HOUR) + min) * SECS_PER_MINUTE) + sec) * USECS_PER_SEC) + fsec;
  }

  /**
   * timestamp2tm() - Convert timestamp data type to POSIX time structure.
   *
   * Note that year is _not_ 1900-based, but is an explicit full value.
   * Also, month is one-based, _not_ zero-based.
   * Returns:
   *	 0 on success
   *	-1 on out of range
   *
   * If attimezone is NULL, the global timezone (including possibly brute forced
   * timezone) will be used.
   */
  public static final void timestampToTimeMeta(long timestamp, TimeMeta tm) {
    long date;
    long time;

    // TODO - If timezone is set, timestamp value should be adjusted here.


    time = timestamp;

    // TMODULO
    date = time / USECS_PER_DAY;
    if (date != 0) {
      time -= date * USECS_PER_DAY;
    }
    if (time < 0) {
      time += USECS_PER_DAY;
      date -= 1;
    }

    /* add offset to go from J2000 back to standard Julian date */
    date += POSTGRES_EPOCH_JDATE;

    /* Julian day routine does not work for negative Julian days */
    if (date < 0 || date > Integer.MAX_VALUE) {
      throw new ValueOutOfRangeException("Timestamp Out Of Scope");
    }

    julianToDate((int) date, tm);
    julianToTime(time, tm);
  }

  /**
   * This method is originated from dt2time in timestamp.c of PostgreSQL.
   *
   * @param julianDate
   * @return hour, min, sec, fsec
   */
  public static final void julianToTime(long julianDate, TimeMeta tm)
  {
    long time = julianDate;

    tm.hours = (int) (time / USECS_PER_HOUR);
    time -= tm.hours * USECS_PER_HOUR;
    tm.minutes = (int) (time / USECS_PER_MINUTE);
    time -= tm.minutes * USECS_PER_MINUTE;
    tm.secs = (int) (time / USECS_PER_SEC);
    tm.fsecs = (int) (time - (tm.secs * USECS_PER_SEC));
  }

  /** maximum possible number of fields in a date * string */
  private static final int MAXDATEFIELDS = 25;

  /**
   * Decode date string which includes delimiters.
   *
   * This method is originated from DecodeDate() in datetime.c of PostgreSQL.
   *
   * @param str The date string like '2013-12-25'.
   * @return The julian date
   */
  public static int decodeDate(String str) {

    int idx = 0;
    int nf = 0;
    int length = str.length();
    char [] dateStr = str.toCharArray();
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

      fields[nf] = str.substring(fieldStartIdx, fieldLength);
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

    return toJulianDate(numFields[0], numFields[1], numFields[2]);
  }

  enum DateTimeFieldType {
    TIME,
    DATE,
    NUMBER,
    STRING,
    TIMEZONE,
    SPECIAL
  }
  /**
   * Break string into tokens based on a date/time context.
   *
   * This method is originated form ParseDateTime() in datetime.c of PostgreSQL.
   *
   * @param str The input string
   */
  public static void parseDateTime(String str, int maxFields) {
    int idx = 0;
    int nf = 0;
    int length = str.length();
    char [] timeStr = str.toCharArray();
    String [] fields = new String[maxFields];
    DateTimeFieldType [] fieldTypes = new DateTimeFieldType[maxFields];

    while (idx < length) {

      /* Ignore spaces between fields */
      if (Character.isSpaceChar(timeStr[idx])) {
        idx++;
        continue;
      }

      /* Record start of current field */
      if (nf >= maxFields) {
        throw new IllegalArgumentException("Too many fields");
      }

      int startIdx = idx;

      /* leading digit? then date or time */
      if (Character.isDigit(timeStr[idx])) {
        idx++;
        while (idx < length && Character.isDigit(timeStr[idx])) {
          idx++;
        }

        if (timeStr[idx] == ':') {
          fieldTypes[nf] = DateTimeFieldType.TIME;

          while (idx <length && (Character.isDigit(timeStr[idx]) || timeStr[idx] == ':' || timeStr[idx] == '.')) {
            idx++;
          }
        }

        /* date field? allow embedded text month */
        else if (timeStr[idx] == '-' || timeStr[idx] == '/' || timeStr[idx] == '.') {

          /* save delimiting character to use later */
          char delim = timeStr[idx];
          idx++;

          /* second field is all digits? then no embedded text month */
          if (Character.isDigit(timeStr[idx])) {
            fieldTypes[nf] = delim == '.' ? DateTimeFieldType.NUMBER : DateTimeFieldType.DATE;

            while (idx < length && Character.isDigit(timeStr[idx])) {
              idx++;
            }

            /*
					   * insist that the delimiters match to get a three-field
					   * date.
					   */
            if (timeStr[idx] == delim) {
              while (idx < length && (Character.isDigit(timeStr[idx]) || timeStr[idx] == delim)) {
                idx++;
              }
            }
          } else {
            fieldTypes[nf] = DateTimeFieldType.DATE;
            while (idx < length && Character.isLetterOrDigit(timeStr[idx]) || timeStr[idx] == delim) {
              idx++;
            }
          }
        } else {
          /*
			     * otherwise, number only and will determine year, month, day, or
			     * concatenated fields later...
			    */
          fieldTypes[nf] = DateTimeFieldType.NUMBER;
        }
      }

      /* Leading decimal point? Then fractional seconds... */
      else if (timeStr[idx] == '.') {
        idx++;
        while (idx < length && Character.isDigit(timeStr[idx])) {
          idx++;
          continue;
        }
        fieldTypes[nf] = DateTimeFieldType.NUMBER;
      }

      // text? then date string, month, day of week, special, or timezone
      else if (Character.isLetter(timeStr[idx])) {
        boolean isDate;
        idx++;
        while (idx < length && Character.isLetter(timeStr[idx])) {
          idx++;
        }



        // Dates can have embedded '-', '/', or '.' separators.  It could
        // also be a timezone name containing embedded '/', '+', '-', '_',
        // or ':' (but '_' or ':' can't be the first punctuation). If the
        // next character is a digit or '+', we need to check whether what
        // we have so far is a recognized non-timezone keyword --- if so,
        // don't believe that this is the start of a timezone.

        isDate = false;
        if (timeStr[idx] == '-' || timeStr[idx] == '/' || timeStr[idx] == '.') {
          isDate = true;
        } else if (timeStr[idx] == '+' || Character.isDigit(timeStr[idx])) {
          // The original ParseDateTime handles this case. But, we currently omit this case.
          throw new IllegalArgumentException("Cannot parse this datetime field " + str.substring(startIdx, idx));
        }

        if (isDate) {
          fieldTypes[nf] = DateTimeFieldType.DATE;

          do {
            idx++;
          } while (idx <length && (timeStr[idx] == '+' || timeStr[idx] == '-' || timeStr[idx] == '/' ||
              timeStr[idx] == '_' || timeStr[idx] == '.' || timeStr[idx] == ':' ||
              Character.isLetterOrDigit(timeStr[idx])));
        }
      }

      // sign? then special or numeric timezone
      else if (timeStr[idx] == '+' || timeStr[idx] == '-') {
        idx++;

        // soak up leading whitespace
        while (idx < length && Character.isSpaceChar(timeStr[idx])) {
          idx++;
        }

        // numeric timezone?
        // note that "DTK_TZ" could also be a signed float or yyyy-mm */
        if (Character.isDigit(timeStr[idx])) {
          fieldTypes[nf] = DateTimeFieldType.TIMEZONE;
          idx++;

          while (idx < length && (Character.isDigit(timeStr[idx]) || timeStr[idx] == ':' || timeStr[idx] == '.' ||
              timeStr[idx] == '-')) {
            idx++;
          }
        }
        /* special? */
        else if (Character.isLetter(timeStr[idx])) {
          fieldTypes[nf] = DateTimeFieldType.SPECIAL;
          idx++;

          while (idx < length && Character.isLetter(timeStr[idx])) {
            idx++;
          }
        } else {
          throw new IllegalArgumentException("BAD Format: " + str.substring(startIdx, idx));
        }
      }
      /* ignore other punctuation but use as delimiter */
      else if (isPunctuation(timeStr[idx])) {
        idx++;
        continue;
      } else {  // otherwise, something is not right...
        throw new IllegalArgumentException("BAD datetime format: " + str.substring(startIdx, idx));
      }

      fields[nf] = str.substring(startIdx, idx);
      System.out.println(fields[nf]);
      nf++;
    }
  }

  /**
   * DecodeDateTime()
   * Interpret previously parsed fields for general date and time.
   * Return 0 if full date, 1 if only time, and negative DTERR code if problems.
   * (Currently, all callers treat 1 as an error return too.)
   *
   *		External format(s):
   *				"<weekday> <month>-<day>-<year> <hour>:<minute>:<second>"
   *				"Fri Feb-7-1997 15:23:27"
   *				"Feb-7-1997 15:23:27"
   *				"2-7-1997 15:23:27"
   *				"1997-2-7 15:23:27"
   *				"1997.038 15:23:27"		(day of year 1-366)
   *		Also supports input in compact time:
   *				"970207 152327"
   *				"97038 152327"
   *				"20011225T040506.789-07"
   *
   * Use the system-provided functions to get the current time zone
   * if not specified in the input string.
   *
   * If the date is outside the range of pg_time_t (in practice that could only
   * happen if pg_time_t is just 32 bits), then assume UTC time zone - thomas
   * 1997-05-27
   *
   * This method is originated form DecodeDateTime in datetime.c of PostgreSQL.
   */
  public static final TimeMeta DecodeDateTime(String [] fields, DateTimeFieldType [] fieldTypes, int nf) {
    int	fmask;
    int tmask;
    int type;


    // We'll insist on at least all of the date fields, but initialize the
    // remaining fields in case they are not set later...
    DateTimeFieldType dtype = DateTimeFieldType.DATE;

    TimeMeta tm = new TimeMeta();

    // don't know daylight savings time status apriori */
    tm.isDST = -1;

    for (int i = 0; i < nf; i++) {

      switch (fieldTypes[i]) {

      case DATE:
        // Integral julian day with attached time zone?
        // All other forms with JD will be separated into
        // distinct fields, so we handle just this case here.
        int julian = decodeDate(fields[i]);
        julianToDate(julian, tm);
        break;

      case TIME:

      case TIMEZONE:
        // TODO
      case SPECIAL:
        // TODO
      }
    }

    return null;
  }

  /**
   * Check whether it is a punctuation character or not.
   * @param c The character to be checked
   * @return True if it is a punctuation character. Otherwise, false.
   */
  public static final boolean isPunctuation(char c) {
    return ((c >= '!' && c <= '/') ||
        (c >= ':' && c <= '@') ||
        (c >= '[' && c <= '`') ||
        (c >= '{' && c <= '~'));
  }

  public enum DateStyle {
    XSO_DATES,
    ISO_DATES,
    SQL_DATES
  };

  /**
   * encodeDateTime()
   * Encode date and time interpreted as local time.
   *
   * tm and fsec are the value to encode, print_tz determines whether to include
   * a time zone (the difference between timestamp and timestamptz types), tz is
   * the numeric time zone offset, tzn is the textual time zone, which if
   * specified will be used instead of tz by some styles, style is the date
   * style, str is where to write the output.
   *
   * Supported date styles:
   *	Postgres - day mon hh:mm:ss yyyy tz
   *	SQL - mm/dd/yyyy hh:mm:ss.ss tz
   *	ISO - yyyy-mm-dd hh:mm:ss+/-tz
   *	German - dd.mm.yyyy hh:mm:ss tz
   *	XSD - yyyy-mm-ddThh:mm:ss.ss+/-tz
   *
   * This method is originated from EncodeDateTime of datetime.c of PostgreSQL.
   */
  public static final String encodeDateTime(TimeMeta tm, DateStyle style) {

    StringBuilder sb = new StringBuilder();
    switch (style) {

    case ISO_DATES:
    case XSO_DATES:
      if (style == DateStyle.ISO_DATES) {
        sb.append(String.format("%04d-%02d-%02d %02d:%02d:",
          (tm.years > 0) ? tm.years : -(tm.years - 1),
          tm.monthOfYear, tm.dayOfMonth, tm.hours, tm.minutes));
      } else {
        sb.append(String.format("%04d-%02d-%02dT%02d:%02d:",
            (tm.years > 0) ? tm.years : -(tm.years - 1),
            tm.monthOfYear, tm.dayOfMonth, tm.hours, tm.minutes));
      }

      appendSeconds(sb, tm.secs, tm.fsecs, 6, true);
      if (tm.years <= 0) {
        sb.append(" BC");
      }
      break;

    case SQL_DATES:
      // Compatible with Oracle/Ingres date formats

    }

    return sb.toString();
  }

  /**
   * Append sections and fractional seconds (if any) at *cp.
   * precision is the max number of fraction digits, fillzeros says to
   * pad to two integral-seconds digits.
   * Note that any sign is stripped from the input seconds values.
   *
   * This method is originated form AppendSeconds in datetime.c of PostgreSQL.
   */
  public static final void appendSeconds(StringBuilder sb, int sec, int fsec, int precision, boolean fillzeros) {
    if (fsec == 0)
    {
      if (fillzeros)
        sb.append(String.format("%02d", Math.abs(sec)));
      else
        sb.append(String.format("%d", Math.abs(sec)));
    } else {
      if (fillzeros) {
        sb.append(String.format("%02d", Math.abs(sec)));
      } else {
        sb.append(String.format("%d", Math.abs(sec)));
      }

      if (precision > 0) {
        int dotIdx = sb.length() + 1;
        sb.append(String.format(".%d", fsec));
        int addedPrecision = sb.length() - dotIdx;
        if (addedPrecision > precision) {
          sb.setLength(sb.length() - (addedPrecision - precision));
        }
      }
      trimTrailingZeros(sb);
    }
  }

  /**
   * trimTrailingZeros()
   * ... resulting from printing numbers with full precision.
   *
   * Before Postgres 8.4, this always left at least 2 fractional digits,
   * but conversations on the lists suggest this isn't desired
   * since showing '0.10' is misleading with values of precision(1).
   *
   * This method is originated form AppendSeconds in datetime.c of PostgreSQL.
   */
  public static void trimTrailingZeros(StringBuilder sb) {
    int len = sb.length();
    while (len > 1 && sb.charAt(len - 1) == '0' && sb.charAt(len - 2) != '.') {
      len--;
      sb.setLength(len);
    }
  }
}
