/*

Copyright (c) 2007-2010  Michael G Schwern

This software originally derived from Paul Sheer's pivotal_gmtime_r.c.

The MIT License:

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.

*/

/*

Programmers who have available to them 64-bit time values as a 'long
long' type can use gmtime64_r() which correctly converts the time even on
32-bit systems. Whether you have 64-bit time values will depend on the
operating system.

gmtime64_r() is a 64-bit equivalent of gmtime_r().

*/

#include <assert.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <errno.h>
#include "time64.h"
#include "time64_limits.h"



static const int days_in_month[2][12] = {
    {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31},
    {31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31},
};

static const int julian_days_by_month[2][12] = {
    {0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334},
    {0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335},
};

static const int length_of_year[2] = { 365, 366 };

/* Some numbers relating to the gregorian cycle */
static const Year     years_in_gregorian_cycle   = 400;
#define               days_in_gregorian_cycle      ((365 * 400) + 100 - 4 + 1)
static const Time64_T seconds_in_gregorian_cycle = days_in_gregorian_cycle * 60LL * 60LL * 24LL;

/* Year range we can trust the time funcitons with */
#define MAX_SAFE_YEAR 2037
#define MIN_SAFE_YEAR 1971

/* 28 year Julian calendar cycle */
#define SOLAR_CYCLE_LENGTH 28

/* Year cycle from MAX_SAFE_YEAR down. */
static const int safe_years_high[SOLAR_CYCLE_LENGTH] = {
    2016, 2017, 2018, 2019,
    2020, 2021, 2022, 2023,
    2024, 2025, 2026, 2027,
    2028, 2029, 2030, 2031,
    2032, 2033, 2034, 2035,
    2036, 2037, 2010, 2011,
    2012, 2013, 2014, 2015
};

/* Year cycle from MIN_SAFE_YEAR up */
static const int safe_years_low[SOLAR_CYCLE_LENGTH] = {
    1996, 1997, 1998, 1971,
    1972, 1973, 1974, 1975,
    1976, 1977, 1978, 1979,
    1980, 1981, 1982, 1983,
    1984, 1985, 1986, 1987,
    1988, 1989, 1990, 1991,
    1992, 1993, 1994, 1995,
};

/* This isn't used, but it's handy to look at */
static const int dow_year_start[SOLAR_CYCLE_LENGTH] = {
    5, 0, 1, 2,     /* 0       2016 - 2019 */
    3, 5, 6, 0,     /* 4  */
    1, 3, 4, 5,     /* 8       1996 - 1998, 1971*/
    6, 1, 2, 3,     /* 12      1972 - 1975 */
    4, 6, 0, 1,     /* 16 */
    2, 4, 5, 6,     /* 20      2036, 2037, 2010, 2011 */
    0, 2, 3, 4      /* 24      2012, 2013, 2014, 2015 */
};

/* Let's assume people are going to be looking for dates in the future.
   Let's provide some cheats so you can skip ahead.
   This has a 4x speed boost when near 2008.
*/
/* Number of days since epoch on Jan 1st, 2008 GMT */
#define CHEAT_DAYS  (1199145600 / 24 / 60 / 60)
#define CHEAT_YEARS 108

#define IS_LEAP(n)      ((!(((n) + 1900) % 400) || (!(((n) + 1900) % 4) && (((n) + 1900) % 100))) != 0)
#define WRAP(a,b,m)     ((a) = ((a) <  0  ) ? ((b)--, (a) + (m)) : (a))

#ifdef USE_SYSTEM_GMTIME
#    define SHOULD_USE_SYSTEM_GMTIME(a)     (       \
    (a) <= SYSTEM_GMTIME_MAX    &&              \
    (a) >= SYSTEM_GMTIME_MIN                    \
)
#else
#    define SHOULD_USE_SYSTEM_GMTIME(a)         (0)
#endif

/* Multi varadic macros are a C99 thing, alas */
#ifdef TIME_64_DEBUG
#    define TIME64_TRACE(format) (fprintf(stderr, format))
#    define TIME64_TRACE1(format, var1)    (fprintf(stderr, format, var1))
#    define TIME64_TRACE2(format, var1, var2)    (fprintf(stderr, format, var1, var2))
#    define TIME64_TRACE3(format, var1, var2, var3)    (fprintf(stderr, format, var1, var2, var3))
#else
#    define TIME64_TRACE(format) ((void)0)
#    define TIME64_TRACE1(format, var1) ((void)0)
#    define TIME64_TRACE2(format, var1, var2) ((void)0)
#    define TIME64_TRACE3(format, var1, var2, var3) ((void)0)
#endif



void copy_tm_to_TM64(const struct tm *src, struct TM *dest) {
    if( src == NULL ) {
        memset(dest, 0, sizeof(*dest));
    }
    else {
#       ifdef USE_TM64
            dest->tm_sec        = src->tm_sec;
            dest->tm_min        = src->tm_min;
            dest->tm_hour       = src->tm_hour;
            dest->tm_mday       = src->tm_mday;
            dest->tm_mon        = src->tm_mon;
            dest->tm_year       = (Year)src->tm_year;
            dest->tm_wday       = src->tm_wday;
            dest->tm_yday       = src->tm_yday;
            dest->tm_isdst      = src->tm_isdst;

#           ifdef HAS_TM_TM_GMTOFF
                dest->tm_gmtoff  = src->tm_gmtoff;
#           endif

#           ifdef HAS_TM_TM_ZONE
                dest->tm_zone  = src->tm_zone;
#           endif

#       else
            /* They're the same type */
            memcpy(dest, src, sizeof(*dest));
#       endif
    }
}


struct TM *gmtime64_r (const Time64_T *in_time, struct TM *p)
{
    int v_tm_sec, v_tm_min, v_tm_hour, v_tm_mon, v_tm_wday;
    Time64_T v_tm_tday;
    int leap;
    Time64_T m;
    Time64_T time = *in_time;
    Year year = 70;
    int cycles = 0;

    assert(p != NULL);

    /* Use the system gmtime() if time_t is small enough */
    if( SHOULD_USE_SYSTEM_GMTIME(*in_time) ) {
        time_t safe_time = (time_t)*in_time;
        struct tm safe_date;
        GMTIME_R(&safe_time, &safe_date);

        copy_tm_to_TM64(&safe_date, p);
        assert(check_tm(p));

        return p;
    }

#ifdef HAS_TM_TM_GMTOFF
    p->tm_gmtoff = 0;
#endif
    p->tm_isdst  = 0;

#ifdef HAS_TM_TM_ZONE
    p->tm_zone   = "UTC";
#endif

    v_tm_sec =  (int)(time % 60);
    time /= 60;
    v_tm_min =  (int)(time % 60);
    time /= 60;
    v_tm_hour = (int)(time % 24);
    time /= 24;
    v_tm_tday = time;

    WRAP (v_tm_sec, v_tm_min, 60);
    WRAP (v_tm_min, v_tm_hour, 60);
    WRAP (v_tm_hour, v_tm_tday, 24);

    v_tm_wday = (int)((v_tm_tday + 4) % 7);
    if (v_tm_wday < 0)
        v_tm_wday += 7;
    m = v_tm_tday;

    if (m >= CHEAT_DAYS) {
        year = CHEAT_YEARS;
        m -= CHEAT_DAYS;
    }

    if (m >= 0) {
        /* Gregorian cycles, this is huge optimization for distant times */
        cycles = (int)(m / (Time64_T) days_in_gregorian_cycle);
        if( cycles ) {
            m -= (cycles * (Time64_T) days_in_gregorian_cycle);
            year += (cycles * years_in_gregorian_cycle);
        }

        /* Years */
        leap = IS_LEAP (year);
        while (m >= (Time64_T) length_of_year[leap]) {
            m -= (Time64_T) length_of_year[leap];
            year++;
            leap = IS_LEAP (year);
        }

        /* Months */
        v_tm_mon = 0;
        while (m >= (Time64_T) days_in_month[leap][v_tm_mon]) {
            m -= (Time64_T) days_in_month[leap][v_tm_mon];
            v_tm_mon++;
        }
    } else {
        year--;

        /* Gregorian cycles */
        cycles = (int)((m / (Time64_T) days_in_gregorian_cycle) + 1);
        if( cycles ) {
            m -= (cycles * (Time64_T) days_in_gregorian_cycle);
            year += (cycles * years_in_gregorian_cycle);
        }

        /* Years */
        leap = IS_LEAP (year);
        while (m < (Time64_T) -length_of_year[leap]) {
            m += (Time64_T) length_of_year[leap];
            year--;
            leap = IS_LEAP (year);
        }

        /* Months */
        v_tm_mon = 11;
        while (m < (Time64_T) -days_in_month[leap][v_tm_mon]) {
            m += (Time64_T) days_in_month[leap][v_tm_mon];
            v_tm_mon--;
        }
        m += (Time64_T) days_in_month[leap][v_tm_mon];
    }

    p->tm_year = (int)year;
    if( p->tm_year != year ) {
#ifdef EOVERFLOW
        errno = EOVERFLOW;
#endif
        return NULL;
    }

    /* At this point m is less than a year so casting to an int is safe */
    p->tm_mday = (int) m + 1;
    p->tm_yday = julian_days_by_month[leap][v_tm_mon] + (int)m;
    p->tm_sec  = v_tm_sec;
    p->tm_min  = v_tm_min;
    p->tm_hour = v_tm_hour;
    p->tm_mon  = v_tm_mon;
    p->tm_wday = v_tm_wday;

    assert(check_tm(p));

    return p;
}

Time64_T timegm64(const struct TM *date) {
    Time64_T days    = 0;
    Time64_T seconds = 0;
    Year     year;
    Year     orig_year = (Year)date->tm_year;
    int      cycles  = 0;

    if( orig_year > 100 ) {
        cycles = (int)((orig_year - 100) / 400);
        orig_year -= cycles * 400;
        days      += (Time64_T)cycles * days_in_gregorian_cycle;
    }
    else if( orig_year < -300 ) {
        cycles = (int)((orig_year - 100) / 400);
        orig_year -= cycles * 400;
        days      += (Time64_T)cycles * days_in_gregorian_cycle;
    }
    TIME64_TRACE3("# timegm/ cycles: %d, days: %lld, orig_year: %lld\n", cycles, days, orig_year);

    if( orig_year > 70 ) {
        year = 70;
        while( year < orig_year ) {
            days += length_of_year[IS_LEAP(year)];
            year++;
        }
    }
    else if ( orig_year < 70 ) {
        year = 69;
        do {
            days -= length_of_year[IS_LEAP(year)];
            year--;
        } while( year >= orig_year );
    }

    days += julian_days_by_month[IS_LEAP(orig_year)][date->tm_mon];
    days += date->tm_mday - 1;

    seconds = days * 60 * 60 * 24;

    seconds += date->tm_hour * 60 * 60;
    seconds += date->tm_min * 60;
    seconds += date->tm_sec;

    return(seconds);
}