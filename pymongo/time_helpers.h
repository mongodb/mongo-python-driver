/*
 * Copyright 2009-2010 10gen, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef TIME_HELPERS_H
#define TIME_HELPERS_H

#include <time.h>

/*
 * Some helpers for dealing with time stuff in a cross platform way.
 */
#if defined(WIN32) || defined(_MSC_VER)
#if defined(_MSC_VER) && (_MSC_VER >= 1400)
#define GMTIME_INVERSE(time_struct) _mkgmtime64(time_struct)
#define GMTIME(timeinfo, seconds) gmtime_s((timeinfo), (seconds))
#define LOCALTIME(timeinfo, seconds) localtime_s((timeinfo), (seconds))
#else
/* No mkgmtime on MSVC before VS 2005.
 * This is terribly gross (see time_helpers.c). */
time_t mkgmtime(const struct tm* tmp);
#define GMTIME_INVERSE(time_struct) mkgmtime((time_struct))
#define GMTIME(timeinfo, seconds) *(timeinfo) = *(gmtime((seconds))), 0
#define LOCALTIME(timeinfo, seconds) *(timeinfo) = *(localtime((seconds))), 0
#endif
#else
#define GMTIME_INVERSE(time_struct) timegm((time_struct))
#define GMTIME(timeinfo, seconds) gmtime_r((seconds), (timeinfo)), 0
#define LOCALTIME(timeinfo, seconds) localtime_r((seconds), (timeinfo)), 0
#endif

#endif
