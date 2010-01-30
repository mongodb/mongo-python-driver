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

/* Copyright (c) 1998-2003 Carnegie Mellon University.  All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in
 *    the documentation and/or other materials provided with the
 *    distribution.
 *
 * 3. The name "Carnegie Mellon University" must not be used to
 *    endorse or promote products derived from this software without
 *    prior written permission. For permission or any other legal
 *    details, please contact
 *      Office of Technology Transfer
 *      Carnegie Mellon University
 *      5000 Forbes Avenue
 *      Pittsburgh, PA  15213-3890
 *      (412) 268-4387, fax: (412) 268-7395
 *      tech-transfer@andrew.cmu.edu
 *
 * 4. Redistributions of any form whatsoever must retain the following
 *    acknowledgment:
 *    "This product includes software developed by Computing Services
 *     at Carnegie Mellon University (http://www.cmu.edu/computing/)."
 *
 * CARNEGIE MELLON UNIVERSITY DISCLAIMS ALL WARRANTIES WITH REGARD TO
 * THIS SOFTWARE, INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY
 * AND FITNESS, IN NO EVENT SHALL CARNEGIE MELLON UNIVERSITY BE LIABLE
 * FOR ANY SPECIAL, INDIRECT OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN
 * AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING
 * OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 *
 *
 */

/*
 * Copyright (c) 1987, 1989, 1993
 *    The Regents of the University of California.  All rights reserved.
 *
 * This code is derived from software contributed to Berkeley by
 * Arthur David Olson of the National Cancer Institute.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. All advertising materials mentioning features or use of this software
 *    must display the following acknowledgement:
 *    This product includes software developed by the University of
 *    California, Berkeley and its contributors.
 * 4. Neither the name of the University nor the names of its contributors
 *    may be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE REGENTS AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE REGENTS OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

/*
** Adapted from code provided by Robert Elz, who writes:
**    The "best" way to do mktime I think is based on an idea of Bob
**    Kridle's (so its said...) from a long time ago. (mtxinu!kridle now).
**    It does a binary search of the time_t space.  Since time_t's are
**    just 32 bits, its a max of 32 iterations (even at 64 bits it
**    would still be very reasonable).
*/

/* This hack is just for MSVC before VS 2005. */
#if defined(WIN32) || defined(_MSC_VER)
#if !defined(_MSC_VER) || (_MSC_VER < 1400)

#include <time.h>

#ifndef WRONG
#define WRONG     (-1)
#endif /* !defined WRONG */

static int tmcomp(atmp, btmp)
     register const struct tm * const atmp;
     register const struct tm * const btmp;
{
    register int      result;

    if ((result = (atmp->tm_year - btmp->tm_year)) == 0 &&
        (result = (atmp->tm_mon - btmp->tm_mon)) == 0 &&
        (result = (atmp->tm_mday - btmp->tm_mday)) == 0 &&
        (result = (atmp->tm_hour - btmp->tm_hour)) == 0 &&
        (result = (atmp->tm_min - btmp->tm_min)) == 0)
        result = atmp->tm_sec - btmp->tm_sec;
    return result;
}

time_t mkgmtime(tmp)
struct tm * const tmp;
{
    register int                  dir;
    register int                  bits;
    register int                  saved_seconds;
    time_t                        t;
    struct tm               yourtm, *mytm;

    yourtm = *tmp;
    saved_seconds = yourtm.tm_sec;
    yourtm.tm_sec = 0;

    /*
    ** Calculate the number of magnitude bits in a time_t
    ** (this works regardless of whether time_t is
    ** signed or unsigned, though lint complains if unsigned).
    */
    for (bits = 0, t = 1; t > 0; ++bits, t <<= 1)
        ;
    /*
    ** If time_t is signed, then 0 is the median value,
    ** if time_t is unsigned, then 1 << bits is median.
    */
    t = (t < 0) ? 0 : ((time_t) 1 << bits);

    /* Some gmtime() implementations are broken and will return
     * NULL for time_ts larger than 40 bits even on 64-bit platforms
     * so we'll just cap it at 40 bits */
    if(bits > 40) bits = 40;

    for ( ; ; ) {
        mytm = gmtime(&t);

        if(!mytm) return WRONG;

        dir = tmcomp(mytm, &yourtm);
        if (dir != 0) {
            if (bits-- < 0)
                return WRONG;
            if (bits < 0)
                --t;
            else if (dir > 0)
                t -= (time_t) 1 << bits;
            else  t += (time_t) 1 << bits;
            continue;
        }
        break;
    }
    t += saved_seconds;
    return t;
}

#endif
#endif
