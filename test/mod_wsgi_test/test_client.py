# Copyright 2012-present MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Test client for mod_wsgi application, see bug PYTHON-353."""
from __future__ import annotations

import _thread as thread
import random
import sys
import threading
import time
from optparse import OptionParser
from urllib.request import urlopen


def parse_args():
    parser = OptionParser(
        """usage: %prog [options] mode url [<url2>...]

  mode:\tparallel or serial"""
    )

    # Should be enough that any connection leak will exhaust available file
    # descriptors.
    parser.add_option(
        "-n",
        "--nrequests",
        type="int",
        dest="nrequests",
        default=50 * 1000,
        help="Number of times to GET the URLs, in total",
    )

    parser.add_option(
        "-t",
        "--nthreads",
        type="int",
        dest="nthreads",
        default=100,
        help="Number of threads with mode 'parallel'",
    )

    parser.add_option(
        "-q",
        "--quiet",
        action="store_false",
        dest="verbose",
        default=True,
        help="Don't print status messages to stdout",
    )

    parser.add_option(
        "-c",
        "--continue",
        action="store_true",
        dest="continue_",
        default=False,
        help="Continue after HTTP errors",
    )

    try:
        options, args = parser.parse_args()
        mode, urls = args[0], args[1:]
    except (ValueError, IndexError):
        parser.print_usage()
        sys.exit(1)

    if mode not in ("parallel", "serial"):
        parser.print_usage()
        sys.exit(1)

    return options, mode, urls


def get(urls):
    url = random.choice(urls)
    urlopen(url).read().strip()


class URLGetterThread(threading.Thread):
    # Class variables.
    counter_lock = threading.Lock()
    counter = 0

    def __init__(self, options, urls, nrequests_per_thread):
        super().__init__()
        self.options = options
        self.urls = urls
        self.nrequests_per_thread = nrequests_per_thread
        self.errors = 0

    def run(self):
        for _i in range(self.nrequests_per_thread):
            try:
                get(urls)
            except Exception as e:
                print(e)

                if not options.continue_:
                    thread.interrupt_main()
                    thread.exit()

                self.errors += 1

            with URLGetterThread.counter_lock:
                URLGetterThread.counter += 1
                counter = URLGetterThread.counter

            should_print = options.verbose and not counter % 1000

            if should_print:
                print(counter)


def main(options, mode, urls):
    start_time = time.time()
    errors = 0
    if mode == "parallel":
        nrequests_per_thread = options.nrequests // options.nthreads

        if options.verbose:
            print(
                "Getting {} {} times total in {} threads, " "{} times per thread".format(
                    urls,
                    nrequests_per_thread * options.nthreads,
                    options.nthreads,
                    nrequests_per_thread,
                )
            )
        threads = [
            URLGetterThread(options, urls, nrequests_per_thread) for _ in range(options.nthreads)
        ]

        for t in threads:
            t.start()

        for t in threads:
            t.join()

        errors = sum([t.errors for t in threads])
        nthreads_with_errors = len([t for t in threads if t.errors])
        if nthreads_with_errors:
            print("%d threads had errors! %d errors in total" % (nthreads_with_errors, errors))
    else:
        assert mode == "serial"
        if options.verbose:
            print(f"Getting {urls} {options.nrequests} times in one thread")

        for i in range(1, options.nrequests + 1):
            try:
                get(urls)
            except Exception as e:
                print(e)
                if not options.continue_:
                    sys.exit(1)

                errors += 1

            if options.verbose and not i % 1000:
                print(i)

        if errors:
            print("%d errors!" % errors)

    if options.verbose:
        print("Completed in %.2f seconds" % (time.time() - start_time))

    if errors:
        # Failure
        sys.exit(1)


if __name__ == "__main__":
    options, mode, urls = parse_args()
    main(options, mode, urls)
