#!/bin/sh
set -o xtrace
set -o errexit

APACHE=$(command -v apache2 || command -v /usr/lib/apache2/mpm-prefork/apache2) || true
if [ -n "$APACHE" ]; then
    APACHE_CONFIG=apache22ubuntu1204.conf
else
    APACHE=$(command -v httpd) || true
    if [ -z "$APACHE" ]; then
        echo "Could not find apache2 binary"
        exit 1
    else
        APACHE_CONFIG=apache22amazon.conf
    fi
fi


PYTHON_VERSION=$(${PYTHON_BINARY} -c "import sys; sys.stdout.write('.'.join(str(val) for val in sys.version_info[:2]))")

export MOD_WSGI_SO=/opt/python/mod_wsgi/python_version/$PYTHON_VERSION/mod_wsgi_version/$MOD_WSGI_VERSION/mod_wsgi.so
export PYTHONHOME=/opt/python/$PYTHON_VERSION

cd ..
$APACHE -k start -f ${PROJECT_DIRECTORY}/test/mod_wsgi_test/${APACHE_CONFIG}
trap "$APACHE -k stop -f ${PROJECT_DIRECTORY}/test/mod_wsgi_test/${APACHE_CONFIG}" EXIT HUP

set +e
wget -t 1 -T 10 -O - "http://localhost:8080${PROJECT_DIRECTORY}"
STATUS=$?
set -e

# Debug
cat error_log

if [ $STATUS != 0 ]; then
    exit $STATUS
fi

${PYTHON_BINARY} ${PROJECT_DIRECTORY}/test/mod_wsgi_test/test_client.py -n 25000 -t 100 parallel http://localhost:8080${PROJECT_DIRECTORY}

${PYTHON_BINARY} ${PROJECT_DIRECTORY}/test/mod_wsgi_test/test_client.py -n 25000 serial http://localhost:8080${PROJECT_DIRECTORY}
