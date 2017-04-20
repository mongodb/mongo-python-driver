#!/bin/sh
set -o xtrace
set -o errexit

# TODO: Rework mod_wsgi directory structure to avoid this mess.
PYTHON_NAME=$(${PYTHON_BINARY} -c "import sys; sys.stdout.write('python%s' % ('.'.join(str(val) for val in sys.version_info[:2])))")
export MOD_WSGI_SO=/opt/python/mod_wsgi/4.5.13/${PYTHON_NAME}/mod_wsgi.so

cd ..
# TODO: Test with Apache 2.2 on Ubuntu 12.04 or Amazon Linux.
apache2 -k start -f ${PROJECT_DIRECTORY}/test/mod_wsgi_test/apache24ubuntu161404.conf
trap "apache2 -k stop -f ${PROJECT_DIRECTORY}/test/mod_wsgi_test/apache24ubuntu161404.conf" EXIT HUP

wget -O - "http://localhost:8080${PROJECT_DIRECTORY}"

# Debug
cat error_log

${PYTHON_BINARY} ${PROJECT_DIRECTORY}/test/mod_wsgi_test/test_client.py -n 25000 -t 100 parallel http://localhost:8080${PROJECT_DIRECTORY}

${PYTHON_BINARY} ${PROJECT_DIRECTORY}/test/mod_wsgi_test/test_client.py -n 25000 serial http://localhost:8080${PROJECT_DIRECTORY}

