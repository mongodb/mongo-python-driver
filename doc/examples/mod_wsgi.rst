.. _pymongo-and-mod_wsgi:

PyMongo and mod_wsgi
====================

To run your application under `mod_wsgi <https://github.com/GrahamDumpleton/mod_wsgi/>`_,
follow these guidelines:

* Run ``mod_wsgi`` in daemon mode with the ``WSGIDaemonProcess`` directive.
* Assign each application to a separate daemon with ``WSGIProcessGroup``.
* Use ``WSGIApplicationGroup %{GLOBAL}`` to ensure your application is running
  in the daemon's main Python interpreter, not a sub interpreter.

For example, this ``mod_wsgi`` configuration ensures an application runs in the
main interpreter::

    <VirtualHost *>
        WSGIDaemonProcess my_process
        WSGIScriptAlias /my_app /path/to/app.wsgi
        WSGIProcessGroup my_process
        WSGIApplicationGroup %{GLOBAL}
    </VirtualHost>

If you have multiple applications that use PyMongo, put each in a separate
daemon, still in the global application group::

    <VirtualHost *>
        WSGIDaemonProcess my_process
        WSGIScriptAlias /my_app /path/to/app.wsgi
        <Location /my_app>
            WSGIProcessGroup my_process
        </Location>

        WSGIDaemonProcess my_other_process
        WSGIScriptAlias /my_other_app /path/to/other_app.wsgi
        <Location /my_other_app>
            WSGIProcessGroup my_other_process
        </Location>

        WSGIApplicationGroup %{GLOBAL}
    </VirtualHost>

Background: ``mod_wsgi`` can run in "embedded" mode when only WSGIScriptAlias
is set, or "daemon" mode with WSGIDaemonProcess. In daemon mode, ``mod_wsgi``
can run your application in the Python main interpreter, or in sub interpreters.
The correct way to run a PyMongo application is in daemon mode, using the main
interpreter.

Python C extensions in general have issues running in multiple
Python sub interpreters. These difficulties are explained in the documentation for
`Py_NewInterpreter <https://docs.python.org/3/c-api/init.html#c.Py_NewInterpreter>`_
and in the `Multiple Python Sub Interpreters
<https://modwsgi.readthedocs.io/en/master/user-guides/application-issues.html#multiple-python-sub-interpreters>`_
section of the ``mod_wsgi`` documentation.

Beginning with PyMongo 2.7, the C extension for BSON detects when it is running
in a sub interpreter and activates a workaround, which adds a small cost to
BSON decoding. To avoid this cost, use ``WSGIApplicationGroup %{GLOBAL}`` to
ensure your application runs in the main interpreter.

Since your program runs in the main interpreter it should not share its
process with any other applications, lest they interfere with each other's
state. Each application should have its own daemon process, as shown in the
example above.
