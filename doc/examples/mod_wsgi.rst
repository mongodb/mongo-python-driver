.. _pymongo-and-mod_wsgi:

PyMongo and mod_wsgi
====================

If you run your application under
`mod_wsgi <http://code.google.com/p/modwsgi/>`_ and you use PyMongo with its C
extensions enabled, follow these guidelines for best performance:

* Run ``mod_wsgi`` in daemon mode with the ``WSGIDaemon`` directive.
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

Background: Python C extensions in general have issues running in multiple
Python sub interpreters. These difficulties are explained in the documentation for
`Py_NewInterpreter <http://docs.python.org/2/c-api/init.html#Py_NewInterpreter>`_
and in the `Multiple Python Sub Interpreters
<https://code.google.com/p/modwsgi/wiki/ApplicationIssues#Multiple_Python_Sub_Interpreters>`_
section of the ``mod_wsgi`` documentation.

Beginning with PyMongo 2.7, the C extension for BSON detects when it is running
in a sub interpreter and activates a workaround, which adds a small cost to
BSON decoding. To avoid this cost, use ``WSGIApplicationGroup %{GLOBAL}`` to
ensure your application runs in the main interpreter.

Since your program runs in the main interpreter it should not share its
process with any other applications, lest they interfere with each other's
state. Each application should have its own daemon process, as shown in the
example above.