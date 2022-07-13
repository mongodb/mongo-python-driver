Datetimes and Timezones
=======================

.. testsetup::

   import datetime
   from pymongo import MongoClient
   from bson.codec_options import CodecOptions
   client = MongoClient()
   client.drop_database('dt_example')
   db = client.dt_example

These examples show how to handle Python :class:`datetime.datetime` objects
correctly in PyMongo.

Basic Usage
-----------

PyMongo uses :class:`datetime.datetime` objects for representing dates and times
in MongoDB documents. Because MongoDB assumes that dates and times are in UTC,
care should be taken to ensure that dates and times written to the database
reflect UTC. For example, the following code stores the current UTC date and
time into MongoDB:

.. doctest::

   >>> result = db.objects.insert_one(
   ...     {"last_modified": datetime.datetime.utcnow()})

Always use :meth:`datetime.datetime.utcnow`, which returns the current time in
UTC, instead of :meth:`datetime.datetime.now`, which returns the current local
time. Avoid doing this:

.. doctest::

   >>> result = db.objects.insert_one(
   ...     {"last_modified": datetime.datetime.now()})

The value for `last_modified` is very different between these two examples, even
though both documents were stored at around the same local time. This will be
confusing to the application that reads them:

.. doctest::

   >>> [doc['last_modified'] for doc in db.objects.find()]  # doctest: +SKIP
   [datetime.datetime(2015, 7, 8, 18, 17, 28, 324000),
    datetime.datetime(2015, 7, 8, 11, 17, 42, 911000)]

:class:`bson.codec_options.CodecOptions` has a `tz_aware` option that enables
"aware" :class:`datetime.datetime` objects, i.e., datetimes that know what
timezone they're in. By default, PyMongo retrieves naive datetimes:

.. doctest::

   >>> result = db.tzdemo.insert_one(
   ...     {'date': datetime.datetime(2002, 10, 27, 6, 0, 0)})
   >>> db.tzdemo.find_one()['date']
   datetime.datetime(2002, 10, 27, 6, 0)
   >>> options = CodecOptions(tz_aware=True)
   >>> db.get_collection('tzdemo', codec_options=options).find_one()['date']  # doctest: +SKIP
   datetime.datetime(2002, 10, 27, 6, 0,
                     tzinfo=<bson.tz_util.FixedOffset object at 0x10583a050>)

Saving Datetimes with Timezones
-------------------------------

When storing :class:`datetime.datetime` objects that specify a timezone
(i.e. they have a `tzinfo` property that isn't ``None``), PyMongo will convert
those datetimes to UTC automatically:

.. doctest::

   >>> import pytz
   >>> pacific = pytz.timezone('US/Pacific')
   >>> aware_datetime = pacific.localize(
   ...     datetime.datetime(2002, 10, 27, 6, 0, 0))
   >>> result = db.times.insert_one({"date": aware_datetime})
   >>> db.times.find_one()['date']
   datetime.datetime(2002, 10, 27, 14, 0)

Reading Time
------------

As previously mentioned, by default all :class:`datetime.datetime` objects
returned by PyMongo will be naive but reflect UTC (i.e. the time as stored in
MongoDB). By setting the `tz_aware` option on
:class:`~bson.codec_options.CodecOptions`, :class:`datetime.datetime` objects
will be timezone-aware and have a `tzinfo` property that reflects the UTC
timezone.

PyMongo 3.1 introduced a `tzinfo` property that can be set on
:class:`~bson.codec_options.CodecOptions` to convert :class:`datetime.datetime`
objects to local time automatically. For example, if we wanted to read all times
out of MongoDB in US/Pacific time:

   >>> from bson.codec_options import CodecOptions
   >>> db.times.find_one()['date']
   datetime.datetime(2002, 10, 27, 14, 0)
   >>> aware_times = db.times.with_options(codec_options=CodecOptions(
   ...     tz_aware=True,
   ...     tzinfo=pytz.timezone('US/Pacific')))
   >>> result = aware_times.find_one()
   datetime.datetime(2002, 10, 27, 6, 0,  # doctest: +NORMALIZE_WHITESPACE
                     tzinfo=<DstTzInfo 'US/Pacific' PST-1 day, 16:00:00 STD>)

Extended Usage
--------------

Python can only represent datetimes within the range allowed by
:attr:`~datetime.datetime.min` and :attr:`~datetime.datetime.max`, whereas
the range of datetimes allowed in BSON can represent any 64-bit number
of milliseconds from the Unix epoch. To deal with this, we can use the
:class:`bson.datetime_ms.DatetimeMS` object, which is a wrapper for the
:class:`int` built-in.

To decode UTC datetime values as :class:`~bson.datetime_ms.DatetimeMS`,
:class:`~bson.codec_options.CodecOptions` should have its
``datetime_conversion`` parameter set to one of the options available in
:class:`bson.datetime_ms.DatetimeConversionOpts`. These include
:attr:`~bson.datetime_ms.DatetimeConversionOpts.DATETIME`,
:attr:`~bson.datetime_ms.DatetimeConversionOpts.DATETIME_MS`,
:attr:`~bson.datetime_ms.DatetimeConversionOpts.DATETIME_AUTO`,
:attr:`~bson.datetime_ms.DatetimeConversionOpts.DATETIME_CLAMP`.
:attr:`~bson.datetime_ms.DatetimeConversionOpts.DATETIME` is the default
option and has the behavior of raising an exception upon attempting to
decode an out-of-range date.
:attr:`~bson.datetime_ms.DatetimeConversionOpts.DATETIME_MS` will only return
:class:`~bson.datetime_ms.DatetimeMS` objects, regardless of whether the
represented datetime is in- or out-of-range.
:attr:`~bson.datetime_ms.DatetimeConversionOpts.DATETIME_AUTO` will return
:class:`~datetime.datetime` if the underlying UTC datetime is within range,
or :class:`~bson.datetime_ms.DatetimeMS` if the underlying datetime
cannot be represented using the builtin Python :class:`~datetime.datetime`.
:attr:`~bson.datetime_ms.DatetimeConversionOpts.DATETIME_CLAMP` will clamp
resulting :class:`~datetime.datetime` objects to be within
:attr:`~datetime.datetime.min` and :attr:`~datetime.datetime.max`
(trimmed to `999000` microseconds).

An example of encoding and decoding using `DATETIME_MS` is as follows:

.. doctest::
    >>> from datetime import datetime
    >>> from bson import encode, decode
    >>> from bson.datetime_ms import DatetimeMS
    >>> from bson.codec_options import CodecOptions,DatetimeConversionOpts
    >>> x = encode({"x": datetime(1970, 1, 1)})
    >>> x
    b'\x10\x00\x00\x00\tx\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
    >>> decode(x, codec_options=CodecOptions(datetime_conversion=DatetimeConversionOpts.DATETIME_MS))
    {'x': DatetimeMS(0)}

:class:`~bson.datetime_ms.DatetimeMS` objects have support for rich comparison
methods against other instances of :class:`~bson.datetime_ms.DatetimeMS`.
They can also be converted to :class:`~datetime.datetime` objects with
:meth:`~bson.datetime_ms.DatetimeMS.to_datetime()`.
