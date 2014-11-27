/*
 * Copyright 2009-2014 MongoDB, Inc.
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

/*
 * This file contains C implementations of some of the functions
 * needed by the bson module. If possible, these implementations
 * should be used to speed up BSON encoding and decoding.
 */

#include "Python.h"
#include "datetime.h"

#include "buffer.h"
#include "time64.h"
#include "encoding_helpers.h"

#define _CBSON_MODULE
#include "_cbsonmodule.h"

/* New module state and initialization code.
 * See the module-initialization-and-state
 * section in the following doc:
 * http://docs.python.org/release/3.1.3/howto/cporting.html
 * which references the following pep:
 * http://www.python.org/dev/peps/pep-3121/
 * */
struct module_state {
    PyObject* Binary;
    PyObject* Code;
    PyObject* ObjectId;
    PyObject* DBRef;
    PyObject* RECompile;
    PyObject* Regex;
    PyObject* UUID;
    PyObject* Timestamp;
    PyObject* MinKey;
    PyObject* MaxKey;
    PyObject* UTC;
    PyTypeObject* REType;
};

/* The Py_TYPE macro was introduced in CPython 2.6 */
#ifndef Py_TYPE
#define Py_TYPE(ob) (((PyObject*)(ob))->ob_type)
#endif

#if PY_MAJOR_VERSION >= 3
#define GETSTATE(m) ((struct module_state*)PyModule_GetState(m))
#else
#define GETSTATE(m) (&_state)
static struct module_state _state;
#endif

/* Maximum number of regex flags */
#define FLAGS_SIZE 7

#define JAVA_LEGACY   5
#define CSHARP_LEGACY 6
#define BSON_MAX_SIZE 2147483647
/* The smallest possible BSON document, i.e. "{}" */
#define BSON_MIN_SIZE 5

/* Get an error class from the bson.errors module.
 *
 * Returns a new ref */
static PyObject* _error(char* name) {
    PyObject* error;
    PyObject* errors = PyImport_ImportModule("bson.errors");
    if (!errors) {
        return NULL;
    }
    error = PyObject_GetAttrString(errors, name);
    Py_DECREF(errors);
    return error;
}

/* Safely downcast from Py_ssize_t to int, setting an
 * exception and returning -1 on error. */
static int
_downcast_and_check(Py_ssize_t size, int extra) {
    if (size > BSON_MAX_SIZE || ((BSON_MAX_SIZE - extra) < size)) {
        PyObject* InvalidStringData = _error("InvalidStringData");
        if (InvalidStringData) {
            PyErr_SetString(InvalidStringData,
                            "String length must be <= 2147483647");
            Py_DECREF(InvalidStringData);
        }
        return -1;
    }
    return (int)size + extra;
}

static PyObject* elements_to_dict(PyObject* self, const char* string,
                                  unsigned max, PyObject* as_class,
                                  unsigned char tz_aware,
                                  unsigned char uuid_subtype,
                                  unsigned char compile_re);

static int _write_element_to_buffer(PyObject* self, buffer_t buffer,
                                    int type_byte, PyObject* value,
                                    unsigned char check_keys,
                                    unsigned char uuid_subtype);

/* Date stuff */
static PyObject* datetime_from_millis(long long millis) {
    /* To encode a datetime instance like datetime(9999, 12, 31, 23, 59, 59, 999999)
     * we follow these steps:
     * 1. Calculate a timestamp in seconds:       253402300799
     * 2. Multiply that by 1000:                  253402300799000
     * 3. Add in microseconds divided by 1000     253402300799999
     *
     * (Note: BSON doesn't support microsecond accuracy, hence the rounding.)
     *
     * To decode we could do:
     * 1. Get seconds: timestamp / 1000:          253402300799
     * 2. Get micros: (timestamp % 1000) * 1000:  999000
     * Resulting in datetime(9999, 12, 31, 23, 59, 59, 999000) -- the expected result
     *
     * Now what if the we encode (1, 1, 1, 1, 1, 1, 111111)?
     * 1. and 2. gives:                           -62135593139000
     * 3. Gives us:                               -62135593138889
     *
     * Now decode:
     * 1. Gives us:                               -62135593138
     * 2. Gives us:                               -889000
     * Resulting in datetime(1, 1, 1, 1, 1, 2, 15888216) -- an invalid result
     *
     * If instead to decode we do:
     * diff = ((millis % 1000) + 1000) % 1000:    111
     * seconds = (millis - diff) / 1000:          -62135593139
     * micros = diff * 1000                       111000
     * Resulting in datetime(1, 1, 1, 1, 1, 1, 111000) -- the expected result
     */
    int diff = (int)(((millis % 1000) + 1000) % 1000);
    int microseconds = diff * 1000;
    Time64_T seconds = (millis - diff) / 1000;
    struct TM timeinfo;
    gmtime64_r(&seconds, &timeinfo);

    return PyDateTime_FromDateAndTime(timeinfo.tm_year + 1900,
                                      timeinfo.tm_mon + 1,
                                      timeinfo.tm_mday,
                                      timeinfo.tm_hour,
                                      timeinfo.tm_min,
                                      timeinfo.tm_sec,
                                      microseconds);
}

static long long millis_from_datetime(PyObject* datetime) {
    struct TM timeinfo;
    long long millis;

    timeinfo.tm_year = PyDateTime_GET_YEAR(datetime) - 1900;
    timeinfo.tm_mon = PyDateTime_GET_MONTH(datetime) - 1;
    timeinfo.tm_mday = PyDateTime_GET_DAY(datetime);
    timeinfo.tm_hour = PyDateTime_DATE_GET_HOUR(datetime);
    timeinfo.tm_min = PyDateTime_DATE_GET_MINUTE(datetime);
    timeinfo.tm_sec = PyDateTime_DATE_GET_SECOND(datetime);

    millis = timegm64(&timeinfo) * 1000;
    millis += PyDateTime_DATE_GET_MICROSECOND(datetime) / 1000;
    return millis;
}

/* Just make this compatible w/ the old API. */
int buffer_write_bytes(buffer_t buffer, const char* data, int size) {
    if (buffer_write(buffer, data, size)) {
        PyErr_NoMemory();
        return 0;
    }
    return 1;
}

static int write_unicode(buffer_t buffer, PyObject* py_string) {
    int size;
    const char* data;
    PyObject* encoded = PyUnicode_AsUTF8String(py_string);
    if (!encoded) {
        return 0;
    }
#if PY_MAJOR_VERSION >= 3
    data = PyBytes_AS_STRING(encoded);
#else
    data = PyString_AS_STRING(encoded);
#endif
    if (!data)
        goto unicodefail;

#if PY_MAJOR_VERSION >= 3
    if ((size = _downcast_and_check(PyBytes_GET_SIZE(encoded), 1)) == -1)
#else
    if ((size = _downcast_and_check(PyString_GET_SIZE(encoded), 1)) == -1)
#endif
        goto unicodefail;

    if (!buffer_write_bytes(buffer, (const char*)&size, 4))
        goto unicodefail;

    if (!buffer_write_bytes(buffer, data, size))
        goto unicodefail;

    Py_DECREF(encoded);
    return 1;

unicodefail:
    Py_DECREF(encoded);
    return 0;
}

/* returns 0 on failure */
static int write_string(buffer_t buffer, PyObject* py_string) {
    int size;
    const char* data;
#if PY_MAJOR_VERSION >= 3
    if (PyUnicode_Check(py_string)){
        return write_unicode(buffer, py_string);
    }
    data = PyBytes_AsString(py_string);
#else
    data = PyString_AsString(py_string);
#endif
    if (!data) {
        return 0;
    }

#if PY_MAJOR_VERSION >= 3
    if ((size = _downcast_and_check(PyBytes_Size(py_string), 1)) == -1)
#else
    if ((size = _downcast_and_check(PyString_Size(py_string), 1)) == -1)
#endif
        return 0;

    if (!buffer_write_bytes(buffer, (const char*)&size, 4)) {
        return 0;
    }
    if (!buffer_write_bytes(buffer, data, size)) {
        return 0;
    }
    return 1;
}

/*
 * Are we in the main interpreter or a sub-interpreter?
 * Useful for deciding if we can use cached pure python
 * types in mod_wsgi.
 */
static int
_in_main_interpreter(void) {
    static PyInterpreterState* main_interpreter = NULL;
    PyInterpreterState* interpreter;

    if (main_interpreter == NULL) {
        interpreter = PyInterpreterState_Head();

        while (PyInterpreterState_Next(interpreter))
            interpreter = PyInterpreterState_Next(interpreter);

        main_interpreter = interpreter;
    }

    return (main_interpreter == PyThreadState_Get()->interp);
}

/*
 * Get a reference to a pure python type. If we are in the
 * main interpreter return the cached object, otherwise import
 * the object we need and return it instead.
 */
static PyObject*
_get_object(PyObject* object, char* module_name, char* object_name) {
    if (_in_main_interpreter()) {
        Py_XINCREF(object);
        return object;
    } else {
        PyObject* imported = NULL;
        PyObject* module = PyImport_ImportModule(module_name);
        if (!module)
            return NULL;
        imported = PyObject_GetAttrString(module, object_name);
        Py_DECREF(module);
        return imported;
    }
}

/* Load a Python object to cache.
 *
 * Returns non-zero on failure. */
static int _load_object(PyObject** object, char* module_name, char* object_name) {
    PyObject* module;

    module = PyImport_ImportModule(module_name);
    if (!module) {
        return 1;
    }

    *object = PyObject_GetAttrString(module, object_name);
    Py_DECREF(module);

    return (*object) ? 0 : 2;
}

/* Load all Python objects to cache.
 *
 * Returns non-zero on failure. */
static int _load_python_objects(PyObject* module) {
    PyObject* empty_string;
    PyObject* compiled;
    struct module_state *state = GETSTATE(module);

    if (_load_object(&state->Binary, "bson.binary", "Binary") ||
        _load_object(&state->Code, "bson.code", "Code") ||
        _load_object(&state->ObjectId, "bson.objectid", "ObjectId") ||
        _load_object(&state->DBRef, "bson.dbref", "DBRef") ||
        _load_object(&state->Timestamp, "bson.timestamp", "Timestamp") ||
        _load_object(&state->MinKey, "bson.min_key", "MinKey") ||
        _load_object(&state->MaxKey, "bson.max_key", "MaxKey") ||
        _load_object(&state->UTC, "bson.tz_util", "utc") ||
        _load_object(&state->RECompile, "re", "compile") ||
        _load_object(&state->Regex, "bson.regex", "Regex")) {
        return 1;
    }
    /* If we couldn't import uuid then we must be on 2.4. Just ignore. */
    if (_load_object(&state->UUID, "uuid", "UUID") == 1) {
        state->UUID = NULL;
        PyErr_Clear();
    }
    /* Reload our REType hack too. */
#if PY_MAJOR_VERSION >= 3
    empty_string = PyBytes_FromString("");
#else
    empty_string = PyString_FromString("");
#endif
    if (empty_string == NULL) {
        state->REType = NULL;
        return 1;
    }
    compiled = PyObject_CallFunction(state->RECompile, "O", empty_string);
    if (compiled == NULL) {
        state->REType = NULL;
        Py_DECREF(empty_string);
        return 1;
    }
    Py_INCREF(Py_TYPE(compiled));
    state->REType = Py_TYPE(compiled);
    Py_DECREF(empty_string);
    Py_DECREF(compiled);
    return 0;
}

static int write_element_to_buffer(PyObject* self, buffer_t buffer,
                                   int type_byte, PyObject* value,
                                   unsigned char check_keys,
                                   unsigned char uuid_subtype) {
    int result;
    if(Py_EnterRecursiveCall(" while encoding an object to BSON "))
        return 0;
    result = _write_element_to_buffer(self, buffer, type_byte,
                                      value, check_keys, uuid_subtype);
    Py_LeaveRecursiveCall();
    return result;
}

static void
_fix_java(const char* in, char* out) {
    int i, j;
    for (i = 0, j = 7; i < j; i++, j--) {
        out[i] = in[j];
        out[j] = in[i];
    }
    for (i = 8, j = 15; i < j; i++, j--) {
        out[i] = in[j];
        out[j] = in[i];
    }
}

static void
_set_cannot_encode(PyObject* value) {
    PyObject* InvalidDocument = _error("InvalidDocument");
    if (InvalidDocument) {
        PyObject* repr = PyObject_Repr(value);
        if (repr) {
#if PY_MAJOR_VERSION >= 3
            PyObject* errmsg = PyUnicode_FromString("Cannot encode object: ");
#else
            PyObject* errmsg = PyString_FromString("Cannot encode object: ");
#endif
            if (errmsg) {
#if PY_MAJOR_VERSION >= 3
                PyObject* error = PyUnicode_Concat(errmsg, repr);
                if (error) {
                    PyErr_SetObject(InvalidDocument, error);
                    Py_DECREF(error);
                }
                Py_DECREF(errmsg);
                Py_DECREF(repr);
#else
                PyString_ConcatAndDel(&errmsg, repr);
                if (errmsg) {
                    PyErr_SetObject(InvalidDocument, errmsg);
                    Py_DECREF(errmsg);
                }
#endif
            } else {
                Py_DECREF(repr);
            }
        }
        Py_DECREF(InvalidDocument);
    }
}

/*
 * Encode a builtin Python regular expression or our custom Regex class.
 *
 * Sets exception and returns 0 on failure.
 */
static int _write_regex_to_buffer(
    buffer_t buffer, int type_byte, PyObject* value) {

    PyObject* py_flags;
    PyObject* py_pattern;
    PyObject* encoded_pattern;
    long int_flags;
    char flags[FLAGS_SIZE];
    char check_utf8 = 0;
    const char* pattern_data;
    int pattern_length, flags_length;
    result_t status;

    /*
     * Both the builtin re type and our Regex class have attributes
     * "flags" and "pattern".
     */
    py_flags = PyObject_GetAttrString(value, "flags");
    if (!py_flags) {
        return 0;
    }
#if PY_MAJOR_VERSION >= 3
    int_flags = PyLong_AsLong(py_flags);
#else
    int_flags = PyInt_AsLong(py_flags);
#endif
    Py_DECREF(py_flags);
    py_pattern = PyObject_GetAttrString(value, "pattern");
    if (!py_pattern) {
        return 0;
    }

    if (PyUnicode_Check(py_pattern)) {
        encoded_pattern = PyUnicode_AsUTF8String(py_pattern);
        Py_DECREF(py_pattern);
        if (!encoded_pattern) {
            return 0;
        }
    } else {
        encoded_pattern = py_pattern;
        check_utf8 = 1;
    }

#if PY_MAJOR_VERSION >= 3
    if (!(pattern_data = PyBytes_AsString(encoded_pattern))) {
        Py_DECREF(encoded_pattern);
        return 0;
    }
    if ((pattern_length = _downcast_and_check(PyBytes_Size(encoded_pattern), 0)) == -1) {
        Py_DECREF(encoded_pattern);
        return 0;
    }
#else
    if (!(pattern_data = PyString_AsString(encoded_pattern))) {
        Py_DECREF(encoded_pattern);
        return 0;
    }
    if ((pattern_length = _downcast_and_check(PyString_Size(encoded_pattern), 0)) == -1) {
        Py_DECREF(encoded_pattern);
        return 0;
    }
#endif
    status = check_string((const unsigned char*)pattern_data,
                          pattern_length, check_utf8, 1);
    if (status == NOT_UTF_8) {
        PyObject* InvalidStringData = _error("InvalidStringData");
        if (InvalidStringData) {
            PyErr_SetString(InvalidStringData,
                            "regex patterns must be valid UTF-8");
            Py_DECREF(InvalidStringData);
        }
        Py_DECREF(encoded_pattern);
        return 0;
    } else if (status == HAS_NULL) {
        PyObject* InvalidDocument = _error("InvalidDocument");
        if (InvalidDocument) {
            PyErr_SetString(InvalidDocument,
                            "regex patterns must not contain the NULL byte");
            Py_DECREF(InvalidDocument);
        }
        Py_DECREF(encoded_pattern);
        return 0;
    }

    if (!buffer_write_bytes(buffer, pattern_data, pattern_length + 1)) {
        Py_DECREF(encoded_pattern);
        return 0;
    }
    Py_DECREF(encoded_pattern);

    flags[0] = 0;

    if (int_flags & 2) {
        STRCAT(flags, FLAGS_SIZE, "i");
    }
    if (int_flags & 4) {
        STRCAT(flags, FLAGS_SIZE, "l");
    }
    if (int_flags & 8) {
        STRCAT(flags, FLAGS_SIZE, "m");
    }
    if (int_flags & 16) {
        STRCAT(flags, FLAGS_SIZE, "s");
    }
    if (int_flags & 32) {
        STRCAT(flags, FLAGS_SIZE, "u");
    }
    if (int_flags & 64) {
        STRCAT(flags, FLAGS_SIZE, "x");
    }
    flags_length = (int)strlen(flags) + 1;
    if (!buffer_write_bytes(buffer, flags, flags_length)) {
        return 0;
    }
    *(buffer_get_buffer(buffer) + type_byte) = 0x0B;
    return 1;
}

/* TODO our platform better be little-endian w/ 4-byte ints! */
/* Write a single value to the buffer (also write its type_byte, for which
 * space has already been reserved.
 *
 * returns 0 on failure */
static int _write_element_to_buffer(PyObject* self, buffer_t buffer,
                                    int type_byte, PyObject* value,
                                    unsigned char check_keys,
                                    unsigned char uuid_subtype) {
    struct module_state *state = GETSTATE(self);
    PyObject* type_marker = NULL;

    /*
     * Don't use PyObject_IsInstance for our custom types. It causes
     * problems with python sub interpreters. Our custom types should
     * have a _type_marker attribute, which we can switch on instead.
     */
    if (PyObject_HasAttrString(value, "_type_marker")) {
        type_marker = PyObject_GetAttrString(value, "_type_marker");
        if (type_marker == NULL) {
            return 0;
        }
    }
    /*
     * Python objects with broken __getattr__ implementations could return
     * arbitrary types for a call to PyObject_GetAttrString. For example
     * pymongo.database.Database returns a new Collection instance for
     * __getattr__ calls with names that don't match an existing attribute
     * or method. In some cases "value" could be a subtype of something
     * we know how to serialize. Make a best effort to encode these types.
     */
#if PY_MAJOR_VERSION >= 3
    if (type_marker && PyLong_CheckExact(type_marker)) {
        long type = PyLong_AsLong(type_marker);
#else
    if (type_marker && PyInt_CheckExact(type_marker)) {
        long type = PyInt_AsLong(type_marker);
#endif
        Py_DECREF(type_marker);
        /* 
         * Py(Long|Int)_AsLong returns -1 for error but -1 is a valid value
         * so we call PyErr_Occurred to differentiate.
         */
        if (type == -1 && PyErr_Occurred()) {
            return 0;
        }
        switch (type) {
        case 5:
            {
                /* Binary */
                PyObject* subtype_object;
                long subtype;
                const char* data;
                int size;

                *(buffer_get_buffer(buffer) + type_byte) = 0x05;
                subtype_object = PyObject_GetAttrString(value, "subtype");
                if (!subtype_object) {
                    return 0;
                }
#if PY_MAJOR_VERSION >= 3
                subtype = PyLong_AsLong(subtype_object);
#else
                subtype = PyInt_AsLong(subtype_object);
#endif
                if (subtype == -1) {
                    Py_DECREF(subtype_object);
                    return 0;
                }
#if PY_MAJOR_VERSION >= 3
                size = _downcast_and_check(PyBytes_Size(value), 0);
#else
                size = _downcast_and_check(PyString_Size(value), 0);
#endif
                if (size == -1) {
                    Py_DECREF(subtype_object);
                    return 0;
                }

                Py_DECREF(subtype_object);
                if (subtype == 2) {
#if PY_MAJOR_VERSION >= 3
                    int other_size = _downcast_and_check(PyBytes_Size(value), 4);
#else
                    int other_size = _downcast_and_check(PyString_Size(value), 4);
#endif
                    if (other_size == -1)
                        return 0;
                    if (!buffer_write_bytes(buffer, (const char*)&other_size, 4)) {
                        return 0;
                    }
                    if (!buffer_write_bytes(buffer, (const char*)&subtype, 1)) {
                        return 0;
                    }
                }
                if (!buffer_write_bytes(buffer, (const char*)&size, 4)) {
                    return 0;
                }
                if (subtype != 2) {
                    if (!buffer_write_bytes(buffer, (const char*)&subtype, 1)) {
                        return 0;
                    }
                }
#if PY_MAJOR_VERSION >= 3
                data = PyBytes_AsString(value);
#else
                data = PyString_AsString(value);
#endif
                if (!data) {
                    return 0;
                }
                if (!buffer_write_bytes(buffer, data, size)) {
                        return 0;
                }
                return 1;
            }
        case 7:
            {
                /* ObjectId */
                const char* data;
                PyObject* pystring = PyObject_GetAttrString(value, "_ObjectId__id");
                if (!pystring) {
                    return 0;
                }
#if PY_MAJOR_VERSION >= 3
                data = PyBytes_AsString(pystring);
#else
                data = PyString_AsString(pystring);
#endif
                if (!data) {
                    Py_DECREF(pystring);
                    return 0;
                }
                if (!buffer_write_bytes(buffer, data, 12)) {
                    Py_DECREF(pystring);
                    return 0;
                }
                Py_DECREF(pystring);
                *(buffer_get_buffer(buffer) + type_byte) = 0x07;
                return 1;
            }
        case 11:
            {
                /* Regex */
                return _write_regex_to_buffer(buffer, type_byte, value);
            }
        case 13:
            {
                /* Code */
                int start_position,
                    length_location,
                    length;

                PyObject* scope = PyObject_GetAttrString(value, "scope");
                if (!scope) {
                    return 0;
                }

                if (!PyDict_Size(scope)) {
                    Py_DECREF(scope);
                    *(buffer_get_buffer(buffer) + type_byte) = 0x0D;
                    return write_string(buffer, value);
                }

                *(buffer_get_buffer(buffer) + type_byte) = 0x0F;

                start_position = buffer_get_position(buffer);
                /* save space for length */
                length_location = buffer_save_space(buffer, 4);
                if (length_location == -1) {
                    PyErr_NoMemory();
                    Py_DECREF(scope);
                    return 0;
                }

                if (!write_string(buffer, value)) {
                    Py_DECREF(scope);
                    return 0;
                }

                if (!write_dict(self, buffer, scope, 0, uuid_subtype, 0)) {
                    Py_DECREF(scope);
                    return 0;
                }
                Py_DECREF(scope);

                length = buffer_get_position(buffer) - start_position;
                memcpy(buffer_get_buffer(buffer) + length_location, &length, 4);
                return 1;
            }
        case 17:
            {
                /* Timestamp */
                PyObject* obj;
                long i;

                obj = PyObject_GetAttrString(value, "inc");
                if (!obj) {
                    return 0;
                }
#if PY_MAJOR_VERSION >= 3
                i = PyLong_AsLong(obj);
#else
                i = PyInt_AsLong(obj);
#endif
                Py_DECREF(obj);
                if (!buffer_write_bytes(buffer, (const char*)&i, 4)) {
                    return 0;
                }

                obj = PyObject_GetAttrString(value, "time");
                if (!obj) {
                    return 0;
                }
#if PY_MAJOR_VERSION >= 3
                i = PyLong_AsLong(obj);
#else
                i = PyInt_AsLong(obj);
#endif
                Py_DECREF(obj);
                if (!buffer_write_bytes(buffer, (const char*)&i, 4)) {
                    return 0;
                }

                *(buffer_get_buffer(buffer) + type_byte) = 0x11;
                return 1;
            }
        case 100:
            {
                /* DBRef */
                PyObject* as_doc = PyObject_CallMethod(value, "as_doc", NULL);
                if (!as_doc) {
                    return 0;
                }
                if (!write_dict(self, buffer, as_doc, 0, uuid_subtype, 0)) {
                    Py_DECREF(as_doc);
                    return 0;
                }
                Py_DECREF(as_doc);
                *(buffer_get_buffer(buffer) + type_byte) = 0x03;
                return 1;
            }
        case 255:
            {
                /* MinKey */
                *(buffer_get_buffer(buffer) + type_byte) = 0xFF;
                return 1;
            }
        case 127:
            {
                /* MaxKey */
                *(buffer_get_buffer(buffer) + type_byte) = 0x7F;
                return 1;
            }
        }
    } else {
        Py_XDECREF(type_marker);
    }

    /* No _type_marker attibute or not one of our types. */

    if (PyBool_Check(value)) {
#if PY_MAJOR_VERSION >= 3
        const long bool = PyLong_AsLong(value);
#else
        const long bool = PyInt_AsLong(value);
#endif
        const char c = bool ? 0x01 : 0x00;
        *(buffer_get_buffer(buffer) + type_byte) = 0x08;
        return buffer_write_bytes(buffer, &c, 1);
    }
#if PY_MAJOR_VERSION >= 3
    else if (PyLong_Check(value)) {
        const long long_value = PyLong_AsLong(value);
#else
    else if (PyInt_Check(value)) {
        const long long_value = PyInt_AsLong(value);
#endif

        const int int_value = (int)long_value;
        if (PyErr_Occurred() || long_value != int_value) { /* Overflow */
            long long long_long_value;
            PyErr_Clear();
            long_long_value = PyLong_AsLongLong(value);
            if (PyErr_Occurred()) { /* Overflow AGAIN */
                PyErr_SetString(PyExc_OverflowError,
                                "MongoDB can only handle up to 8-byte ints");
                return 0;
            }
            *(buffer_get_buffer(buffer) + type_byte) = 0x12;
            return buffer_write_bytes(buffer, (const char*)&long_long_value, 8);
        }
        *(buffer_get_buffer(buffer) + type_byte) = 0x10;
        return buffer_write_bytes(buffer, (const char*)&int_value, 4);
#if PY_MAJOR_VERSION < 3
    } else if (PyLong_Check(value)) {
        const long long long_long_value = PyLong_AsLongLong(value);
        if (PyErr_Occurred()) { /* Overflow */
            PyErr_SetString(PyExc_OverflowError,
                            "MongoDB can only handle up to 8-byte ints");
            return 0;
        }
        *(buffer_get_buffer(buffer) + type_byte) = 0x12;
        return buffer_write_bytes(buffer, (const char*)&long_long_value, 8);
#endif
    } else if (PyFloat_Check(value)) {
        const double d = PyFloat_AsDouble(value);
        *(buffer_get_buffer(buffer) + type_byte) = 0x01;
        return buffer_write_bytes(buffer, (const char*)&d, 8);
    } else if (value == Py_None) {
        *(buffer_get_buffer(buffer) + type_byte) = 0x0A;
        return 1;
    } else if (PyDict_Check(value)) {
        *(buffer_get_buffer(buffer) + type_byte) = 0x03;
        return write_dict(self, buffer, value, check_keys, uuid_subtype, 0);
    } else if (PyList_Check(value) || PyTuple_Check(value)) {
        Py_ssize_t items, i;
        int start_position,
            length_location,
            length;
        char zero = 0;

        *(buffer_get_buffer(buffer) + type_byte) = 0x04;
        start_position = buffer_get_position(buffer);

        /* save space for length */
        length_location = buffer_save_space(buffer, 4);
        if (length_location == -1) {
            PyErr_NoMemory();
            return 0;
        }

        if ((items = PySequence_Size(value)) > BSON_MAX_SIZE) {
            PyObject* BSONError = _error("BSONError");
            if (BSONError) {
                PyErr_SetString(BSONError,
                                "Too many items to serialize.");
                Py_DECREF(BSONError);
            }
            return 0;
        }
        for(i = 0; i < items; i++) {
            int list_type_byte = buffer_save_space(buffer, 1);
            char name[16];
            PyObject* item_value;

            if (list_type_byte == -1) {
                PyErr_NoMemory();
                return 0;
            }
            INT2STRING(name, (int)i);
            if (!buffer_write_bytes(buffer, name, (int)strlen(name) + 1)) {
                return 0;
            }

            if (!(item_value = PySequence_GetItem(value, i)))
                return 0;
            if (!write_element_to_buffer(self, buffer, list_type_byte,
                                         item_value, check_keys, uuid_subtype)) {
                Py_DECREF(item_value);
                return 0;
            }
            Py_DECREF(item_value);
        }

        /* write null byte and fill in length */
        if (!buffer_write_bytes(buffer, &zero, 1)) {
            return 0;
        }
        length = buffer_get_position(buffer) - start_position;
        memcpy(buffer_get_buffer(buffer) + length_location, &length, 4);
        return 1;
#if PY_MAJOR_VERSION >= 3
    /* Python3 special case. Store bytes as BSON binary subtype 0. */
    } else if (PyBytes_Check(value)) {
        int subtype = 0;
        int size;
        const char* data = PyBytes_AS_STRING(value);
        if (!data)
            return 0;
        if ((size = _downcast_and_check(PyBytes_GET_SIZE(value), 0)) == -1)
            return 0;
        *(buffer_get_buffer(buffer) + type_byte) = 0x05;
        if (!buffer_write_bytes(buffer, (const char*)&size, 4)) {
            return 0;
        }
        if (!buffer_write_bytes(buffer, (const char*)&subtype, 1)) {
            return 0;
        }
        if (!buffer_write_bytes(buffer, data, size)) {
            return 0;
        }
        return 1;
#else
    /* PyString_Check only works in Python 2.x. */
    } else if (PyString_Check(value)) {
        result_t status;
        const char* data;
        int size;
        if (!(data = PyString_AS_STRING(value)))
            return 0;
        if ((size = _downcast_and_check(PyString_GET_SIZE(value), 1)) == -1)
            return 0;
        *(buffer_get_buffer(buffer) + type_byte) = 0x02;
        status = check_string((const unsigned char*)data, size - 1, 1, 0);

        if (status == NOT_UTF_8) {
            PyObject* InvalidStringData = _error("InvalidStringData");
            if (InvalidStringData) {
                PyObject* repr = PyObject_Repr(value);
                char* repr_as_cstr = repr ? PyString_AsString(repr) : NULL;
                if (repr_as_cstr) {
                    PyObject *message = PyString_FromFormat(
                        "strings in documents must be valid UTF-8: %s",
                        repr_as_cstr);

                    if (message) {
                        PyErr_SetObject(InvalidStringData, message);
                        Py_DECREF(message);
                    }
                } else {
                    /* repr(value) failed, use a generic message. */
                    PyErr_SetString(
                        InvalidStringData,
                        "strings in documents must be valid UTF-8");
                }
                Py_XDECREF(repr);
                Py_DECREF(InvalidStringData);
            }
            return 0;
        }
        if (!buffer_write_bytes(buffer, (const char*)&size, 4)) {
            return 0;
        }
        if (!buffer_write_bytes(buffer, data, size)) {
            return 0;
        }
        return 1;
#endif
    } else if (PyUnicode_Check(value)) {
        *(buffer_get_buffer(buffer) + type_byte) = 0x02;
        return write_unicode(buffer, value);
    } else if (PyDateTime_Check(value)) {
        long long millis;
        PyObject* utcoffset = PyObject_CallMethod(value, "utcoffset", NULL);
        if (utcoffset == NULL)
            return 0;
        if (utcoffset != Py_None) {
            PyObject* result = PyNumber_Subtract(value, utcoffset);
            Py_DECREF(utcoffset);
            if (!result) {
                return 0;
            }
            millis = millis_from_datetime(result);
            Py_DECREF(result);
        } else {
            millis = millis_from_datetime(value);
        }
        *(buffer_get_buffer(buffer) + type_byte) = 0x09;
        return buffer_write_bytes(buffer, (const char*)&millis, 8);
    } else if (PyObject_TypeCheck(value, state->REType)) {
        return _write_regex_to_buffer(buffer, type_byte, value);
    }
    
    /* 
     * Try UUID last since we have to import
     * it if we're in a sub-interpreter.
     *
     * If we're running under python 2.4 there likely
     * isn't a uuid module.
     */
    if (state->UUID) {
        PyObject* uuid_type = _get_object(state->UUID, "uuid", "UUID");
        if (uuid_type && PyObject_IsInstance(value, uuid_type)) {
            /* Just a special case of Binary above, but
             * simpler to do as a separate case. */
            PyObject* bytes;
            /* Could be bytes, bytearray, str... */
            const char* data;
            /* UUID is always 16 bytes */
            int size = 16;
            int subtype;

            Py_DECREF(uuid_type);

            if (uuid_subtype == JAVA_LEGACY || uuid_subtype == CSHARP_LEGACY) {
                subtype = 3;
            }
            else {
                subtype = uuid_subtype;
            }

            *(buffer_get_buffer(buffer) + type_byte) = 0x05;
            if (!buffer_write_bytes(buffer, (const char*)&size, 4)) {
                return 0;
            }
            if (!buffer_write_bytes(buffer, (const char*)&subtype, 1)) {
                return 0;
            }

            if (uuid_subtype == CSHARP_LEGACY) {
               /* Legacy C# byte order */
                bytes = PyObject_GetAttrString(value, "bytes_le");
            }
            else {
                bytes = PyObject_GetAttrString(value, "bytes");
            }
            if (!bytes) {
                return 0;
            }
#if PY_MAJOR_VERSION >= 3
            /* Work around http://bugs.python.org/issue7380 */
            if (PyByteArray_Check(bytes)) {
                data = PyByteArray_AsString(bytes);
            }
            else {
                data = PyBytes_AsString(bytes);
            }
#else
            data = PyString_AsString(bytes);
#endif
            if (data == NULL) {
                Py_DECREF(bytes);
                return 0;
            }
            if (uuid_subtype == JAVA_LEGACY) {
                /* Store in legacy java byte order. */
                char as_legacy_java[16];
                _fix_java(data, as_legacy_java);
                if (!buffer_write_bytes(buffer, as_legacy_java, size)) {
                    Py_DECREF(bytes);
                    return 0;
                }
            }
            else {
                if (!buffer_write_bytes(buffer, data, size)) {
                    Py_DECREF(bytes);
                    return 0;
                }
            }
            Py_DECREF(bytes);
            return 1;
        } else {
            Py_XDECREF(uuid_type);
        }
    }
    /* We can't determine value's type. Fail. */
    _set_cannot_encode(value);
    return 0;
}

static int check_key_name(const char* name, int name_length) {

    if (name_length > 0 && name[0] == '$') {
        PyObject* InvalidDocument = _error("InvalidDocument");
        if (InvalidDocument) {
#if PY_MAJOR_VERSION >= 3
            PyObject* errmsg = PyUnicode_FromFormat(
                    "key '%s' must not start with '$'", name);
#else
            PyObject* errmsg = PyString_FromFormat(
                    "key '%s' must not start with '$'", name);
#endif
            if (errmsg) {
                PyErr_SetObject(InvalidDocument, errmsg);
                Py_DECREF(errmsg);
            }
            Py_DECREF(InvalidDocument);
        }
        return 0;
    }
    if (strchr(name, '.')) {
        PyObject* InvalidDocument = _error("InvalidDocument");
        if (InvalidDocument) {
#if PY_MAJOR_VERSION >= 3
            PyObject* errmsg = PyUnicode_FromFormat(
                    "key '%s' must not contain '.'", name);
#else
            PyObject* errmsg = PyString_FromFormat(
                    "key '%s' must not contain '.'", name);
#endif
            if (errmsg) {
                PyErr_SetObject(InvalidDocument, errmsg);
                Py_DECREF(errmsg);
            }
            Py_DECREF(InvalidDocument);
        }
        return 0;
    }
    return 1;
}

/* Write a (key, value) pair to the buffer.
 *
 * Returns 0 on failure */
int write_pair(PyObject* self, buffer_t buffer, const char* name, int name_length,
               PyObject* value, unsigned char check_keys,
               unsigned char uuid_subtype, unsigned char allow_id) {
    int type_byte;

    /* Don't write any _id elements unless we're explicitly told to -
     * _id has to be written first so we do so, but don't bother
     * deleting it from the dictionary being written. */
    if (!allow_id && strcmp(name, "_id") == 0) {
        return 1;
    }

    type_byte = buffer_save_space(buffer, 1);
    if (type_byte == -1) {
        PyErr_NoMemory();
        return 0;
    }
    if (check_keys && !check_key_name(name, name_length)) {
        return 0;
    }
    if (!buffer_write_bytes(buffer, name, name_length + 1)) {
        return 0;
    }
    if (!write_element_to_buffer(self, buffer, type_byte,
                                 value, check_keys, uuid_subtype)) {
        return 0;
    }
    return 1;
}

int decode_and_write_pair(PyObject* self, buffer_t buffer,
                          PyObject* key, PyObject* value,
                          unsigned char check_keys,
                          unsigned char uuid_subtype, unsigned char top_level) {
    PyObject* encoded;
    const char* data;
    int size;
    if (PyUnicode_Check(key)) {
        encoded = PyUnicode_AsUTF8String(key);
        if (!encoded) {
            return 0;
        }
#if PY_MAJOR_VERSION >= 3
        if (!(data = PyBytes_AS_STRING(encoded))) {
            Py_DECREF(encoded);
            return 0;
        }
        if ((size = _downcast_and_check(PyBytes_GET_SIZE(encoded), 1)) == -1) {
            Py_DECREF(encoded);
            return 0;
        }
#else
        if (!(data = PyString_AS_STRING(encoded))) {
            Py_DECREF(encoded);
            return 0;
        }
        if ((size = _downcast_and_check(PyString_GET_SIZE(encoded), 1)) == -1) {
            Py_DECREF(encoded);
            return 0;
        }
#endif
        if (strlen(data) != (size_t)(size - 1)) {
            PyObject* InvalidDocument = _error("InvalidDocument");
            if (InvalidDocument) {
                PyErr_SetString(InvalidDocument,
                                "Key names must not contain the NULL byte");
                Py_DECREF(InvalidDocument);
            }
            Py_DECREF(encoded);
            return 0;
        }
#if PY_MAJOR_VERSION < 3
    } else if (PyString_Check(key)) {
        result_t status;
        encoded = key;
        Py_INCREF(encoded);

        if (!(data = PyString_AS_STRING(encoded))) {
            Py_DECREF(encoded);
            return 0;
        }
        if ((size = _downcast_and_check(PyString_GET_SIZE(encoded), 1)) == -1) {
            Py_DECREF(encoded);
            return 0;
        }
        status = check_string((const unsigned char*)data, size - 1, 1, 1);

        if (status == NOT_UTF_8) {
            PyObject* InvalidStringData = _error("InvalidStringData");
            if (InvalidStringData) {
                PyErr_SetString(InvalidStringData,
                                "strings in documents must be valid UTF-8");
                Py_DECREF(InvalidStringData);
            }
            Py_DECREF(encoded);
            return 0;
        } else if (status == HAS_NULL) {
            PyObject* InvalidDocument = _error("InvalidDocument");
            if (InvalidDocument) {
                PyErr_SetString(InvalidDocument,
                                "Key names must not contain the NULL byte");
                Py_DECREF(InvalidDocument);
            }
            Py_DECREF(encoded);
            return 0;
        }
#endif
    } else {
        PyObject* InvalidDocument = _error("InvalidDocument");
        if (InvalidDocument) {
            PyObject* repr = PyObject_Repr(key);
            if (repr) {
#if PY_MAJOR_VERSION >= 3
                PyObject* errmsg = PyUnicode_FromString(
                        "documents must have only string keys, key was ");
#else
                PyObject* errmsg = PyString_FromString(
                        "documents must have only string keys, key was ");
#endif
                if (errmsg) {
#if PY_MAJOR_VERSION >= 3
                    PyObject* error = PyUnicode_Concat(errmsg, repr);
                    if (error) {
                        PyErr_SetObject(InvalidDocument, error);
                        Py_DECREF(error);
                    }
                    Py_DECREF(errmsg);
                    Py_DECREF(repr);
#else
                    PyString_ConcatAndDel(&errmsg, repr);
                    if (errmsg) {
                        PyErr_SetObject(InvalidDocument, errmsg);
                        Py_DECREF(errmsg);
                    }
#endif
                } else {
                    Py_DECREF(repr);
                }
            }
            Py_DECREF(InvalidDocument);
        }
        return 0;
    }

    /* If top_level is True, don't allow writing _id here - it was already written. */
    if (!write_pair(self, buffer, data,
                    size - 1, value, check_keys, uuid_subtype, !top_level)) {
        Py_DECREF(encoded);
        return 0;
    }

    Py_DECREF(encoded);
    return 1;
}

/* returns 0 on failure */
int write_dict(PyObject* self, buffer_t buffer,
               PyObject* dict, unsigned char check_keys,
               unsigned char uuid_subtype, unsigned char top_level) {
    PyObject* key;
    PyObject* iter;
    char zero = 0;
    int length;
    int length_location;

    if (!PyDict_Check(dict)) {
        PyObject* repr = PyObject_Repr(dict);
        if (repr) {
#if PY_MAJOR_VERSION >= 3
            PyObject* errmsg = PyUnicode_FromString(
                "encoder expected a mapping type but got: ");
            if (errmsg) {
                PyObject* error = PyUnicode_Concat(errmsg, repr);
                if (error) {
                    PyErr_SetObject(PyExc_TypeError, error);
                    Py_DECREF(error);
                }
                Py_DECREF(errmsg);
                Py_DECREF(repr);
            }
#else
            PyObject* errmsg = PyString_FromString(
                "encoder expected a mapping type but got: ");
            if (errmsg) {
                PyString_ConcatAndDel(&errmsg, repr);
                if (errmsg) {
                    PyErr_SetObject(PyExc_TypeError, errmsg);
                    Py_DECREF(errmsg);
                }
            }
#endif
            else {
                Py_DECREF(repr);
            }
        } else {
            PyErr_SetString(PyExc_TypeError,
                            "encoder expected a mapping type");
        }
        return 0;
    }

    length_location = buffer_save_space(buffer, 4);
    if (length_location == -1) {
        PyErr_NoMemory();
        return 0;
    }

    /* Write _id first if this is a top level doc. */
    if (top_level) {
        PyObject* _id = PyDict_GetItemString(dict, "_id");
        if (_id) {
            if (!write_pair(self, buffer, "_id", 3,
                            _id, check_keys, uuid_subtype, 1)) {
                return 0;
            }
        }
    }

    iter = PyObject_GetIter(dict);
    if (iter == NULL) {
        return 0;
    }
    while ((key = PyIter_Next(iter)) != NULL) {
        PyObject* value = PyDict_GetItem(dict, key);
        if (!value) {
            PyErr_SetObject(PyExc_KeyError, key);
            Py_DECREF(key);
            Py_DECREF(iter);
            return 0;
        }
        if (!decode_and_write_pair(self, buffer, key, value,
                                   check_keys, uuid_subtype, top_level)) {
            Py_DECREF(key);
            Py_DECREF(iter);
            return 0;
        }
        Py_DECREF(key);
    }
    Py_DECREF(iter);

    /* write null byte and fill in length */
    if (!buffer_write_bytes(buffer, &zero, 1)) {
        return 0;
    }
    length = buffer_get_position(buffer) - length_location;
    memcpy(buffer_get_buffer(buffer) + length_location, &length, 4);
    return 1;
}

static PyObject* _cbson_dict_to_bson(PyObject* self, PyObject* args) {
    PyObject* dict;
    PyObject* result;
    unsigned char check_keys;
    unsigned char uuid_subtype;
    unsigned char top_level = 1;
    buffer_t buffer;

    if (!PyArg_ParseTuple(args, "Obb|b", &dict,
                          &check_keys, &uuid_subtype, &top_level)) {
        return NULL;
    }

    buffer = buffer_new();
    if (!buffer) {
        PyErr_NoMemory();
        return NULL;
    }

    if (!write_dict(self, buffer, dict, check_keys, uuid_subtype, top_level)) {
        buffer_free(buffer);
        return NULL;
    }

    /* objectify buffer */
#if PY_MAJOR_VERSION >= 3
    result = Py_BuildValue("y#", buffer_get_buffer(buffer),
                           buffer_get_position(buffer));
#else
    result = Py_BuildValue("s#", buffer_get_buffer(buffer),
                           buffer_get_position(buffer));
#endif
    buffer_free(buffer);
    return result;
}

static PyObject* get_value(PyObject* self, const char* buffer, unsigned* position,
                           unsigned char type, unsigned max, PyObject* as_class,
                           unsigned char tz_aware, unsigned char uuid_subtype,
                           unsigned char compile_re) {
    struct module_state *state = GETSTATE(self);

    PyObject* value = NULL;
    switch (type) {
    case 1:
        {
            double d;
            if (max < 8) {
                goto invalid;
            }
            memcpy(&d, buffer + *position, 8);
            value = PyFloat_FromDouble(d);
            *position += 8;
            break;
        }
    case 2:
    case 14:
        {
            unsigned value_length;
            if (max < 4) {
                goto invalid;
            }
            memcpy(&value_length, buffer + *position, 4);
            /* Encoded string length + string */
            if (!value_length || max < value_length || max < 4 + value_length) {
                goto invalid;
            }
            *position += 4;
            /* Strings must end in \0 */
            if (buffer[*position + value_length - 1]) {
                goto invalid;
            }
            value = PyUnicode_DecodeUTF8(buffer + *position, value_length - 1, "strict");
            if (!value) {
                goto invalid;
            }
            *position += value_length;
            break;
        }
    case 3:
        {
            PyObject* collection;
            unsigned size;
            if (max < 4) {
                goto invalid;
            }
            memcpy(&size, buffer + *position, 4);
            if (size < BSON_MIN_SIZE || max < size) {
                goto invalid;
            }
            /* Check for bad eoo */
            if (buffer[*position + size - 1]) {
                goto invalid;
            }
            value = elements_to_dict(self, buffer + *position + 4,
                                     size - 5, as_class, tz_aware, uuid_subtype,
                                     compile_re);
            if (!value) {
                goto invalid;
            }

            /* Decoding for DBRefs */
            collection = PyDict_GetItemString(value, "$ref");
            if (collection) { /* DBRef */
                PyObject* dbref = NULL;
                PyObject* dbref_type;
                PyObject* id;
                PyObject* database;

                Py_INCREF(collection);
                PyDict_DelItemString(value, "$ref");

                id = PyDict_GetItemString(value, "$id");
                if (id == NULL) {
                    id = Py_None;
                    Py_INCREF(id);
                } else {
                    Py_INCREF(id);
                    PyDict_DelItemString(value, "$id");
                }

                database = PyDict_GetItemString(value, "$db");
                if (database == NULL) {
                    database = Py_None;
                    Py_INCREF(database);
                } else {
                    Py_INCREF(database);
                    PyDict_DelItemString(value, "$db");
                }

                if ((dbref_type = _get_object(state->DBRef, "bson.dbref", "DBRef"))) {
                    dbref = PyObject_CallFunctionObjArgs(dbref_type, collection, id, database, value, NULL);
                    Py_DECREF(dbref_type);
                }
                Py_DECREF(value);
                value = dbref;

                Py_DECREF(id);
                Py_DECREF(collection);
                Py_DECREF(database);
            }

            *position += size;
            break;
        }
    case 4:
        {
            unsigned size, end;

            if (max < 4) {
                goto invalid;
            }
            memcpy(&size, buffer + *position, 4);
            if (size < BSON_MIN_SIZE || max < size) {
                goto invalid;
            }
            end = *position + size - 1;
            /* Check for bad eoo */
            if (buffer[end]) {
                goto invalid;
            }
            *position += 4;

            value = PyList_New(0);
            if (!value) {
                goto invalid;
            }
            while (*position < end) {
                PyObject* to_append;

                unsigned char bson_type = (unsigned char)buffer[(*position)++];

                size_t key_size = strlen(buffer + *position);
                if (max < key_size) {
                    Py_DECREF(value);
                    goto invalid;
                }
                /* just skip the key, they're in order. */
                *position += (unsigned)key_size + 1;
                if (Py_EnterRecursiveCall(" while decoding a list value")) {
                    Py_DECREF(value);
                    goto invalid;
                }
                to_append = get_value(self, buffer, position, bson_type,
                                      max - (unsigned)key_size,
                                      as_class, tz_aware, uuid_subtype,
                                      compile_re);
                Py_LeaveRecursiveCall();
                if (!to_append) {
                    Py_DECREF(value);
                    goto invalid;
                }
                PyList_Append(value, to_append);
                Py_DECREF(to_append);
            }
            (*position)++;
            break;
        }
    case 5:
        {
            PyObject* data;
            PyObject* st;
            PyObject* type_to_create;
            unsigned length;
            unsigned char subtype;

            if (max < 5) {
                goto invalid;
            }
            memcpy(&length, buffer + *position, 4);
            if (max < length) {
                goto invalid;
            }

            subtype = (unsigned char)buffer[*position + 4];
            *position += 5;
            if (subtype == 2 && length < 4) {
                goto invalid;
            }
#if PY_MAJOR_VERSION >= 3
            /* Python3 special case. Decode BSON binary subtype 0 to bytes. */
            if (subtype == 0) {
                value = PyBytes_FromStringAndSize(buffer + *position, length);
                *position += length;
                break;
            }
            if (subtype == 2) {
                data = PyBytes_FromStringAndSize(buffer + *position + 4, length - 4);
            } else {
                data = PyBytes_FromStringAndSize(buffer + *position, length);
            }
#else
            if (subtype == 2) {
                data = PyString_FromStringAndSize(buffer + *position + 4, length - 4);
            } else {
                data = PyString_FromStringAndSize(buffer + *position, length);
            }
#endif
            if (!data) {
                goto invalid;
            }
            /* Encode as UUID, not Binary */
            if ((subtype == 3 || subtype == 4) && state->UUID) {
                PyObject* kwargs;
                PyObject* args = PyTuple_New(0);
                /* UUID should always be 16 bytes */
                if (!args || length != 16) {
                    Py_DECREF(data);
                    goto invalid;
                }
                kwargs = PyDict_New();
                if (!kwargs) {
                    Py_DECREF(data);
                    Py_DECREF(args);
                    goto invalid;
                }

                /*
                 * From this point, we hold refs to args, kwargs, and data.
                 * If anything fails, goto uuiderror to clean them up.
                 */
                if (uuid_subtype == CSHARP_LEGACY) {
                    /* Legacy C# byte order */
                    if ((PyDict_SetItemString(kwargs, "bytes_le", data)) == -1)
                        goto uuiderror;
                }
                else {
                    if (uuid_subtype == JAVA_LEGACY) {
                        /* Convert from legacy java byte order */
                        char big_endian[16];
                        _fix_java(buffer + *position, big_endian);
                        /* Free the previously created PyString object */
                        Py_DECREF(data);
#if PY_MAJOR_VERSION >= 3
                        data = PyBytes_FromStringAndSize(big_endian, length);
#else
                        data = PyString_FromStringAndSize(big_endian, length);
#endif
                        if (data == NULL)
                            goto uuiderror;
                    }
                    if ((PyDict_SetItemString(kwargs, "bytes", data)) == -1)
                        goto uuiderror;
                
                }
                if ((type_to_create = _get_object(state->UUID, "uuid", "UUID"))) {
                    value = PyObject_Call(type_to_create, args, kwargs);
                    Py_DECREF(type_to_create);
                }

                Py_DECREF(args);
                Py_DECREF(kwargs);
                Py_DECREF(data);
                if (!value) {
                    goto invalid;
                }

                *position += length;
                break;

            uuiderror:
                Py_DECREF(args);
                Py_DECREF(kwargs);
                Py_XDECREF(data);
                goto invalid;
            }

#if PY_MAJOR_VERSION >= 3
            st = PyLong_FromLong(subtype);
#else
            st = PyInt_FromLong(subtype);
#endif
            if (!st) {
                Py_DECREF(data);
                goto invalid;
            }
            if ((type_to_create = _get_object(state->Binary, "bson.binary", "Binary"))) {
                value = PyObject_CallFunctionObjArgs(type_to_create, data, st, NULL);
                Py_DECREF(type_to_create);
            }
            Py_DECREF(st);
            Py_DECREF(data);
            if (!value) {
                goto invalid;
            }
            *position += length;
            break;
        }
    case 6:
    case 10:
        {
            value = Py_None;
            Py_INCREF(value);
            break;
        }
    case 7:
        {
            PyObject* objectid_type;
            if (max < 12) {
                goto invalid;
            }
            if ((objectid_type = _get_object(state->ObjectId, "bson.objectid", "ObjectId"))) {
#if PY_MAJOR_VERSION >= 3
                value = PyObject_CallFunction(objectid_type, "y#", buffer + *position, 12);
#else
                value = PyObject_CallFunction(objectid_type, "s#", buffer + *position, 12);
#endif
                Py_DECREF(objectid_type);
            }
            *position += 12;
            break;
        }
    case 8:
        {
            value = buffer[(*position)++] ? Py_True : Py_False;
            Py_INCREF(value);
            break;
        }
    case 9:
        {
            PyObject* utc_type;
            PyObject* naive;
            PyObject* replace;
            PyObject* args;
            PyObject* kwargs;
            long long millis;
            if (max < 8) {
                goto invalid;
            }
            memcpy(&millis, buffer + *position, 8);
            naive = datetime_from_millis(millis);
            *position += 8;
            if (!tz_aware) { /* In the naive case, we're done here. */
                value = naive;
                break;
            }

            if (!naive) {
                goto invalid;
            }
            replace = PyObject_GetAttrString(naive, "replace");
            Py_DECREF(naive);
            if (!replace) {
                goto invalid;
            }
            args = PyTuple_New(0);
            if (!args) {
                Py_DECREF(replace);
                goto invalid;
            }
            kwargs = PyDict_New();
            if (!kwargs) {
                Py_DECREF(replace);
                Py_DECREF(args);
                goto invalid;
            }
            utc_type = _get_object(state->UTC, "bson.tz_util", "utc");
            if (!utc_type || PyDict_SetItemString(kwargs, "tzinfo", utc_type) == -1) {
                Py_DECREF(replace);
                Py_DECREF(args);
                Py_DECREF(kwargs);
                Py_XDECREF(utc_type);
                goto invalid;
            }
            Py_XDECREF(utc_type);
            value = PyObject_Call(replace, args, kwargs);
            Py_DECREF(replace);
            Py_DECREF(args);
            Py_DECREF(kwargs);
            break;
        }
    case 11:
        {
            PyObject* compile_func;
            PyObject* pattern;
            int flags;
            size_t flags_length, i;
            size_t pattern_length = strlen(buffer + *position);
            if (pattern_length > BSON_MAX_SIZE || max < pattern_length) {
                goto invalid;
            }
            pattern = PyUnicode_DecodeUTF8(buffer + *position, pattern_length, "strict");
            if (!pattern) {
                goto invalid;
            }
            *position += (unsigned)pattern_length + 1;
            flags_length = strlen(buffer + *position);
            if (flags_length > BSON_MAX_SIZE ||
                    (BSON_MAX_SIZE - pattern_length) < flags_length) {
                Py_DECREF(pattern);
                goto invalid;
            }
            if (max < pattern_length + flags_length) {
                Py_DECREF(pattern);
                goto invalid;
            }
            flags = 0;
            for (i = 0; i < flags_length; i++) {
                if (buffer[*position + i] == 'i') {
                    flags |= 2;
                } else if (buffer[*position + i] == 'l') {
                    flags |= 4;
                } else if (buffer[*position + i] == 'm') {
                    flags |= 8;
                } else if (buffer[*position + i] == 's') {
                    flags |= 16;
                } else if (buffer[*position + i] == 'u') {
                    flags |= 32;
                } else if (buffer[*position + i] == 'x') {
                    flags |= 64;
                }
            }
            *position += (unsigned)flags_length + 1;

            /*
             * Use re.compile() if we're configured to compile regular
             * expressions, else create an instance of our Regex class.
             */
            if (compile_re) {
                compile_func = _get_object(state->RECompile, "re", "compile");
            } else {
                compile_func = _get_object(state->Regex, "bson.regex", "Regex");
            }

            if (compile_func) {
                value = PyObject_CallFunction(compile_func, "Oi", pattern, flags);
                Py_DECREF(compile_func);
            }
            Py_DECREF(pattern);
            break;
        }
    case 12:
        {
            unsigned coll_length;
            PyObject* collection;
            PyObject* id = NULL;
            PyObject* objectid_type;
            PyObject* dbref_type;

            if (max < 4) {
                goto invalid;
            }
            memcpy(&coll_length, buffer + *position, 4);
            /* Encoded string length + string + 12 byte ObjectId */
            if (!coll_length || max < coll_length || max < 4 + coll_length + 12) {
                goto invalid;
            }
            *position += 4;
            /* Strings must end in \0 */
            if (buffer[*position + coll_length - 1]) {
                goto invalid;
            }

            collection = PyUnicode_DecodeUTF8(buffer + *position,
                                              coll_length - 1, "strict");
            if (!collection) {
                goto invalid;
            }
            *position += coll_length;

            if ((objectid_type = _get_object(state->ObjectId, "bson.objectid", "ObjectId"))) {
#if PY_MAJOR_VERSION >= 3
                id = PyObject_CallFunction(objectid_type, "y#", buffer + *position, 12);
#else
                id = PyObject_CallFunction(objectid_type, "s#", buffer + *position, 12);
#endif
                Py_DECREF(objectid_type);
            }
            if (!id) {
                Py_DECREF(collection);
                goto invalid;
            }
            *position += 12;
            if ((dbref_type = _get_object(state->DBRef, "bson.dbref", "DBRef"))) {
                value = PyObject_CallFunctionObjArgs(dbref_type, collection, id, NULL);
                Py_DECREF(dbref_type);
            }
            Py_DECREF(collection);
            Py_DECREF(id);
            break;
        }
    case 13:
        {
            PyObject* code;
            PyObject* code_type;
            unsigned value_length;
            if (max < 4) {
                goto invalid;
            }
            memcpy(&value_length, buffer + *position, 4);
            /* Encoded string length + string */
            if (!value_length || max < value_length || max < 4 + value_length) {
                goto invalid;
            }
            *position += 4;
            /* Strings must end in \0 */
            if (buffer[*position + value_length - 1]) {
                goto invalid;
            }
            code = PyUnicode_DecodeUTF8(buffer + *position, value_length - 1, "strict");
            if (!code) {
                goto invalid;
            }
            *position += value_length;
            if ((code_type = _get_object(state->Code, "bson.code", "Code"))) {
                value = PyObject_CallFunctionObjArgs(code_type, code, NULL, NULL);
                Py_DECREF(code_type);
            }
            Py_DECREF(code);
            break;
        }
    case 15:
        {
            unsigned c_w_s_size;
            unsigned code_size;
            unsigned scope_size;
            PyObject* code;
            PyObject* scope;
            PyObject* code_type;

            if (max < 8) {
                goto invalid;
            }

            memcpy(&c_w_s_size, buffer + *position, 4);
            *position += 4;

            if (max < c_w_s_size) {
                goto invalid;
            }

            memcpy(&code_size, buffer + *position, 4);
            /* code_w_scope length + code length + code + scope length */
            if (!code_size || max < code_size || max < 4 + 4 + code_size + 4) {
                goto invalid;
            }
            *position += 4;
            /* Strings must end in \0 */
            if (buffer[*position + code_size - 1]) {
                goto invalid;
            }
            code = PyUnicode_DecodeUTF8(buffer + *position, code_size - 1, "strict");
            if (!code) {
                goto invalid;
            }
            *position += code_size;

            memcpy(&scope_size, buffer + *position, 4);
            if (scope_size < BSON_MIN_SIZE) {
                Py_DECREF(code);
                goto invalid;
            }
            /* code length + code + scope length + scope */
            if ((4 + code_size + 4 + scope_size) != c_w_s_size) {
                Py_DECREF(code);
                goto invalid;
            }

            /* Check for bad eoo */
            if (buffer[*position + scope_size - 1]) {
                goto invalid;
            }
            scope = elements_to_dict(self, buffer + *position + 4,
                                     scope_size - 5, (PyObject*)&PyDict_Type,
                                     tz_aware, uuid_subtype, compile_re);
            if (!scope) {
                Py_DECREF(code);
                goto invalid;
            }
            *position += scope_size;

            if ((code_type = _get_object(state->Code, "bson.code", "Code"))) {
                value = PyObject_CallFunctionObjArgs(code_type, code, scope, NULL);
                Py_DECREF(code_type);
            }
            Py_DECREF(code);
            Py_DECREF(scope);
            break;
        }
    case 16:
        {
            int i;
            if (max < 4) {
                goto invalid;
            }
            memcpy(&i, buffer + *position, 4);
#if PY_MAJOR_VERSION >= 3
            value = PyLong_FromLong(i);
#else
            value = PyInt_FromLong(i);
#endif
            if (!value) {
                goto invalid;
            }
            *position += 4;
            break;
        }
    case 17:
        {
            unsigned int time, inc;
            PyObject* timestamp_type;
            if (max < 8) {
                goto invalid;
            }
            memcpy(&inc, buffer + *position, 4);
            memcpy(&time, buffer + *position + 4, 4);
            if ((timestamp_type = _get_object(state->Timestamp, "bson.timestamp", "Timestamp"))) {
                value = PyObject_CallFunction(timestamp_type, "II", time, inc);
                Py_DECREF(timestamp_type);
            }
            *position += 8;
            break;
        }
    case 18:
        {
            long long ll;
            if (max < 8) {
                goto invalid;
            }
            memcpy(&ll, buffer + *position, 8);
            value = PyLong_FromLongLong(ll);
            if (!value) {
                goto invalid;
            }
            *position += 8;
            break;
        }
    case 255:
        {
            PyObject* minkey_type = _get_object(state->MinKey, "bson.min_key", "MinKey");
            if (!minkey_type)
                goto invalid;
            value = PyObject_CallFunctionObjArgs(minkey_type, NULL);
            Py_DECREF(minkey_type);
            break;
        }
    case 127:
        {
            PyObject* maxkey_type = _get_object(state->MaxKey, "bson.max_key", "MaxKey");
            if (!maxkey_type)
                goto invalid;
            value = PyObject_CallFunctionObjArgs(maxkey_type, NULL);
            Py_DECREF(maxkey_type);
            break;
        }
    default:
        {
            PyObject* InvalidDocument = _error("InvalidDocument");
            if (InvalidDocument) {
                PyErr_SetString(InvalidDocument,
                                "no c decoder for this type yet");
                Py_DECREF(InvalidDocument);
            }
            goto invalid;
        }
    }

    if (value) {
        return value;
    }

    invalid:

    /*
     * Wrap any non-InvalidBSON errors in InvalidBSON.
     */
    if (PyErr_Occurred()) {
        PyObject *etype, *evalue, *etrace;
        PyObject *InvalidBSON;

        /*
         * Calling _error clears the error state, so fetch it first.
         */
        PyErr_Fetch(&etype, &evalue, &etrace);
        InvalidBSON = _error("InvalidBSON");
        if (InvalidBSON) {
            if (!PyErr_GivenExceptionMatches(etype, InvalidBSON)) {
                /*
                 * Raise InvalidBSON(str(e)).
                 */
                Py_DECREF(etype);
                etype = InvalidBSON;

                if (evalue) {
                    PyObject *msg = PyObject_Str(evalue);
                    Py_DECREF(evalue);
                    evalue = msg;
                }
                PyErr_NormalizeException(&etype, &evalue, &etrace);
            } else {
                /*
                 * The current exception matches InvalidBSON, so we don't need
                 * this reference after all.
                 */
                Py_DECREF(InvalidBSON);
            }
        }
        /* Steals references to args. */
        PyErr_Restore(etype, evalue, etrace);
    } else {
        PyObject *InvalidBSON = _error("InvalidBSON");
        if (InvalidBSON) {
            PyErr_SetString(InvalidBSON, "invalid length or type code");
            Py_DECREF(InvalidBSON);
        }
    }
    return NULL;
}

static PyObject* _elements_to_dict(PyObject* self, const char* string,
                                   unsigned max, PyObject* as_class,
                                   unsigned char tz_aware,
                                   unsigned char uuid_subtype,
                                   unsigned char compile_re) {
    unsigned position = 0;
    PyObject* dict = PyObject_CallObject(as_class, NULL);
    if (!dict) {
        return NULL;
    }
    while (position < max) {
        PyObject* name;
        PyObject* value;

        unsigned char type = (unsigned char)string[position++];
        size_t name_length = strlen(string + position);
        if (name_length > BSON_MAX_SIZE || position + name_length >= max) {
            PyObject* InvalidBSON = _error("InvalidBSON");
            if (InvalidBSON) {
                PyErr_SetNone(InvalidBSON);
                Py_DECREF(InvalidBSON);
            }
            Py_DECREF(dict);
            return NULL;
        }
        name = PyUnicode_DecodeUTF8(string + position, name_length, "strict");
        if (!name) {
            Py_DECREF(dict);
            return NULL;
        }
        position += (unsigned)name_length + 1;
        value = get_value(self, string, &position, type,
                          max - position, as_class, tz_aware, uuid_subtype,
                          compile_re);
        if (!value) {
            Py_DECREF(name);
            Py_DECREF(dict);
            return NULL;
        }

        PyObject_SetItem(dict, name, value);
        Py_DECREF(name);
        Py_DECREF(value);
    }
    return dict;
}

static PyObject* elements_to_dict(PyObject* self, const char* string,
                                  unsigned max, PyObject* as_class,
                                  unsigned char tz_aware,
                                  unsigned char uuid_subtype,
                                  unsigned char compile_re) {
    PyObject* result;
    if (Py_EnterRecursiveCall(" while decoding a BSON document"))
        return NULL;
    result = _elements_to_dict(self, string, max,
                               as_class, tz_aware, uuid_subtype, compile_re);
    Py_LeaveRecursiveCall();
    return result;
}

static PyObject* _cbson_bson_to_dict(PyObject* self, PyObject* args) {
    int size;
    Py_ssize_t total_size;
    const char* string;
    PyObject* bson;
    PyObject* as_class;
    unsigned char tz_aware;
    unsigned char uuid_subtype;
    unsigned char compile_re;

    PyObject* dict;
    PyObject* remainder;
    PyObject* result;

    if (!PyArg_ParseTuple(
            args, "OObbb", &bson, &as_class, &tz_aware, &uuid_subtype, &compile_re)) {
        return NULL;
    }

#if PY_MAJOR_VERSION >= 3
    if (!PyBytes_Check(bson)) {
        PyErr_SetString(PyExc_TypeError, "argument to _bson_to_dict must be a bytes object");
#else
    if (!PyString_Check(bson)) {
        PyErr_SetString(PyExc_TypeError, "argument to _bson_to_dict must be a string");
#endif
        return NULL;
    }
#if PY_MAJOR_VERSION >= 3
    total_size = PyBytes_Size(bson);
#else
    total_size = PyString_Size(bson);
#endif
    if (total_size < BSON_MIN_SIZE) {
        PyObject* InvalidBSON = _error("InvalidBSON");
        if (InvalidBSON) {
            PyErr_SetString(InvalidBSON,
                            "not enough data for a BSON document");
            Py_DECREF(InvalidBSON);
        }
        return NULL;
    }

#if PY_MAJOR_VERSION >= 3
    string = PyBytes_AsString(bson);
#else
    string = PyString_AsString(bson);
#endif
    if (!string) {
        return NULL;
    }

    memcpy(&size, string, 4);
    if (size < BSON_MIN_SIZE) {
        PyObject* InvalidBSON = _error("InvalidBSON");
        if (InvalidBSON) {
            PyErr_SetString(InvalidBSON, "invalid message size");
            Py_DECREF(InvalidBSON);
        }
        return NULL;
    }

    if (total_size < size || total_size > BSON_MAX_SIZE) {
        PyObject* InvalidBSON = _error("InvalidBSON");
        if (InvalidBSON) {
            PyErr_SetString(InvalidBSON, "objsize too large");
            Py_DECREF(InvalidBSON);
        }
        return NULL;
    }

    if (size != total_size || string[size - 1]) {
        PyObject* InvalidBSON = _error("InvalidBSON");
        if (InvalidBSON) {
            PyErr_SetString(InvalidBSON, "bad eoo");
            Py_DECREF(InvalidBSON);
        }
        return NULL;
    }

    dict = elements_to_dict(self, string + 4, (unsigned)size - 5,
                            as_class, tz_aware, uuid_subtype, compile_re);
    if (!dict) {
        return NULL;
    }
#if PY_MAJOR_VERSION >= 3
    remainder = PyBytes_FromStringAndSize(string + size, total_size - size);
#else
    remainder = PyString_FromStringAndSize(string + size, total_size - size);
#endif
    if (!remainder) {
        Py_DECREF(dict);
        return NULL;
    }
    result = Py_BuildValue("OO", dict, remainder);
    Py_DECREF(dict);
    Py_DECREF(remainder);
    return result;
}

static PyObject* _cbson_decode_all(PyObject* self, PyObject* args) {
    int size;
    Py_ssize_t total_size;
    const char* string;
    PyObject* bson;
    PyObject* dict;
    PyObject* result;
    PyObject* as_class = (PyObject*)&PyDict_Type;
    unsigned char tz_aware = 1;
    unsigned char uuid_subtype = 3;
    unsigned char compile_re = 1;

    if (!PyArg_ParseTuple(
            args, "O|Obbb",
            &bson, &as_class, &tz_aware, &uuid_subtype, &compile_re)) {
        return NULL;
    }

#if PY_MAJOR_VERSION >= 3
    if (!PyBytes_Check(bson)) {
        PyErr_SetString(PyExc_TypeError, "argument to decode_all must be a bytes object");
#else
    if (!PyString_Check(bson)) {
        PyErr_SetString(PyExc_TypeError, "argument to decode_all must be a string");
#endif
        return NULL;
    }
#if PY_MAJOR_VERSION >= 3
    total_size = PyBytes_Size(bson);
    string = PyBytes_AsString(bson);
#else
    total_size = PyString_Size(bson);
    string = PyString_AsString(bson);
#endif
    if (!string) {
        return NULL;
    }

    if (!(result = PyList_New(0)))
        return NULL;

    while (total_size > 0) {
        if (total_size < BSON_MIN_SIZE) {
            PyObject* InvalidBSON = _error("InvalidBSON");
            if (InvalidBSON) {
                PyErr_SetString(InvalidBSON,
                                "not enough data for a BSON document");
                Py_DECREF(InvalidBSON);
            }
            Py_DECREF(result);
            return NULL;
        }

        memcpy(&size, string, 4);
        if (size < BSON_MIN_SIZE) {
            PyObject* InvalidBSON = _error("InvalidBSON");
            if (InvalidBSON) {
                PyErr_SetString(InvalidBSON, "invalid message size");
                Py_DECREF(InvalidBSON);
            }
            Py_DECREF(result);
            return NULL;
        }

        if (total_size < size) {
            PyObject* InvalidBSON = _error("InvalidBSON");
            if (InvalidBSON) {
                PyErr_SetString(InvalidBSON, "objsize too large");
                Py_DECREF(InvalidBSON);
            }
            Py_DECREF(result);
            return NULL;
        }

        if (string[size - 1]) {
            PyObject* InvalidBSON = _error("InvalidBSON");
            if (InvalidBSON) {
                PyErr_SetString(InvalidBSON, "bad eoo");
                Py_DECREF(InvalidBSON);
            }
            Py_DECREF(result);
            return NULL;
        }

        dict = elements_to_dict(self, string + 4, (unsigned)size - 5,
                                as_class, tz_aware, uuid_subtype, compile_re);
        if (!dict) {
            Py_DECREF(result);
            return NULL;
        }
        PyList_Append(result, dict);
        Py_DECREF(dict);
        string += size;
        total_size -= size;
    }

    return result;
}

static PyMethodDef _CBSONMethods[] = {
    {"_dict_to_bson", _cbson_dict_to_bson, METH_VARARGS,
     "convert a dictionary to a string containing its BSON representation."},
    {"_bson_to_dict", _cbson_bson_to_dict, METH_VARARGS,
     "convert a BSON string to a SON object."},
    {"decode_all", _cbson_decode_all, METH_VARARGS,
     "convert binary data to a sequence of documents."},
    {NULL, NULL, 0, NULL}
};

#if PY_MAJOR_VERSION >= 3
#define INITERROR return NULL
static int _cbson_traverse(PyObject *m, visitproc visit, void *arg) {
    Py_VISIT(GETSTATE(m)->Binary);
    Py_VISIT(GETSTATE(m)->Code);
    Py_VISIT(GETSTATE(m)->ObjectId);
    Py_VISIT(GETSTATE(m)->DBRef);
    Py_VISIT(GETSTATE(m)->RECompile);
    Py_VISIT(GETSTATE(m)->Regex);
    Py_VISIT(GETSTATE(m)->UUID);
    Py_VISIT(GETSTATE(m)->Timestamp);
    Py_VISIT(GETSTATE(m)->MinKey);
    Py_VISIT(GETSTATE(m)->MaxKey);
    Py_VISIT(GETSTATE(m)->UTC);
    Py_VISIT(GETSTATE(m)->REType);
    return 0;
}

static int _cbson_clear(PyObject *m) {
    Py_CLEAR(GETSTATE(m)->Binary);
    Py_CLEAR(GETSTATE(m)->Code);
    Py_CLEAR(GETSTATE(m)->ObjectId);
    Py_CLEAR(GETSTATE(m)->DBRef);
    Py_CLEAR(GETSTATE(m)->RECompile);
    Py_CLEAR(GETSTATE(m)->Regex);
    Py_CLEAR(GETSTATE(m)->UUID);
    Py_CLEAR(GETSTATE(m)->Timestamp);
    Py_CLEAR(GETSTATE(m)->MinKey);
    Py_CLEAR(GETSTATE(m)->MaxKey);
    Py_CLEAR(GETSTATE(m)->UTC);
    Py_CLEAR(GETSTATE(m)->REType);
    return 0;
}

static struct PyModuleDef moduledef = {
    PyModuleDef_HEAD_INIT,
    "_cbson",
    NULL,
    sizeof(struct module_state),
    _CBSONMethods,
    NULL,
    _cbson_traverse,
    _cbson_clear,
    NULL
};

PyMODINIT_FUNC
PyInit__cbson(void)
#else
#define INITERROR return
PyMODINIT_FUNC
init_cbson(void)
#endif
{
    PyObject *m;
    PyObject *c_api_object;
    static void *_cbson_API[_cbson_API_POINTER_COUNT];

    PyDateTime_IMPORT;
    if (PyDateTimeAPI == NULL) {
        INITERROR;
    }

    /* Export C API */
    _cbson_API[_cbson_buffer_write_bytes_INDEX] = (void *) buffer_write_bytes;
    _cbson_API[_cbson_write_dict_INDEX] = (void *) write_dict;
    _cbson_API[_cbson_write_pair_INDEX] = (void *) write_pair;
    _cbson_API[_cbson_decode_and_write_pair_INDEX] = (void *) decode_and_write_pair;

#if PY_VERSION_HEX >= 0x03010000
    /* PyCapsule is new in python 3.1 */
    c_api_object = PyCapsule_New((void *) _cbson_API, "_cbson._C_API", NULL);
#else
    c_api_object = PyCObject_FromVoidPtr((void *) _cbson_API, NULL);
#endif
    if (c_api_object == NULL)
        INITERROR;

#if PY_MAJOR_VERSION >= 3
    m = PyModule_Create(&moduledef);
#else
    m = Py_InitModule("_cbson", _CBSONMethods);
#endif
    if (m == NULL) {
        Py_DECREF(c_api_object);
        INITERROR;
    }

    /* Import several python objects */
    if (_load_python_objects(m)) {
        Py_DECREF(c_api_object);
#if PY_MAJOR_VERSION >= 3
        Py_DECREF(m);
#endif
        INITERROR;
    }

    if (PyModule_AddObject(m, "_C_API", c_api_object) < 0) {
        Py_DECREF(c_api_object);
#if PY_MAJOR_VERSION >= 3
        Py_DECREF(m);
#endif
        INITERROR;
    }

#if PY_MAJOR_VERSION >= 3
    return m;
#endif
}
