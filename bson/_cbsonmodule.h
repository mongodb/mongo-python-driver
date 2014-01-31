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

#ifndef _CBSONMODULE_H
#define _CBSONMODULE_H

/* Py_ssize_t was new in python 2.5. See conversion
 * guidlines in http://www.python.org/dev/peps/pep-0353
 * */
#if PY_VERSION_HEX < 0x02050000 && !defined(PY_SSIZE_T_MIN)
typedef int Py_ssize_t;
#define PY_SSIZE_T_MAX INT_MAX
#define PY_SSIZE_T_MIN INT_MIN
#endif

#if defined(WIN32) || defined(_MSC_VER)
/*
 * This macro is basically an implementation of asprintf for win32
 * We print to the provided buffer to get the string value as an int.
 */
#if defined(_MSC_VER) && (_MSC_VER >= 1400)
#define INT2STRING(buffer, i)                                       \
    _snprintf_s((buffer),                                           \
                 _scprintf("%d", (i)) + 1,                          \
                 _scprintf("%d", (i)) + 1,                          \
                 "%d",                                              \
                 (i))
#define STRCAT(dest, n, src) strcat_s((dest), (n), (src))
#else
#define INT2STRING(buffer, i)                                       \
    _snprintf((buffer),                                             \
               _scprintf("%d", (i)) + 1,                            \
               "%d",                                                \
              (i))
#define STRCAT(dest, n, src) strcat((dest), (src))
#endif
#else
#define INT2STRING(buffer, i) snprintf((buffer), sizeof((buffer)), "%d", (i))
#define STRCAT(dest, n, src) strcat((dest), (src))
#endif

/* C API functions */
#define _cbson_buffer_write_bytes_INDEX 0
#define _cbson_buffer_write_bytes_RETURN int
#define _cbson_buffer_write_bytes_PROTO (buffer_t buffer, const char* data, int size)

#define _cbson_write_dict_INDEX 1
#define _cbson_write_dict_RETURN int
#define _cbson_write_dict_PROTO (PyObject* self, buffer_t buffer, PyObject* dict, unsigned char check_keys, unsigned char uuid_subtype, unsigned char top_level)

#define _cbson_write_pair_INDEX 2
#define _cbson_write_pair_RETURN int
#define _cbson_write_pair_PROTO (PyObject* self, buffer_t buffer, const char* name, int name_length, PyObject* value, unsigned char check_keys, unsigned char uuid_subtype, unsigned char allow_id)

#define _cbson_decode_and_write_pair_INDEX 3
#define _cbson_decode_and_write_pair_RETURN int
#define _cbson_decode_and_write_pair_PROTO (PyObject* self, buffer_t buffer, PyObject* key, PyObject* value, unsigned char check_keys, unsigned char uuid_subtype, unsigned char top_level)

/* Total number of C API pointers */
#define _cbson_API_POINTER_COUNT 4

#ifdef _CBSON_MODULE
/* This section is used when compiling _cbsonmodule */

static _cbson_buffer_write_bytes_RETURN buffer_write_bytes _cbson_buffer_write_bytes_PROTO;

static _cbson_write_dict_RETURN write_dict _cbson_write_dict_PROTO;

static _cbson_write_pair_RETURN write_pair _cbson_write_pair_PROTO;

static _cbson_decode_and_write_pair_RETURN decode_and_write_pair _cbson_decode_and_write_pair_PROTO;

#else
/* This section is used in modules that use _cbsonmodule's API */

static void **_cbson_API;

#define buffer_write_bytes (*(_cbson_buffer_write_bytes_RETURN (*)_cbson_buffer_write_bytes_PROTO) _cbson_API[_cbson_buffer_write_bytes_INDEX])

#define write_dict (*(_cbson_write_dict_RETURN (*)_cbson_write_dict_PROTO) _cbson_API[_cbson_write_dict_INDEX])

#define write_pair (*(_cbson_write_pair_RETURN (*)_cbson_write_pair_PROTO) _cbson_API[_cbson_write_pair_INDEX])

#define decode_and_write_pair (*(_cbson_decode_and_write_pair_RETURN (*)_cbson_decode_and_write_pair_PROTO) _cbson_API[_cbson_decode_and_write_pair_INDEX])

#define _cbson_IMPORT _cbson_API = (void **)PyCapsule_Import("_cbson._C_API", 0)

#endif

#endif // _CBSONMODULE_H
