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

#ifndef _CBSONMODULE_H
#define _CBSONMODULE_H

/* C API functions */
#define _cbson_buffer_write_bytes_INDEX 0
#define _cbson_buffer_write_bytes_RETURN int
#define _cbson_buffer_write_bytes_PROTO (PyObject* self, buffer_t buffer, const char* data, int size)

#define _cbson_write_dict_INDEX 1
#define _cbson_write_dict_RETURN int
#define _cbson_write_dict_PROTO (PyObject* self, buffer_t buffer, PyObject* dict, unsigned char check_keys, unsigned char top_level)

#define _cbson_write_pair_INDEX 2
#define _cbson_write_pair_RETURN int
#define _cbson_write_pair_PROTO (PyObject* self, buffer_t buffer, const char* name, Py_ssize_t name_length, PyObject* value, unsigned char check_keys, unsigned char allow_id)

#define _cbson_decode_and_write_pair_INDEX 3
#define _cbson_decode_and_write_pair_RETURN int
#define _cbson_decode_and_write_pair_PROTO (PyObject* self, buffer_t buffer, PyObject* key, PyObject* value, unsigned char check_keys, unsigned char top_level)

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
