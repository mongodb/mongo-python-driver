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

#ifndef _CBSON_H
#define _CBSON_H

#include <Python.h>
#include <stdio.h>
#include "buffer.h"

#if PY_VERSION_HEX < 0x02050000 && !defined(PY_SSIZE_T_MIN)
typedef unsigned int Py_ssize_t;
#define PY_SSIZE_T_MAX UINT_MAX
#define PY_SSIZE_T_MIN 0
#endif

int buffer_write_bytes(buffer_t buffer, const char* data, int size);

int write_dict(buffer_t buffer, PyObject* dict,
               unsigned char check_keys, unsigned char uuid_subtype,
               unsigned char top_level);

int write_pair(buffer_t buffer, const char* name, Py_ssize_t name_length,
               PyObject* value, unsigned char check_keys,
               unsigned char uuid_subtype, unsigned char allow_id);

int decode_and_write_pair(buffer_t buffer, PyObject* key, PyObject* value,
                          unsigned char check_keys, unsigned char uuid_subtype,
                          unsigned char top_level);

PyMODINIT_FUNC init_cbson(void);
#endif
