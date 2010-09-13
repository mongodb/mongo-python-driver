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

#include <Python.h>

/* A buffer representing some data being encoded to BSON. */
typedef struct {
    char* buffer;
    int size;
    int position;
} bson_buffer;

bson_buffer* buffer_new(void);

int buffer_save_bytes(bson_buffer* buffer, int size);

int buffer_write_bytes(bson_buffer* buffer, const char* bytes, int size);

void buffer_free(bson_buffer* buffer);

int write_dict(bson_buffer* buffer, PyObject* dict,
               unsigned char check_keys, unsigned char top_level);

PyObject* elements_to_dict(const char* string, int max,
                           PyObject* as_class, unsigned char tz_aware);

int write_pair(bson_buffer* buffer, const char* name,
               Py_ssize_t name_length, PyObject* value,
               unsigned char check_keys, unsigned char allow_id);

int decode_and_write_pair(bson_buffer* buffer,
                          PyObject* key, PyObject* value,
                          unsigned char check_keys, unsigned char top_level);
