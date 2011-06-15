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

/*
 * This file contains C implementations of some of the functions
 * needed by the message module. If possible, these implementations
 * should be used to speed up message creation.
 */

#include <Python.h>

#include "_cbson.h"
#include "buffer.h"

/* Get an error class from the pymongo.errors module.
 *
 * Returns a new ref */
static PyObject* _error(char* name) {
    PyObject* error;
    PyObject* errors = PyImport_ImportModule("pymongo.errors");
    if (!errors) {
        return NULL;
    }
    error = PyObject_GetAttrString(errors, name);
    Py_DECREF(errors);
    return error;
}

/* add a lastError message on the end of the buffer.
 * returns 0 on failure */
static int add_last_error(buffer_t buffer, int request_id, PyObject* args) {
    int message_start;
    int document_start;
    int message_length;
    int document_length;
    PyObject* key;
    PyObject* value;
    Py_ssize_t pos = 0;
    PyObject* one;

    message_start = buffer_save_space(buffer, 4);
    if (message_start == -1) {
        PyErr_NoMemory();
        return 0;
    }
    if (!buffer_write_bytes(buffer, (const char*)&request_id, 4) ||
        !buffer_write_bytes(buffer,
                            "\x00\x00\x00\x00"  /* responseTo */
                            "\xd4\x07\x00\x00"  /* opcode */
                            "\x00\x00\x00\x00"  /* options */
                            "admin.$cmd\x00"    /* collection name */
                            "\x00\x00\x00\x00"  /* skip */
                            "\xFF\xFF\xFF\xFF", /* limit (-1) */
                            31)) {
        return 0;
    }

    /* save space for length */
    document_start = buffer_save_space(buffer, 4);
    if (document_start == -1) {
        PyErr_NoMemory();
        return 0;
    }

    /* getlasterror: 1 */
    one = PyLong_FromLong(1);
    if (!write_pair(buffer, "getlasterror", 12, one, 0, 1)) {
        Py_DECREF(one);
        return 0;
    }
    Py_DECREF(one);

    /* getlasterror options */
    while (PyDict_Next(args, &pos, &key, &value)) {
        if (!decode_and_write_pair(buffer, key, value, 0, 0)) {
            return 0;
        }
    }

    /* EOD */
    if (!buffer_write_bytes(buffer, "\x00", 1)) {
        return 0;
    }

    message_length = buffer_get_position(buffer) - message_start;
    document_length = buffer_get_position(buffer) - document_start;
    memcpy(buffer_get_buffer(buffer) + message_start, &message_length, 4);
    memcpy(buffer_get_buffer(buffer) + document_start, &document_length, 4);
    return 1;
}

static PyObject* _cbson_insert_message(PyObject* self, PyObject* args) {
    /* NOTE just using a random number as the request_id */
    int request_id = rand();
    char* collection_name = NULL;
    int collection_name_length;
    PyObject* docs;
    int before, cur_size, max_size = 0;
    int list_length;
    int i;
    unsigned char check_keys;
    unsigned char safe;
    PyObject* last_error_args;
    buffer_t buffer;
    int length_location;
    PyObject* result;

    if (!PyArg_ParseTuple(args, "et#ObbO",
                          "utf-8",
                          &collection_name,
                          &collection_name_length,
                          &docs, &check_keys, &safe, &last_error_args)) {
        return NULL;
    }

    buffer = buffer_new();
    if (!buffer) {
        PyErr_NoMemory();
        PyMem_Free(collection_name);
        return NULL;
    }

    // save space for message length
    length_location = buffer_save_space(buffer, 4);
    if (length_location == -1) {
        PyMem_Free(collection_name);
        PyErr_NoMemory();
        return NULL;
    }
    if (!buffer_write_bytes(buffer, (const char*)&request_id, 4) ||
        !buffer_write_bytes(buffer,
                            "\x00\x00\x00\x00"
                            "\xd2\x07\x00\x00"
                            "\x00\x00\x00\x00",
                            12) ||
        !buffer_write_bytes(buffer,
                            collection_name,
                            collection_name_length + 1)) {
        PyMem_Free(collection_name);
        buffer_free(buffer);
        return NULL;
    }

    PyMem_Free(collection_name);

    list_length = PyList_Size(docs);
    if (list_length <= 0) {
        PyObject* InvalidOperation = _error("InvalidOperation");
        PyErr_SetString(InvalidOperation, "cannot do an empty bulk insert");
        Py_DECREF(InvalidOperation);
        buffer_free(buffer);
        return NULL;
    }
    for (i = 0; i < list_length; i++) {
        PyObject* doc = PyList_GetItem(docs, i);
        before = buffer_get_position(buffer);
        if (!write_dict(buffer, doc, check_keys, 1)) {
            buffer_free(buffer);
            return NULL;
        }
        cur_size = buffer_get_position(buffer) - before;
        max_size = (cur_size > max_size) ? cur_size : max_size;
    }

    memcpy(buffer_get_buffer(buffer) + length_location,
           buffer_get_buffer(buffer) + buffer_get_position(buffer), 4);

    if (safe) {
        if (!add_last_error(buffer, request_id, last_error_args)) {
            buffer_free(buffer);
            return NULL;
        }
    }

    /* objectify buffer */
    result = Py_BuildValue("is#i", request_id,
                           buffer_get_buffer(buffer),
                           buffer_get_position(buffer),
                           max_size);
    buffer_free(buffer);
    return result;
}

static PyObject* _cbson_update_message(PyObject* self, PyObject* args) {
    /* NOTE just using a random number as the request_id */
    int request_id = rand();
    char* collection_name = NULL;
    int collection_name_length;
    int before, cur_size, max_size = 0;
    PyObject* doc;
    PyObject* spec;
    unsigned char multi;
    unsigned char upsert;
    unsigned char safe;
    PyObject* last_error_args;
    int options;
    buffer_t buffer;
    int length_location;
    PyObject* result;

    if (!PyArg_ParseTuple(args, "et#bbOObO",
                          "utf-8",
                          &collection_name,
                          &collection_name_length,
                          &upsert, &multi, &spec, &doc, &safe,
                          &last_error_args)) {
        return NULL;
    }

    options = 0;
    if (upsert) {
        options += 1;
    }
    if (multi) {
        options += 2;
    }
    buffer = buffer_new();
    if (!buffer) {
        PyErr_NoMemory();
        PyMem_Free(collection_name);
        return NULL;
    }

    // save space for message length
    length_location = buffer_save_space(buffer, 4);
    if (length_location == -1) {
        PyMem_Free(collection_name);
        PyErr_NoMemory();
        return NULL;
    }
    if (!buffer_write_bytes(buffer, (const char*)&request_id, 4) ||
        !buffer_write_bytes(buffer,
                            "\x00\x00\x00\x00"
                            "\xd1\x07\x00\x00"
                            "\x00\x00\x00\x00",
                            12) ||
        !buffer_write_bytes(buffer,
                            collection_name,
                            collection_name_length + 1) ||
        !buffer_write_bytes(buffer, (const char*)&options, 4)) {
        buffer_free(buffer);
        PyMem_Free(collection_name);
        return NULL;
    }

    before = buffer_get_position(buffer);
    if (!write_dict(buffer, spec, 0, 1)) {
        buffer_free(buffer);
        PyMem_Free(collection_name);
        return NULL;
    }
    max_size = buffer_get_position(buffer) - before;

    before = buffer_get_position(buffer);
    if (!write_dict(buffer, doc, 0, 1)) {
        buffer_free(buffer);
        PyMem_Free(collection_name);
        return NULL;
    }
    cur_size = buffer_get_position(buffer) - before;
    max_size = (cur_size > max_size) ? cur_size : max_size;

    PyMem_Free(collection_name);

    memcpy(buffer_get_buffer(buffer) + length_location,
           buffer_get_buffer(buffer) + buffer_get_position(buffer), 4);

    if (safe) {
        if (!add_last_error(buffer, request_id, last_error_args)) {
            buffer_free(buffer);
            return NULL;
        }
    }

    /* objectify buffer */
    result = Py_BuildValue("is#i", request_id,
                           buffer_get_buffer(buffer),
                           buffer_get_position(buffer),
                           max_size);
    buffer_free(buffer);
    return result;
}

static PyObject* _cbson_query_message(PyObject* self, PyObject* args) {
    /* NOTE just using a random number as the request_id */
    int request_id = rand();
    unsigned int options;
    char* collection_name = NULL;
    int collection_name_length;
    int begin, cur_size, max_size = 0;
    int num_to_skip;
    int num_to_return;
    PyObject* query;
    PyObject* field_selector = Py_None;
    buffer_t buffer;
    int length_location;
    PyObject* result;

    if (!PyArg_ParseTuple(args, "Iet#iiO|O",
                          &options,
                          "utf-8",
                          &collection_name,
                          &collection_name_length,
                          &num_to_skip, &num_to_return,
                          &query, &field_selector)) {
        return NULL;
    }
    buffer = buffer_new();
    if (!buffer) {
        PyErr_NoMemory();
        PyMem_Free(collection_name);
        return NULL;
    }

    // save space for message length
    length_location = buffer_save_space(buffer, 4);
    if (length_location == -1) {
        PyMem_Free(collection_name);
        PyErr_NoMemory();
        return NULL;
    }
    if (!buffer_write_bytes(buffer, (const char*)&request_id, 4) ||
        !buffer_write_bytes(buffer, "\x00\x00\x00\x00\xd4\x07\x00\x00", 8) ||
        !buffer_write_bytes(buffer, (const char*)&options, 4) ||
        !buffer_write_bytes(buffer, collection_name,
                            collection_name_length + 1) ||
        !buffer_write_bytes(buffer, (const char*)&num_to_skip, 4) ||
        !buffer_write_bytes(buffer, (const char*)&num_to_return, 4)) {
        buffer_free(buffer);
        PyMem_Free(collection_name);
        return NULL;
    }

    begin = buffer_get_position(buffer);
    if (!write_dict(buffer, query, 0, 1)) {
        buffer_free(buffer);
        PyMem_Free(collection_name);
        return NULL;
    }
    max_size = buffer_get_position(buffer) - begin;

    if (field_selector != Py_None) {
        begin = buffer_get_position(buffer);
        if (!write_dict(buffer, field_selector, 0, 1)) {
            buffer_free(buffer);
            PyMem_Free(collection_name);
            return NULL;
        }
        cur_size = buffer_get_position(buffer) - begin;
        max_size = (cur_size > max_size) ? cur_size : max_size;
    }

    PyMem_Free(collection_name);

    memcpy(buffer_get_buffer(buffer) + length_location,
           buffer_get_buffer(buffer) + buffer_get_position(buffer), 4);

    /* objectify buffer */
    result = Py_BuildValue("is#i", request_id,
                           buffer_get_buffer(buffer),
                           buffer_get_position(buffer),
                           max_size);
    buffer_free(buffer);
    return result;
}

static PyObject* _cbson_get_more_message(PyObject* self, PyObject* args) {
    /* NOTE just using a random number as the request_id */
    int request_id = rand();
    char* collection_name = NULL;
    int collection_name_length;
    int num_to_return;
    long long cursor_id;
    buffer_t buffer;
    int length_location;
    PyObject* result;

    if (!PyArg_ParseTuple(args, "et#iL",
                          "utf-8",
                          &collection_name,
                          &collection_name_length,
                          &num_to_return,
                          &cursor_id)) {
        return NULL;
    }
    buffer = buffer_new();
    if (!buffer) {
        PyErr_NoMemory();
        PyMem_Free(collection_name);
        return NULL;
    }

    // save space for message length
    length_location = buffer_save_space(buffer, 4);
    if (length_location == -1) {
        PyMem_Free(collection_name);
        PyErr_NoMemory();
        return NULL;
    }
    if (!buffer_write_bytes(buffer, (const char*)&request_id, 4) ||
        !buffer_write_bytes(buffer,
                            "\x00\x00\x00\x00"
                            "\xd5\x07\x00\x00"
                            "\x00\x00\x00\x00", 12) ||
        !buffer_write_bytes(buffer,
                            collection_name,
                            collection_name_length + 1) ||
        !buffer_write_bytes(buffer, (const char*)&num_to_return, 4) ||
        !buffer_write_bytes(buffer, (const char*)&cursor_id, 8)) {
        buffer_free(buffer);
        PyMem_Free(collection_name);
        return NULL;
    }

    PyMem_Free(collection_name);

    memcpy(buffer_get_buffer(buffer) + length_location,
           buffer_get_buffer(buffer) + buffer_get_position(buffer), 4);

    /* objectify buffer */
    result = Py_BuildValue("is#", request_id,
                           buffer_get_buffer(buffer),
                           buffer_get_position(buffer));
    buffer_free(buffer);
    return result;
}

static PyMethodDef _CMessageMethods[] = {
    {"_insert_message", _cbson_insert_message, METH_VARARGS,
     "create an insert message to be sent to MongoDB"},
    {"_update_message", _cbson_update_message, METH_VARARGS,
     "create an update message to be sent to MongoDB"},
    {"_query_message", _cbson_query_message, METH_VARARGS,
     "create a query message to be sent to MongoDB"},
    {"_get_more_message", _cbson_get_more_message, METH_VARARGS,
     "create a get more message to be sent to MongoDB"},
    {NULL, NULL, 0, NULL}
};

PyMODINIT_FUNC init_cmessage(void) {
    PyObject *m;

    /* TODO is this necessary?
     *
     * We import _cbson here to make sure that it's init function has
     * been run.
     */
    m = PyImport_ImportModule("bson._cbson");
    Py_DECREF(m);

    m = Py_InitModule("_cmessage", _CMessageMethods);
    if (m == NULL) {
        return;
    }
}
