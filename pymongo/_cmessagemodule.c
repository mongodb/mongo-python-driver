/*
 * Copyright 2009-2012 10gen, Inc.
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

#include "Python.h"

#include "_cbsonmodule.h"
#include "buffer.h"

struct module_state {
    PyObject* _cbson;
};

/* See comments about module initialization in _cbsonmodule.c */
#if PY_MAJOR_VERSION >= 3
#define GETSTATE(m) ((struct module_state*)PyModule_GetState(m))
#else
#define GETSTATE(m) (&_state)
static struct module_state _state;
#endif

#if PY_MAJOR_VERSION >= 3
#define BYTES_FORMAT_STRING "y#"
#else
#define BYTES_FORMAT_STRING "s#"
#endif

#define DOC_TOO_LARGE_FMT "BSON document too large (%d bytes)" \
                          " - the connected server supports" \
                          " BSON document sizes up to %ld bytes."

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
static int add_last_error(PyObject* self, buffer_t buffer,
                          int request_id, char* ns, int nslen, PyObject* args) {
    struct module_state *state = GETSTATE(self);

    int message_start;
    int document_start;
    int message_length;
    int document_length;
    PyObject* key;
    PyObject* value;
    Py_ssize_t pos = 0;
    PyObject* one;
    char *p = strchr(ns, '.');
    /* Length of the database portion of ns. */
    nslen = p ? (int)(p - ns) : nslen;

    message_start = buffer_save_space(buffer, 4);
    if (message_start == -1) {
        PyErr_NoMemory();
        return 0;
    }
    if (!buffer_write_bytes(buffer, (const char*)&request_id, 4) ||
        !buffer_write_bytes(buffer,
                            "\x00\x00\x00\x00"  /* responseTo */
                            "\xd4\x07\x00\x00"  /* opcode */
                            "\x00\x00\x00\x00", /* options */
                            12) ||
        !buffer_write_bytes(buffer,
                            ns, nslen) ||       /* database */
        !buffer_write_bytes(buffer,
                            ".$cmd\x00"         /* collection name */
                            "\x00\x00\x00\x00"  /* skip */
                            "\xFF\xFF\xFF\xFF", /* limit (-1) */
                            14)) {
        return 0;
    }

    /* save space for length */
    document_start = buffer_save_space(buffer, 4);
    if (document_start == -1) {
        PyErr_NoMemory();
        return 0;
    }

    /* getlasterror: 1 */
    if (!(one = PyLong_FromLong(1)))
        return 0;
    if (!write_pair(state->_cbson, buffer, "getlasterror", 12, one, 0, 4, 1)) {
        Py_DECREF(one);
        return 0;
    }
    Py_DECREF(one);

    /* getlasterror options */
    while (PyDict_Next(args, &pos, &key, &value)) {
        if (!decode_and_write_pair(state->_cbson, buffer, key, value, 0, 4, 0)) {
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

static int init_insert_buffer(buffer_t buffer, int request_id, int options,
                              const char* coll_name, int coll_name_len) {
    /* Save space for message length */
    int length_location = buffer_save_space(buffer, 4);
    if (length_location == -1) {
        PyErr_NoMemory();
        return length_location;
    }
    if (!buffer_write_bytes(buffer, (const char*)&request_id, 4) ||
        !buffer_write_bytes(buffer,
                            "\x00\x00\x00\x00"
                            "\xd2\x07\x00\x00",
                            8) ||
        !buffer_write_bytes(buffer, (const char*)&options, 4) ||
        !buffer_write_bytes(buffer,
                            coll_name,
                            coll_name_len + 1)) {
        return -1;
    }
    return length_location;
}

static PyObject* _cbson_insert_message(PyObject* self, PyObject* args) {
    /* Note: As of PyMongo 2.6, this function is no longer used. It
     * is being kept (with tests) for backwards compatibility with 3rd
     * party libraries that may currently be using it, but will likely
     * be removed in a future release. */
    struct module_state *state = GETSTATE(self);

    /* NOTE just using a random number as the request_id */
    int request_id = rand();
    char* collection_name = NULL;
    int collection_name_length;
    PyObject* docs;
    PyObject* doc;
    PyObject* iterator;
    int before, cur_size, max_size = 0;
    int options = 0;
    unsigned char check_keys;
    unsigned char safe;
    unsigned char continue_on_error;
    unsigned char uuid_subtype;
    PyObject* last_error_args;
    buffer_t buffer;
    int length_location, message_length;
    PyObject* result;

    if (!PyArg_ParseTuple(args, "et#ObbObb",
                          "utf-8",
                          &collection_name,
                          &collection_name_length,
                          &docs, &check_keys, &safe,
                          &last_error_args,
                          &continue_on_error, &uuid_subtype)) {
        return NULL;
    }
    if (continue_on_error) {
        options += 1;
    }

    buffer = buffer_new();
    if (!buffer) {
        PyErr_NoMemory();
        PyMem_Free(collection_name);
        return NULL;
    }

    length_location = init_insert_buffer(buffer,
                                         request_id,
                                         options,
                                         collection_name,
                                         collection_name_length);
    if (length_location == -1) {
        PyMem_Free(collection_name);
        buffer_free(buffer);
        return NULL;
    }

    iterator = PyObject_GetIter(docs);
    if (iterator == NULL) {
        PyObject* InvalidOperation = _error("InvalidOperation");
        if (InvalidOperation) {
            PyErr_SetString(InvalidOperation, "input is not iterable");
            Py_DECREF(InvalidOperation);
        }
        buffer_free(buffer);
        PyMem_Free(collection_name);
        return NULL;
    }
    while ((doc = PyIter_Next(iterator)) != NULL) {
        before = buffer_get_position(buffer);
        if (!write_dict(state->_cbson, buffer, doc, check_keys, uuid_subtype, 1)) {
            Py_DECREF(doc);
            Py_DECREF(iterator);
            buffer_free(buffer);
            PyMem_Free(collection_name);
            return NULL;
        }
        Py_DECREF(doc);
        cur_size = buffer_get_position(buffer) - before;
        max_size = (cur_size > max_size) ? cur_size : max_size;
    }
    Py_DECREF(iterator);

    if (PyErr_Occurred()) {
        buffer_free(buffer);
        PyMem_Free(collection_name);
        return NULL;
    }

    if (!max_size) {
        PyObject* InvalidOperation = _error("InvalidOperation");
        if (InvalidOperation) {
            PyErr_SetString(InvalidOperation, "cannot do an empty bulk insert");
            Py_DECREF(InvalidOperation);
        }
        buffer_free(buffer);
        PyMem_Free(collection_name);
        return NULL;
    }

    message_length = buffer_get_position(buffer) - length_location;
    memcpy(buffer_get_buffer(buffer) + length_location, &message_length, 4);

    if (safe) {
        if (!add_last_error(self, buffer, request_id, collection_name,
                            collection_name_length, last_error_args)) {
            buffer_free(buffer);
            PyMem_Free(collection_name);
            return NULL;
        }
    }

    PyMem_Free(collection_name);

    /* objectify buffer */
    result = Py_BuildValue("i" BYTES_FORMAT_STRING "i", request_id,
                           buffer_get_buffer(buffer),
                           buffer_get_position(buffer),
                           max_size);
    buffer_free(buffer);
    return result;
}

PyDoc_STRVAR(_cbson_insert_message_doc,
"Create an insert message to be sent to MongoDB\n\
\n\
Note: As of PyMongo 2.6, this function is no longer used. It\n\
is being kept (with tests) for backwards compatibility with 3rd\n\
party libraries that may currently be using it, but will likely\n\
be removed in a future release.");

static PyObject* _cbson_update_message(PyObject* self, PyObject* args) {
    /* NOTE just using a random number as the request_id */
    struct module_state *state = GETSTATE(self);

    int request_id = rand();
    char* collection_name = NULL;
    int collection_name_length;
    int before, cur_size, max_size = 0;
    PyObject* doc;
    PyObject* spec;
    unsigned char multi;
    unsigned char upsert;
    unsigned char safe;
    unsigned char check_keys;
    unsigned char uuid_subtype;
    PyObject* last_error_args;
    int options;
    buffer_t buffer;
    int length_location, message_length;
    PyObject* result;

    if (!PyArg_ParseTuple(args, "et#bbOObObb",
                          "utf-8",
                          &collection_name,
                          &collection_name_length,
                          &upsert, &multi, &spec, &doc, &safe,
                          &last_error_args, &check_keys, &uuid_subtype)) {
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
    if (!write_dict(state->_cbson, buffer, spec, 0, uuid_subtype, 1)) {
        buffer_free(buffer);
        PyMem_Free(collection_name);
        return NULL;
    }
    max_size = buffer_get_position(buffer) - before;

    before = buffer_get_position(buffer);
    if (!write_dict(state->_cbson, buffer, doc, check_keys, uuid_subtype, 1)) {
        buffer_free(buffer);
        PyMem_Free(collection_name);
        return NULL;
    }
    cur_size = buffer_get_position(buffer) - before;
    max_size = (cur_size > max_size) ? cur_size : max_size;

    message_length = buffer_get_position(buffer) - length_location;
    memcpy(buffer_get_buffer(buffer) + length_location, &message_length, 4);

    if (safe) {
        if (!add_last_error(self, buffer, request_id, collection_name,
                            collection_name_length, last_error_args)) {
            buffer_free(buffer);
            PyMem_Free(collection_name);
            return NULL;
        }
    }

    PyMem_Free(collection_name);

    /* objectify buffer */
    result = Py_BuildValue("i" BYTES_FORMAT_STRING "i", request_id,
                           buffer_get_buffer(buffer),
                           buffer_get_position(buffer),
                           max_size);
    buffer_free(buffer);
    return result;
}

static PyObject* _cbson_query_message(PyObject* self, PyObject* args) {
    /* NOTE just using a random number as the request_id */
    struct module_state *state = GETSTATE(self);

    int request_id = rand();
    unsigned int options;
    char* collection_name = NULL;
    int collection_name_length;
    int begin, cur_size, max_size = 0;
    int num_to_skip;
    int num_to_return;
    PyObject* query;
    PyObject* field_selector = Py_None;
    unsigned char uuid_subtype = 3;
    buffer_t buffer;
    int length_location, message_length;
    PyObject* result;

    if (!PyArg_ParseTuple(args, "Iet#iiO|Ob",
                          &options,
                          "utf-8",
                          &collection_name,
                          &collection_name_length,
                          &num_to_skip, &num_to_return,
                          &query, &field_selector, &uuid_subtype)) {
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
    if (!write_dict(state->_cbson, buffer, query, 0, uuid_subtype, 1)) {
        buffer_free(buffer);
        PyMem_Free(collection_name);
        return NULL;
    }
    max_size = buffer_get_position(buffer) - begin;

    if (field_selector != Py_None) {
        begin = buffer_get_position(buffer);
        if (!write_dict(state->_cbson, buffer, field_selector, 0, uuid_subtype, 1)) {
            buffer_free(buffer);
            PyMem_Free(collection_name);
            return NULL;
        }
        cur_size = buffer_get_position(buffer) - begin;
        max_size = (cur_size > max_size) ? cur_size : max_size;
    }

    PyMem_Free(collection_name);

    message_length = buffer_get_position(buffer) - length_location;
    memcpy(buffer_get_buffer(buffer) + length_location, &message_length, 4);

    /* objectify buffer */
    result = Py_BuildValue("i" BYTES_FORMAT_STRING "i", request_id,
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
    int length_location, message_length;
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

    message_length = buffer_get_position(buffer) - length_location;
    memcpy(buffer_get_buffer(buffer) + length_location, &message_length, 4);

    /* objectify buffer */
    result = Py_BuildValue("i" BYTES_FORMAT_STRING, request_id,
                           buffer_get_buffer(buffer),
                           buffer_get_position(buffer));
    buffer_free(buffer);
    return result;
}

static PyObject* _cbson_do_batched_insert(PyObject* self, PyObject* args) {
    struct module_state *state = GETSTATE(self);

    /* NOTE just using a random number as the request_id */
    int request_id = rand();
    int options = 0, max_size = 0;
    int length_location, message_length;
    int collection_name_length;
    char* collection_name = NULL;
    PyObject* docs;
    PyObject* doc;
    PyObject* iterator;
    PyObject* client;
    PyObject* last_error_args;
    PyObject* result;
    PyObject* max_bson_size_obj;
    PyObject* max_message_size_obj;
    PyObject* send_message_result;
    unsigned char check_keys;
    unsigned char safe;
    unsigned char continue_on_error;
    unsigned char uuid_subtype;
    long max_bson_size;
    long max_message_size;
    buffer_t buffer;
    PyObject *exc_type = NULL, *exc_value = NULL, *exc_trace = NULL;

    if (!PyArg_ParseTuple(args, "et#ObbObbO",
                          "utf-8",
                          &collection_name,
                          &collection_name_length,
                          &docs, &check_keys, &safe,
                          &last_error_args,
                          &continue_on_error,
                          &uuid_subtype, &client)) {
        return NULL;
    }
    if (continue_on_error) {
        options += 1;
    }

    max_bson_size_obj = PyObject_GetAttrString(client, "max_bson_size");
#if PY_MAJOR_VERSION >= 3
    max_bson_size = PyLong_AsLong(max_bson_size_obj);
#else
    max_bson_size = PyInt_AsLong(max_bson_size_obj);
#endif
    Py_XDECREF(max_bson_size_obj);
    if (max_bson_size == -1) {
        PyMem_Free(collection_name);
        return NULL;
    }

    max_message_size_obj = PyObject_GetAttrString(client, "max_message_size");
#if PY_MAJOR_VERSION >= 3
    max_message_size = PyLong_AsLong(max_message_size_obj);
#else
    max_message_size = PyInt_AsLong(max_message_size_obj);
#endif
    Py_XDECREF(max_message_size_obj);
    if (max_message_size == -1) {
        PyMem_Free(collection_name);
        return NULL;
    }

    buffer = buffer_new();
    if (!buffer) {
        PyErr_NoMemory();
        PyMem_Free(collection_name);
        return NULL;
    }

    length_location = init_insert_buffer(buffer,
                                         request_id,
                                         options,
                                         collection_name,
                                         collection_name_length);
    if (length_location == -1) {
        goto insertfail;
    }

    iterator = PyObject_GetIter(docs);
    if (iterator == NULL) {
        PyObject* InvalidOperation = _error("InvalidOperation");
        if (InvalidOperation) {
            PyErr_SetString(InvalidOperation, "input is not iterable");
            Py_DECREF(InvalidOperation);
        }
        goto insertfail;
    }
    while ((doc = PyIter_Next(iterator)) != NULL) {
        int before = buffer_get_position(buffer);
        int cur_size;
        if (!write_dict(state->_cbson, buffer, doc, check_keys, uuid_subtype, 1)) {
            Py_DECREF(doc);
            goto iterfail;
        }
        Py_DECREF(doc);

        cur_size = buffer_get_position(buffer) - before;
        max_size = (cur_size > max_size) ? cur_size : max_size;
        if (cur_size > max_bson_size) {
            PyObject* InvalidDocument = _error("InvalidDocument");
            if (InvalidDocument) {
#if PY_MAJOR_VERSION >= 3
                PyObject* error = PyUnicode_FromFormat(DOC_TOO_LARGE_FMT,
                                                       cur_size, max_bson_size);
#else
                PyObject* error = PyString_FromFormat(DOC_TOO_LARGE_FMT,
                                                      cur_size, max_bson_size);
#endif
                if (error) {
                    PyErr_SetObject(InvalidDocument, error);
                    Py_DECREF(error);
                }
                Py_DECREF(InvalidDocument);
            }
            goto iterfail;
        }

        /* We have enough data, send this batch. */
        if (buffer_get_position(buffer) > max_message_size) {
            int new_request_id = rand();
            int message_start;
            PyObject* send_gle = Py_False;
            buffer_t new_buffer = buffer_new();
            if (!new_buffer) {
                PyErr_NoMemory();
                goto iterfail;
            }
            message_start = init_insert_buffer(new_buffer,
                                               new_request_id,
                                               options,
                                               collection_name,
                                               collection_name_length);
            if (message_start == -1) {
                buffer_free(new_buffer);
                goto iterfail;
            }

            /* Copy the overflow encoded document into the new buffer. */
            if (!buffer_write_bytes(new_buffer,
                (const char*)buffer_get_buffer(buffer) + before, cur_size)) {
                buffer_free(new_buffer);
                goto iterfail;
            }

            /* Roll back to the beginning of this document. */
            buffer_update_position(buffer, before);
            message_length = buffer_get_position(buffer) - length_location;
            memcpy(buffer_get_buffer(buffer) + length_location, &message_length, 4);

            /* If we are doing unacknowledged writes *and* continue_on_error
             * is True it's pointless (and slower) to send GLE. */
            if (safe || !continue_on_error) {
                send_gle = Py_True;
                if (!add_last_error(self, buffer, request_id, collection_name,
                                    collection_name_length, last_error_args)) {
                    buffer_free(new_buffer);
                    goto iterfail;
                }
            }
            /* Objectify buffer */
            result = Py_BuildValue("i" BYTES_FORMAT_STRING, request_id,
                                   buffer_get_buffer(buffer),
                                   buffer_get_position(buffer));
            buffer_free(buffer);
            buffer = new_buffer;
            request_id = new_request_id;
            length_location = message_start;

            send_message_result = PyObject_CallMethod(client, "_send_message",
                                                      "NO", result, send_gle);

            if (!send_message_result) {
                PyObject *etype = NULL, *evalue = NULL, *etrace = NULL;
                PyObject* OperationFailure;
                PyErr_Fetch(&etype, &evalue, &etrace);
                OperationFailure = _error("OperationFailure");
                if (OperationFailure) {
                    if (PyErr_GivenExceptionMatches(etype, OperationFailure)) {
                        if (!safe || continue_on_error) {
                            Py_DECREF(OperationFailure);
                            if (!safe) {
                                /* We're doing unacknowledged writes and
                                 * continue_on_error is False. Just return. */
                                Py_DECREF(etype);
                                Py_DECREF(evalue);
                                Py_DECREF(etrace);
                                Py_DECREF(iterator);
                                buffer_free(buffer);
                                PyMem_Free(collection_name);
                                Py_RETURN_NONE;
                            }
                            /* continue_on_error is True, store the error
                             * details to re-raise after the final batch */
                            Py_XDECREF(exc_type);
                            Py_XDECREF(exc_value);
                            Py_XDECREF(exc_trace);
                            exc_type = etype;
                            exc_value = evalue;
                            exc_trace = etrace;
                            continue;
                        }
                    }
                    Py_DECREF(OperationFailure);
                }
                /* This isn't OperationFailure, we couldn't
                 * import OperationFailure, or we are doing
                 * acknowledged writes. Re-raise immediately. */
                PyErr_Restore(etype, evalue, etrace);
                goto iterfail;
            } else {
                Py_DECREF(send_message_result);
            }
        }
    }
    Py_DECREF(iterator);

    if (PyErr_Occurred()) {
        goto insertfail;
    }

    if (!max_size) {
        PyObject* InvalidOperation = _error("InvalidOperation");
        if (InvalidOperation) {
            PyErr_SetString(InvalidOperation, "cannot do an empty bulk insert");
            Py_DECREF(InvalidOperation);
        }
        goto insertfail;
    }

    message_length = buffer_get_position(buffer) - length_location;
    memcpy(buffer_get_buffer(buffer) + length_location, &message_length, 4);

    if (safe) {
        if (!add_last_error(self, buffer, request_id, collection_name,
                            collection_name_length, last_error_args)) {
            goto insertfail;
        }
    }

    PyMem_Free(collection_name);

    /* objectify buffer */
    result = Py_BuildValue("i" BYTES_FORMAT_STRING, request_id,
                           buffer_get_buffer(buffer),
                           buffer_get_position(buffer));
    buffer_free(buffer);

    /* Send the last (or only) batch */
    send_message_result = PyObject_CallMethod(client, "_send_message", "NN",
                                              result,
                                              PyBool_FromLong((long)safe));

    if (!send_message_result) {
        Py_XDECREF(exc_type);
        Py_XDECREF(exc_value);
        Py_XDECREF(exc_trace);
        return NULL;
    } else {
        Py_DECREF(send_message_result);
    }

    if (exc_type) {
        /* Re-raise any previously stored exception
         * due to continue_on_error being True */
        PyErr_Restore(exc_type, exc_value, exc_trace);
        return NULL;
    }

    Py_RETURN_NONE;

iterfail:
    Py_DECREF(iterator);
insertfail:
    Py_XDECREF(exc_type);
    Py_XDECREF(exc_value);
    Py_XDECREF(exc_trace);
    buffer_free(buffer);
    PyMem_Free(collection_name);
    return NULL;
}

static PyMethodDef _CMessageMethods[] = {
    {"_insert_message", _cbson_insert_message, METH_VARARGS,
     _cbson_insert_message_doc},
    {"_update_message", _cbson_update_message, METH_VARARGS,
     "create an update message to be sent to MongoDB"},
    {"_query_message", _cbson_query_message, METH_VARARGS,
     "create a query message to be sent to MongoDB"},
    {"_get_more_message", _cbson_get_more_message, METH_VARARGS,
     "create a get more message to be sent to MongoDB"},
    {"_do_batched_insert", _cbson_do_batched_insert, METH_VARARGS,
     "insert a batch of documents, splitting the batch as needed"},
    {NULL, NULL, 0, NULL}
};

#if PY_MAJOR_VERSION >= 3
#define INITERROR return NULL
static int _cmessage_traverse(PyObject *m, visitproc visit, void *arg) {
    Py_VISIT(GETSTATE(m)->_cbson);
    return 0;
}

static int _cmessage_clear(PyObject *m) {
    Py_CLEAR(GETSTATE(m)->_cbson);
    return 0;
}

static struct PyModuleDef moduledef = {
        PyModuleDef_HEAD_INIT,
        "_cmessage",
        NULL,
        sizeof(struct module_state),
        _CMessageMethods,
        NULL,
        _cmessage_traverse,
        _cmessage_clear,
        NULL
};

PyMODINIT_FUNC
PyInit__cmessage(void)
#else
#define INITERROR return
PyMODINIT_FUNC
init_cmessage(void)
#endif
{
    PyObject *_cbson;
    PyObject *c_api_object;
    PyObject *m;
    struct module_state *state;

    /* Store a reference to the _cbson module since it's needed to call some
     * of its functions
     */
    _cbson = PyImport_ImportModule("bson._cbson");
    if (_cbson == NULL) {
        INITERROR;
    }

    /* Import C API of _cbson
     * The header file accesses _cbson_API to call the functions
     */
    c_api_object = PyObject_GetAttrString(_cbson, "_C_API");
    if (c_api_object == NULL) {
        Py_DECREF(_cbson);
        INITERROR;
    }
#if PY_VERSION_HEX >= 0x03010000
    _cbson_API = (void **)PyCapsule_GetPointer(c_api_object, "_cbson._C_API");
#else
    _cbson_API = (void **)PyCObject_AsVoidPtr(c_api_object);
#endif
    if (_cbson_API == NULL) {
        Py_DECREF(c_api_object);
        Py_DECREF(_cbson);
        INITERROR;
    }

#if PY_MAJOR_VERSION >= 3
    m = PyModule_Create(&moduledef);
#else
    m = Py_InitModule("_cmessage", _CMessageMethods);
#endif
    if (m == NULL) {
        Py_DECREF(c_api_object);
        Py_DECREF(_cbson);
        INITERROR;
    }

    state = GETSTATE(m);
    state->_cbson = _cbson;

    Py_DECREF(c_api_object);

#if PY_MAJOR_VERSION >= 3
    return m;
#endif
}
