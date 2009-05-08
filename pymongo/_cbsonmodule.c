/*
 * Copyright 2009 10gen, Inc.
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
 * This file contains C implementations of some of the functions needed by the
 * bson module. If possible, these implementations should be used to speed up
 * BSON encoding and decoding.
 */

#include <Python.h>
#include <datetime.h>
#include <time.h>

static PyObject* CBSONError;
static PyObject* _cbson_dict_to_bson(PyObject* self, PyObject* dict);
static PyObject* SON;
static PyObject* Binary;
static PyObject* Code;
static PyObject* ObjectId;
static PyObject* DBRef;
static PyObject* RECompile;

#if PY_VERSION_HEX < 0x02050000 && !defined(PY_SSIZE_T_MIN)
typedef int Py_ssize_t;
#define PY_SSIZE_T_MAX INT_MAX
#define PY_SSIZE_T_MIN INT_MIN
#endif

#define INITIAL_BUFFER_SIZE 256

/* A buffer representing some data being encoded to BSON. */
typedef struct {
    char* buffer;
    int size;
    int position;
} bson_buffer;

static int write_dict(bson_buffer* buffer, PyObject* dict);
static PyObject* elements_to_dict(const char* string, int max);

static bson_buffer* buffer_new(void) {
    bson_buffer* buffer;
    buffer = (bson_buffer*)malloc(sizeof(bson_buffer));
    if (!buffer) {
        PyErr_NoMemory();
        return NULL;
    }
    buffer->size = INITIAL_BUFFER_SIZE;
    buffer->position = 0;
    buffer->buffer = (char*)malloc(INITIAL_BUFFER_SIZE);
    if (!buffer->buffer) {
        PyErr_NoMemory();
        return NULL;
    }
    return buffer;
}

static void buffer_free(bson_buffer* buffer) {
    if (buffer == NULL) {
        return;
    }
    free(buffer->buffer);
    free(buffer);
}

/* returns zero on failure */
static int buffer_resize(bson_buffer* buffer, int min_length) {
    int size = buffer->size;
    if (size >= min_length) {
        return 1;
    }
    while (size < min_length) {
        size *= 2;
    }
    buffer->buffer = (char*)realloc(buffer->buffer, size);
    if (!buffer->buffer) {
        PyErr_NoMemory();
        return 0;
    }
    buffer->size = size;
    return 1;
}

/* returns zero on failure */
static int buffer_assure_space(bson_buffer* buffer, int size) {
    if (buffer->position + size <= buffer->size) {
        return 1;
    }
    return buffer_resize(buffer, buffer->position + size);
}

/* returns offset for writing, or -1 on failure */
static int buffer_save_bytes(bson_buffer* buffer, int size) {
    if (!buffer_assure_space(buffer, size)) {
        return -1;
    }
    int position = buffer->position;
    buffer->position += size;
    return position;
}

/* returns zero on failure */
static int buffer_write_bytes(bson_buffer* buffer, const char* bytes, int size) {
    if (!buffer_assure_space(buffer, size)) {
        return 0;
    }
    memcpy(buffer->buffer + buffer->position, bytes, size);
    buffer->position += size;
    return 1;
}

static char* shuffle_oid(const char* oid) {
    char* shuffled = (char*) malloc(12);

    if (!shuffled) {
        PyErr_NoMemory();
        return NULL;
    }

    int i;
    for (i = 0; i < 8; i++) {
        shuffled[i] = oid[7 - i];
    }
    for (i = 0; i < 4; i++) {
        shuffled[i + 8] = oid[11 - i];
    }

    return shuffled;
}

/* returns 0 on failure */
static int write_shuffled_oid(bson_buffer* buffer, const char* oid) {
    int offset = buffer_save_bytes(buffer, 12);
    if (offset == -1) {
        return 0;
    }
    char* position = buffer->buffer + offset;
    int i;
    for (i = 0; i < 8; i++) {
        position[i] = oid[7 - i];
    }
    for (i = 0; i < 4; i++) {
        position[i + 8] = oid[11 - i];
    }
    return 1;
}

/* returns 0 on failure */
static int write_string(bson_buffer* buffer, PyObject* py_string) {
    const char* string = PyString_AsString(py_string);
    if (!string) {
        return 1;
    }
    int string_length = strlen(string) + 1;

    if (!buffer_write_bytes(buffer, (const char*)&string_length, 4)) {
        return 0;
    }
    if (!buffer_write_bytes(buffer, string, string_length)) {
        return 0;
    }
    return 1;
}

/* TODO our platform better be little-endian w/ 4-byte ints! */
/* returns 0 on failure */
static int write_element_to_buffer(bson_buffer* buffer, int type_byte, PyObject* value, int try_reload) {
    /* TODO this isn't quite the same as the Python version:
     * here we check for type equivalence, not isinstance in some
     * places. */
    if (PyInt_CheckExact(value)) {
        *(buffer->buffer + type_byte) = 0x10;
        const int int_value = (int)PyInt_AsLong(value);
        return buffer_write_bytes(buffer, (const char*)&int_value, 4);
    } else if (PyLong_CheckExact(value)) {
        *(buffer->buffer + type_byte) = 0x10;
        const int int_value = (int)PyLong_AsLong(value); // TODO handle overflow here
        return buffer_write_bytes(buffer, (const char*)&int_value, 4);
    } else if (PyBool_Check(value)) {
        *(buffer->buffer + type_byte) = 0x08;
        const long bool = PyInt_AsLong(value);
        const char c = bool ? 0x01 : 0x00;
        return buffer_write_bytes(buffer, &c, 1);
    } else if (PyFloat_CheckExact(value)) {
        *(buffer->buffer + type_byte) = 0x01;
        const double d = PyFloat_AsDouble(value);
        return buffer_write_bytes(buffer, (const char*)&d, 8);
    } else if (value == Py_None) {
        *(buffer->buffer + type_byte) = 0x0A;
        return 1;
    } else if (PyDict_Check(value)) {
        *(buffer->buffer + type_byte) = 0x03;
        return write_dict(buffer, value);
    } else if (PyList_CheckExact(value)) {
        *(buffer->buffer + type_byte) = 0x04;
        int start_position = buffer->position;

        // save space for length
        int length_location = buffer_save_bytes(buffer, 4);
        if (length_location == -1) {
            return 0;
        }


        int items = PyList_Size(value);
        int i;
        for(i = 0; i < items; i++) {
            int list_type_byte = buffer_save_bytes(buffer, 1);
            if (type_byte == -1) {
                return 0;
            }
            char* name;
            asprintf(&name, "%d", i);
            if (!name) {
                PyErr_NoMemory();
                return 0;
            }
            if (!buffer_write_bytes(buffer, name, strlen(name) + 1)) {
                free(name);
                return 0;
            }
            free(name);

            PyObject* item_value = PyList_GetItem(value, i);
            if (!write_element_to_buffer(buffer, list_type_byte, item_value, 1)) {
                return 0;
            }
        }

        // write null byte and fill in length
        char zero = 0;
        if (!buffer_write_bytes(buffer, &zero, 1)) {
            return 0;
        }
        int length = buffer->position - start_position;
        memcpy(buffer->buffer + length_location, &length, 4);
        return 1;
    } else if (PyObject_IsInstance(value, Binary)) {
        *(buffer->buffer + type_byte) = 0x05;
        PyObject* subtype_object = PyObject_GetAttrString(value, "subtype");
        if (!subtype_object) {
            return 0;
        }
        const long long_subtype = PyInt_AsLong(subtype_object);
        Py_DECREF(subtype_object);
        const char subtype = (const char)long_subtype;
        const int length = PyString_Size(value);
        if (subtype == 2) {
            const int other_length = length + 4;
            if (!buffer_write_bytes(buffer, (const char*)&other_length, 4)) {
                return 0;
            }
            if (!buffer_write_bytes(buffer, &subtype, 1)) {
                return 0;
            }
        }
        if (!buffer_write_bytes(buffer, (const char*)&length, 4)) {
            return 0;
        }
        if (subtype != 2) {
            if (!buffer_write_bytes(buffer, &subtype, 1)) {
                return 0;
            }
        }
        const char* string = PyString_AsString(value);
        if (!string) {
            return 0;
        }
        if (!buffer_write_bytes(buffer, string, length)) {
            return 0;
        }
        return 1;
    } else if (PyObject_IsInstance(value, Code)) {
        *(buffer->buffer + type_byte) = 0x0F;

        int start_position = buffer->position;
        // save space for length
        int length_location = buffer_save_bytes(buffer, 4);
        if (length_location == -1) {
            return 0;
        }

        if (!write_string(buffer, value)) {
            return 0;
        }

        PyObject* scope = PyObject_GetAttrString(value, "scope");
        if (!scope) {
            return 0;
        }
        if (!write_dict(buffer, scope)) {
            Py_DECREF(scope);
            return 0;
        }
        Py_DECREF(scope);

        int length = buffer->position - start_position;
        memcpy(buffer->buffer + length_location, &length, 4);
        return 1;
    } else if (PyString_Check(value)) {
        *(buffer->buffer + type_byte) = 0x02;
        // we have to do the encoding so we can fail fast if they give us non utf-8
        PyObject* encoded = PyString_AsEncodedObject(value, "utf-8", "strict");
        if (!encoded) {
            return 0;
        }
        int result = write_string(buffer, encoded);
        Py_DECREF(encoded);
        return result;
    } else if (PyUnicode_Check(value)) {
        *(buffer->buffer + type_byte) = 0x02;
        PyObject* encoded = PyUnicode_AsUTF8String(value);
        if (!encoded) {
            return 0;
        }
        int result = write_string(buffer, encoded);
        Py_DECREF(encoded);
        return result;
    } else if (PyDateTime_CheckExact(value)) {
        time_t rawtime;
        time(&rawtime);
        struct tm* timeinfo = localtime(&rawtime);
        timeinfo->tm_year = PyDateTime_GET_YEAR(value) - 1900;
        timeinfo->tm_mon = PyDateTime_GET_MONTH(value) - 1;
        timeinfo->tm_mday = PyDateTime_GET_DAY(value);
        timeinfo->tm_hour = PyDateTime_DATE_GET_HOUR(value);
        timeinfo->tm_min = PyDateTime_DATE_GET_MINUTE(value);
        timeinfo->tm_sec = PyDateTime_DATE_GET_SECOND(value);
        long long time_since_epoch = timegm(timeinfo);
        time_since_epoch = time_since_epoch * 1000;
        time_since_epoch += PyDateTime_DATE_GET_MICROSECOND(value) / 1000;

        *(buffer->buffer + type_byte) = 0x09;
        return buffer_write_bytes(buffer, (const char*)&time_since_epoch, 8);
    } else if (PyObject_IsInstance(value, ObjectId)) {
        PyObject* pystring = PyObject_Str(value);
        if (!pystring) {
            return 0;
        }
        const char* pre_shuffle = PyString_AsString(pystring);
        if (!pre_shuffle) {
            Py_DECREF(pystring);
            return 0;
        }
        if (!write_shuffled_oid(buffer, pre_shuffle)) {
            Py_DECREF(pystring);
            return 0;
        }
        Py_DECREF(pystring);
        *(buffer->buffer + type_byte) = 0x07;
        return 1;
    } else if (PyObject_IsInstance(value, DBRef)) {
        *(buffer->buffer + type_byte) = 0x03;
        int start_position = buffer->position;

        // save space for length
        int length_location = buffer_save_bytes(buffer, 4);
        if (length_location == -1) {
            return 0;
        }

        PyObject* collection_object = PyObject_GetAttrString(value, "collection");
        if (!collection_object) {
            return 0;
        }
        PyObject* encoded_collection = PyUnicode_AsUTF8String(collection_object);
        Py_DECREF(collection_object);
        if (!encoded_collection) {
            return 0;
        }
        const char* collection = PyString_AsString(encoded_collection);
        if (!collection) {
            Py_DECREF(encoded_collection);
            return 0;
        }
        PyObject* id_object = PyObject_GetAttrString(value, "id");
        if (!id_object) {
            Py_DECREF(encoded_collection);
            return 0;
        }

        if (!buffer_write_bytes(buffer, "\x02$ref\x00", 6)) {
            Py_DECREF(encoded_collection);
            Py_DECREF(id_object);
            return 0;
        }
        int collection_length = strlen(collection) + 1;
        if (!buffer_write_bytes(buffer, (const char*)&collection_length, 4)) {
            Py_DECREF(encoded_collection);
            Py_DECREF(id_object);
            return 0;
        }
        if (!buffer_write_bytes(buffer, collection, collection_length)) {
            Py_DECREF(encoded_collection);
            Py_DECREF(id_object);
            return 0;
        }
        Py_DECREF(encoded_collection);

        int type_pos = buffer_save_bytes(buffer, 1);
        if (type_pos == -1) {
            Py_DECREF(id_object);
            return 0;
        }
        if (!buffer_write_bytes(buffer, "$id\x00", 4)) {
            Py_DECREF(id_object);
            return 0;
        }
        if (!write_element_to_buffer(buffer, type_pos, id_object, 1)) {
            Py_DECREF(id_object);
            return 0;
        }
        Py_DECREF(id_object);

        // write null byte and fill in length
        char zero = 0;
        if (!buffer_write_bytes(buffer, &zero, 1)) {
            return 0;
        }
        int length = buffer->position - start_position;
        memcpy(buffer->buffer + length_location, &length, 4);
        return 1;
    }
    else if (PyObject_HasAttrString(value, "pattern") &&
               PyObject_HasAttrString(value, "flags")) { // TODO just a proxy for checking if it is a compiled re
        PyObject* py_flags = PyObject_GetAttrString(value, "flags");
        if (!py_flags) {
            return 0;
        }
        long int_flags = PyInt_AsLong(py_flags);
        Py_DECREF(py_flags);
        PyObject* py_pattern = PyObject_GetAttrString(value, "pattern");
        if (!py_pattern) {
            return 0;
        }
        const char* pattern =  PyString_AsString(py_pattern);
        char flags[7];
        flags[0] = 0;
        // TODO don't hardcode these
        if (int_flags & 2) {
            strcat(flags, "i");
        }
        if (int_flags & 4) {
            strcat(flags, "l");
        }
        if (int_flags & 8) {
            strcat(flags, "m");
        }
        if (int_flags & 16) {
            strcat(flags, "s");
        }
        if (int_flags & 32) {
            strcat(flags, "u");
        }
        if (int_flags & 64) {
            strcat(flags, "x");
        }
        int pattern_length = strlen(pattern) + 1;
        int flags_length = strlen(flags) + 1;

        if (!buffer_write_bytes(buffer, pattern, pattern_length)) {
            Py_DECREF(py_pattern);
            return 0;
        }
        Py_DECREF(py_pattern);
        if (!buffer_write_bytes(buffer, flags, flags_length)) {
            return 0;
        }
        *(buffer->buffer + type_byte) = 0x0B;
        return 1;
    } else if (try_reload) {
        // reload
        PyObject* binary_module = PyImport_ImportModule("pymongo.binary");
        Binary = PyObject_GetAttrString(binary_module, "Binary");
        Py_DECREF(binary_module);

        PyObject* code_module = PyImport_ImportModule("pymongo.code");
        Code = PyObject_GetAttrString(code_module, "Code");
        Py_DECREF(code_module);

        PyObject* objectid_module = PyImport_ImportModule("pymongo.objectid");
        ObjectId = PyObject_GetAttrString(objectid_module, "ObjectId");
        Py_DECREF(objectid_module);

        PyObject* dbref_module = PyImport_ImportModule("pymongo.dbref");
        DBRef = PyObject_GetAttrString(dbref_module, "DBRef");
        Py_DECREF(dbref_module);

        // try again, but just one more time
        return write_element_to_buffer(buffer, type_byte, value, 0);
    }

    PyObject* errmsg = PyString_FromString("Cannot encode object: ");
    PyObject* repr = PyObject_Repr(value);
    PyString_ConcatAndDel(&errmsg, repr);
    PyErr_SetString(CBSONError, PyString_AsString(errmsg));
    Py_DECREF(errmsg);
    return 0;
}

static int write_son(bson_buffer* buffer, PyObject* dict, int start_position, int length_location) {
    PyObject* keys = PyObject_CallMethod(dict, "keys", NULL);
    if (!keys) {
        return 0;
    }
    int items = PyList_Size(keys);
    int i;
    for(i = 0; i < items; i++) {
        PyObject* key = PyList_GetItem(keys, i);
        if (!key) {
            Py_DECREF(keys);
            return 0;
        }
        PyObject* value = PyDict_GetItem(dict, key);
        if (!value) {
            Py_DECREF(keys);
            return 0;
        }
        int type_byte = buffer_save_bytes(buffer, 1);
        if (type_byte == -1) {
            Py_DECREF(keys);
            return 0;
        }
        PyObject* encoded;
        if (PyUnicode_CheckExact(key)) {
            encoded = PyUnicode_AsUTF8String(key);
            if (!encoded) {
                Py_DECREF(keys);
                return 0;
            }
        } else {
            encoded = key;
            Py_INCREF(encoded);
        }
        Py_ssize_t name_length = PyString_Size(encoded);
        const char* name = PyString_AsString(encoded);
        if (!name) {
            Py_DECREF(keys);
            Py_DECREF(encoded);
            return 0;
        }
        if (!buffer_write_bytes(buffer, name, name_length + 1)) {
            Py_DECREF(keys);
            Py_DECREF(encoded);
            return 0;
        }
        Py_DECREF(encoded);
        if (!write_element_to_buffer(buffer, type_byte, value, 1)) {
            Py_DECREF(keys);
            return 0;
        }
    }
    Py_DECREF(keys);
    return 1;
}

/* returns 0 on failure */
static int write_dict(bson_buffer* buffer, PyObject* dict) {
    int start_position = buffer->position;

    // save space for length
    int length_location = buffer_save_bytes(buffer, 4);
    if (length_location == -1) {
        return 0;
    }

    if (PyDict_CheckExact(dict)) {
        PyObject* key;
        PyObject* value;
        Py_ssize_t pos = 0;
        while (PyDict_Next(dict, &pos, &key, &value)) {
            int type_byte = buffer_save_bytes(buffer, 1);
            if (type_byte == -1) {
                return 0;
            }
            PyObject* encoded;
            if (PyUnicode_CheckExact(key)) {
                encoded = PyUnicode_AsUTF8String(key);
                if (!encoded) {
                    return 0;
                }
            } else {
                encoded = key;
                Py_INCREF(encoded);
            }
            Py_ssize_t name_length = PyString_Size(encoded);
            const char* name = PyString_AsString(encoded);
            if (!name) {
                Py_DECREF(encoded);
                return 0;
            }
            if (!buffer_write_bytes(buffer, name, name_length + 1)) {
                Py_DECREF(encoded);
                return 0;
            }
            Py_DECREF(encoded);
            if (!write_element_to_buffer(buffer, type_byte, value, 1)) {
                return 0;
            }
        }
    } else if (PyObject_IsInstance(dict, SON)) {
        if (!write_son(buffer, dict, start_position, length_location)) {
            return 0;
        }
    } else {
        // Try getting the SON class again
        // This can be necessary if pymongo.son has been reloaded, since
        // reloading the module breaks IsInstance.
        PyObject* son_module = PyImport_ImportModule("pymongo.son");
        SON = PyObject_GetAttrString(son_module, "SON");
        Py_DECREF(son_module);
        if (PyObject_IsInstance(dict, SON)) {
            if (!write_son(buffer, dict, start_position, length_location)) {
                return 0;
            }
        } else {
            PyObject* errmsg = PyString_FromString("encoder expected a mapping type but got: ");
            PyObject* repr = PyObject_Repr(dict);
            PyString_ConcatAndDel(&errmsg, repr);
            PyErr_SetString(PyExc_TypeError, PyString_AsString(errmsg));
            Py_DECREF(errmsg);
            return 0;
        }
    }

    // write null byte and fill in length
    char zero = 0;
    if (!buffer_write_bytes(buffer, &zero, 1)) {
        return 0;
    }
    int length = buffer->position - start_position;
    memcpy(buffer->buffer + length_location, &length, 4);
    return 1;
}

static PyObject* _cbson_dict_to_bson(PyObject* self, PyObject* dict) {
    bson_buffer* buffer = buffer_new();
    if (!buffer) {
        return NULL;
    }

    if (!write_dict(buffer, dict)) {
        buffer_free(buffer);
        return NULL;
    }

     // objectify buffer
    PyObject* result = Py_BuildValue("s#", buffer->buffer, buffer->position);
    buffer_free(buffer);
    return result;
}

static PyObject* get_value(const char* buffer, int* position, int type) {
    PyObject* value;
    switch (type) {
    case 1:
        {
            double d;
            memcpy(&d, buffer + *position, 8);
            value = PyFloat_FromDouble(d);
            if (!value) {
                return NULL;
            }
            *position += 8;
            break;
        }
    case 2:
    case 13:
    case 14:
        {
            *position += 4;
            int value_length = strlen(buffer + *position);
            value = PyUnicode_DecodeUTF8(buffer + *position, value_length, "strict");
            if (!value) {
                return NULL;
            }
            *position += value_length + 1;
            break;
        }
    case 3:
        {
            int size;
            memcpy(&size, buffer + *position, 4);
            if (strcmp(buffer + *position + 5, "$ref") == 0) { // DBRef
                int offset = *position + 14;
                int collection_length = strlen(buffer + offset);
                PyObject* collection = PyUnicode_DecodeUTF8(buffer + offset, collection_length, "strict");
                if (!collection) {
                    return NULL;
                }
                offset += collection_length + 1;
                char id_type = buffer[offset];
                offset += 5;
                PyObject* id = get_value(buffer, &offset, (int)id_type);
                if (!id) {
                    Py_DECREF(collection);
                    return NULL;
                }
                value = PyObject_CallFunctionObjArgs(DBRef, collection, id, NULL);
                Py_DECREF(collection);
                Py_DECREF(id);
            } else {
                value = elements_to_dict(buffer + *position + 4, size - 5);
                if (!value) {
                    return NULL;
                }
            }
            *position += size;
            break;
        }
    case 4:
        {
            int size;
            memcpy(&size, buffer + *position, 4);
            int end = *position + size - 1;
            *position += 4;

            value = PyList_New(0);
            if (!value) {
                return NULL;
            }
            while (*position < end) {
                int type = (int)buffer[(*position)++];
                int key_size = strlen(buffer + *position);
                *position += key_size + 1; // just skip the key, they're in order.
                PyObject* to_append = get_value(buffer, position, type);
                if (!to_append) {
                    return NULL;
                }
                PyList_Append(value, to_append);
                Py_DECREF(to_append);
            }
            (*position)++;
            break;
        }
    case 5:
        {
            int length;
            memcpy(&length, buffer + *position, 4);
            int subtype = (unsigned char)buffer[*position + 4];
            PyObject* data;
            if (subtype == 2) {
                data = PyString_FromStringAndSize(buffer + *position + 9, length - 4);
            } else {
                data = PyString_FromStringAndSize(buffer + *position + 5, length);
            }
            if (!data) {
                return NULL;
            }
            PyObject* st = PyInt_FromLong(subtype);
            if (!st) {
                Py_DECREF(data);
                return NULL;
            }
            value = PyObject_CallFunctionObjArgs(Binary, data, st, NULL);
            Py_DECREF(st);
            Py_DECREF(data);
            if (!value) {
                return NULL;
            }
            *position += length + 5;
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
            char* shuffled = shuffle_oid(buffer + *position);
            value = PyObject_CallFunction(ObjectId, "s#", shuffled, 12);
            free(shuffled);
            if (!value) {
                return NULL;
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
            long long millis;
            memcpy(&millis, buffer + *position, 8);
            int microseconds = (millis % 1000) * 1000;
            time_t seconds = millis / 1000;
            struct tm* timeinfo = gmtime(&seconds);

            value = PyDateTime_FromDateAndTime(timeinfo->tm_year + 1900,
                                               timeinfo->tm_mon + 1,
                                               timeinfo->tm_mday,
                                               timeinfo->tm_hour,
                                               timeinfo->tm_min,
                                               timeinfo->tm_sec,
                                               microseconds);
            *position += 8;
            break;
        }
    case 11:
        {
            int pattern_length = strlen(buffer + *position);
            PyObject* pattern = PyUnicode_DecodeUTF8(buffer + *position, pattern_length, "strict");
            if (!pattern) {
                return NULL;
            }
            *position += pattern_length + 1;
            int flags_length = strlen(buffer + *position);
            int flags = 0;
            int i;
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
            *position += flags_length + 1;
            value = PyObject_CallFunction(RECompile, "Oi", pattern, flags);
            Py_DECREF(pattern);
            break;
        }
    case 12:
        {
            *position += 4;
            int collection_length = strlen(buffer + *position);
            PyObject* collection = PyUnicode_DecodeUTF8(buffer + *position, collection_length, "strict");
            if (!collection) {
                return NULL;
            }
            *position += collection_length + 1;
            char* shuffled = shuffle_oid(buffer + *position);
            PyObject* id = PyObject_CallFunction(ObjectId, "s#", shuffled, 12);
            free(shuffled);
            if (!id) {
                Py_DECREF(collection);
                return NULL;
            }
            *position += 12;
            value = PyObject_CallFunctionObjArgs(DBRef, collection, id, NULL);
            Py_DECREF(collection);
            Py_DECREF(id);
            break;
        }
    case 15:
        {
            *position += 8;
            int code_length = strlen(buffer + *position);
            PyObject* code = PyUnicode_DecodeUTF8(buffer + *position, code_length, "strict");
            if (!code) {
                return NULL;
            }
            *position += code_length + 1;

            int scope_size;
            memcpy(&scope_size, buffer + *position, 4);
            PyObject* scope = elements_to_dict(buffer + *position + 4, scope_size - 5);
            if (!scope) {
                Py_DECREF(code);
                return NULL;
            }
            *position += scope_size;

            value = PyObject_CallFunctionObjArgs(Code, code, scope, NULL);
            Py_DECREF(code);
            Py_DECREF(scope);
            break;
        }
    case 16:
        {
            int i;
            memcpy(&i, buffer + *position, 4);
            value = PyInt_FromLong(i);
            if (!value) {
                return NULL;
            }
            *position += 4;
            break;
        }
    case 17:
        {
            int i;
            int j;
            memcpy(&i, buffer + *position, 4);
            memcpy(&j, buffer + *position + 4, 4);
            value = Py_BuildValue("(ii)", i, j);
            if (!value) {
                return NULL;
            }
            *position += 8;
            break;
        }
    default:
        PyErr_SetString(CBSONError, "no c decoder for this type yet");
        return NULL;
    }
    return value;
}

static PyObject* elements_to_dict(const char* string, int max) {
    PyObject* dict = PyDict_New();
    if (!dict) {
        return NULL;
    }
    int position = 0;
    while (position < max) {
        int type = (int)string[position++];
        int name_length = strlen(string + position);
        PyObject* name = PyUnicode_DecodeUTF8(string + position, name_length, "strict");
        if (!name) {
            return NULL;
        }
        position += name_length + 1;
        PyObject* value = get_value(string, &position, type);
        if (!value) {
            return NULL;
        }

        PyDict_SetItem(dict, name, value);
        Py_DECREF(name);
        Py_DECREF(value);
    }
    return dict;
}

static PyObject* _cbson_bson_to_dict(PyObject* self, PyObject* bson) {
    if (!PyString_Check(bson)) {
        PyErr_SetString(PyExc_TypeError, "argument to _bson_to_dict must be a string");
        return NULL;
    }
    Py_ssize_t total_size = PyString_Size(bson);
    int size;
    const char* string = PyString_AsString(bson);
    if (!string) {
        return NULL;
    }
    memcpy(&size, string, 4);

    PyObject* dict = elements_to_dict(string + 4, size - 5);
    if (!dict) {
        return NULL;
    }
    PyObject* remainder = PyString_FromStringAndSize(string + size, total_size - size);
    if (!remainder) {
        Py_DECREF(dict);
        return NULL;
    }
    PyObject* result = Py_BuildValue("OO", dict, remainder);
    Py_DECREF(dict);
    Py_DECREF(remainder);
    return result;
}

static PyMethodDef _CBSONMethods[] = {
    {"_dict_to_bson", _cbson_dict_to_bson, METH_O,
     "convert a dictionary to a string containing it's BSON representation."},
    {"_bson_to_dict", _cbson_bson_to_dict, METH_O,
     "convert a BSON string to a SON object."},
    {NULL, NULL, 0, NULL}
};

PyMODINIT_FUNC init_cbson(void) {
    PyDateTime_IMPORT;
    PyObject *m;
    m = Py_InitModule("_cbson", _CBSONMethods);
    if (m == NULL) {
        return;
    }

    PyObject* errors_module = PyImport_ImportModule("pymongo.errors");
    CBSONError = PyObject_GetAttrString(errors_module, "InvalidDocument");
    Py_DECREF(errors_module);

    PyObject* son_module = PyImport_ImportModule("pymongo.son");
    SON = PyObject_GetAttrString(son_module, "SON");
    Py_DECREF(son_module);

    PyObject* binary_module = PyImport_ImportModule("pymongo.binary");
    Binary = PyObject_GetAttrString(binary_module, "Binary");
    Py_DECREF(binary_module);

    PyObject* code_module = PyImport_ImportModule("pymongo.code");
    Code = PyObject_GetAttrString(code_module, "Code");
    Py_DECREF(code_module);

    PyObject* objectid_module = PyImport_ImportModule("pymongo.objectid");
    ObjectId = PyObject_GetAttrString(objectid_module, "ObjectId");
    Py_DECREF(objectid_module);

    PyObject* dbref_module = PyImport_ImportModule("pymongo.dbref");
    DBRef = PyObject_GetAttrString(dbref_module, "DBRef");
    Py_DECREF(dbref_module);

    PyObject* re_module = PyImport_ImportModule("re");
    RECompile = PyObject_GetAttrString(re_module, "compile");
    Py_DECREF(re_module);
}
