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
static PyObject* _wrap_py_string_as_object(PyObject* string);
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

static PyObject* build_element(const char type, const char* name, const int length, const char* data) {
    int name_length = strlen(name) + 1;
    int built_length = 1 + name_length + length;
    char* built = (char*)malloc(built_length);
    if (!built) {
        PyErr_NoMemory();
        return NULL;
    }

    built[0] = type;
    memcpy(built + 1, name, name_length);
    memcpy(built + 1 + name_length, data, length);

    PyObject* result = Py_BuildValue("s#", built, built_length);
    free(built);
    return result;
}

static PyObject* build_string(int type, PyObject* py_string, const char* name) {
    const char* string = PyString_AsString(py_string);
    if (!string) {
        return NULL;
    }
    int string_length = strlen(string) + 1;
    int data_length = 4 + string_length;

    char* data = (char*)malloc(data_length);
    if (!data) {
        PyErr_NoMemory();
        return NULL;
    }
    memcpy(data, &string_length, 4);
    memcpy(data + 4, string, string_length);

    PyObject* result = build_element(type, name, data_length, data);
    free(data);
    return result;
}

/* TODO our platform better be little-endian w/ 4-byte ints! */
static PyObject* _cbson_element_to_bson(PyObject* self, PyObject* args) {
    const char* name;
    PyObject* value;
    if (!PyArg_ParseTuple(args, "etO", "utf-8", &name, &value)) {
        return NULL;
    }

    /* TODO this isn't quite the same as the Python version:
     * here we check for type equivalence, not isinstance in some
     * places. */
    if (PyString_CheckExact(value)) {
        // we have to do the encoding so we can fail fast if they give us non utf-8
        PyObject* encoded = PyString_AsEncodedObject(value, "utf-8", "strict");
        if (!encoded) {
            return NULL;
        }
        PyObject* result = build_string(0x02, encoded, name);
        Py_DECREF(encoded);
        return result;
    } else if (PyUnicode_CheckExact(value)) {
        PyObject* encoded = PyUnicode_AsUTF8String(value);
        if (!encoded) {
            return NULL;
        }
        PyObject* result = build_string(0x02, encoded, name);
        Py_DECREF(encoded);
        return result;
    } else if (PyInt_CheckExact(value)) {
        int int_value = (int)PyInt_AsLong(value);
        return build_element(0x10, name, 4, (char*)&int_value);
    } else if (PyLong_CheckExact(value)) {
        int int_value = (int)PyLong_AsLong(value); // TODO handle overflow here
        return build_element(0x10, name, 4, (char*)&int_value);
    } else if (PyBool_Check(value)) {
        long bool = PyInt_AsLong(value);
        char c = bool ? 0x01 : 0x00;
        return build_element(0x08, name, 1, &c);
    } else if (PyFloat_CheckExact(value)) {
        double d = PyFloat_AsDouble(value);
        return build_element(0x01, name, 8, (char*)&d);
    } else if (value == Py_None) {
        return build_element(0x0A, name, 0, 0);
    } else if (PyDict_Check(value)) {
        PyObject* object = _cbson_dict_to_bson(self, value);
        if (!object) {
            return NULL;
        }
        PyObject* result = build_element(0x03, name, PyString_Size(object), PyString_AsString(object));
        Py_DECREF(object);
        return result;
    } else if (PyList_CheckExact(value)) {
        PyObject* string = PyString_FromString("");
        int items = PyList_Size(value);
        int i;
        for(i = 0; i < items; i++) {
            char* name;
            asprintf(&name, "%d", i);
            if (!name) {
                Py_DECREF(string);
                PyErr_NoMemory();
                return NULL;
            }
            PyObject* args = Py_BuildValue("sO", name, PyList_GetItem(value, i));
            free(name);
            if (!args) {
                Py_DECREF(string);
                return NULL;
            }
            PyObject* element = _cbson_element_to_bson(self, args);
            Py_DECREF(args);
            if (!element) {
                Py_DECREF(string);
                return NULL;
            }
            PyString_ConcatAndDel(&string, element);
        }
        PyObject* object = _wrap_py_string_as_object(string);
        PyObject* result = build_element(0x04, name, PyString_Size(object), PyString_AsString(object));
        Py_DECREF(object);
        return result;
    } else if (PyObject_IsInstance(value, Binary)) {
        PyObject* subtype_object = PyObject_CallMethod(value, "subtype", NULL);
        if (!subtype_object) {
            return NULL;
        }
        long subtype = PyInt_AsLong(subtype_object);
        Py_DECREF(subtype_object);
        PyObject* string;
        int length = PyString_Size(value);
        if (subtype == 2) {
            string = PyString_FromStringAndSize((char*)&length, 4);
            PyString_Concat(&string, value);
            length += 4;
        } else {
            string = value;
            Py_INCREF(string);
        }
        char* data = malloc(5 + length);
        if (!data) {
            Py_DECREF(string);
            PyErr_NoMemory();
            return NULL;
        }
        const char* string_data = PyString_AsString(string);
        if (!string_data) {
            Py_DECREF(string);
            free(data);
            return NULL;
        }
        memcpy(data, &length, 4);
        data[4] = (char)subtype;
        memcpy(data + 5, string_data, length);
        Py_DECREF(string);
        PyObject* result = build_element(0x05, name, length + 5, data);
        free(data);
        return result;
    } else if (PyObject_IsInstance(value, Code)) {
        return build_string(0x0D, value, name);
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
        return build_element(0x09, name, 8, (char*)&time_since_epoch);
    } else if (PyObject_IsInstance(value, ObjectId)) {
        PyObject* pystring = PyObject_Str(value);
        if (!pystring) {
            return NULL;
        }
        const char* pre_shuffle = PyString_AsString(pystring);
        if (!pre_shuffle) {
            Py_DECREF(pystring);
            return NULL;
        }
        char* shuffled = shuffle_oid(pre_shuffle);
        Py_DECREF(pystring);
        PyObject* result = build_element(0x07, name, 12, shuffled);
        free(shuffled);
        return result;
    } else if (PyObject_IsInstance(value, DBRef)) {
        PyObject* collection_object = PyObject_CallMethod(value, "collection", NULL);
        if (!collection_object) {
            return NULL;
        }
        PyObject* encoded_collection = PyUnicode_AsUTF8String(collection_object);
        Py_DECREF(collection_object);
        if (!encoded_collection) {
            return NULL;
        }
        const char* collection = PyString_AsString(encoded_collection);
        if (!collection) {
            Py_DECREF(encoded_collection);
            return NULL;
        }
        PyObject* id_object = PyObject_CallMethod(value, "id", NULL);
        if (!id_object) {
            Py_DECREF(encoded_collection);
            return NULL;
        }
        PyObject* id_str = PyObject_Str(id_object);
        Py_DECREF(id_object);
        if (!id_str) {
            Py_DECREF(encoded_collection);
            return NULL;
        }
        const char* id = PyString_AsString(id_str);
        if (!id) {
            Py_DECREF(encoded_collection);
            Py_DECREF(id_str);
            return NULL;
        }
        char* shuffled = shuffle_oid(id);
        Py_DECREF(id_str);

        int collection_length = strlen(collection) + 1;
        char* data = (char*)malloc(4 + collection_length + 12);
        if (!data) {
            Py_DECREF(encoded_collection);
            free(shuffled);
            PyErr_NoMemory();
            return NULL;
        }
        memcpy(data, &collection_length, 4);
        memcpy(data + 4, collection, collection_length);
        Py_DECREF(encoded_collection);
        memcpy(data + 4 + collection_length, shuffled, 12);
        free(shuffled);

        PyObject* result = build_element(0x0C, name, 16 + collection_length, data);
        free(data);
        return result;
    } else if (PyObject_HasAttrString(value, "pattern") &&
               PyObject_HasAttrString(value, "flags")) { // TODO just a proxy for checking if it is a compiled re
        PyObject* py_flags = PyObject_GetAttrString(value, "flags");
        if (!py_flags) {
            return NULL;
        }
        long int_flags = PyInt_AsLong(py_flags);
        Py_DECREF(py_flags);
        PyObject* py_pattern = PyObject_GetAttrString(value, "pattern");
        if (!py_pattern) {
            return NULL;
        }
        const char* pattern =  PyString_AsString(py_pattern);
        char flags[10];
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
        char* data = malloc(pattern_length + flags_length);
        if (!data) {
            Py_DECREF(py_pattern);
            PyErr_NoMemory();
            return NULL;
        }
        memcpy(data, pattern, pattern_length);
        Py_DECREF(py_pattern);
        memcpy(data + pattern_length, flags, flags_length);
        PyObject* result = build_element(0x0B, name, pattern_length + flags_length, data);
        free(data);
        return result;
    }
    PyErr_SetString(CBSONError, "no c encoder for this type yet");
    return NULL;
}

static PyObject* _wrap_py_string_as_object(PyObject* string) {
    int length = PyString_Size(string) + 5;
    char* data = (char*)malloc(length);
    if (!data) {
        Py_DECREF(string);
        PyErr_NoMemory();
        return NULL;
    }
    const char* elements = PyString_AsString(string);
    memcpy(data, &length, 4);
    memcpy(data + 4, elements, length - 5);
    data[length - 1] = 0x00;
    Py_DECREF(string);

    PyObject* result = Py_BuildValue("s#", data, length);
    free(data);
    return result;
}

static PyObject* _cbson_dict_to_bson(PyObject* self, PyObject* dict) {
    if (PyDict_CheckExact(dict)) {
        PyObject* key;
        PyObject* value;
        Py_ssize_t pos = 0;
        PyObject* string = PyString_FromString("");
        if (!string) {
            return NULL;
        }
        while (PyDict_Next(dict, &pos, &key, &value)) {
            PyObject* args = Py_BuildValue("OO", key, value);
            if (!args) {
                return NULL;
            }
            PyObject* element = _cbson_element_to_bson(self, args);
            if (!element) {
                return NULL;
            }
            PyString_ConcatAndDel(&string, element);
            Py_DECREF(args);
        }
        return _wrap_py_string_as_object(string);
    } else if (PyObject_IsInstance(dict, SON)) {
        PyObject* string = PyString_FromString("");
        if (!string) {
            return NULL;
        }
        PyObject* keys = PyObject_CallMethod(dict, "keys", NULL);
        if (!keys) {
            Py_DECREF(string);
            return NULL;
        }
        int items = PyList_Size(keys);
        int i;
        for(i = 0; i < items; i++) {
            PyObject* name = PyList_GetItem(keys, i);
            if (!name) {
                Py_DECREF(string);
                Py_DECREF(keys);
                return NULL;
            }
            PyObject* args = Py_BuildValue("OO", name, PyDict_GetItem(dict, name));
            if (!args) {
                Py_DECREF(string);
                Py_DECREF(keys);
                return NULL;
            }
            PyObject* element = _cbson_element_to_bson(self, args);
            Py_DECREF(args);
            if (!element) {
                Py_DECREF(string);
                Py_DECREF(keys);
                return NULL;
            }
            PyString_ConcatAndDel(&string, element);
        }
        Py_DECREF(keys);
        return _wrap_py_string_as_object(string);
    }
    PyErr_SetString(PyExc_TypeError, "argument to from_dict must be a mapping type");
    return NULL;
}

static PyObject* _elements_to_dict(PyObject* elements) {
    PyObject* dict = PyDict_New();
    if (!dict) {
        return NULL;
    }
    const char* string = PyString_AsString(elements);
    int max = PyString_Size(elements);
    int position = 0;
    while (position < max) {
        int type = (int)string[position++];
        int name_length = strlen(string + position);
        PyObject* name = PyUnicode_DecodeUTF8(string + position, name_length, "strict");
        if (!name) {
            return NULL;
        }
        position += name_length + 1;
        PyObject* value;
        switch (type) {
        case 1:
            {
                double d;
                memcpy(&d, string + position, 8);
                value = PyFloat_FromDouble(d);
                if (!value) {
                    return NULL;
                }
                position += 8;
                break;
            }
        case 2:
        case 13:
        case 14:
            {
                position += 4;
                int value_length = strlen(string + position);
                value = PyUnicode_DecodeUTF8(string + position, value_length, "strict");
                if (!value) {
                    return NULL;
                }
                position += value_length + 1;
                break;
            }
        case 3:
            {
                int size;
                memcpy(&size, string + position, 4);
                PyObject* array_elements = PyString_FromStringAndSize(string + position + 4, size - 5);
                if (!array_elements) {
                    return NULL;
                }
                value = _elements_to_dict(array_elements);
                Py_DECREF(array_elements);
                if (!value) {
                    return NULL;
                }
                position += size;
                break;
            }
        case 4:
            {
                int size;
                memcpy(&size, string + position, 4);
                PyObject* array_elements = PyString_FromStringAndSize(string + position + 4, size - 5);
                if (!array_elements) {
                    return NULL;
                }
                PyObject* array_dict = _elements_to_dict(array_elements);
                Py_DECREF(array_elements);
                if (!array_dict) {
                    return NULL;
                }
                position += size;

                value = PyList_New(0);
                int length = PyDict_Size(array_dict);
                int i;
                for (i = 0; i < length; i++) {
                    char* key;
                    asprintf(&key, "%d", i);
                    if (!key) {
                        Py_DECREF(array_dict);
                        PyErr_NoMemory();
                        return NULL;
                    }
                    PyObject* to_append = PyDict_GetItemString(array_dict, key);
                    free(key);
                    if (!to_append) {
                        Py_DECREF(array_dict);
                        return NULL;
                    }
                    PyList_Append(value, to_append);
                }
                Py_DECREF(array_dict);
                break;
            }
        case 5:
            {
                int length;
                memcpy(&length, string + position, 4);
                int subtype = (unsigned char)string[position + 4];
                PyObject* data;
                if (subtype == 2) {
                    data = PyString_FromStringAndSize(string + position + 9, length - 4);
                } else {
                    data = PyString_FromStringAndSize(string + position + 5, length);
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
                position += length + 5;
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
                char* shuffled = shuffle_oid(string + position);
                value = PyObject_CallFunction(ObjectId, "s#", shuffled, 12);
                free(shuffled);
                if (!value) {
                    return NULL;
                }
                position += 12;
                break;
            }
        case 8:
            {
                value = string[position++] ? Py_True : Py_False;
                Py_INCREF(value);
                break;
            }
        case 9:
            {
                long long millis;
                memcpy(&millis, string + position, 8);
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
                position += 8;
                break;
            }
        case 11:
            {
                int pattern_length = strlen(string + position);
                PyObject* pattern = PyUnicode_DecodeUTF8(string + position, pattern_length, "strict");
                if (!pattern) {
                    return NULL;
                }
                position += pattern_length + 1;
                int flags_length = strlen(string + position);
                int flags = 0;
                int i;
                for (i = 0; i < flags_length; i++) {
                    if (string[position + i] == 'i') {
                        flags |= 2;
                    } else if (string[position + i] == 'l') {
                        flags |= 4;
                    } else if (string[position + i] == 'm') {
                        flags |= 8;
                    } else if (string[position + i] == 's') {
                        flags |= 16;
                    } else if (string[position + i] == 'u') {
                        flags |= 32;
                    } else if (string[position + i] == 'x') {
                        flags |= 64;
                    }
                }
                position += flags_length + 1;
                value = PyObject_CallFunction(RECompile, "Oi", pattern, flags);
                Py_DECREF(pattern);
                break;
            }
        case 12:
            {
                position += 4;
                int collection_length = strlen(string + position);
                PyObject* collection = PyUnicode_DecodeUTF8(string + position, collection_length, "strict");
                if (!collection) {
                    return NULL;
                }
                position += collection_length + 1;
                char* shuffled = shuffle_oid(string + position);
                PyObject* id = PyObject_CallFunction(ObjectId, "s#", shuffled, 12);
                free(shuffled);
                if (!id) {
                    Py_DECREF(collection);
                    return NULL;
                }
                position += 12;
                value = PyObject_CallFunctionObjArgs(DBRef, collection, id, NULL);
                Py_DECREF(collection);
                Py_DECREF(id);
                break;
            }
        case 16:
            {
                int i;
                memcpy(&i, string + position, 4);
                value = PyInt_FromLong(i);
                if (!value) {
                    return NULL;
                }
                position += 4;
                break;
            }
        default:
            PyErr_SetString(CBSONError, "no c decoder for this type yet");
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
    int size;
    const char* string = PyString_AsString(bson);
    if (!string) {
        return NULL;
    }
    memcpy(&size, string, 4);

    PyObject* elements = PySequence_GetSlice(bson, 4, size - 1);
    if (!elements) {
        return NULL;
    }
    PyObject* dict = _elements_to_dict(elements);
    Py_DECREF(elements);
    if (!dict) {
        return NULL;
    }
    PyObject* remainder = PySequence_GetSlice(bson, size, PyString_Size(bson));
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

    CBSONError = PyErr_NewException("_cbson.error", NULL, NULL);
    Py_INCREF(CBSONError);
    PyModule_AddObject(m, "error", CBSONError);

    PyObject* son_module = PyImport_ImportModule("pymongo.son");
    SON = PyObject_GetAttrString(son_module, "SON");

    PyObject* binary_module = PyImport_ImportModule("pymongo.binary");
    Binary = PyObject_GetAttrString(binary_module, "Binary");

    PyObject* code_module = PyImport_ImportModule("pymongo.code");
    Code = PyObject_GetAttrString(code_module, "Code");

    PyObject* objectid_module = PyImport_ImportModule("pymongo.objectid");
    ObjectId = PyObject_GetAttrString(objectid_module, "ObjectId");

    PyObject* dbref_module = PyImport_ImportModule("pymongo.dbref");
    DBRef = PyObject_GetAttrString(dbref_module, "DBRef");

    PyObject* re_module = PyImport_ImportModule("re");
    RECompile = PyObject_GetAttrString(re_module, "compile");
}
