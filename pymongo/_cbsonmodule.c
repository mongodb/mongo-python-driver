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
#include <string.h>

static char* shuffle_oid(char* oid) {
  char* shuffled = (char*) malloc(12);
  int i;

  if (!shuffled) {
    PyErr_NoMemory();
    return NULL;
  }

  for (i = 0; i < 8; i++) {
    shuffled[i] = oid[7 - i];
  }
  for (i = 0; i < 4; i++) {
    shuffled[i + 8] = oid[11 - i];
  }

  return shuffled;
}

static PyObject* _cbson_shuffle_oid(PyObject* self, PyObject* args) {
  PyObject* result;
  char* data;
  char* shuffled;
  int length;

  if (!PyArg_ParseTuple(args, "s#", &data, &length)) {
    return NULL;
  }

  if (length != 12) {
    PyErr_SetString(PyExc_ValueError, "oid must be of length 12");
  }

  shuffled = shuffle_oid(data);
  if (!shuffled) {
    return NULL;
  }

  result = Py_BuildValue("s#", shuffled, 12);
  free(shuffled);
  return result;
}

static PyMethodDef _CBSONMethods[] = {
  {"_shuffle_oid", _cbson_shuffle_oid, METH_VARARGS,
   "shuffle an ObjectId into proper byte order."},
  {NULL, NULL, 0, NULL}
};

PyMODINIT_FUNC init_cbson(void) {
  (void) Py_InitModule("_cbson", _CBSONMethods);
}
