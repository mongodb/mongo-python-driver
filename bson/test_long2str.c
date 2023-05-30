#define PY_SSIZE_T_CLEAN
#include "Python.h"
#include "_cbsonmodule.h"
#include <time.h>

/* Test that new long_to_str function is same as old INT2STRING macro */

/* Just enough space to hold LLONG_MIN and null terminator */
#define BUF_SIZE 21

static void INT2STRING(long long i, char* buffer) {
    snprintf(buffer, BUF_SIZE, "%lld", i);
    return;
}

static void time_run(void (*f)(long long val, char* str), char* name) {
    // Time common values
    int reps = 1000;
    float startTime, endTime;
    startTime = (float)clock() / CLOCKS_PER_SEC;
    for (int i = 0; i < reps; i++) {
        for (Py_ssize_t num = 0; num < 10000; num++) {
            char str[BUF_SIZE];
            f((long long)num, str);
        }
    }
    endTime = (float)clock() / CLOCKS_PER_SEC;
    printf("%s: %f\n", name, endTime - startTime);
}

static PyObject* test(PyObject* self, PyObject* args) {
    // Test extreme values
    Py_ssize_t maxNum = PY_SSIZE_T_MAX;
    Py_ssize_t minNum = PY_SSIZE_T_MIN;
    char str_1[BUF_SIZE];
    char str_2[BUF_SIZE];
    long_to_str((long long)minNum, str_1);
    INT2STRING((long long)minNum, str_2);
    assert(strcmp(str_1, str_2) == 0);
    long_to_str((long long)maxNum, str_1);
    INT2STRING((long long)maxNum, str_2);
    assert(strcmp(str_1, str_2) == 0);

    // Test common values
    for (Py_ssize_t num = 0; num < 10000; num++) {
        char str_1[BUF_SIZE];
        char str_2[BUF_SIZE];
        long_to_str((long long)num, str_1);
        INT2STRING((long long)num, str_2);
        assert(strcmp(str_1, str_2) == 0);
    }

    // Time common values
    time_run(long_to_str, "long_to_str");
    time_run(INT2STRING, "INT2STRING");

    return args;
}

static PyMethodDef test_long2str_methods[] = {
    {"test", test, METH_VARARGS,
     "Test conversion of extreme and common Py_ssize_t values."},
    {NULL, NULL, 0, NULL}
};

static struct PyModuleDef moduledef = {
    PyModuleDef_HEAD_INIT,
    "test_long2str",
    NULL,
    -1,
    test_long2str_methods,
    NULL,
    NULL,
    NULL,
    NULL
};

PyMODINIT_FUNC
PyInit_test_long2str(void)
{
    PyObject *m;
    // Not needed but avoids compilation warning
    _cbson_API = NULL;
    m = PyModule_Create(&moduledef);
    if (m == NULL) {
        return NULL;
    }

    return m;
}
