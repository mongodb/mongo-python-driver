cmake_minimum_required(VERSION 3.18...3.22)

project(pymongo LANGUAGES C)

find_package(Python COMPONENTS Interpreter Development.Module REQUIRED)

Python_add_library(_bson MODULE bson/_cbsonmodule.c WITH_SOABI)
include_directories(bson)
Python_add_library(_pymongo MODULE pymongo/_cmessagemodule.c WITH_SOABI)

install(TARGETS _bson LIBRARY DESTINATION bson)
install(TARGETS _pymongo LIBRARY DESTINATION pymongo)
