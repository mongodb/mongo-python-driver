cmake_minimum_required(VERSION 3.18...3.22)

project(pymongo LANGUAGES C)

find_package(Python COMPONENTS Interpreter Development.Module REQUIRED)

Python_add_library(_cbson MODULE bson/_cbsonmodule.c bson/buffer.c bson/time64.c WITH_SOABI)
include_directories(bson)
Python_add_library(_cmessage MODULE pymongo/_cmessagemodule.c bson/_cbsonmodule.c bson/buffer.c bson/time64.c WITH_SOABI)

install(TARGETS _cbson LIBRARY DESTINATION bson)
install(TARGETS _cmessage LIBRARY DESTINATION pymongo)
