from __future__ import annotations

import asyncio
import functools
import inspect
import threading
from typing import Any, Optional


class TaskRunner:
    """A class that runs an event loop in a thread."""

    def __init__(self):
        self.__loop = asyncio.new_event_loop()
        self.__loop_thread = threading.Thread(target=self._runner, daemon=True)
        self.__loop_thread.start()

    def close(self):
        if self.__loop and not self.__loop.is_closed():
            self.__loop.stop()

    def _runner(self):
        loop = self.__loop
        assert loop is not None
        try:
            loop.run_forever()
        finally:
            loop.stop()

    def run(self, coro):
        """Run a coroutine on the event loop and return the result"""
        fut = asyncio.run_coroutine_threadsafe(coro, self.__loop)
        try:
            return fut.result()
        except StopAsyncIteration as e:
            raise StopIteration from e

    def schedule(self, coro):
        """Schedule a coroutine on the event loop as a task"""
        return asyncio.run_coroutine_threadsafe(coro, self.__loop)


class TaskRunnerPool:
    """A singleton class that manages a pool of task runners."""

    __instance = None

    @staticmethod
    def getInstance():
        if TaskRunnerPool.__instance is None:
            TaskRunnerPool()
        assert TaskRunnerPool.__instance is not None
        return TaskRunnerPool.__instance

    def __init__(self):
        if TaskRunnerPool.__instance is not None:
            raise Exception("This class is a singleton!")
        else:
            TaskRunnerPool.__instance = self
        self._runner: TaskRunner = TaskRunner()
        self._semaphore = threading.Semaphore(1)

    def __del__(self):
        self.close()

    def run(self, coro):
        with self._semaphore:
            return self._runner.run(coro)

    def schedule(self, coro):
        with self._semaphore:
            return self._runner.schedule(coro)

    def close(self):
        self._runner.close()


def wrap_class(cls, wrapper, result):
    if isinstance(wrapper, str):
        return cls._wrap(result)
    else:
        return wrapper.wrap(result)


def delegate_property(*, wrapper_class: Optional[Any] = None):
    """Decorate a given property to delegate it to the delegate class.
    :Parameters:
     - `wrapper_class`:     An optional class to wrap around an asynchronous return type.
    """

    def class_wrapper(prop):
        @property
        @functools.wraps(prop)
        def wrapped(self):
            try:
                delegated = getattr(self._delegate, prop.__name__)
            except AttributeError:
                raise
            if wrapper_class:
                return wrap_class(self, wrapper_class, delegated)
            else:
                return delegated

        return wrapped

    return class_wrapper


def delegate_method(*, wrapper_class: Optional[Any] = None):
    """Decorate a given method to delegate it to the delegate class.
    :Parameters:
     - `wrapper_class`:     An optional class to wrap around an asynchronous return type.
    """

    def class_wrapper(method):
        @functools.wraps(method)
        def wrapped(self, *args, **kwargs):
            try:
                delegated = getattr(self._delegate, method.__name__)
            except AttributeError:
                raise

            result = delegated(*args, **kwargs)
            if wrapper_class:
                return wrap_class(self, wrapper_class, result)
            else:
                return result

        return wrapped

    return class_wrapper


def synchronize(
    *,
    wrapper_class: Optional[Any] = None,
    async_method_name: Optional[str] = None,
    doc: Optional[str] = None,
):
    """Decorate a given method so it runs a synchronous version of an identically-named asynchronous method.
    The method runs on an event loop.
    :Parameters:
     - `wrapper_class`:     An optional class to wrap around an asynchronous return type.
     - `async_method_name`: Optionally override the name of the async method.
     - `doc`:               Optionally override the async version of method's docstring.
    """

    def name_wrapper(method):
        @functools.wraps(method)
        def wrapped(self, *args, **kwargs):
            runner = TaskRunnerPool.getInstance()
            delegate = self._delegate if hasattr(self, "_delegate") else self
            if async_method_name is not None:
                async_method = getattr(delegate, async_method_name)
            else:
                try:
                    async_method = getattr(delegate, method.__name__)
                except AttributeError:
                    raise

            if doc is not None:
                async_method.__doc__ = doc

            coro = async_method(*args, **kwargs)

            if not inspect.isawaitable(coro):
                raise RuntimeError(f"Tried to synchronize an already synchronous function: {coro}")

            if wrapper_class:
                return wrap_class(self, wrapper_class, runner.run(coro))
            else:
                return runner.run(coro)

        return wrapped

    return name_wrapper


def schedule_task(coroutine):
    try:
        asyncio.get_running_loop()
        return coroutine
    except RuntimeError:
        runner = TaskRunnerPool.getInstance()
        runner.schedule(coroutine)
