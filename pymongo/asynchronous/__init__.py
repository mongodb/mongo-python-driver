from __future__ import annotations

import asyncio
import functools
import queue
import threading
from concurrent.futures import wait
from typing import Any, Optional


class TaskRunner:
    """A class that runs an event loop in a thread."""

    def __init__(self):
        self.__loop = asyncio.new_event_loop()
        self.__loop.set_debug(True)
        self.__loop_thread = threading.Thread(target=self._runner, daemon=True)
        self.__loop_thread.start()
        self.waiting = False
        self.lock = threading.Lock()
        self.errors = queue.Queue()

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
        with self.lock:
            self.waiting = True
        try:
            wait([fut])
            return fut.result()
        except Exception as e:
            self.errors.put(e)
        finally:
            with self.lock:
                self.waiting = False

    def schedule(self, coro):
        """Schedule a coroutine on the event loop as a task"""
        return asyncio.run_coroutine_threadsafe(coro, self.__loop)

    def check_errors(self):
        if self.errors.qsize():
            error = self.errors.get()
            if isinstance(error, StopAsyncIteration):
                raise StopIteration
            else:
                raise error


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
        self._semaphore = threading.Semaphore(1)
        self._runners: list[TaskRunner] = []

    def __del__(self):
        self.close()

    def run(self, coro):
        with self._semaphore:
            for runner in self._runners:
                with runner.lock:
                    waiting = runner.waiting
                if not waiting:
                    res = runner.run(coro)
                    runner.check_errors()
                    return res
            runner = TaskRunner()
            self._runners.append(runner)
            res = runner.run(coro)
            runner.check_errors()
            return res

    def schedule(self, coro):
        with self._semaphore:
            for runner in self._runners:
                return runner.schedule(coro)
            runner = TaskRunner()
            self._runners.append(runner)
            return runner.schedule(coro)

    def close(self):
        for runner in self._runners:
            runner.close()
        self._runners = []


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
                return wrapper_class.wrap(delegated)
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
                return wrapper_class.wrap(result)
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

            if wrapper_class:
                return wrapper_class.wrap(runner.run(coro))
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
