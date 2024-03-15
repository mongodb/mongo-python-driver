from __future__ import annotations

import asyncio
import functools
import queue
import threading
from concurrent.futures import wait
from typing import Optional, Type


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

    def close(self):
        for runner in self._runners:
            runner.close()
        self._runners = []


def synchronize(
    async_class: Optional[Type], async_method_name: Optional[str] = None, doc: Optional[str] = None
):
    """Decorate a given method so it runs a synchronous version of an identically-named asynchronous method.
    The method runs on an event loop.
    :Parameters:
     - 'async_class`:       The class to pull the asynchronous method from. Defaults to the calling class.
     - `async_method_name`: Optionally override the name of the async method.
     - `doc`:               Optionally override the async version of method's docstring.
    """

    def class_wrapper(method):
        @functools.wraps(method)
        def wrapped(self, *args, **kwargs):
            runner = TaskRunnerPool.getInstance()
            if async_class is not None:
                a_class = async_class
            else:
                a_class = self.__class__
            if async_method_name is not None:
                async_method = getattr(a_class, async_method_name)
            else:
                try:
                    async_method = getattr(a_class, method.__name__)
                except AttributeError:
                    raise

            if doc is not None:
                async_method.__doc__ = doc

            coro = async_method(self, *args, **kwargs)

            # If we're already running in a runner thread, just return the coroutine
            try:
                asyncio.get_running_loop()
                return coro
            except RuntimeError:
                return runner.run(coro)

        return wrapped

    return class_wrapper
