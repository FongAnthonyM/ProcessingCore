#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" processingcore.py
Description:
"""
__author__ = "Anthony Fong"
__copyright__ = "Copyright 2020, Anthony Fong"
__credits__ = ["Anthony Fong"]
__license__ = ""
__version__ = "1.0.0"
__maintainer__ = "Anthony Fong"
__email__ = ""
__status__ = "Prototype"

# Default Libraries #
import asyncio
import multiprocessing
from multiprocessing import Process, Pool, Lock, Event, Queue, Pipe
import queue
import warnings
import time

# Downloaded Libraries #


# Local Libraries #


# Definitions #
# Classes #
class BroadcastPipe(object):
    # Construction/Destruction
    def __init__(self, name):
        self.name = name

        self.send_connections = {}
        self.recv_connections = {}

    # Pipe
    def create_pipe(self, name, duplex=True):
        self.send_connections[name], self.recv_connections[name] = Pipe(duplex=duplex)
        return self.send_connections[name], self.recv_connections[name]

    def set_connections(self, name, send, recv):
        self.send_connections[name] = send
        self.recv_connections[name] = recv

    def set_send_connection(self, name, send):
        self.send_connections[name] = send

    def set_recv_connection(self, name, recv):
        self.recv_connections[name] = recv

    def get_send_connection(self, name):
        return self.send_connections[name]

    def get_recv_connection(self, name):
        return self.recv_connections[name]

    # Object Query
    def poll(self):
        output = {}
        for name, connection in self.recv_connections.items():
            output[name] = connection.poll()
        return output

    def all_empty(self):
        for connection in self.recv_connections.values():
            if not connection.poll():
                return False
        return True

    def any_empty(self):
        for connection in self.recv_connections.values():
            if connection.poll():
                return True
        return False

    # Transmission
    def send(self, obj):
        for connection in self.send_connections.values():
            connection.send(obj)

    def send_bytes(self, obj, **kwargs):
        for connection in self.send_connections.values():
            connection.send_bytes(obj, **kwargs)

    def recv(self, name, poll=True, timeout=0.0):
        connection = self.recv_connections[name]
        if not poll or connection.poll(timeout=timeout):
            return connection.recv()
        else:
            return None

    def recv_wait(self, name, timeout=0.0, interval=0.0):
        connection = self.recv_connections[name]
        start_time = time.perf_counter()
        while not connection.poll():
            time.sleep(interval)
            if (time.perf_counter() - start_time) >= timeout:
                warnings.warn()
        return connection.recv()

    async def recv_wait_async(self, name, timeout=0.0, interval=0.0):
        connection = self.recv_connections[name]
        start_time = time.perf_counter()
        while not connection.poll():
            await asyncio.sleep(interval)
            if (time.perf_counter() - start_time) >= timeout:
                warnings.warn()
        return connection.recv()

    def recv_bytes(self, name, poll=True, timeout=0.0, **kwargs):
        connection = self.recv_connections[name]
        if not poll or connection.poll(timeout=timeout):
            return connection.recv(**kwargs)
        else:
            return None

    def recv_bytes_wait(self, name, timeout=0.0, interval=0.0, **kwargs):
        connection = self.recv_connections[name]
        start_time = time.perf_counter()
        while not connection.poll():
            time.sleep(interval)
            if (time.perf_counter() - start_time) >= timeout:
                warnings.warn()
        return connection.recv_bytes(**kwargs)

    async def recv_bytes_wait_async(self, name, timeout=0.0, interval=0.0, **kwargs):
        connection = self.recv_connections[name]
        start_time = time.perf_counter()
        while not connection.poll():
            await asyncio.sleep(interval)
            if (time.perf_counter() - start_time) >= timeout:
                warnings.warn()
        return connection.recv_bytes(**kwargs)

    def clear_recv(self, name):
        connection = self.recv_connections[name]
        while connection.poll:
            connection.recv()


class BroadcastQueue(object):
    # Construction/Destruction
    def __init__(self, name):
        self.name = name

        self.queues = {}

    # Queue
    def create_queue(self, name, maxsize=None):
        self.queues[name] = Queue(maxsize=maxsize)
        return self.queues[name]

    def set_queue(self, name, q):
        self.queues[name] = q

    def get_queue(self, name):
        return self.queues[name]

    # Object Query
    def qsize(self):
        output = {}
        for name, q in self.queues.items():
            output[name] = q.qsize()
        return output

    def empty(self):
        output = {}
        for name, q in self.queues.items():
            output[name] = q.empty()
        return output

    def all_empty(self):
        for q in self.queues.values():
            if not q.empty():
                return False
        return True

    def any_empty(self):
        for q in self.queues.values():
            if q.empty():
                return True
        return True

    def all_full(self):
        for q in self.queues.values():
            if not q.full():
                return False
        return True

    def any_full(self):
        for q in self.queues.values():
            if q.full():
                return True
        return False

    # Transmission
    def put(self, obj, block=False, timeout=0.0):
        for q in self.queues.values():
            try:
                q.put(obj, block=block, timeout=timeout)
            except queue.Full:
                pass  # add a warning here

    def get(self, name, block=True, timeout=0.0):
        return self.queues[name].get(block=block, timeout=timeout)

    def get_wait(self, name, timeout=0.0, interval=0.0):
        connection = self.queues[name]
        start_time = time.perf_counter()
        while connection.empty():
            time.sleep(interval)
            if (time.perf_counter() - start_time) >= timeout:
                warnings.warn()
        return connection.get()

    async def get_wait_async(self, name, timeout=0.0, interval=0.0):
        connection = self.queues[name]
        start_time = time.perf_counter()
        while connection.empty():
            await asyncio.sleep(interval)
            if (time.perf_counter() - start_time) >= timeout:
                warnings.warn()
        return connection.get()


class InputsHandler(object):
    # Construction/Destruction
    def __init__(self, name=""):
        self.name = name

        self.inputs = {}
        self.queues = {}
        self.pipes = {}
        self.broadcasters = {}

    def __getitem__(self, item):
        return self.inputs[item]

    # Queues
    def create_queue(self, name, maxsize=0):
        self.inputs[name] = Queue(maxsize=maxsize)
        self.queues[name] = self.inputs[name]
        return self.inputs[name]

    def add_queue(self, name, q):
        self.inputs[name] = q
        self.queues[name] = self.inputs[name]

    def clear_queues(self):
        for q in self.queues:
            del self.inputs[q]
        self.queues.clear()

    # Pipes
    def create_pipe(self, name, duplex=True):
        output, self.inputs[name] = Pipe(duplex=duplex)
        self.pipes[name] = self.inputs[name]
        return output

    def add_pipe(self, name, pipe):
        self.inputs[name] = pipe
        self.pipes[name] = self.inputs[name]

    def clear_pipes(self):
        for pipe in self.pipes:
            del self.inputs[pipe]
        self.pipes.clear()

    # Broadcasters
    def create_broadcast(self, name):
        broadcaster = BroadcastPipe(name=name)
        _, self.inputs[name] = broadcaster.create_pipe(name)
        self.broadcasters[name] = self.inputs[name]
        return broadcaster

    def add_broadcast(self, name, broadcaster):
        if isinstance(broadcaster, BroadcastPipe):
            self.inputs[name] = broadcaster.recv_connections(name)
        else:
            self.inputs[name] = broadcaster
        self.broadcasters = self.inputs[name]

    def clear_broadcasts(self):
        for broadcast in self.broadcasters:
            del self.inputs[broadcast]
        self.broadcasters.clear()

    # All
    def clear_all(self):
        self.inputs.clear()
        self.queues.clear()
        self.pipes.clear()
        self.broadcasters.clear()

    # Transmission
    def get_item(self, name, **kwargs):
        if name in self.broadcasters:
            return self.safe_pipe_recv(self.broadcasters[name], **kwargs)
        elif name in self.pipes:
            return self.safe_pipe_recv(self.pipes[name], **kwargs)
        elif name in self.queues:
            return self.queues[name].get(**kwargs)

    def get_item_wait(self, name, timeout=0.0, interval=0.0):
        if name in self.broadcasters:
            connection = self.broadcasters[name]
            start_time = time.perf_counter()
            while not connection.poll():
                time.sleep(interval)
                if (time.perf_counter() - start_time) >= timeout:
                    warnings.warn()
            return connection.recv()
        elif name in self.pipes:
            connection = self.pipes[name]
            start_time = time.perf_counter()
            while not connection.poll():
                time.sleep(interval)
                if (time.perf_counter() - start_time) >= timeout:
                    warnings.warn()
            return connection.recv()
        elif name in self.queues:
            connection = self.queues[name]
            start_time = time.perf_counter()
            while connection.empty():
                time.sleep(interval)
                if (time.perf_counter() - start_time) >= timeout:
                    warnings.warn()
            return connection.get()

    async def get_item_wait_async(self, name, timeout=0.0, interval=0.0):
        if name in self.broadcasters:
            connection = self.broadcasters[name]
            start_time = time.perf_counter()
            while not connection.poll():
                await asyncio.sleep(interval)
                if (time.perf_counter() - start_time) >= timeout:
                    warnings.warn()
            return connection.recv()
        elif name in self.pipes:
            connection = self.pipes[name]
            start_time = time.perf_counter()
            while not connection.poll():
                await asyncio.sleep(interval)
                if (time.perf_counter() - start_time) >= timeout:
                    warnings.warn()
            return connection.recv()
        elif name in self.queues:
            connection = self.queues[name]
            start_time = time.perf_counter()
            while connection.empty():
                await asyncio.sleep(interval)
                if (time.perf_counter() - start_time) >= timeout:
                    warnings.warn()
            return connection.get()

    @staticmethod
    def safe_pipe_recv(pipe, poll=True, timeout=None):
        if not poll or pipe.poll(timeout=timeout):
            return pipe.recv()
        else:
            return None


class OutputsHandler(object):
    # Construction/Destruction
    def __init__(self, name=""):
        self.name = name

        self.outputs = {}
        self.queues = {}
        self.pipes = {}
        self.broadcasters = {}

    def __getitem__(self, item):
        return self.outputs[item]

    # Queues
    def create_queue(self, name, maxsize=0):
        self.outputs[name] = Queue(maxsize=maxsize)
        self.queues[name] = self.outputs[name]
        return self.outputs[name]

    def add_queue(self, name, q):
        self.outputs[name] = q
        self.queues[name] = self.outputs[name]

    def clear_queues(self):
        for q in self.queues:
            del self.outputs[q]
        self.queues.clear()

    # Pipes
    def create_pipe(self, name, duplex=True):
        self.outputs[name], input_ = Pipe(duplex=duplex)
        self.pipes[name] = self.outputs[name]
        return input_

    def add_pipe(self, name, pipe):
        self.outputs[name] = pipe
        self.pipes[name] = self.outputs[name]

    def clear_pipes(self):
        for pipe in self.pipes:
            del self.outputs[pipe]
        self.pipes.clear()

    # Broadcasters
    def create_broadcast(self, name):
        broadcaster = BroadcastPipe(name=name)
        self.outputs[name] = broadcaster
        self.broadcasters[name] = self.outputs[name]
        return broadcaster

    def add_broadcast(self, name, broadcaster):
        self.outputs[name] = broadcaster
        self.broadcasters = self.outputs[name]

    def clear_broadcasts(self):
        for broadcast in self.broadcasters:
            del self.outputs[broadcast]
        self.broadcasters.clear()

    # All
    def clear_all(self):
        self.outputs.clear()
        self.queues.clear()
        self.pipes.clear()
        self.broadcasters.clear()

    # Transmission
    def send_item(self, name, item, **kwargs):
        if name in self.broadcasters:
            return self.broadcasters[name].send(item, **kwargs)
        elif name in self.pipes:
            return self.pipes[name].send(item, **kwargs)
        elif name in self.queues:
            return self.queues[name].put(item, **kwargs)
        else:
            warnings.warn()


class ProcessTask(object):
    # Construction/Destruction
    def __init__(self, name=None, allow_setup=True, init=True, kwargs={}):
        self.name = name
        self.kwargs = kwargs
        self.allow_setup = allow_setup
        self.stop_event = Event()
        self.events = {}
        self.locks = {}

        self.inputs = None
        self.outputs = None

        self._runtime_setup = self.setup
        self._runtime_task = self.task
        self.async_loop = asyncio.get_event_loop()

        if init:
            self.construct()

    # Constructors
    def construct(self):
        self.create_io()

    # IO
    def create_io(self):
        self.inputs = InputsHandler(name=self.name)
        self.outputs = OutputsHandler(name=self.name)

        self.inputs.create_queue("SelfStop")

    # Multiprocess Event
    def create_event(self, name):
        self.events[name] = Event()
        return self.events[name]

    def set_event(self, name, event):
        self.events[name] = event

    # Multiprocess Lock
    def create_lock(self, name):
        self.locks[name] = Lock()
        return self.locks[name]

    def set_lock(self, name, lock):
        self.locks[name] = lock

    # Task
    def setup(self):
        self.async_loop = asyncio.get_event_loop()

    def task(self, name=None):
        pass

    async def task_async(self, **kwargs):
        if kwargs:
            self.kwargs = kwargs
        return self._runtime_task(**self.kwargs)

    def task_loop(self, **kwargs):
        if kwargs:
            self.kwargs = kwargs
        while not self.stop_event.is_set():
            if not self.inputs.get_input_item("SelfStop"):
                self._runtime_task(**self.kwargs)
            else:
                self.stop_event.set()

    async def task_loop_async(self, **kwargs):
        while not self.stop_event.is_set():
            if not self.inputs.get_input_item("SelfStop"):
                packaged_task = asyncio.create_task(self.task_async(**kwargs))
                await packaged_task
            else:
                self.stop_event.set()

    # Execution
    def run(self, **kwargs):
        if self.allow_setup:
            self._runtime_setup()

        if kwargs:
            self.kwargs = kwargs
        self._runtime_task(**self.kwargs)

    async def run_async_coro(self, **kwargs):
        if self.allow_setup:
            self._runtime_setup()

        await self.task_async(**kwargs)

    def run_async(self, **kwargs):
        asyncio.run(self.run_async_coro(**kwargs))

    async def run_async_await(self, **kwargs):
        await self.run_async_coro(**kwargs)

    def run_async_task(self, **kwargs):
        return asyncio.create_task(self.run_async_coro(**kwargs))

    def start(self, **kwargs):
        if self.allow_setup:
            self._runtime_setup()
        self.task_loop(**kwargs)

    async def start_async_coro(self, **kwargs):
        if self.allow_setup:
            self._runtime_setup()
        await self.task_loop_async(**kwargs)

    def start_async(self, **kwargs):
        asyncio.run(self.start_async_coro(**kwargs))

    async def start_async_await(self, **kwargs):
        await self.start_async_coro(**kwargs)

    def start_async_task(self, **kwargs):
        return asyncio.create_task(self.start_async_coro(**kwargs))

    def reset(self):
        self.stop_event.clear()

    def stop(self):
        self.stop_event.set()


class MultiUnitTask(ProcessTask):
    # Construction/Destruction
    def __init__(self, name=None, allow_setup=True, init=True, kwargs={}):
        super().__init__(name, allow_setup, init=False, kwargs=kwargs)

        self._execution_order = []
        self.units = {}

        if init:
            self.construct()

    @property
    def execution_order(self):
        return self._execution_order

    @execution_order.setter
    def execution_order(self, value):
        if len(value) == len(self.units):
            self._execution_order.clear()
            self._execution_order.extend(value)
        else:
            warnings.warn()

    def keys(self):
        return self.units.keys()

    def values(self):
        return self.units.values()

    def items(self):
        return self.units.items()

    def append(self, name, unit, setup=False, process=False, kwargs={}):
        self.units[name] = {"unit": unit, "setup": setup, "kwargs": kwargs}

    def pop(self, name):
        return self.units.pop(name)

    def clear(self):
        self.units.clear()

    def setup(self):
        if self.execution_order is None:
            names = self.units
        else:
            names = self.execution_order
        for name in names:
            unit = self.units[name]
            if unit["setup"]:
                unit.allow_setup = False
                unit.setup()

    def task(self, name=None, asyn=""):
        if not asyn:
            self.units_run()
            return None
        elif asyn == "await":
            return self.units_async_await()
        else:
            return self.units_async_task()

    def units_run(self):
        if self.execution_order is None:
            names = self.units
        else:
            names = self.execution_order

        for name in names:
            unit = self.units[name]
            unit.run(**unit["kwargs"])

    async def units_async_await(self):
        if self.execution_order is None:
            names = self.units
        else:
            names = self.execution_order

        for name in names:
            unit = self.units[name]
            unit.run_async_await(**unit["kwargs"])

    async def units_async_task(self):
        tasks = []
        if self.execution_order is None:
            names = self.units
        else:
            names = self.execution_order

        for name in names:
            unit = self.units[name]
            tasks.append(unit.run_async_task(**unit["kwargs"]))
        for task in tasks:
            await task


class SeparateProcess(object):
    available_cpus = multiprocessing.cpu_count()

    # Construction/Destruction
    def __init__(self, target=None, name=None, daemon=False, init=False, kwargs={}):
        self._name = name
        self._daemon = daemon
        self._target = target
        self._target_kwargs = kwargs

        self._process = None

        if init:
            self.construct()

    @property
    def name(self):
        if self.process is not None:
            return self.process.name
        else:
            return self._name
        
    @name.setter
    def name(self, value):
        self._name = value
        if self.process is not None:
            self.process.name = value

    @property
    def daemon(self):
        if self.process is not None:
            return self.process.daemon
        else:
            return self._daemon

    @daemon.setter
    def daemon(self, value):
        self._daemon = value
        if self.process is not None:
            self.process.daemon = value

    @property
    def target(self):
        if self.process is not None:
            return self.process._target
        else:
            return self._target

    @target.setter
    def target(self, value):
        self._target = value
        if self.process is not None:
            self.process = Process(target=value, name=self.name, daemon=self.daemon, kwargs=self.target_kwargs)

    @property
    def target_kwargs(self):
        if self.process is not None:
            return self.process._kwargs
        else:
            return self._target_kwargs

    @target_kwargs.setter
    def target_kwargs(self, value):
        self._target_kwargs = value
        if self.process is not None:
            self.process = Process(target=self._target, name=self.name, daemon=self.daemon, kwargs=value)

    @property
    def is_alive(self):
        return self.process.is_alive()

    @property
    def process(self):
        return self._process

    @process.setter
    def process(self, value):
        self._process = value
        self._name = value.name
        self._daemon = value.daemon
        self._target = value._target
        self._target_kwargs = value._kwargs

    # Constructors
    def construct(self, target=None, daemon=False, **kwargs):
        self.create_process(target, daemon, **kwargs)

    # Process
    def create_process(self, target=None, daemon=False, **kwargs):
        if target is not None:
            self.target = target
        if kwargs:
            self.target_kwargs = kwargs
        if daemon is not None:
            self.daemon = daemon
        self.process = Process(target=self.target, name=self.name, daemon=self.daemon, kwargs=self.target_kwargs)

    def set_process(self, process):
        self.process = process

    # Execution
    def run(self):
        self.process.run()

    def start(self):
        self.process.start()

    def join(self):
        self.process.join()

    def restart(self):
        if isinstance(self.process, Process):
            if self.process.is_alive():
                self.terminate()
            self.process = Process(target=self.target, name=self.name, daemon=self.daemon, kwargs=self.target_kwargs)
        else:
            pass

    def terminate(self):
        self.process.terminate()

    def close(self):
        if isinstance(self.process, Process):
            if self.process.is_alive():
                self.terminate()
            self.process.close()


class ProcessingUnit(object):
    DEFAULT_TASK = ProcessTask

    # Construction/Destruction
    def __init__(self, name=None, allow_setup=True, separate_process=False, init=True):
        self.name = name
        self.separate_process = separate_process
        self._is_processing = False
        self.allow_setup = allow_setup

        self._runtime_setup = self.setup
        self.task = None
        self._runtime_task = None

        self.processing_pool = None
        self.process = None

        if init:
            self.construct(name=name)

    @property
    def is_processing(self):
        if self.process is not None and self.separate_process:
            self._is_processing = self.process.is_alive
        return self._is_processing

    @property
    def inputs(self):
        if self.task is not None:
            return self.task.inputs
        else:
            return None

    @inputs.setter
    def inputs(self, value):
        if self.task is not None:
            self.task.inputs = value
        else:
            raise NameError

    @property
    def outputs(self):
        if self.task is not None:
            return self.task.outputs
        else:
            return None

    @outputs.setter
    def outputs(self, value):
        if self.task is not None:
            self.task.outputs = value
        else:
            raise NameError

    # Constructors
    def construct(self, name=None):
        pass

    # Task
    def set_task(self, task):
        self.task = task

    # IO
    def create_io(self):
        self.task.create_io()

    # Process
    def create_process(self, name=None, daemon=False, kwargs={}):
        if name is None:
            name = self.name
        self.process = SeparateProcess(name=name, daemon=daemon, kwargs=kwargs)

    def set_process(self, process):
        self.process = process

    # Setup
    def setup(self):
        pass

    def use_task_setup(self):
        self.task.allow_setup = False
        self._runtime_setup = self.task.setup

    # Execution
    def run(self, **kwargs):
        if self.allow_setup:
            self._runtime_setup()

        if self.separate_process:
            self.process.create_process(self.task.run, kwargs=kwargs)
            self.process.start()
        else:
            self.task.run(**kwargs)

    async def run_async_coro(self, **kwargs):
        if self.allow_setup:
            self._runtime_setup()

        if self.separate_process:
            self.process.create_process(self.task.run_async_coro, kwargs=kwargs)
            self.process.start()
        else:
            self.task.run_async_coro(**kwargs)

    def run_async(self, **kwargs):
        asyncio.run(self.run_async_coro(**kwargs))

    async def run_async_await(self, **kwargs):
        await self.run_async_coro(**kwargs)

    def run_async_task(self, **kwargs):
        return asyncio.create_task(self.run_async_coro(**kwargs))

    def start(self, **kwargs):
        if self.allow_setup:
            self._runtime_setup()

        if self.separate_process:
            self.process.create_process(self.task.start, kwargs=kwargs)
            self.process.start()
        else:
            self.task.start(**kwargs)

    async def start_async_coro(self, **kwargs):
        if self.allow_setup:
            self._runtime_setup()

        if self.separate_process:
            self.process.create_process(self.task.start_async_coro, kwargs=kwargs)
            self.process.start()
        else:
            self.task.start_async_coro(**kwargs)

    def start_async(self, **kwargs):
        asyncio.run(self.start_async_coro(**kwargs))

    async def start_async_wait(self, **kwargs):
        await self.start_async_coro(**kwargs)

    def start_async_task(self, **kwargs):
        return asyncio.create_task(self.start_async_coro(**kwargs))

    def stop(self):
        if self.separate_process:
            self.task.stop()

    def join(self):
        if self.separate_process:
            self.process.join()

    def terminate(self):
        if self.separate_process:
            self.process.terminate()


class ProcessingCluster(ProcessingUnit):
    def __init__(self, name=None, init=True):
        super().__init__(name=name, init=False)

        if init:
            self.construct(name)

    @property
    def execution_order(self):
        return self.task.execution_order

    @execution_order.setter
    def execution_order(self, value):
        self.task.execution_order = value

    @property
    def units(self):
        return self.task.units

    @units.setter
    def units(self, value):
        self.task.units = value

    def construct(self, name=None):
        self.set_task(MultiUnitTask(name=name))

    def keys(self):
        return self.task.keys()

    def values(self):
        return self.task.values()

    def items(self):
        return self.task.items()

    def append(self, name, unit, setup=False, process=False):
        # {"unit": unit, "setup": setup, "process": process}
        self.task.units[name] = {"unit": unit, "setup": setup, "process": process}

    def pop(self, name):
        return self.task.pop(name)

    def clear(self):
        self.task.clear()


# Main #
if __name__ == "__main__":
    pass
