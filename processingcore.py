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
class Interrupt(object):
    def __init__(self, master=None):
        self.master = master
        self.event = Event()

    def __bool__(self):
        return self.check()

    def check(self):
        if self.master:
            self.event.set()
        return self.event.is_set()

    def set(self):
        self.event.set()

    def reset(self):
        self.event.clear()


class Interrupts(object):
    # Construction/Destruction
    def __init__(self, **kwargs):
        self.master_interrupt = Interrupt()
        self.data = {**kwargs}

    # Container Methods
    def __len__(self):
        return len(self.data)

    def __getitem__(self, item):
        return self.get(item)

    def __setitem__(self, key, value):
        self.data[key] = value

    def __delitem__(self, key):
        del self.data[key]

    # Methods
    def add(self, name):
        if name not in self.data:
            self.data[name] = Interrupt(master=self.master_interrupt)
        return self.data[name]

    def set(self, name, interrupt=None):
        if interrupt is None:
            self.data[name] = Interrupt(self.master_interrupt)
        else:
            self.data[name] = interrupt
        return self.data[name]

    def remove(self, name):
        del self.data[name]

    def pop(self, k, d=None):
        return self.data.pop(k, d)

    def popitem(self):
        return self.data.popitem()

    def clear(self):
        self.data.clear()

    def get(self, name):
        return self.data[name]

    def items(self):
        return self.data.items()

    def keys(self):
        return self.data.keys()

    def values(self):
        return self.data.values()

    def check(self, name):
        return bool(self.get(name))

    def interrupt(self, name):
        self.data[name].set()

    def interrupt_all(self):
        for interrupt in self.data:
            interrupt.set()

    def interrupt_all_processes(self):
        self.master_interrupt.set()

    def reset(self, name):
        self.data[name].reset()

    def reset_all(self):
        for interrupt in self.data:
            interrupt.reset()

    def reset_all_processes(self):
        self.master_interrupt.reset()


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

    def recv_wait(self, name, timeout=None, interval=0.0):
        connection = self.recv_connections[name]
        start_time = time.perf_counter()
        while not connection.poll():
            time.sleep(interval)
            if timeout is not None and (time.perf_counter() - start_time) >= timeout:
                warnings.warn()
                return None
        return connection.recv()

    async def recv_wait_async(self, name, timeout=None, interval=0.0):
        connection = self.recv_connections[name]
        start_time = time.perf_counter()
        while not connection.poll():
            await asyncio.sleep(interval)
            if timeout is not None and (time.perf_counter() - start_time) >= timeout:
                warnings.warn()
                return None
        return connection.recv()

    def recv_bytes(self, name, poll=True, timeout=0.0, **kwargs):
        connection = self.recv_connections[name]
        if not poll or connection.poll(timeout=timeout):
            return connection.recv(**kwargs)
        else:
            return None

    def recv_bytes_wait(self, name, timeout=None, interval=0.0, **kwargs):
        connection = self.recv_connections[name]
        start_time = time.perf_counter()
        while not connection.poll():
            time.sleep(interval)
            if timeout is not None and (time.perf_counter() - start_time) >= timeout:
                warnings.warn()
                return None
        return connection.recv_bytes(**kwargs)

    async def recv_bytes_wait_async(self, name, timeout=None, interval=0.0, **kwargs):
        connection = self.recv_connections[name]
        start_time = time.perf_counter()
        while not connection.poll():
            await asyncio.sleep(interval)
            if timeout is not None and (time.perf_counter() - start_time) >= timeout:
                warnings.warn()
                return None
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

    def get_wait(self, name, timeout=None, interval=0.0):
        connection = self.queues[name]
        start_time = time.perf_counter()
        while connection.empty():
            time.sleep(interval)
            if timeout is not None and (time.perf_counter() - start_time) >= timeout:
                warnings.warn()
                return None
        return connection.get()

    async def get_wait_async(self, name, timeout=None, interval=0.0):
        connection = self.queues[name]
        start_time = time.perf_counter()
        while connection.empty():
            await asyncio.sleep(interval)
            if timeout is not None and (time.perf_counter() - start_time) >= timeout:
                warnings.warn()
                return None
        return connection.get()


class InputsHandler(object):
    # Construction/Destruction
    def __init__(self, name=""):
        self.name = name

        self.interrupts = Interrupts()

        self.inputs = {}
        self.events = {}
        self.queues = {}
        self.pipes = {}
        self.broadcasters = {}

    def __getitem__(self, item):
        return self.inputs[item]

    # Constructors/Destructors
    def destruct(self):
        self.stop_all()

    # Events
    def create_event(self, name):
        self.inputs[name] = Event()
        self.events[name] = self.inputs[name]
        return self.inputs[name]

    def add_event(self, name, event):
        self.inputs[name] = event
        self.events[name] = event

    def clear_events(self):
        for event in self.events:
            del self.inputs[event]
        self.events.clear()

    def wait_for_event(self, name, reset=True, timeout=None, interval=0.0):
        event = self.events[name]
        interrupt = self.interrupts.add(name)
        start_time = time.perf_counter()
        while not interrupt:
            if event.is_set():
                if reset:
                    event.clear()
                return True
            else:
                if timeout is not None and (time.perf_counter() - start_time) >= timeout:
                    warnings.warn()
                    return None
                time.sleep(interval)
        interrupt.reset()
        return None

    async def wait_for_event_async(self, name, reset=True, timeout=None, interval=0.0):
        event = self.events[name]
        interrupt = self.interrupts.add(name)
        start_time = time.perf_counter()
        while not interrupt:
            if event.is_set():
                if reset:
                    event.clear()
                return True
            else:
                if timeout is not None and (time.perf_counter() - start_time) >= timeout:
                    warnings.warn()
                    return None
                await asyncio.sleep(interval)
        interrupt.reset()
        return None

    # Queues
    def create_queue(self, name, maxsize=0):
        self.inputs[name] = Queue(maxsize=maxsize)
        self.queues[name] = self.inputs[name]
        return self.inputs[name]

    def add_queue(self, name, q):
        self.inputs[name] = q
        self.queues[name] = q

    def clear_queues(self):
        for q in self.queues:
            del self.inputs[q]
        self.queues.clear()

    def wait_for_queue(self, name, timeout=None, interval=0.0):
        q = self.queues[name]
        interrupt = self.interrupts.add(name)
        start_time = time.perf_counter()
        while not interrupt:
            try:
                return q.get(block=False)
            except queue.Empty:
                if timeout is not None and (time.perf_counter() - start_time) >= timeout:
                    warnings.warn()
                    return None
                time.sleep(interval)
        interrupt.reset()
        return None

    async def wait_for_queue_async(self, name, timeout=None, interval=0.0):
        q = self.queues[name]
        interrupt = self.interrupts.add(name)
        start_time = time.perf_counter()
        while not interrupt:
            try:
                return q.get(block=False)
            except queue.Empty:
                if timeout is not None and (time.perf_counter() - start_time) >= timeout:
                    warnings.warn()
                    return None
                await asyncio.sleep(interval)
        interrupt.reset()
        return None

    # Pipes
    def create_pipe(self, name, duplex=True):
        output, self.inputs[name] = Pipe(duplex=duplex)
        self.pipes[name] = self.inputs[name]
        return output

    def add_pipe(self, name, pipe):
        self.inputs[name] = pipe
        self.pipes[name] = pipe

    def clear_pipes(self):
        for pipe in self.pipes:
            del self.inputs[pipe]
        self.pipes.clear()

    def wait_for_pipe(self, name, timeout=None, interval=0.0):
        connection = self.pipes[name]
        interrupt = self.interrupts.add(name)
        start_time = time.perf_counter()
        while not interrupt:
            if connection.poll():
                return connection.recv()
            else:
                if timeout is not None and (time.perf_counter() - start_time) >= timeout:
                    warnings.warn()
                    return None
                time.sleep(interval)
        interrupt.reset()
        return None

    async def wait_for_pipe_async(self, name, timeout=None, interval=0.0):
        connection = self.pipes[name]
        interrupt = self.interrupts.add(name)
        start_time = time.perf_counter()
        while not interrupt:
            if connection.poll():
                return connection.recv()
            else:
                if timeout is not None and (time.perf_counter() - start_time) >= timeout:
                    warnings.warn()
                    return None
                await asyncio.sleep(interval)
        interrupt.reset()
        return None

    # Broadcasters
    def create_broadcast(self, name):
        broadcaster = BroadcastPipe(name=name)
        _, self.inputs[name] = broadcaster.create_pipe(name)
        self.broadcasters[name] = self.inputs[name]
        return broadcaster

    def add_broadcast(self, name, broadcaster):
        if isinstance(broadcaster, BroadcastPipe):
            if name not in broadcaster.recv_connections:
                broadcaster.create_pipe(name)
            self.inputs[name] = broadcaster.recv_connections[name]
        else:
            self.inputs[name] = broadcaster
        self.broadcasters[name] = self.inputs[name]

    def clear_broadcasts(self):
        for broadcast in self.broadcasters:
            del self.inputs[broadcast]
        self.broadcasters.clear()

    def wait_for_broadcast(self, name, timeout=None, interval=0.0):
        connection = self.broadcasters[name]
        interrupt = self.interrupts.add(name)
        start_time = time.perf_counter()
        while not interrupt:
            if connection.poll():
                return connection.recv()
            else:
                if timeout is not None and (time.perf_counter() - start_time) >= timeout:
                    warnings.warn()
                    return None
                time.sleep(interval)
        interrupt.reset()
        return None

    async def wait_for_broadcast_async(self, name, timeout=None, interval=0.0):
        connection = self.broadcasters[name]
        interrupt = self.interrupts.add(name)
        start_time = time.perf_counter()
        while not interrupt:
            if connection.poll():
                return connection.recv()
            else:
                if timeout is not None and (time.perf_counter() - start_time) >= timeout:
                    warnings.warn()
                    return None
                await asyncio.sleep(interval)
        interrupt.reset()
        return None

    # All
    def clear_all(self):
        self.inputs.clear()
        self.events.clear()
        self.queues.clear()
        self.pipes.clear()
        self.broadcasters.clear()

    # Transmission
    def get_item(self, name, reset=True, **kwargs):
        if name in self.events:
            if self.events[name].is_set():
                if reset:
                    self.events[name].clear()
                return True
            else:
                return False
        elif name in self.queues:
            return self.safe_queue_get(self.queues[name], **kwargs)
        elif name in self.pipes:
            return self.safe_pipe_recv(self.pipes[name], **kwargs)
        elif name in self.broadcasters:
            return self.safe_pipe_recv(self.broadcasters[name], **kwargs)
        else:
            warnings.warn()

    def get_item_wait(self, name, timeout=None, interval=0.0, reset=True):
        if name in self.events:
            return self.wait_for_event(name=name, reset=reset, timeout=timeout, interval=interval)
        elif name in self.queues:
            return self.wait_for_queue(name=name, timeout=timeout, interval=interval)
        elif name in self.pipes:
            return self.wait_for_pipe(name=name, timeout=timeout, interval=interval)
        elif name in self.broadcasters:
            return self.wait_for_broadcast(name=name, timeout=timeout, interval=interval)
        else:
            warnings.warn()

    async def get_item_wait_async(self, name, timeout=None, interval=0.0, reset=True):
        if name in self.events:
            return await self.wait_for_event_async(name=name, reset=reset, timeout=None, interval=interval)
        if name in self.queues:
            return await self.wait_for_queue_async(name=name, timeout=timeout, interval=interval)
        elif name in self.pipes:
            return await self.wait_for_pipe_async(name=name, timeout=timeout, interval=interval)
        elif name in self.broadcasters:
            return await self.wait_for_broadcast_async(name=name, timeout=timeout, interval=interval)
        else:
            warnings.warn()

    def stop_all(self):
        self.interrupts.interrupt_all_processes()

    @staticmethod
    def safe_pipe_recv(pipe, poll=True, timeout=0.0):
        if not poll or pipe.poll(timeout=timeout):
            return pipe.recv()
        else:
            return None

    @staticmethod
    def safe_queue_get(q, block=False, timeout=None):
        try:
            return q.get(block=block, timeout=timeout)
        except queue.Empty:
            return None


class OutputsHandler(object):
    # Construction/Destruction
    def __init__(self, name=""):
        self.name = name

        self.interrupts = Interrupts()

        self.outputs = {}
        self.events = {}
        self.queues = {}
        self.pipes = {}
        self.broadcasters = {}

    def __getitem__(self, item):
        return self.outputs[item]

    # Constructors/Destructors
    def destruct(self):
        self.stop_all()

    # Events
    def create_event(self, name):
        self.outputs[name] = Event()
        self.events[name] = self.outputs[name]
        return self.outputs[name]

    def add_event(self, name, event):
        self.outputs[name] = event
        self.events[name] = event

    def clear_events(self):
        for event in self.events:
            del self.outputs[event]
        self.events.clear()

    def event_wait(self, name, timeout=None, interval=0.0):
        self.events[name].set()
        return self.wait_for_event_clear(name=name, timeout=timeout, interval=interval)

    async def event_wait_async(self, name, timeout=None, interval=0.0):
        self.events[name].set()
        return await self.wait_for_event_clear_async(name=name, timeout=timeout, interval=interval)

    def wait_for_event_clear(self, name, timeout=None, interval=0.0):
        event = self.events[name]
        interrupt = self.interrupts.add(name)
        start_time = time.perf_counter()
        while not interrupt:
            if not event.is_set():
                return True
            else:
                if timeout is not None and (time.perf_counter() - start_time) >= timeout:
                    warnings.warn()
                    return None
                time.sleep(interval)
        interrupt.reset()
        return None

    async def wait_for_event_clear_async(self, name, timeout=None, interval=0.0):
        event = self.events[name]
        interrupt = self.interrupts.add(name)
        start_time = time.perf_counter()
        while not interrupt.is_set():
            if not event.is_set():
                return True
            else:
                if timeout is not None and (time.perf_counter() - start_time) >= timeout:
                    warnings.warn()
                    return None
                await asyncio.sleep(interval)
        interrupt.reset()
        return None

    # Queues
    def create_queue(self, name, maxsize=0):
        self.outputs[name] = Queue(maxsize=maxsize)
        self.queues[name] = self.outputs[name]
        return self.outputs[name]

    def add_queue(self, name, q):
        self.outputs[name] = q
        self.queues[name] = q

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
        self.pipes[name] = pipe

    def clear_pipes(self):
        for pipe in self.pipes:
            del self.outputs[pipe]
        self.pipes.clear()

    # Broadcasters
    def create_broadcast(self, name):
        broadcaster = BroadcastPipe(name=name)
        self.outputs[name] = broadcaster
        self.broadcasters[name] = broadcaster
        return broadcaster

    def add_broadcast(self, name, broadcaster):
        self.outputs[name] = broadcaster
        self.broadcasters[name] = broadcaster

    def clear_broadcasts(self):
        for broadcast in self.broadcasters:
            del self.outputs[broadcast]
        self.broadcasters.clear()

    # All
    def clear_all(self):
        self.outputs.clear()
        self.events.clear()
        self.queues.clear()
        self.pipes.clear()
        self.broadcasters.clear()

    # Transmission
    def send_item(self, name, item, **kwargs):
        if name in self.events:
            self.events[name].set()
        elif name in self.queues:
            return self.queues[name].put(item, **kwargs)
        elif name in self.pipes:
            return self.pipes[name].send(item, **kwargs)
        elif name in self.broadcasters:
            return self.broadcasters[name].send(item, **kwargs)
        else:
            warnings.warn()

    def stop_all(self):
        self.interrupts.interrupt_all_processes()


class Task(object):
    # Construction/Destruction
    def __init__(self, name=None, allow_setup=True, allow_closure=True,
                 s_kwargs={}, t_kwargs={}, c_kwargs={}, init=True):
        self.name = name
        self.async_loop = asyncio.get_event_loop()
        self.setup_kwargs = s_kwargs
        self.task_kwargs = t_kwargs
        self.closure_kwargs = c_kwargs
        self.allow_setup = allow_setup
        self.allow_closure = allow_closure
        self.stop_event = Event()
        self.events = {}
        self.locks = {}

        self.inputs = None
        self.outputs = None

        self._execute_setup = self.setup
        self._execute_task = self.task
        self._execute_task_loop = self.task_loop
        self._execute_closure = self.closure

        if init:
            self.construct()

    # Pickling
    def __getstate__(self):
        out_dict = self.__dict__
        del out_dict["async_loop"]
        return out_dict

    def __setstate__(self, in_dict):
        in_dict["async_loop"] = asyncio.get_event_loop()
        self.__dict__ = in_dict

    # Constructors/Destructors
    def construct(self):
        self.construct_io()

    # State Methods
    def is_async(self):
        if asyncio.iscoroutinefunction(self._execute_setup) or asyncio.iscoroutinefunction(self._execute_closure) or \
           asyncio.iscoroutinefunction(self._execute_task):
            return True
        else:
            return False

    # IO
    def construct_io(self):
        self.inputs = InputsHandler(name=self.name)
        self.outputs = OutputsHandler(name=self.name)

        self.inputs.create_event("SelfStop")

        self.create_io()

    def create_io(self):
        pass

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

    # Setup
    def setup(self):
        pass

    # Task
    def task(self):
        pass

    def task_loop(self, **kwargs):
        while not self.stop_event.is_set():
            if not self.inputs.get_item("SelfStop"):
                self._execute_task(**kwargs)
            else:
                self.stop_event.set()

    async def task_loop_async(self, **kwargs):
        while not self.stop_event.is_set():
            if not self.inputs.get_item("SelfStop"):
                await asyncio.create_task(self._execute_task(**kwargs))
            else:
                self.stop_event.set()

    # Closure
    def closure(self):
        pass

    # Normal Execute Methods
    def run_normal(self, s_kwargs={}, t_kwargs={}, c_kwargs={}):
        # Optionally run Setup
        if self.allow_setup:
            if s_kwargs:
                self.setup_kwargs = s_kwargs
            self._execute_setup(**self.setup_kwargs)

        # Run Task
        if t_kwargs:
            self.task_kwargs = t_kwargs
        self._execute_task(**self.task_kwargs)

        # Optionally run Closure
        if self.allow_closure:
            if c_kwargs:
                self.closure_kwargs = c_kwargs
            self._execute_closure(**self.closure_kwargs)

    def start_normal(self, s_kwargs={}, t_kwargs={}, c_kwargs={}):
        # Prep
        self.prepare_task_loop()

        # Optionally run Setup
        if self.allow_setup:
            if s_kwargs:
                self.setup_kwargs = s_kwargs
            self._execute_setup(**self.setup_kwargs)

        # Run Task Loop
        if t_kwargs:
            self.task_kwargs = t_kwargs
        self._execute_task_loop(**self.task_kwargs)

        # Optionally run Closure
        if self.allow_closure:
            if c_kwargs:
                self.closure_kwargs = c_kwargs
            self._execute_closure(**self.closure_kwargs)

    # Async Execute Methods
    async def run_coro(self, s_kwargs={}, t_kwargs={}, c_kwargs={}):
        # Optionally run Setup
        if self.allow_setup:
            if s_kwargs:
                self.setup_kwargs = s_kwargs
            if asyncio.iscoroutinefunction(self._execute_setup):
                await self._execute_setup(**self.setup_kwargs)
            else:
                self._execute_setup(**self.setup_kwargs)

        # Run Task
        if t_kwargs:
            self.task_kwargs = t_kwargs
        if asyncio.iscoroutinefunction(self._execute_task):
            await self._execute_task(**self.task_kwargs)
        else:
            self._execute_task(**self.task_kwargs)

        # Optionally run Closure
        if self.allow_closure:
            if c_kwargs:
                self.closure_kwargs = c_kwargs
            if asyncio.iscoroutinefunction(self._execute_closure):
                await self._execute_closure(**self.closure_kwargs)
            else:
                self._execute_closure(**self.closure_kwargs)

    async def start_coro(self, s_kwargs={}, t_kwargs={}, c_kwargs={}):
        self.prepare_task_loop()

        # Optionally run Setup
        if self.allow_setup:
            if s_kwargs:
                self.setup_kwargs = s_kwargs
            if asyncio.iscoroutinefunction(self._execute_setup):
                await self._execute_setup(**self.setup_kwargs)
            else:
                self._execute_setup(**self.setup_kwargs)

        # Run Task
        if t_kwargs:
            self.task_kwargs = t_kwargs
        if asyncio.iscoroutinefunction(self._execute_task):
            await self._execute_task_loop(**self.task_kwargs)
        else:
            self._execute_task_loop(**self.task_kwargs)

        # Optionally run Closure
        if self.allow_closure:
            if c_kwargs:
                self.closure_kwargs = c_kwargs
            if asyncio.iscoroutinefunction(self._execute_closure):
                await self._execute_closure(**self.closure_kwargs)
            else:
                self._execute_closure(**self.closure_kwargs)

    # Set Execution Methods
    def set_setup(self, func, kwargs={}):
        if kwargs:
            self.setup_kwargs = kwargs
        self._execute_setup = func

    def set_task(self, func, kwargs={}):
        if kwargs:
            self.task_kwargs = kwargs
        self._execute_task = func

    def set_task_loop(self, func):
        self._execute_task_loop = func

    def prepare_task_loop(self):
        if self._execute_task_loop == self.task_loop:
            if asyncio.iscoroutinefunction(self._execute_task):
                self._execute_task_loop = self.task_loop_async
        elif self._execute_task_loop == self.task_loop_async:
            if not asyncio.iscoroutinefunction(self._execute_task):
                self._execute_task_loop = self.task_loop

    def set_closure(self, func, kwargs={}):
        if kwargs:
            self.closure_kwargs = kwargs
        self._execute_closure = func

    # Execution
    def run(self, s_kwargs={}, t_kwargs={}, c_kwargs={}):
        if self.is_async():
            asyncio.run(self.run_coro(s_kwargs, t_kwargs, c_kwargs))
        else:
            self.run_normal(s_kwargs, t_kwargs, c_kwargs)

    def run_async_task(self, s_kwargs={}, t_kwargs={}, c_kwargs={}):
        return asyncio.create_task(self.run_coro(s_kwargs, t_kwargs, c_kwargs))

    def start(self, s_kwargs={}, t_kwargs={}, c_kwargs={}):
        if self.is_async():
            asyncio.run(self.start_coro(s_kwargs, t_kwargs, c_kwargs))
        else:
            self.start_normal(s_kwargs, t_kwargs, c_kwargs)

    def start_async_task(self, s_kwargs={}, t_kwargs={}, c_kwargs={}):
        return asyncio.create_task(self.start_coro(s_kwargs, t_kwargs, c_kwargs))

    def stop(self):
        self.stop_event.set()
        self.inputs.stop_all()
        self.outputs.stop_all()

    def reset(self):
        self.stop_event.clear()


class MultiUnitTask(Task):
    SETTING_NAMES = {"unit", "start", "setup", "closure", "s_kwargs", "t_kwargs", "c_kwargs"}

    # Construction/Destruction
    def __init__(self, name=None, units={}, execution_kwargs={}, order=(),
                 allow_setup=True, allow_closure=True, s_kwargs={}, t_kwargs={}, c_kwargs={}, init=True):
        # Run Parent __init__ but only construct in child
        super().__init__(name=name, allow_setup=allow_setup, allow_closure=allow_closure,
                         s_kwargs=s_kwargs, t_kwargs=t_kwargs, c_kwargs=c_kwargs, init=False)

        # Attributes
        self._execution_order = ()

        self.units = {}
        self.execution_kwargs = {}

        # Optionally Construct this object
        if init:
            self.construct(units=units, execution_kwargs=execution_kwargs, order=order)

    @property
    def execution_order(self):
        return self._execution_order

    @execution_order.setter
    def execution_order(self, value):
        if len(value) == len(self.units):
            self._execution_order = value
        else:
            warnings.warn()

    # Container Methods
    def __len__(self):
        return len(self.units)

    def __getitem__(self, item):
        return self.units[item]

    def __delitem__(self, key):
        del self.execution_kwargs[key]
        del self.units[key]

    # Constructors/Destructors
    def construct(self, units={}, execution_kwargs={}, order=()):
        super().construct()
        if units:
            self.extend(units=units, execution_kwargs=execution_kwargs)
        if order:
            self.execution_order = order

    # Container Methods
    def keys(self):
        return self.units.keys()

    def values(self):
        return self.units.values()

    def items(self):
        return self.units.items()

    def set_unit(self, name, unit, start=True, setup=False, closure=False, s_kwargs={}, t_kwargs={}, c_kwargs={}):
        self.units[name] = unit
        self.execution_kwargs[name] = {"start": start, "setup": setup, "closure": closure,
                                       "s_kwargs": s_kwargs, "t_kwargs": t_kwargs, "c_kwargs": c_kwargs}

    def extend(self, units, execution_kwargs={}):
        if execution_kwargs:
            if set(execution_kwargs).issubset(units):
                for name, unit in units.items():
                    if set(execution_kwargs[name]).issubset(self.SETTING_NAMES):
                        self.set_unit(name, unit, **execution_kwargs[name])
                    else:
                        warnings.warn()
                else:
                    warnings.warn()
        else:
            for name, unit in units.items():
                self.set_unit(name, unit)

    def pop(self, name):
        del self.execution_kwargs[name]
        return self.units.pop(name)

    def clear(self):
        self.execution_kwargs.clear()
        self.units.clear()

    def all_async(self):
        for unit in self.units.values():
            if not unit.is_async():
                return False
        return True

    def any_async(self):
        for unit in self.units.values():
            if unit.is_async():
                return True
        return False

    # Setup
    def setup(self):
        if not self.execution_order:
            names = self.units
        else:
            names = self.execution_order

        for name in names:
            unit = self.units[name]
            execution_kwargs = self.execution_kwargs[name]
            if execution_kwargs["setup"]:
                unit.allow_setup = False
                unit.setup(**execution_kwargs["c_kwargs"])
            if execution_kwargs["closure"]:
                unit.allow_closure = False

    # Task
    def task(self):
        if not self.execution_order:
            names = self.units
        else:
            names = self.execution_order

        for name in names:
            unit = self.units[name]
            start = self.execution_kwargs[name]["start"]
            s_kwargs = self.execution_kwargs[name]["s_kwargs"]
            t_kwargs = self.execution_kwargs[name]["t_kwargs"]
            c_kwargs = self.execution_kwargs[name]["c_kwargs"]
            if start:
                unit.start(s_kwargs, t_kwargs, c_kwargs)
            else:
                unit.run(s_kwargs, t_kwargs, c_kwargs)

    async def task_async(self):
        tasks = []
        if not self.execution_order:
            names = self.units
        else:
            names = self.execution_order

        for name in names:
            unit = self.units[name]
            start = self.execution_kwargs[name]["start"]
            s_kwargs = self.execution_kwargs[name]["s_kwargs"]
            t_kwargs = self.execution_kwargs[name]["t_kwargs"]
            c_kwargs = self.execution_kwargs[name]["c_kwargs"]
            if start:
                if unit.is_async():
                    tasks.append(unit.start_async_task(s_kwargs, t_kwargs, c_kwargs))
                else:
                    unit.start(s_kwargs, t_kwargs, c_kwargs)
            else:
                if unit.is_async():
                    tasks.append(unit.run_async_task(s_kwargs, t_kwargs, c_kwargs))
                else:
                    unit.run(s_kwargs, t_kwargs, c_kwargs)

        for task in tasks:
            await task

    # Closure
    def closure(self):
        if not self.execution_order:
            names = self.units
        else:
            names = self.execution_order

        for name in names:
            unit = self.units[name]
            execution_kwargs = self.execution_kwargs[name]
            if execution_kwargs["closure"]:
                unit.closure(**execution_kwargs["c_kwargs"])

    # Set Execution Methods
    def prepare_task(self):
        if self._execute_task == self.task:
            if self.any_async():
                self._execute_task = self.task_async
        elif self._execute_task == self.task_async:
            if not self.any_async():
                self._execute_task = self.task

    # Execution
    def run(self, s_kwargs={}, t_kwargs={}, c_kwargs={}):
        self.prepare_task()
        super().run(s_kwargs, t_kwargs, c_kwargs)

    def run_async_task(self, s_kwargs={}, t_kwargs={}, c_kwargs={}):
        self.prepare_task()
        return super().run_async_task(s_kwargs, t_kwargs, c_kwargs)

    def start(self, s_kwargs={}, t_kwargs={}, c_kwargs={}):
        self.prepare_task()
        super().start(s_kwargs, t_kwargs, c_kwargs)

    def start_async_task(self, s_kwargs={}, t_kwargs={}, c_kwargs={}):
        self.prepare_task()
        return super().start_async_task(s_kwargs, t_kwargs, c_kwargs)

    def stop(self, join=True, timeout=None):
        super().stop()
        if not self.execution_order:
            names = self.units
        else:
            names = self.execution_order

        for name in names:
            self.units[name]["unit"].stop(join=False)

        for name in names:
            self.units[name]["unit"].join(timeout=timeout)

    def reset(self):
        super().reset()
        if not self.execution_order:
            names = self.units
        else:
            names = self.execution_order

        for name in names:
            self.units[name]["unit"].reset()


class SeparateProcess(object):
    available_cpus = multiprocessing.cpu_count()

    # Construction/Destruction
    def __init__(self, target=None, name=None, daemon=False, init=False, kwargs={}):
        self._name = name
        self._daemon = daemon
        self._target = target
        self._target_kwargs = kwargs
        self.method_wrapper = run_method

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

    # State
    def is_alive(self):
        if self.process is None:
            return False
        else:
            return self.process.is_alive()

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

    # Task
    def target_object_method(self, obj, method, kwargs={}):
        kwargs["obj"] = obj
        kwargs["method"] = method
        self.target_kwargs = kwargs
        self.process = Process(target=self.method_wrapper, name=self.name, daemon=self.daemon, kwargs=self.target_kwargs)

    # Execution
    def run(self):
        self.process.run()

    def start(self):
        self.process.start()

    def join(self, timeout=None):
        return self.process.join(timeout=timeout)

    async def join_async(self, timeout=None, interval=0.0):
        start_time = time.perf_counter()
        while self.process.join(0) is None:
            await asyncio.sleep(interval)
            if timeout is not None and (time.perf_counter() - start_time) >= timeout:
                return None

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
    DEFAULT_TASK = Task

    # Construction/Destruction
    def __init__(self, name=None, task=None, to_kwargs={},
                 separate_process=False, daemon=False, p_kwargs={},
                 allow_setup=False, allow_closure=False, init=True):
        self.name = name
        self.unit_setup_kwargs = {}
        self.unit_closure_kwargs = {}
        self.allow_setup = allow_setup
        self.allow_closure = allow_closure
        self.await_closure = False

        self.separate_process = separate_process
        self._is_processing = False
        self.process = None
        self.processing_pool = None

        self._execute_setup = self.setup
        self._task_object = None
        self._execute_closure = self.closure
        self._joined = True

        if init:
            self.construct(name=name, task=task, to_kwargs=to_kwargs, daemon=daemon, p_kwargs=p_kwargs)

    @property
    def task_object(self):
        if self.is_processing():
            warnings.warn()
        return self._task_object

    @task_object.setter
    def task_object(self, value):
        if self.is_processing():
            warnings.warn()
        self._task_object = value

    @property
    def events(self):
        if self.task_object is not None:
            return self.task_object.events
        else:
            return None

    @events.setter
    def events(self, value):
        if self.task_object is not None:
            self.task_object.events = value
        else:
            raise NameError

    @property
    def locks(self):
        if self.task_object is not None:
            return self.task_object.locks
        else:
            return None

    @locks.setter
    def locks(self, value):
        if self.task_object is not None:
            self.task_object.locks = value
        else:
            raise NameError

    @property
    def inputs(self):
        if self.task_object is not None:
            return self.task_object.inputs
        else:
            return None

    @inputs.setter
    def inputs(self, value):
        if self.task_object is not None:
            self.task_object.inputs = value
        else:
            raise NameError

    @property
    def outputs(self):
        if self.task_object is not None:
            return self.task_object.outputs
        else:
            return None

    @outputs.setter
    def outputs(self, value):
        if self.task_object is not None:
            self.task_object.outputs = value
        else:
            raise NameError

    # Constructors
    def construct(self, name=None, task=None, to_kwargs={}, daemon=False, p_kwargs={}):
        if self.separate_process:
            self.new_process(name=name, daemon=daemon, kwargs=p_kwargs)
        if task is None:
            if self.task_object is None:
                self.default_task_object(**to_kwargs)
        else:
            self.task_object = task

    # State
    def is_async(self):
        if asyncio.iscoroutinefunction(self._execute_setup) or asyncio.iscoroutinefunction(self._execute_closure):
            return True
        elif not self.separate_process and self.task_object.is_async():
            return True
        else:
            return False

    def is_processing(self):
        if self.process is not None and self.separate_process:
            self._is_processing = self.process.is_alive()
        return self._is_processing

    # Process
    def new_process(self, name=None, daemon=False, kwargs={}):
        if name is None:
            name = self.name
        self.process = SeparateProcess(name=name, daemon=daemon, kwargs=kwargs)

    def set_process(self, process):
        self.process = process

    # Set Task Object
    def default_task_object(self, **kwargs):
        self.task_object = self.DEFAULT_TASK(name=self.name, **kwargs)

    # Setup
    def setup(self):
        pass

    # Closure
    def closure(self):
        pass

    # Normal Execution Methods
    def run_normal(self, s_kwargs={}, t_kwargs={}, c_kwargs={}):
        self._joined = False
        kwargs = {"s_kwargs": s_kwargs, "t_kwargs": t_kwargs, "c_kwargs": c_kwargs}
        # Optionally run Setup
        if self.allow_setup:
            self._execute_setup(**self.unit_setup_kwargs)

        # Run Task
        if self.separate_process:
            self.process.target_object_method(self.task_object, "run", kwargs=kwargs)
            self.process.start()
        else:
            self.task_object.run(**kwargs)

        # Optionally run Closure
        if self.allow_closure:
            if self.separate_process:
                if self.await_closure:
                    self.process.join()
                else:
                    warnings.warn()
            self._execute_closure(**self.unit_closure_kwargs)
        self._joined = True

    def start_normal(self, s_kwargs={}, t_kwargs={}, c_kwargs={}):
        self._joined = False
        kwargs = {"s_kwargs": s_kwargs, "t_kwargs": t_kwargs, "c_kwargs": c_kwargs}
        # Optionally run Setup
        if self.allow_setup:
            self._execute_setup(**self.unit_setup_kwargs)

        # Run Task
        if self.separate_process:
            self.process.target_object_method(self.task_object, "start", kwargs=kwargs)
            self.process.start()
        else:
            self.task_object.start(**kwargs)

        # Optionally run Closure
        if self.allow_closure:
            if self.separate_process:
                if self.await_closure:
                    self.process.join()
                else:
                    warnings.warn()
            self._execute_closure(**self.unit_closure_kwargs)
        self._joined = True

    # Async Execute Methods
    async def run_coro(self, s_kwargs={}, t_kwargs={}, c_kwargs={}):
        self._joined = False
        kwargs = {"s_kwargs": s_kwargs, "t_kwargs": t_kwargs, "c_kwargs": c_kwargs}
        # Optionally run Setup
        if self.allow_setup:
            if asyncio.iscoroutinefunction(self._execute_setup):
                await self._execute_setup(**self.unit_setup_kwargs)
            else:
                self._execute_setup(**self.unit_setup_kwargs)

        # Run Task
        if self.separate_process:
            self.process.target_object_method(self.task_object, "run", kwargs=kwargs)
            self.process.start()
        else:
            if self.task_object.is_async():
                await self.task_object.run_coro(**kwargs)
            else:
                self.task_object.run(**kwargs)

            # Optionally run Closure
            if self.allow_closure:
                if self.separate_process:
                    if self.await_closure:
                        await self.process.join_async()
                    else:
                        warnings.warn()
                if asyncio.iscoroutinefunction(self._execute_closure):
                    await self._execute_closure(**self.unit_closure_kwargs)
                else:
                    self._execute_closure(**self.unit_closure_kwargs)
        self._joined = True

    async def start_coro(self, s_kwargs={}, t_kwargs={}, c_kwargs={}):
        self._joined = False
        kwargs = {"s_kwargs": s_kwargs, "t_kwargs": t_kwargs, "c_kwargs": c_kwargs}
        # Optionally run Setup
        if self.allow_setup:
            if asyncio.iscoroutinefunction(self._execute_setup):
                await self._execute_setup(**self.unit_setup_kwargs)
            else:
                self._execute_setup(**self.unit_setup_kwargs)

        # Run Task
        if self.separate_process:
            self.process.target_object_method(self.task_object, "start", kwargs=kwargs)
            self.process.start()
        else:
            if self.task_object.is_async():
                await self.task_object.start_coro(**kwargs)
            else:
                self.task_object.start(**kwargs)

        # Optionally run Closure
        if self.allow_closure:
            if self.separate_process:
                if self.await_closure:
                    await self.process.join_async()
                else:
                    warnings.warn()
            if asyncio.iscoroutinefunction(self._execute_closure):
                await self._execute_closure(**self.unit_closure_kwargs)
            else:
                self._execute_closure(**self.unit_closure_kwargs)
        self._joined = True

    # Set Execution Methods
    def set_setup(self, func, kwargs={}):
        if kwargs:
            self.unit_setup_kwargs = kwargs
        self._execute_setup = func

    def use_task_setup(self):
        self.task_object.allow_setup = False
        self._execute_setup = self.task_object.setup

    def set_closure(self, func, kwargs={}):
        if kwargs:
            self.unit_closure_kwargs = kwargs
        self._execute_closure = func

    def use_task_closure(self):
        self.task_object.allow_closure = False
        self._execute_closure = self.task_object.closure

    # Execution
    def run(self, s_kwargs={}, t_kwargs={}, c_kwargs={}):
        if self.is_async():
            asyncio.run(self.run_coro(s_kwargs, t_kwargs, c_kwargs))
        else:
            self.run_normal(s_kwargs, t_kwargs, c_kwargs)

    def run_async_task(self, s_kwargs={}, t_kwargs={}, c_kwargs={}):
        return asyncio.create_task(self.run_coro(s_kwargs, t_kwargs, c_kwargs))

    def start(self, s_kwargs={}, t_kwargs={}, c_kwargs={}):
        if self.is_async():
            asyncio.run(self.start_coro(s_kwargs, t_kwargs, c_kwargs))
        else:
            self.start_normal(s_kwargs, t_kwargs, c_kwargs)

    def start_async_task(self, s_kwargs={}, t_kwargs={}, c_kwargs={}):
        return asyncio.create_task(self.start_coro(s_kwargs, t_kwargs, c_kwargs))

    def join(self, timeout=None):
        start_time = time.perf_counter()
        while not self._joined:
            if timeout is not None and (time.perf_counter() - start_time) >= timeout:
                return None

        timeout = timeout - (time.perf_counter() - start_time)

        if self.separate_process:
            self.process.join(timeout=timeout)
        return None

    async def join_async(self, timeout=None, interval=0.0):
        start_time = time.perf_counter()
        while not self._joined:
            await asyncio.sleep(interval)
            if timeout is not None and (time.perf_counter() - start_time) >= timeout:
                return None

        timeout = timeout - (time.perf_counter() - start_time)

        if self.separate_process:
            await self.process.join_async(timeout=timeout, interval=interval)
        return None

    def stop(self, join=True, timeout=None):
        self.task_object.stop()
        if join:
            self.join(timeout=timeout)

    async def stop_async(self, join=True, timeout=None, interval=0.0):
        self.task_object.stop()
        if join:
            await self.join_async(timeout=timeout, interval=interval)

    def reset(self):
        self.task_object.reset()

    def terminate(self):
        if self.separate_process:
            self.process.terminate()


class ProcessingCluster(ProcessingUnit):
    DEFAULT_TASK = MultiUnitTask

    # Construction/Destruction
    def __init__(self, name=None, task=None, to_kwargs={},
                 separate_process=False, daemon=False, p_kwargs={},
                 allow_setup=True, allow_closure=True, init=True):
        # Run Parent __init__ but only construct in child
        super().__init__(name=name, task=task, to_kwargs=to_kwargs,
                         separate_process=separate_process, daemon=daemon, p_kwargs=p_kwargs,
                         allow_setup=allow_setup, allow_closure=allow_closure, init=False)

        if init:
            self.construct(name)

    @property
    def execution_order(self):
        return self.task_object.execution_order

    @execution_order.setter
    def execution_order(self, value):
        self.task_object.execution_order = value

    @property
    def units(self):
        return self.task_object.units

    @units.setter
    def units(self, value):
        self.task_object.units = value

    # Container Magic Methods
    def __len__(self):
        return len(self.task_object)

    def __getitem__(self, item):
        return self.task_object[item]

    def __delitem__(self, key):
        del self.task_object[key]

    # Container Methods
    def keys(self):
        return self.task_object.keys()

    def values(self):
        return self.task_object.values()

    def items(self):
        return self.task_object.items()

    def set_unit(self, name, unit, start=True, setup=False, closure=False, s_kwargs={}, t_kwargs={}, c_kwargs={}):
        self.task_object.set_unit(name, unit, start, setup, closure, s_kwargs, t_kwargs, c_kwargs)

    def extend(self, units):
        self.task_object.extend(units=units)

    def pop(self, name):
        return self.task_object.pop(name)

    def clear(self):
        self.task_object.clear()

    # Execution
    def stop(self, join=True, timeout=None):
        self.task_object.stop(join=join, timeout=timeout)
        if join:
            self.join(timeout=timeout)


# Functions #
def run_method(obj, method, **kwargs):
    return getattr(obj, method)(**kwargs)


# Main #
if __name__ == "__main__":
    pass
