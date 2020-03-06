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

# Downloaded Libraries #


# Local Libraries #


# Definitions #
# Classes #
class BroadcastPipe(object):
    def __init__(self, name):
        self.name = name

        self.send_connections = {}
        self.recv_connections = {}

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

    def send(self, obj):
        for connection in self.send_connections.values():
            connection.send(obj)

    def recv(self, name, poll=True, timeout=None):
        connection = self.recv_connections[name]
        if not poll or connection.poll(timeout=timeout):
            return connection.recv()
        else:
            return None

    def clear_recv(self, name):
        connection = self.recv_connections[name]
        while connection.poll:
            connection.recv()


class BroadcastQueue(object):
    def __init__(self, name):
        self.name = name

        self.queues = {}

    def create_queue(self, name, maxsize=None):
        self.queues[name] = Queue(maxsize=maxsize)
        return self.queues[name]

    def set_queue(self, name, q):
        self.queues[name] = q

    def get_queue(self, name):
        return self.queues[name]

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

    def put(self, obj, block=False, timeout=None):
        for q in self.queues.values():
            try:
                q.put(obj, block=block, timeout=timeout)
            except queue.Full:
                pass  # add a warning here

    def get(self, name, block=True, timeout=None):
        return self.queues[name].get(block=block, timeout=timeout)


class InputsHandler(object):
    def __init__(self, name=""):
        self.name = name

        self.inputs = {}
        self.queues = {}
        self.pipes = {}
        self.broadcasters = {}

    def __getitem__(self, item):
        return self.inputs[item]

    def create_queue(self, name, maxsize=None):
        self.inputs[name] = Queue(maxsize=maxsize)
        self.queues[name] = self.inputs[name]
        return self.inputs[name]

    def set_queue(self, name, q):
        self.inputs[name] = q
        self.queues[name] = self.inputs[name]

    def create_pipe(self, name, duplex=True):
        output, self.inputs[name] = Pipe(duplex=duplex)
        self.pipes[name] = self.inputs[name]
        return output

    def set_pipe(self, name, pipe):
        self.inputs[name] = pipe
        self.pipes[name] = self.inputs[name]

    def create_broadcast(self, name):
        broadcaster = BroadcastPipe(name=name)
        _, self.inputs[name] = broadcaster.create_pipe(name)
        self.broadcasters[name] = self.inputs[name]
        return broadcaster

    def set_broadcast(self, name, broadcaster):
        if isinstance(broadcaster, BroadcastPipe):
            self.inputs[name] = broadcaster.recv_connections(name)
        else:
            self.inputs[name] = broadcaster
        self.broadcasters = self.inputs[name]

    def get_input_item(self, name, **kwargs):
        if name in self.broadcasters:
            return self.safe_pipe_recv(self.broadcasters[name], **kwargs)
        elif name in self.pipes:
            return self.safe_pipe_recv(self.pipes[name], **kwargs)
        elif name in self.queues:
            return self.queues[name].get(**kwargs)

    @staticmethod
    def safe_pipe_recv(pipe, poll=True, timeout=None):
        if not poll or pipe.poll(timeout=timeout):
            return pipe.recv()
        else:
            return None


class OutputsHandler(object):
    def __init__(self, name=""):
        self.name = name

        self.outputs = {}
        self.queues = {}
        self.pipes = {}
        self.broadcasters = {}

    def __getitem__(self, item):
        return self.outputs[item]

    def create_queue(self, name, maxsize=None):
        self.outputs[name] = Queue(maxsize=maxsize)
        self.queues[name] = self.outputs[name]
        return self.outputs[name]

    def set_queue(self, name, q):
        self.outputs[name] = q
        self.queues[name] = self.outputs[name]

    def create_pipe(self, name, duplex=True):
        self.outputs[name], input_ = Pipe(duplex=duplex)
        self.pipes[name] = self.outputs[name]
        return input_

    def set_pipe(self, name, pipe):
        self.outputs[name] = pipe
        self.pipes[name] = self.outputs[name]

    def create_broadcast(self, name):
        broadcaster = BroadcastPipe(name=name)
        self.outputs[name] = broadcaster
        self.broadcasters[name] = self.outputs[name]
        return broadcaster

    def set_broadcast(self, name, broadcaster):
        self.outputs[name] = broadcaster
        self.broadcasters = self.outputs[name]

    def send_output_item(self, name, item, **kwargs):
        if name in self.broadcasters:
            return self.broadcasters[name].send(item, **kwargs)
        elif name in self.pipes:
            return self.pipes[name].send(item, **kwargs)
        elif name in self.queues:
            return self.queues[name].put(item, **kwargs)


class ProcessTask(object):
    def __init__(self, name=None, init=True, kwargs={}):
        self.name = name
        self.kwargs = kwargs
        self.stop_event = Event()
        self.events = {}
        self.locks = {}

        self.inputs = None
        self.outputs = None

        if init:
            self.construct()

    # Construct Methods
    def construct(self):
        self.create_io()

    # IO
    def create_io(self):
        self.inputs = InputsHandler(name=self.name)
        self.outputs = OutputsHandler(name=self.name)

        self.inputs.create_queue("SelfStop")

    # Multiprocess Event Methods
    def create_event(self, name):
        self.events[name] = Event()
        return self.events[name]

    def set_event(self, name, event):
        self.events[name] = event

    # Multiprocess Lock Methods
    def create_lock(self, name):
        self.locks[name] = Lock()
        return self.locks[name]

    def set_lock(self, name, lock):
        self.locks[name] = lock

    # Execution Methods
    def run(self, **kwargs):
        self.setup()
        self.task(**kwargs)

    def start(self, **kwargs):
        self.setup()
        self.task_loop(**kwargs)

    def restart(self, **kwargs):
        self.stop_event.clear()
        self.start(**kwargs)

    def stop(self):
        self.stop_event.set()

    # Task Methods
    def setup(self):
        pass

    def task(self, name=None):
        pass

    def task_loop(self, **kwargs):
        while not self.stop_event.is_set():
            if not self.inputs.get_input_item("SelfStop"):
                self.task(**kwargs)


class SeparateProcess(object):
    def __init__(self, target=None, name=None, daemon=None, init=False, kwargs={}):
        self.name = name
        self.target = target
        self.target_kwargs = kwargs
        self.daemon = daemon

        self.process = None

        if init:
            self.construct()

    @property
    def is_alive(self):
        return self.process.is_alive()

    def construct(self, target=None, daemon=None, **kwargs):
        self.create_process(target, daemon, **kwargs)

    def create_process(self, target=None, daemon=None, **kwargs):
        if target is not None:
            self.target = target
        if kwargs:
            self.target_kwargs = kwargs
        if daemon is not None:
            self.daemon = daemon
        self.process = Process(target=self.target, name=self.name, daemon=self.daemon, kwargs=self.target_kwargs)

    def run(self):
        self.process.run()

    def start(self):
        self.process.start()

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

    def __init__(self, name=None, init=True):
        self.name = name
        self.is_multiprocessing = False
        self._is_processing = False

        self.task = None
        self.task_start = None

        self.processing_pool = None
        self.process = None

        if init:
            self.construct()

    @property
    def is_processing(self):
        if self.is_multiprocessing:
            return self.process.is_alive
        else:
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

    # Construction Methods
    def construct(self):
        pass

    # Task Methods
    def create_task(self, **kwargs):
        self.task = self.DEFAULT_TASK(**kwargs)
        self.task_start = self.task.start

    def set_task(self, task):
        self.task = task
        self.task_start = self.task.start

    # IO
    def create_io(self):
        self.task.create_io()

    # Process Methods
    def create_process(self, task_start=None, name=None, daemon=None, kwargs={}):
        if task_start is not None:
            self.task_start = task_start
        else:
            task_start = self.task_start
        if name is None:
            name = self.name
        self.process = SeparateProcess(target=task_start, name=name, daemon=daemon, kwargs=kwargs)

    def set_process(self, process):
        self.process = process

    # Execution Methods
    def setup(self):
        pass

    def run(self, **kwargs):
        self.setup()
        self.task.run(**kwargs)

    def start(self):
        self.setup()
        self.process.start()


class ProcessingCluster(ProcessingUnit):
    def __init__(self, name=None, init=True):
        super().__init__(name=name, init=False)

        if init:
            self.construct()



if __name__ == "__main__":
    pass
