#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" advancedlogging.py
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
import abc
import copy
import datetime
import logging
import logging.config
import logging.handlers
import warnings

# Downloaded Libraries #

# Local Libraries #


# Definitions #
# Classes #
class ObjectInheritor(abc.ABC):
    _attributes_as_parents = []

    def __copy__(self):
        new = type(self)()
        new.__dict__.update(self.__dict__)
        return new

    def __deepcopy__(self, memo={}):
        new = type(self)()
        for attribute in self._attributes_as_parents:
            if attribute in dir(self):
                parent_object = copy.deepcopy(super().__getattribute__(attribute))
                setattr(new, attribute, parent_object)
        new.__dict__.update(self.__dict__)
        return new

    # Attribute Access
    def __getattribute__(self, name):
        # Check if item is in self and if not check in the object parents
        if name[0:2] != "__" and name not in {"_attributes_as_parents"} and \
           name not in self._attributes_as_parents and name not in dir(self):
            # Iterate through all object parents to find attribute
            for attribute in self._attributes_as_parents:
                parent_object = super().__getattribute__(attribute)
                if name in dir(parent_object):
                    return getattr(parent_object, name)

        # If the item is an attribute in self or not in any object parent return attribute
        return super().__getattribute__(name)

    def __setattr__(self, name, value):
        # Check if item is in self and if not check in object parents
        if name not in self._attributes_as_parents and name not in dir(self):
            # Iterate through all indirect parents to find attribute
            for attribute in self._attributes_as_parents:
                if attribute in dir(self):
                    parent_object = super().__getattribute__(attribute)
                    if name in dir(parent_object):
                        return setattr(parent_object, name, value)

        # If the item is an attribute in self or not in any indirect parent set as attribute
        return super().__setattr__(name, value)

    def _setattr(self, name, value):
        super().__setattr__(name, value)


class PreciseFormatter(logging.Formatter):
    converter = datetime.datetime.fromtimestamp
    default_msec_format = "%s.%06d"

    def formatTime(self, record, datefmt=None):
        ct = self.converter(record.created)
        if datefmt:
            s = ct.strftime(datefmt)
        else:
            t = ct.strftime(self.default_time_format)
            s = self.default_msec_format % (t, ct.microsecond)
        return s


class AdvancedLogger(ObjectInheritor):
    _attributes_as_parents = ["_logger"]

    @classmethod
    def from_config(cls, name, fname, defaults=None, disable_existing_loggers=True, **kwargs):
        logging.config.fileConfig(fname, defaults, disable_existing_loggers)
        return cls(name, **kwargs)

    # Construction/Destruction
    def __init__(self, obj=None, module_of_class="(Not Given)", init=True):
        self.allow_append = False

        self._logger = None
        self.module_of_class = module_of_class
        self.module_of_object = "(Not Given)"
        self.append_message = ""

        if init:
            self.construct(obj)

    @property
    def name_parent(self):
        return self.name.rsplit('.', 1)[0]

    @property
    def name_stem(self):
        return self.name.rsplit('.', 1)[0]

    # Pickling
    def __getstate__(self):
        out_dict = self.__dict__.copy()
        out_dict["disabled"] = self.disabled
        out_dict["level"] = self.getEffectiveLevel()
        out_dict["propagate"] = self.propagate
        out_dict["filters"] = copy.deepcopy(self.filters)
        out_dict["handlers"] = []
        for handler in self.handlers:
            lock = handler.__dict__.pop("lock")
            stream = handler.__dict__.pop("stream")
            out_dict["handlers"].append(copy.deepcopy(handler))
            handler.__dict__["lock"] = lock
            handler.__dict__["stream"] = stream
        return out_dict

    def __setstate__(self, in_dict):
        in_dict["_logger"].disabled = in_dict.pop("disabled")
        in_dict["_logger"].setLevel(in_dict.pop("level"))
        in_dict["_logger"].propagate = in_dict.pop("propagate")
        in_dict["_logger"].filters = in_dict.pop("filters")
        in_dict["_logger"].handlers = _rebuild_handlers(in_dict.pop("handlers"))
        self.__dict__ = in_dict

    # Constructors/Destructors
    def construct(self, obj=None):
        if isinstance(obj, logging.Logger):
            self._logger = obj
        else:
            self._logger = logging.getLogger(obj)

    # Logger Editing
    def set_logger(self, logger):
        self._logger = logger

    def fileConfig(self, name, fname, defaults=None, disable_existing_loggers=True):
        logging.config.fileConfig(fname, defaults, disable_existing_loggers)
        self._logger = logging.getLogger(name)

    def copy_logger_attributes(self, logger):
        logger.propagate = self.propagate
        logger.setLevel(self.getEffectiveLevel())
        for filter_ in self.filters:
            logger.addFilter(filter_)
        for handler in self.handlers:
            logger.addHandler(handler)
        return logger

    def setParent(self, parent):
        new_logger = parent.getChild(self.name_stem)
        self.copy_logger_attributes(new_logger)
        self._logger = new_logger

    # Defaults
    def append_module_info(self):
        self.append_message = "Class' Module: %s Object Module: %s " % (self.module_of_class, self.module_of_object)
        self.allow_append = True

    def add_default_stream_handler(self, level=logging.DEBUG):
        handler = logging.StreamHandler()
        handler.setLevel(level)
        formatter = PreciseFormatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.addHandler(handler)

    # Override Logger Methods
    def debug(self, msg, *args, append=None, **kwargs):
        if append or (append is None and self.allow_append):
            msg = self.append_message + msg
        self._logger.debug(msg, *args, **kwargs)

    def info(self, msg, *args, append=None, **kwargs):
        if append or (append is None and self.allow_append):
            msg = self.append_message + msg
        self._logger.info(msg, *args, **kwargs)

    def warning(self, msg, *args, append=None, **kwargs):
        if append or (append is None and self.allow_append):
            msg = self.append_message + msg
        self._logger.warning(msg, *args, **kwargs)

    def error(self, msg, *args, append=None, **kwargs):
        if append or (append is None and self.allow_append):
            msg = self.append_message + msg
        self._logger.error(msg, *args, **kwargs)

    def critical(self, msg, *args, append=None, **kwargs):
        if append or (append is None and self.allow_append):
            msg = self.append_message + msg
        self._logger.critical(msg, *args, **kwargs)

    def log(self, level, msg, *args, append=None, **kwargs):
        if append or (append is None and self.allow_append):
            msg = self.append_message + msg
        self._logger.critical(level, msg, *args, **kwargs)

    def exception(self, msg, *args, append=None, **kwargs):
        if append or (append is None and self.allow_append):
            msg = self.append_message + msg
        self._logger.exception(msg, *args, **kwargs)


class ObjectWithLogging(abc.ABC):
    class_loggers = {}

    def __init__(self):
        self.loggers = self.class_loggers.copy()

    # Logging
    def traceback_formatting(self, func, msg, name=""):
        return "%s(%s) -> %s: %s" % (self.__class__, name, func, msg)


# Functions #
def _rebuild_handlers(handlers):
    new_handlers = []
    for handler in handlers:
        if isinstance(handler, logging.handlers.QueueHandler):
            kwargs = {"queue", handler.queue}
        elif isinstance(handler, logging.handlers.BufferingHandler):
            kwargs = {"capacity": handlers.capacity}
        elif isinstance(handler, logging.handlers.HTTPHandler):
            kwargs = {"host": handler.host, "url": handler.url, "method": handler.method, "secure": handler.secure,
                      "credentials": handler.credentials, "context": handler.context}
        elif isinstance(handler, logging.handlers.NTEventLogHandler):
            kwargs = {"appname": handler.appname, "dllname": handler.dllname, "logtype": handler.logtype}
        elif isinstance(handler, logging.handlers.SMTPHandler):
            kwargs = {"mailhost": handler.mailhost, "fromaddr": handler.fromaddr, "toaddrs": handler.toaddrs,
                      "subject": handler.subject, "credentials": handler.credentials, "secure": handler.secure,
                      "timeout": handler.timeout}
        elif isinstance(handler, logging.handlers.SysLogHandler):
            kwargs = {"address": handler.address, "facility": handler.facility, "socktype": handler.socktype}
        elif isinstance(handler, logging.handlers.SocketHandler):
            kwargs = {"host": handler.host, "port": handler.port}
        elif isinstance(handler, logging.FileHandler):
            kwargs = {"filename": handler.baseFilename, "mode": handler.mode,
                      "encoding": handler.encoding, "delay": handler.delay}
        elif isinstance(handler, logging.StreamHandler):
            kwargs = {}
            warnings.warn("StreamHandler stream cannot be pickled, using default stream (Hint: Define StreamHandler in Process)")
        else:
            warnings.warn()
            continue
        new_handler = type(handler)(**kwargs)
        new_handler.__dict__.update(handler.__dict__)
        new_handlers.append(new_handler)
    return new_handlers


# Main #
if __name__ == "__main__":
    pass
