#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" processingtesting.py
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
import pathlib
import datetime
from dateutil.tz import tzlocal

# Downloaded Libraries #
import pynwb
from pynwb import NWBFile
from pynwb.ecephys import ElectricalSeries
from nwbext_ecog import ECoGSubject
from pynwb.spec import NWBNamespaceBuilder, NWBGroupSpec, NWBAttributeSpec

# Local Libraries #
from processingcore import ProcessingTask, SeparateProcess, ProcessingUnit


# Definitions #
# Classes #
class NWBContainer(NWBFile):
    VERSION = "0.0.0"
    DATA_KWARGS = {"chunks": True, "maxshape": (None, None), "compression": "gzip", "compression_opts": 4}

    # Instantiation, Copy, Destruction
    def __init__(self, path=None, init=False, **kwargs):
        super().__init__(**kwargs)
        self._path = None

        self.path = path

        self.cargs = self.DATA_KWARGS
        self.default_datasets_parameters = self.cargs.copy()

        self.container = None

        if init:
            self.construct()

    @property
    def path(self):
        return self._path

    @path.setter
    def path(self, value):
        if isinstance(value, pathlib.Path) or value is None:
            self._path = value
        else:
            self._path = pathlib.Path(value)

    @property
    def is_open(self):
        pass

    @property
    def file_attrs_names(self):
        pass

    @property
    def dataset_names(self):
        pass

    def __copy__(self):
        pass

    def __deepcopy__(self, memo={}):
        pass

    def __del__(self):
        pass

    # Pickling
    def __getstate__(self):
        pass

    def __setstate__(self, state):
        pass

    # Attribute Access
    def __getattribute__(self, item):
        pass

    def __setattr__(self, key, value):
        pass

    # Container Magic Methods
    def __len__(self):
        pass

    def __getitem__(self, item):
        pass

    def __setitem__(self, key, value):
        pass

    # Context Managers
    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self.close()

    # Constructors
    def construct(self, open_=False, **kwargs):
        pass

    def create_container(self):
        self.container = NWBFile()

    # File Creation
    def create_file(self, open_=False):
        pass

    def construct_file_attributes(self, value=""):
        pass

    def construct_file_datasets(self, **kwargs):
        pass

    # Copy Methods
    def copy(self):
        return self.__copy__()

    def deepcopy(self, path=None, init=False, memo={}):
        pass

    # Getters and Setters
    def get(self, item):
        pass

    # File Attributes
    def get_file_attribute(self, item):
        pass

    def get_file_attributes(self):
        pass

    def set_file_attribute(self, key, value):
        pass

    def add_file_attributes(self, items):
        pass

    def clear_attributes(self):
        pass

    def load_attributes(self):
        pass

    def list_attributes(self):
        pass

    # Datasets
    def create_dataset(self, name, data=None, **kwargs):
        pass

    def get_dataset(self, item):
        pass

    def set_dataset(self, name, data=None, **kwargs):
        pass

    def add_file_datasets(self, items):
        pass

    def clear_datasets(self):
        pass

    def load_datasets(self):
        pass

    def get_dataset_names(self):
        pass

    def list_dataset_names(self):
        pass

    #  Mapping Items Methods
    def items(self):
        pass

    def items_file_attributes(self):
        pass

    def items_datasets(self):
        pass

    # Mapping Keys Methods
    def keys(self):
        pass

    def keys_file_attributes(self):
        pass

    def keys_datasets(self):
        pass

    # Mapping Pop Methods
    def pop(self, key):
        pass

    def pop_file_attribute(self, key):
        pass

    def pop_dataset(self, key):
        pass

    # Mapping Update Methods
    def update_file_attrs(self, **kwargs):
        pass

    def update_datasets(self, **kwargs):
        pass

    # File Methods
    def open(self, mode="a", exc=False, validate=False, **kwargs):
        pass

    def close(self):
        pass

    # General Methods
    def append2dataset(self, name, data, axis=0):
        pass

    def report_file_structure(self):
        pass

    def validate_file_structure(self, file_type=True, o_attrs=True, f_attrs=False, o_datasets=True, f_datasets=False):
        pass


class NWBTask(ProcessingTask):
    pass


class NWBProcessor(ProcessingUnit):
    DEFAULT_TASK = NWBTask


# Functions #


# Main #
if __name__ == "__main__":
    pass

