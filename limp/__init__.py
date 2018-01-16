# -*- coding: utf-8 -*-
"""
Limp
====

Limp provides a few simple tools for parallel computing, wrapping gently around
multiprocessing.

Computation graphs
------------------

The core data structure for limp is the execution graph, a directed acyclic
graph represented by a dict of vertices where each key is the ID of a task and
each value is a triple of (function, arg/args/kwargs, computation cost).
Edges are implicitly defined by dependencies among the args or kwargs.

NOTE: dicts are interpreted as kwargs and lists or tuples as args, meaning that
a dict, list or tuple that is the sole input to a function must be enclosed in
a tuple, i.e. ``{0: (sum, ([1, 2, 3, 4],), 0)}``.

Dependencies
------------

A dependency is a triple of (task ID or index, key (if any) and communication
cost). The key may be a single key, index or slice, or it may be an iterable of
such values to be applied in sequence. For example, the key ``('values', 2)``
extracts the value 5 from the dict ``{'values': [1, 3, 5]}``. Dependency
instances are created by limp.Dependency(task, key, cost) where cost is
optional and defaults to 0.

Example
-------

A simple usage example computing (3^2 + 4^2) - (3^2 * 10/2):

>>> import limp
>>> n_processes = 3
>>> my_graph = {0: (lambda x: x**2, 3, 10.8),
                1: (lambda x: x**2, 4, 10.8),
                2: (lambda x: x/2, 10, 11),
                3: (lambda x, y: x + y, (limp.Dependency(0, None, 1),
                                         limp.Dependency(1, None, 1), 10.7),
                4: (lambda x, y: x*y, (limp.Dependency(0, None, 1),
                                       limp.Dependency(2, None, 1)), 10.8),
                5: (lambda x, y: x - y, (limp.Dependency(3, None, 1),
                                         limp.Dependency(4, None, 1)), 10.9)}
>>> task_lists, task_ids = limp.earliest_finish_time(my_graph, n_processes)
>>> limp.execute_task_lists(task_lists, task_ids)
{0: 9, 1: 16, 2: 5, 3: 25, 4: 45, 5: -20}

"""

from ._classes import Dependency, Communication, Err
from ._exceptions import DependencyError, TimeoutError
from ._scheduling import earliest_finish_time, remove_duplicates
from ._execution import execute_task_lists

__version__ = "0.5"
