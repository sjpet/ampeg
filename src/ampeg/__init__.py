# -*- coding: utf-8 -*-
"""Ampeg provides a few simple tools for parallel computing, wrapping gently
around the multiprocessing module.

Computation graphs
------------------

The core data structure for ampeg is the computation graph, a directed acyclic
graph represented by a dict of vertices where each key is the ID of a task and
each value is a triple of (function, arguments, computation cost). Edges are
implicitly given by dependencies among the arguments.

Arguments may be in the form of a single argument, a list or tuple of
positional arguments or a dict of keyword arguments. As a result, if a list,
tuple or dict is the sole argument to a function, it must be enclosed in a
tuple, i.e. ``{0: (sum, ([1, 2, 3, 4],), 0)}``.

Example
-------

A simple usage example computing :math:`(3^2 + 4^2) - (3^2 * 10/2)`:

>>> import ampeg as ag
>>> n_processes = 3
>>> my_graph = {0: (lambda x: x**2, 3, 10.8),
...             1: (lambda x: x**2, 4, 10.8),
...             2: (lambda x: x/2, 10, 11),
...             3: (lambda x, y: x + y,
...                 (ag.Dependency(0, None, 1),
...                  ag.Dependency(1, None, 1)),
...                 10.7),
...             4: (lambda x, y: x*y,
...                 (ag.Dependency(0, None, 1),
...                  ag.Dependency(2, None, 1)),
...                 10.8),
...             5: (lambda x, y: x - y,
...                 (ag.Dependency(3, None, 1),
...                  ag.Dependency(4, None, 1)),
...                 10.9)}
>>> task_lists, task_ids = \\
...     ag.earliest_finish_time(my_graph, n_processes)
>>> ag.execute_task_lists(task_lists, task_ids)
{0: 9, 1: 16, 2: 5, 3: 25, 4: 45, 5: -20}

"""

from ._classes import Dependency, Communication, Err
from ._exceptions import DependencyError, TaskTimeoutError
from ._scheduling import (earliest_finish_time, remove_duplicates, prefix)
from ._execution import execute_task_lists
from ._helpers import to_dot

name = "ampeg"
__version__ = "0.1.2"
