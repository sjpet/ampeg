====
Limp
====

Limp is a Lightweight framework for Multiprocessing. It provides generic
functions for scheduling and execution of a set of dependent or independent
computational tasks over multiple processes using the multiprocessing package.

Requirements
------------
Python 2.6 or later
Python 3.3 or later

Installation
------------

Limp is not (yet?) on pypi so manual installation is required.

Usage
-----

Limp exposes a scheduling function ``earliest_finish_time``, an execution
function ``execute_task_lists`` and a class ``Dependency``. The former takes a
directed acyclic graph (DAG) and a number of processes and produces a set of
task lists for each process and a corresponding set of task IDs for translating
the execution result. These two form the input to ``execute_task_lists``, which
returns a dict with the result of each task in the original graph.

The DAG is represented by a python dict of vertices where each key is the ID of
a task and each value is a triple of (function, args or kwargs, computation
cost). Edges are implicitly defined by instances of the ``Dependency`` class in
the args or kwargs.

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

The Dependency class
--------------------

A dependency is a triple of (task ID or index, key (if any) and communication
cost). The key may be a single key, index or slice, or it may be an iterable of
such values to be applied in sequence. For example, the key ``('values', 2)``
extracts the value 5 from the dict ``{'values': [1, 3, 5]}``. Dependency 
instances are created using ``limp.Dependency(task, key, cost)`` where cost is
optional and defaults to 0.
