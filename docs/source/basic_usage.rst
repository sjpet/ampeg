===========
Basic Usage
===========

To install :mod:`limp`, simply navigate to the source directory and run

.. code-block:: bash

   python setup.py install

Computation Graphs
------------------

:mod:`limp` is based around directed acyclic graphs called computation graphs.
Each vertex of such a graph is a computational subtask and each edge represents
a dependency on another subtask. A computational sub-task consist of a function
and some arguments. In addition, each sub-task has a label (task ID) and a
computational cost associated with it. Edges have a weight representing the
cost of communicating the corresponding data between processes.

Computation graphs are implemented as ``Dict[Hashable, Subtask]``, where
``Subtask`` is a tuple of ``(Callable, Any, Number)`` holding the function,
argument(s) and estimated computation cost. The second field may be a single
argument of any kind, a list or tuple of positional arguments or a dict of
keyword arguments. Edges are defined by the :class:`limp.Dependency` class
used as a placeholder argument.

As a toy example, the task of computing :math:`3^2 + 8^2` can be divided into
three subtasks: computing the square of 3, computing the square of 8 and
summing the two values. The corresponding computation graph has three nodes:
`square 1`, `square 2` and `sum`; and two edges: `{square 1, sum}` and
`{square 2, sum}`. In Python, we implement this as:

>>> import limp
>>> my_graph = {"square 1": (lambda x: x**2, 3, 8),
...             "square 2": (lambda x: x**2, 8, 8),
...             "sum": (lambda x, y: x + y,
...                     [limp.Dependency("square 1", None, 1),
...                      limp.Dependency("square 2", None, 1)],
...                     1)}

Scheduling Subtasks on Multiple Processes
-----------------------------------------

:meth:`limp.earliest_finish_time` uses Heterogenous Earliest Finish Time
(HEFT) to schedule subtasks on number of processes given by the user. As the
name implies, HEFT assumes that all processes are created equal and thus that
the computational cost of a subtask is independent of the process in which it
is executed, and that the communication cost for a dependency is the same for
any pair of processes and any direction.

The function returns two nested lists with length equal to the number of
separate processes. The first is the actual task lists for each process, the
second holds the task IDs associated with each task.

>>> task_lists, task_ids = limp.earliest_finish_time(my_graph, 2)

Before constructing the task lists, :meth:`limp.earliest_finish_time`
removes any duplicate tasks (i.e. tasks with identical functions and arguments,
including dependencies on other identical tasks). The removed tasks are still
included in the task ID lists, which means that the dict returned by
:meth:`limp.execute_task_lists` will reflect the input graph.

Executing Task Lists
--------------------

:meth:`limp.execute_task_lists` takes the two nested lists returned by
:meth:`limp.earliest_finish_time`, executes the tasks on separate processes
before collecting the results and combining them into a dict mapping task IDs
to results.

>>> limp.execute_task_lists(task_lists, task_ids)
{'sum': 73, 'square 1': 9, 'square 2': 64}

Filtering Results
-----------------

For large graphs, it might not be necessary to return the results of each
subtask. For this reason, :meth:`limp.earliest_finish_time` takes an optional
keyword argument ``output_tasks: List[Hashable]``:

>>> limp.execute_task_lists(*limp.earliest_finish_time(
...     my_graph, 2, output_tasks=["sum"]))
{'sum': 73}

Inter-process Communication
---------------------------

:mod:`limp` uses queues to send results between processes. By default, a
process will wait indefinitely when instructed to receive data from another
process. If desired, a time limit may be imposed by passing the optional 
keyword argument ``timeout: Number`` to :meth:`limp.earliest_finish_time`. If
no data has been received within the allowed time, a :exc:`limp.TimeoutError`
is raised. Similarly, :meth:`limp.execute_task_lists` has a default time limit
of 60 seconds to collect the results from all child processes, and takes the
optional keyword argument ``timeout: Number`` to change this limit.

Cost Feedback
-------------

To evaluate computation- and communication cost estimates,
:meth:`limp.execute_task_lists` can return the approximate costs associated
with each task and dependency by setting the optional keyword argument
``costs`` to ``True``. The returned dict then has a key ``"costs"``, which maps
to another dict with the same structure which, instead of results, contains
tuples of ``(float, List[(Hashable, float)])`` holding the computation time and
a list of approximated communication times for dependencies that were computed
in a different process.
