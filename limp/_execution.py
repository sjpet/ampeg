#  -*- coding: utf-8 -*-
"""Functions supporting multiprocessing execution.

@author: Stefan Peterson
"""

import sys
import traceback
import multiprocessing as mp
from time import time

from ._classes import Dependency, Communication, Err
from ._exceptions import DependencyError, TimeoutError
from ._helpers import is_iterable, recursive_map


def expand_args(args, results):
    """Recursively replace any dependencies in a dict of keyword arguments with
    the actual results.

    Parameters
    ----------
    args : any type t
        A dict of keyword arguments, an iterable of arguments or a single
        argument
    results : [result]
        A list of results

    Returns
    -------
    t
        Input argument(s) with any dependencies replaced
    """

    def f(x):
        if isinstance(x, Dependency):
            if x[1] is None:
                r = results[x[0]][0]
            elif is_iterable(x[1]):
                r = expand_recursively(results[x[0]][0], x[1])
            else:
                r = results[x[0]][0][x[1]]

            if isinstance(r, Err):
                raise DependencyError.default(r)
            else:
                return r

        return x

    return recursive_map(f, args)


def expand_recursively(result, keys):
    """Recursively replace a nested dependency.

    Parameters
    ----------
    result : dict or iterable
        A result
    keys : iterable
        A list of keys to apply in order
    top_level : bool, optional
        Indicates that the current function call is not a recursion, default
        is True


    Returns
    -------
    value
        The dependency value
    """

    if keys:
        next_key = keys[0]
        if len(keys) > 1:    # Only recurse if necessary
            return expand_recursively(result[next_key], keys[1:])
        else:
            return result[next_key]
    else:
        return result


def costs_dict(results, task_ids):
    """Compute a dict of approximate costs.

    Parameters
    ----------
    results : [[(result, time)]]
        A nested list of task_ids to sort execution results
    task_ids : [[task_id, [task_id] or Communication]], optional
        A nested list of task_ids to sort execution results

    Returns
    -------
    dict
        Approximate costs in a dict with task IDs as keys and tuples of
        (computational cost, {dependency: communication cost}) as values
    """

    costs_ = {}
    mux_tasks = {}
    iterator = enumerate(results) if task_ids is None \
        else zip(task_ids, results)
    for task_ids_k, results_k in iterator:
        for task, result in zip(task_ids_k, results_k):
            if isinstance(task, Communication):
                sender, recipients = task
                for recipient in recipients:
                    if recipient in costs_:
                        if sender in costs_[recipient]:
                            costs_[recipient][1][sender] += result[1]
                        else:
                            costs_[recipient][1][sender] = result[1]
                    else:
                        costs_[recipient] = (None, {sender: result[1]})
            elif is_iterable(task):
                costs_[task[0]] = (result[1],
                                   costs_[task[0]][1] if task[0] in costs_
                                   else {})
                for t in task[1:]:
                    mux_tasks[t] = task[0]
            elif task is not None:
                costs_[task] = (result[1],
                                costs_[task][1] if task in costs_ else {})

    for mux_task, proper_task in mux_tasks.items():
        costs_[mux_task] = costs_[proper_task]

    return costs_


def execute_task_list(task_list, lock=None, pipe=None, costs=False):
    """Sequentially execute a task list, handling any inter-task dependencies.

    Parameters
    ----------
    task_list : [(function, kwargs)]
        A list of tasks and their kwargs
    lock : Optional[multiprocessing.Lock]
        A lock used to synchronize return of the result to a master process
    pipe : Optional[multiprocessing.Pipe]
        A pipe used to return the result list to a master process
    costs : bool, optional
        Include start and end times for each task

    Returns
    -------
    [dict]
        A list of result dicts
    """

    results = []
    for k, (task, args) in enumerate(task_list):
        this_start = time()
        try:
            expanded_args = expand_args(args, results)
            if isinstance(expanded_args, dict):
                this_result = task(**expanded_args)
            else:
                this_result = task(*expanded_args)
        except Exception as e:
            _, _, tb = sys.exc_info()
            this_result = Err(e, traceback.extract_tb(tb))
        this_end = time()

        results.append((this_result,) if costs is False
                       else (this_result, this_end - this_start))

    if lock is not None:
        lock.acquire()
        pipe.send(results)
        pipe.close()
        lock.release()

    return results


def execute_task_lists(task_lists, task_ids=None, costs=False, timeout=60):
    """Execute a number of task list over equally many processes.

    Parameters
    ----------
    task_lists : [[(function, kwargs)]]
        A nested list of tasks and their kwargs
    task_ids : [[task_id, [task_id] or Communication]], optional
        A nested list of task_ids to sort execution results
    costs : bool, optional
        Include approximate costs if True. Default is False.
    timeout : int, optional
        Timeout in seconds for collecting results from spawned processes,
        default is 60.

    Returns
    -------
    dict
        The result dict. If task_ids is not None, the keys are task IDs and the
        values task results, otherwise the keys are process indices and the
        values are lists of task results. In addition, if costs is True, there
        is a key named "costs", the value of which is another dict. If task_ids
        is not None, the keys of this dict are Task IDs and the values are
        tuples of (computation cost, [dependency, communication cost]),
        otherwise the keys are process indices and the values are lists of
        execution times.
    """

    n_processes = len(task_lists)

    # Set up pipes
    pipes = [[None for _ in range(n_processes)] for _ in range(n_processes)]
    for source in range(n_processes):
        for sink in range(n_processes):
            if source < sink:
                x, y = mp.Pipe()
                pipes[source][sink] = x
                pipes[sink][source] = y

    # Start execution of other task lists
    p = [None for _ in range(n_processes)]
    locks = [None for _ in range(n_processes)]

    for k in range(1, n_processes):
        locks[k] = mp.Lock()
        locks[k].acquire()
        p[k] = mp.Process(target=execute_task_list,
                          args=(task_lists[k], locks[k], pipes[k][0], costs))
        p[k].start()

    # Execute own task list
    results = [execute_task_list(task_lists[0], costs=costs)]

    # Collect results from other processes
    for k in range(1, n_processes):
        locks[k].release()
        if pipes[0][k].poll(timeout) is False:
            timeout_error = TimeoutError.default(k)
            results.append([(Err(timeout_error),) for _ in task_lists[k]])
        else:
            results.append(pipes[0][k].recv())
        p[k].join()

    results_ = {}
    iterator = enumerate(results) if task_ids is None \
        else zip(task_ids, results)
    for task_ids_k, results_k in iterator:
        for task, result in zip(task_ids_k, results_k):
            if isinstance(task, Communication):
                pass
            elif is_iterable(task):
                for t in task:
                    if t is not None:
                        results_[t] = result[0]
            elif task is not None:
                results_[task] = result[0]

    if costs is True:
        results_["costs"] = costs_dict(results, task_ids)

    return results_
