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


def inflate_results(results):
    """Inflate a dict of results, expanding any tuple keys.

    Parameters
    ----------
    results : dict
        A flat dict of results

    Returns
    -------
    dict
        A nested dict of results
    """
    inflated_results = {}
    for key, val in results.iteritems():
        if isinstance(key, tuple):
            current_level = inflated_results
            for key_ in key[:-1]:
                if key_ not in current_level:
                    current_level[key_] = {}
                current_level = current_level[key_]
            current_level[key[-1]] = val
        else:
            inflated_results[key] = val

    return inflated_results


def collect_results(results, task_ids):
    """Collect execution result lists into a dict, using keys from lists of
    task IDs.

    Parameters
    ----------
    results : List[List[Any]]
        A nested list of execution results
    task_ids : List[List[task ID]]
        A nested list of task IDs

    Returns
    -------
    dict
        A nested dict of results
    """
    results_ = {}
    iterator = enumerate(results) if task_ids is None \
        else zip(task_ids, results)
    for task_ids_k, results_k in iterator:
        for task, result in zip(task_ids_k, results_k):
            if isinstance(task, Communication):
                pass
            elif isinstance(task, list):
                for t in task:
                    if t is not None:
                        results_[t] = result[0]
            elif task is not None:
                results_[task] = result[0]

    return results_


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
            elif is_iterable(expanded_args):
                this_result = task(*expanded_args)
            else:
                this_result = task(expanded_args)
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


def execute_task_lists(task_lists,
                       task_ids=None,
                       inflate=False,
                       costs=False,
                       timeout=60):
    """Execute a number of task list over equally many processes.

    Parameters
    ----------
    task_lists : List[List[(function, kwargs)]]
        A nested list of tasks and their kwargs
    task_ids : Optional[List[List[Union[task_id, [task_id], Communication]]]]
        A nested list of task_ids to sort execution results
    inflate : Optional[bool]
        Inflate tuple keys in the results if True. Default is False.
    costs : Optional[bool]
        Include approximate costs if True. Default is False.
    timeout : Optional[int]
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

    # Fetch results from child processes
    for k in range(1, n_processes):
        locks[k].release()
        if pipes[0][k].poll(timeout) is False:
            timeout_error = TimeoutError.default(k)
            results.append([(Err(timeout_error),) for _ in task_lists[k]])
        else:
            results.append(pipes[0][k].recv())
        p[k].join()

    results_ = collect_results(results, task_ids)

    if inflate is True:
        results_ = inflate_results(results_)

    if costs is True:
        costs_key = "costs"
        k = 0
        while costs_key in results_:
            costs_key = "costs_{}".format(k)
            k += 1
        if not costs_key == "costs":
            message = "'costs' is already a task ID, using '{costs_key}' " \
                      "instead"
            raise UserWarning(message.format(costs_key=costs_key))

        results_["costs"] = costs_dict(results, task_ids)

    return results_
