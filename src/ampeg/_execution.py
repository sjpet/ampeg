#  -*- coding: utf-8 -*-
"""Functions supporting multiprocessing execution.

@author: Stefan Peterson
"""

import sys
import traceback
import multiprocessing as mp
from time import time

import six
from six.moves.queue import Empty

from ._classes import Dependency, Communication, Err
from ._exceptions import DependencyError, TaskTimeoutError
from ._helpers import is_iterable, recursive_map
from ._scheduling import send


def expand_args(args, results):
    """Recursively replace any dependencies in a dict of keyword arguments with
    the actual results.

    Parameters
    ----------
    args : Any
        A dict of keyword arguments, an iterable of arguments or a single
        argument
    results : List[Any]
        A list of results

    Returns
    -------
    Any
        Input argument(s) with any dependencies replaced
    """

    def f(x):
        if isinstance(x, Dependency):
            task_result = results[x.task_id][0]
            if isinstance(task_result, Err):
                raise DependencyError.default(task_result)

            if x.key is None:
                r = task_result
            elif is_iterable(x.key):
                r = expand_recursively(task_result, x.key)
            else:
                r = get_any(task_result, x.key)

            if isinstance(r, Err):
                raise DependencyError.default(r)

            return r

        return x

    return recursive_map(f, args)


def expand_recursively(result, keys):
    """Recursively replace a nested dependency.

    Parameters
    ----------
    result : Iterable[Any]
        A result
    keys : Iterable[Hashable]
        A list of keys to apply in order

    Returns
    -------
    Any
        The dependency value
    """

    if keys:
        next_key = keys[0]
        if len(keys) > 1:    # Only recurse if necessary
            return expand_recursively(get_any(result, next_key), keys[1:])
        else:
            return get_any(result, next_key)
    else:
        return result


def inflate_results(results):
    """Inflate a dict of results, expanding any tuple keys.

    Parameters
    ----------
    results : Dict[Hashable, Any]
        A flat dict of results

    Returns
    -------
    Dict[Any]
        A nested dict of results
    """
    inflated_results = {}
    for key, val in six.iteritems(results):
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
    task_ids : Union[\
            List[List[Union[Hashable, List[Hashable], Communication]]], None]
        A nested list of task IDs

    Returns
    -------
    Dict[Hashable, Any]
    """
    results_ = {}
    iterator = enumerate(results) if task_ids is None \
        else zip(task_ids, results)
    for task_ids_k, results_k in iterator:
        if task_ids is None:
            inner_ids = ((task_ids_k, q) for q in range(len(results_k)))
        else:
            inner_ids = task_ids_k
        for task, result in zip(inner_ids, results_k):
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
    results : List[List[(Any, float)]]
        A nested list of task_ids to sort execution results
    task_ids : Union[\
            List[List[Union[Hashable, List[Hashable], Communication]]], None]
        A nested list of task_ids to sort execution results

    Returns
    -------
    Dict[Hashable, (float, Dict[Hashable, float])]
        Approximate costs in a dict with task IDs as keys and tuples of
        (computational cost, {dependency: communication cost}) as values
    """

    costs_ = {}
    mux_tasks = {}
    iterator = enumerate(results) if task_ids is None \
        else zip(task_ids, results)
    for task_ids_k, results_k in iterator:
        if task_ids is None:
            inner_ids = ((task_ids_k, q) for q in range(len(results_k)))
        else:
            inner_ids = task_ids_k
        for task, result in zip(inner_ids, results_k):
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
            elif isinstance(task, list):
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


def execute_task_list(task_list, pipe=None, costs=False):
    """Sequentially execute a task list, handling any inter-task dependencies.

    Parameters
    ----------
    task_list : List[(Callable, Any)]
        A list of tasks and their arguments
    pipe : Optional[multiprocessing.Pipe]
        A pipe used to return the result list to a master process
    costs : Optional[bool]
        Include start and end times for each task

    Returns
    -------
    List[Union[(Any, Number), (Any,)]]
        A list of results
    """

    results = []
    for k, (task, args) in enumerate(task_list):
        this_result = None
        this_start = time()

        try:
            expanded_args = expand_args(args, results)
        except Exception as e:
            _, _, tb = sys.exc_info()
            err = Err(e, traceback.extract_tb(tb))
            if task == send and isinstance(args, dict):
                expanded_args = {key: val for key, val in six.iteritems(args)}
                expanded_args["result"] = err
            elif task == send:
                expanded_args = [args[0], err] + args[2:]
            else:
                expanded_args = None
                this_result = err

        if this_result is None:
            try:
                if isinstance(expanded_args, dict):
                    this_result = task(**expanded_args)
                elif is_iterable(expanded_args):
                    this_result = task(*expanded_args)
                else:
                    this_result = task(expanded_args)
            except Exception as e:
                _, _, tb = sys.exc_info()
                if isinstance(e, Empty):
                    e = TaskTimeoutError.default(None)
                this_result = Err(e, traceback.extract_tb(tb))

        this_end = time()

        results.append((this_result,) if costs is False
                       else (this_result, this_end - this_start))

    if pipe is not None:
        pipe.send(results)
        pipe.close()

    return results


def execute_task_lists(task_lists,
                       task_ids=None,
                       inflate=False,
                       costs=False,
                       timeout=None):
    """Execute a number of task list over equally many processes.

    Parameters
    ----------
    task_lists : List[List[(Callable, Any)]]
        A list of task lists.
    task_ids : Optional[\
            List[List[Union[Hashable, List[Hashable], Communication]]]]
        A list of lists for mapping execution results to task IDs. Default is
        None.
    inflate : Optional[bool]
        Inflate tuple keys in the results if True. Default is False.
    costs : Optional[bool]
        Include approximate costs if True. Default is False.
    timeout : Optional[Number]
        Optional timeout in seconds for collecting results from spawned
        processes, default is None.

    Returns
    -------
    Dict[Hashable, Any]
        A dict of results. where the keys are task IDs and the values task
        results. If ``task_ids`` is None, the task ID is (process index,
        task index in process). In addition, if costs is True, there
        is a key named "costs", the value of which is another dict with the
        same keys and nesting, and tuples of (computation cost, [(predecessor
        task ID, communication cost)]) as values.
    """

    n_processes = len(task_lists)

    # Start execution of other task lists
    pipes = [None for _ in range(n_processes)]
    p = [None for _ in range(n_processes)]
    for k in range(1, n_processes):
        pipes[k] = mp.Pipe()
        p[k] = mp.Process(target=execute_task_list,
                          args=(task_lists[k], pipes[k][1], costs))
        p[k].start()

    # Execute own task list
    results = [execute_task_list(task_lists[0], costs=costs)]

    # Fetch all results and join child processes
    for k in range(1, n_processes):
        if pipes[k][0].poll() is True or timeout is None:
            results.append(pipes[k][0].recv())
        elif not p[k].is_alive() or pipes[k][0].poll(timeout) is False:
            timeout_error = TaskTimeoutError.default(k)
            results.append([(Err(timeout_error),) for _ in task_lists[k]])
        pipes[k][0].close()

    for k in range(1, n_processes):
        if p[k].is_alive():
            p[k].join(1)

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

        if inflate is True:
            results_[costs_key] = inflate_results(costs_dict(results,
                                                             task_ids))
        else:
            results_[costs_key] = costs_dict(results, task_ids)

    return results_


def get_any(x, key):
    """Get a value from an object by attribute name, key or index.

    Parameters
    ----------
    x : Any
    key : Hashable

    Returns
    -------
    Any
    """
    try:
        return x[key]
    except (TypeError, IndexError, KeyError) as e:
        if isinstance(key, str) and hasattr(x, key):
            return x.__dict__[key]
        
        raise e

