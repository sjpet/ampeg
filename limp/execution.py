#  -*- coding: utf-8 -*-
"""Functions supporting multiprocessing execution.

@author: Stefan Peterson
"""

import multiprocessing as mp

from .classes import Dependency
from .helpers import is_iterable


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

    if isinstance(args, Dependency):
        if args[1] is None:
            return results[args[0]]
        elif is_iterable(args[1]):
            return expand_recursively(results[args[0]], args[1])
        else:
            return results[args[0]][args[1]]

    elif isinstance(args, dict):
        return {key: expand_args(val, results) for (key, val) in args.items()}

    elif isinstance(args, tuple):
        return tuple(expand_args(v, results) for v in args)

    elif isinstance(args, list):
        return [expand_args(v, results) for v in args]

    else:
        return args


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


def execute_task_list(task_list, lock=None, pipe=None):
    """Sequentially execute a task list, handling any inter-task dependencies.

    Parameters
    ----------
    task_list : [(function, kwargs)]
        A list of tasks and their kwargs
    lock : Optional[multiprocessing.Lock]
        A lock used to synchronize return of the result to a master process
    pipe : Optional[multiprocessing.Pipe]
        A pipe used to return the result list to a master process

    Returns
    -------
    [dict]
        A list of result dicts
    """

    results = []
    for (task, args) in task_list:
        if isinstance(args, dict):
            results.append(task(**expand_args(args, results)))
        else:
            results.append(task(*expand_args(args, results)))

    if lock is not None:
        lock.acquire()
        pipe.send(results)
        pipe.close()
        lock.release()

    return results


def execute_task_lists(task_lists, task_ids=None):
    """Execute a number of task list over equally many processes.

    Parameters
    ----------
    task_lists : [[(function, kwargs)]]
        A nested list of tasks and their kwargs
    task_ids : [[task_id or [task_id]]], optional
        A nested list of task_ids to sort execution results

    Returns
    -------
    [[dict]] or dict
        A nested list of result dicts or, if task_ids where given, a single
        result dict
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
                          args=(task_lists[k], locks[k], pipes[k][0]))
        p[k].start()

    # Execute own task list
    results = [execute_task_list(task_lists[0])]

    # Collect results from other processes
    for k in range(1, n_processes):
        locks[k].release()
        results.append(pipes[0][k].recv())
        p[k].join()

    if task_ids is None:
        return results

    else:
        results_ = {}
        for task_ids_k, results_k in zip(task_ids, results):
            for task, result in zip(task_ids_k, results_k):
                if is_iterable(task):
                    for t in task:
                        results_[t] = result
                elif task is not None:
                    results_[task] = result

        # results_ = {task: result for k in range(n_processes)
        #             for task, result in zip(task_ids[k], results[k]) if
        #             task is not None}
        #
        # if multiplexing_keys is not None:
        #     for key, additional_keys in multiplexing_keys:
        #         for key_ in additional_keys:
        #             results_[key_] = results_[key]

        return results_
