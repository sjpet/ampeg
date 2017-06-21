#  -*- coding: utf-8 -*-
"""Functions for scheduling tasks over several proceses.

@author: Stefan Peterson
"""

import multiprocessing as mp

from .classes import Dependency
from .helpers import (inf, is_iterable, reverse_graph, equivalent_args)


# ## Helper functions

def add_process_index(t, p):
    """Add a process index to a schedule slot.

    Parameters
    ----------
    t : (task_id, Float, Float)
    p : Int

    Returns
    -------
    (task_id, Int, Float, Float)
    """
    return t[0], p, t[1], t[2]


def overlaps(slot_a, slot_b):
    """Check if two slots overlap.

    Parameters
    ----------
    slot_a : (task_id, Float, Float)
        The first slot, given as a (task, start, finish)-tuple
    slot_b : (task_id, Float, Float)
        The second slot, given as a (task, start, finish)-tuple

    Returns
    -------
    bool
        True if the slots overlap
    """

    _, start_a, finish_a = slot_a
    _, start_b, finish_b = slot_b

    if start_b >= finish_a or start_a >= finish_b:
        return False
    else:
        return True


def precedes(slot_a, slot_b):
    """Check if one slot precedes another.

    Parameters
    ----------
    slot_a : (Int, Float, Float)
        The first slot, given as a (task, start, finish)-tuple
    slot_b : (Int, Float, Float)
        The second slot, given as a (task, start, finish)-tuple

    Returns
    -------
    bool
        True if slot_a precedes slot_b
    """

    _, _, finish = slot_a
    _, start, _ = slot_b

    if finish <= start:
        return True
    else:
        return False


def relabel_dependencies(args, labels):
    """Relabel dependencies in a task.

    Parameters
    ----------
    args : dict or iterable
        A dict of keyword arguments or an iterable of arguments
    labels : dict
        A dict of task reference replacements

    Returns
    -------
    dict or iterable
        A new dict or iterable of arguments
    """

    if isinstance(args, dict):
        new_args = {}
        for (key, val) in args.items():
            if isinstance(val, Dependency):
                predecessor, key_, cost = val
                if predecessor in labels:
                    new_args[key] = Dependency(labels[predecessor], key_, cost)
                else:
                    new_args[key] = val
            elif is_iterable(val):
                new_args[key] = relabel_dependencies(val, labels)
            else:
                new_args[key] = val

    elif is_iterable(args):
        new_args = []
        for val in args:
            if isinstance(val, Dependency):
                predecessor, key_, cost = val
                new_args.append(Dependency(labels[predecessor], key_, cost))
            elif is_iterable(val):
                new_args.append(relabel_dependencies(val, labels))
            else:
                new_args.append(val)

    else:
        raise TypeError("Arguments must be supplied as a dict "
                        "or in an iterable")

    return new_args


def list_dependencies(args):
    """Recursively list dependencies in a dict of keyword arguments.

    Parameters
    ----------
    args : dict or iterable
        A dict of keyword arguments or an iterable of arguments

    Returns
    -------
    list
        A list of dependencies
    """

    dependencies = []

    if isinstance(args, dict):
        iterable_args = args.values()
    elif is_iterable(args):
        iterable_args = args
    else:
        raise TypeError("Arguments must be supplied as a dict "
                        "or in an iterable")

    for val in iterable_args:
        if isinstance(val, dict):
            dependencies.extend(list_dependencies(val))
        elif isinstance(val, Dependency):
            predecessor, _, _ = val
            dependencies.append(predecessor)

    return list(set(dependencies))


def successor_graph(graph):
    """

    Parameters
    ----------
    graph : dict
        A directed acyclic graph representing the computations where each
        vertex represents a computational task

    Returns
    -------
    dict
        A simplified graph including showing only successor tasks for each task
    """

    return {key: [key_ for (key_, val_) in graph.items()
                  if key in list_dependencies(val_[1])]
            for (key, val) in graph.items()}


def predecessor_graph(graph):
    """

    Parameters
    ----------
    graph : dict
        A directed acyclic graph representing the computations where each
        vertex represents a computational task

    Returns
    -------
    dict
        A simplified graph including showing only successor tasks for each task
    """
    return reverse_graph(successor_graph(graph))


def list_communication_costs(args):
    """Recursively list communication costs in a dict of keyword arguments.

    Parameters
    ----------
    args : dict or iterable
        A dict of keyword arguments or an iterable of arguments

    Returns
    -------
    list
        A list of dependencies
    """

    communication_costs = {}

    if isinstance(args, dict):
        iterable_args = args.values()
    elif is_iterable(args):
        iterable_args = args
    else:
        raise TypeError("Arguments must be supplied as a dict "
                        "or in an iterable")

    for val in iterable_args:
        if isinstance(val, dict):
            for key, max_val in list_communication_costs(val):
                if key in communication_costs:
                    communication_costs[key].append(max_val)
                else:
                    communication_costs[key] = [max_val]
        elif isinstance(val, Dependency):
            predecessor, _, cost = val
            if predecessor in communication_costs:
                communication_costs[predecessor].append(cost)
            else:
                communication_costs[predecessor] = [cost]

    return [(key, max(val)) for (key, val) in communication_costs.items()]


def costs(graph):
    """Collect the computation and communication costs from a graph.

    Parameters
    ----------
    graph : dict
        A directed acyclic graph representing the computations where each
        vertex represents a computational task and each edge a dependency

    Returns
    -------
    dict
        A dict of computational costs per task
    dict
        A dict of communication costs per task
    """

    computational_costs = {key: val[2] for key, val in graph.items()}
    communication_costs = {key: list_communication_costs(val[1])
                           for key, val in graph.items()}

    return computational_costs, communication_costs


def generate_task_lists(graph, schedule):
    """Generate a set of task lists from a graph and schedule.

    Parameters
    ----------
    graph : dict
        A directed acyclic graph representing the computations where each
        vertex represents a computational task
    schedule : [[(Int, Float, Float)]]
        Task schedule represented as a nested list of (task, start, finish)-
        tuples, one list per processor

    Returns
    -------
    [[(function, kwargs)]]
        A set of task lists
    [[task_id]]
        A nested list for mapping execution results to task IDs
    """

    num_processes = len(schedule)

    pipes = [[None for _ in range(num_processes)]
             for _ in range(num_processes)]
    for source in range(num_processes):
        for sink in range(num_processes):
            if source < sink:
                x, y = mp.Pipe()
                pipes[source][sink] = x
                pipes[sink][source] = y

    task_process = {slot[0]: process
                    for process in range(len(schedule))
                    for slot in schedule[process]}

    successor_processes = \
        {key: list(set(task_process[task] for task in val
                       if not task_process[task] == task_process[key]))
         for key, val in successor_graph(graph).items()}

    # Flatten the schedule and sort by finish time (reverse order)
    flat_schedule = sorted([add_process_index(t, p)
                            for p in range(len(schedule))
                            for t in schedule[p]],
                           key=lambda t: t[3],
                           reverse=True)

    task_lists = [[] for _ in range(num_processes)]
    task_ids = [[] for _ in range(num_processes)]
    receive_queue = [[] for _ in range(num_processes)]
    task_indices = [{} for _ in range(num_processes)]

    while flat_schedule:
        this_task, process, start, finish = flat_schedule.pop()

        # Add any queued receive tasks
        removals = []
        for k in range(len(receive_queue[process])):
            task_, process_, finish_ = receive_queue[process][k]
            if finish_ < start:
                task_indices[process][task_] = len(task_lists[process])
                task_lists[process].append((receive,
                                            {'pipe': pipes[process][process_]}))
                task_ids[process].append(None)
                removals.append(k)

        receive_queue[process] = [receive_queue[process][k]
                                  for k in range(len(receive_queue[process]))
                                  if k not in removals]

        # Add the next task
        task_index = len(task_lists[process])
        task_lists[process].append(
            (graph[this_task][0],
             relabel_dependencies(graph[this_task][1],
                                  task_indices[process])))
        task_ids[process].append(this_task)
        task_indices[process][this_task] = task_index

        # Add any required send tasks
        for k in successor_processes[this_task]:
            task_lists[process].append((send, {'result': Dependency(task_index,
                                                                    None),
                                               'pipe': pipes[process][k]}))
            task_ids[process].append(None)
            receive_queue[k].append((this_task, process, finish))

    return task_lists, task_ids


def multiplex_task_ids(task_ids, multiplexing_keys):
    """Multiplex task ids.

    Parameters
    ----------
    task_ids : [[task_id]]
        A nested list for mapping execution results to task IDs
    multiplexing_keys : dict
        A dict of multiplexing keys

    Returns
    -------
    [[task_id or (task_id,)]]
        A nested list for mapping execution results to task IDs, including
        multiplexed tasks
    """

    task_ids_ = [[p for p in q] for q in task_ids]

    if multiplexing_keys == {}:
        return task_ids_

    for k in range(len(task_ids_)):
        for kk in range(len(task_ids_[k])):
            this_task_id = task_ids_[k][kk]
            if this_task_id in multiplexing_keys:
                task_ids_[k][kk] = (this_task_id,
                                    *multiplexing_keys[this_task_id])

    return task_ids_


def idle_slots(schedule):
    """List the idle time slots in a single processor schedule.

    Parameters
    ----------
    schedule : [(Int, Float, Float)]
        Single processor task schedule represented as a list of
        (task, start, finish)-tuples

    Returns
    -------
    [(float, float)]
        List of (start, length)-tuples
    """

    idle_schedule = []
    last = 0

    for slot in schedule:
        _, start, finish = slot
        length = start - last
        if length > 0:
            idle_schedule.append((last, start))
        elif length < 0:
            raise ValueError("Schedule must be sorted")
        last = finish

    idle_schedule.append((last, inf))

    return idle_schedule


def add_slot(task, start, finish, schedule):
    """Add a new slot to a single processor schedule.

    Parameters
    ----------
    task : Int
        Index of the task
    start : Float
        Starting time
    finish : Float
        Finish time
    schedule : [(Int, Float, Float)]
        Single processor task schedule represented as a list of
        (task, start, finish)-tuples

    Returns
    -------
    [(Int, Float, Float)]
        Updated schedule
    """

    new_slot = (task, start, finish)

    schedule_ = [slot for slot in schedule]

    succeeds = [precedes(new_slot, slot) for slot in schedule_]
    if any(succeeds):
        schedule_.insert(succeeds.index(True), new_slot)
    else:
        schedule_.append(new_slot)

    return schedule_


def available(min_length, schedule):
    """Find the earliest available time slot of some minimum length in a
    single processor schedule.

    Parameters
    ----------
    min_length : float
        The required length
    schedule : [(Int, Float, Float)]
        Single processor task schedule represented as a list of
        (task, start, finish)-tuples

    Returns
    -------
    Float
    """

    for time_slot in idle_slots(schedule):
        start_time, length = time_slot
        if length >= min_length:
            return start_time

    raise ValueError("No available time slot found.")


def actual_finish_time(task, schedule):
    """Find the finish time and assigned processor for a given task.

    Parameters
    ----------
    task : Int
        Index of the task
    schedule : [[(Int, Float, Float)]]
        Task schedule represented as a nested list of (task, start, finish)-
        tuples, one list per processor

    Returns
    -------
    Float
        Task finish time
    Int
        Index of the processor on which the task is executed
    """

    for processor in range(len(schedule)):
        for slot in schedule[processor]:
            this_task, _, finish_time = slot
            if this_task == task:
                return finish_time, processor

    return -1, -1


def est(task,
        processor,
        dependencies,
        computation_costs,
        communication_costs,
        schedule):
    """Compute the earliest possible starting time for a task on a given
    processor.

    Parameters
    ----------
    task : Int
        Index of the task
    processor : Int
        Index of the processor
    dependencies : Iterable[Int]
        Indices of predecessor tasks
    computation_costs : Array-like
        Computational costs per task
    communication_costs : Array-like
        Communication costs per task
    schedule : Array-like

    Returns
    -------
    Float
    """

    est_candidates = []

    for this_task in dependencies:
        (finish_time, this_processor) = actual_finish_time(this_task, schedule)

        if this_processor == processor:
            est_candidates.append(finish_time)
        else:
            communication_cost = [c for k, c in communication_costs[task] if
                                  k == this_task][0]
            est_candidates.append(finish_time + communication_cost)

    est_candidates.append(available(computation_costs[task],
                                    schedule[processor]))

    return max(est_candidates)


# ## Preprocessing functions

def remove_duplicates(graph):
    """Remove duplicate tasks from a graph.

    Parameters
    ----------
    graph : dict
        A directed acyclic graph representing the computations where each
        vertex represents a computational task

    Returns
    -------
    dict
        An equivalent graph without duplicates
    dict
        A dict of multiplexing keys from the shortened to the original graph
    """

    graph_ = graph.copy()

    successors = successor_graph(graph_)
    predecessors = predecessor_graph(graph_)

    reduced_graph = {}
    multiplexing_keys = {}
    this_tier = [key for key, p in predecessors.items() if not p]

    while this_tier:
        for key in this_tier:
            val = graph_[key]
            existing_key = None
            for key_, val_ in reduced_graph.items():
                if val[0] == val_[0] and equivalent_args(val[1], val_[1]):
                    existing_key = key_
                    break
            if existing_key is None:
                reduced_graph[key] = val

            else:
                # Use maximum cost
                reduced_graph[existing_key] = (
                    val[0],
                    val[1],
                    max(val[2], reduced_graph[existing_key][2]))

                # Update dependencies of successors
                for successor in successors[key]:
                    val_ = graph_[successor]
                    graph_[successor] = (
                        val_[0],
                        relabel_dependencies(val_[1], {key: existing_key}),
                        val_[2])

                if existing_key in multiplexing_keys:
                    multiplexing_keys[existing_key].append(key)
                else:
                    multiplexing_keys[existing_key] = [key]

            predecessors.pop(key)
            for key_ in predecessors:
                if key in predecessors[key_]:
                    predecessors[key_].remove(key)

            this_tier = [key for key, p in predecessors.items() if not p]

    return reduced_graph, multiplexing_keys


# ## Glue functions

def send(result, pipe):
    """Send a result from one process to another.

    Parameters
    ----------
    result : dict
    pipe : multiprocessing.Pipe
    """

    pipe.send(result)
    return None


def receive(pipe):
    """Receive a result from another process.

    Parameters
    ----------
    pipe : multiprocessing.Pipe

    Returns
    -------
    dict
    """

    return pipe.recv()


# ## Scheduling heuristics

def add_task_eft(task,
                 dependencies,
                 computation_costs,
                 communication_costs,
                 schedule):
    """Add a task

    Parameters
    ----------
    task : Int
        Index of the task
    dependencies : Iterable[Int]
        Indices of predecessor tasks
    computation_costs : Array-like
        Computational costs per task
    communication_costs : Array-like
        Communication costs per task
    schedule : Array-like

    Returns
    -------
    [[(Int, Float, Float)]]
        Updated schedule
    """

    candidates = [est(task,
                      k,
                      dependencies,
                      computation_costs,
                      communication_costs,
                      schedule) for k in range(len(schedule))]

    best_slot = min(candidates)
    processor = candidates.index(best_slot)

    schedule[processor] = add_slot(task,
                                   best_slot,
                                   best_slot + computation_costs[task],
                                   schedule[processor])

    return schedule


def upward_rank(graph):
    """Compute the upward rank of each task in a graph (defined as the
    total cost plus the sum of the ranks of all immediate successor tasks).

    Parameters
    ----------
    graph : dict
        A directed acyclic graph representing the computations where each
        vertex represents a computational task and each edge indicates a
        successor task

    Returns
    -------
    dict
        A dict containing the rank of each task
    """

    computation_costs, communication_costs = costs(graph)

    mean_communication_costs = {k: sum(c for _, c in t)/len(t) if t else 0
                                for k, t in communication_costs.items()}

    graph_ = successor_graph(graph)

    ranks = {k: 0 for k in graph.keys()}

    exit_tasks = [k for (k, s) in graph_.items() if s == []]

    for task in exit_tasks:
        ranks[task] = computation_costs[task] + \
                                mean_communication_costs[task]

    predecessors = [k for (k, s) in graph_.items()
                    if any(q in s for q in exit_tasks)]

    while len(predecessors) > 0:
        for task in predecessors:
            ranks[task] = \
                computation_costs[task] + mean_communication_costs[task] + \
                sum(ranks[k] for k in graph_[task])

        predecessors = [k for (k, s) in graph_.items()
                        if any(q in s for q in predecessors)]

    return ranks


def earliest_finish_time(graph, n_processes):
    """Generate a set of task lists from a graph.

    Parameters
    ----------
    graph : dict
        A directed acyclic graph representing the computations where each
        vertex represents a computational task
    n_processes : Int
        Number of processes to use

    Returns
    -------
    [[(function, kwargs)]]
        A set of task lists
    [[task_id or [task_id]]]
        A nested list for mapping execution results to task IDs
    """

    graph_, multiplexing_keys = remove_duplicates(graph)

    computation_costs, communication_costs = costs(graph_)

    predecessors = predecessor_graph(graph_)
    ranks = upward_rank(graph_)

    task_ids = [task for task in graph_.keys()]
    ranks_ = [ranks[task] for task in task_ids]

    task_priority = [task_ids[k] for k in sorted(range(len(task_ids)),
                                                 key=ranks_.__getitem__)]

    schedule = [[] for _ in range(n_processes)]

    while not task_priority == []:
        next_task = task_priority.pop()
        schedule = add_task_eft(next_task,
                                predecessors[next_task],
                                computation_costs,
                                communication_costs,
                                schedule)

    task_lists, task_ids = generate_task_lists(graph_, schedule)

    return task_lists, multiplex_task_ids(task_ids, multiplexing_keys)
