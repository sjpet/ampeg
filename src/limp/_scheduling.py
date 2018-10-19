#  -*- coding: utf-8 -*-
"""Functions for scheduling tasks over several proceses.

@author: Stefan Peterson
"""

import multiprocessing as mp

import six

from ._classes import Dependency, Communication
from ._helpers import (inf,
                       is_iterable,
                       successor_graph,
                       reverse_graph,
                       equivalent_args,
                       demux,
                       recursive_map)


# ## Helper functions

def add_process_index(t, p):
    """Add a process index to a schedule slot.

    Parameters
    ----------
    t : (Hashable, float, float)
    p : int

    Returns
    -------
    (Hashable, int, float, float)
    """
    return t[0], p, t[1], t[2]


def overlaps(slot_a, slot_b):
    """Check if two slots overlap.

    Parameters
    ----------
    slot_a : (Hashable, float, float)
        The first slot, given as a (task, start, finish)-tuple
    slot_b : (Hashable, float, float)
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
    slot_a : (int, float, float)
        The first slot, given as a (task, start, finish)-tuple
    slot_b : (int, float, float)
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
    args : Any
        A single argument, an iterable of positional arguments or a dict of
        keyword arguments
    labels : Dict[Hashable, Hashable]
        A dict of task ID replacements

    Returns
    -------
    Any
        A new dict or iterable of arguments
    """

    def f(x):
        if isinstance(x, Dependency):
            predecessor, key_, cost = x
            if predecessor in labels:
                return Dependency(labels[predecessor], key_, cost)
        return x

    return recursive_map(f, args)


def predecessor_graph(graph):
    """Generate a graph showing the predecessor tasks for each task.

    Parameters
    ----------
    graph : Dict[Hashable, (Callable, Any, Number)]
        A directed acyclic graph representing the computations where each
        vertex represents a computational task

    Returns
    -------
    Dict[Hashable, List[Hashable]]
        A simplified graph including showing only predecessor tasks for each
        task
    """
    return reverse_graph(successor_graph(graph))


def list_communication_costs(args):
    """Recursively list communication costs in a dict of keyword arguments.

    Parameters
    ----------
    args : Any
        A single argument, an iterable of arguments or a dict of keyword
        arguments

    Returns
    -------
    List[(Hashable, float)]
        A list of communication costs
    """

    communication_costs = {}

    if isinstance(args, Dependency):
        return [(args.task_id, args.communication_cost)]
    elif isinstance(args, dict):
        iterable_args = args.values()
    elif is_iterable(args):
        iterable_args = args
    else:
        return []

    for val in iterable_args:
        if isinstance(val, dict) or is_iterable(val):
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
    graph : Dict[Hashable, (Callable, Any, Number)]
        A directed acyclic graph representing the computations where each
        vertex represents a computational task and each edge a dependency

    Returns
    -------
    Dict[Hashable, float]
        A dict of computational costs per task
    Dict[Hashable, List[(Hashable, float)]
        A dict of communication costs per task
    """

    computational_costs = {key: val[2] for key, val in graph.items()}
    communication_costs = {key: list_communication_costs(val[1])
                           for key, val in graph.items()}

    return computational_costs, communication_costs


def generate_task_lists(graph, schedule, timeout):
    """Generate a set of task lists from a graph and schedule.

    Parameters
    ----------
    graph : Dict[Hashable, (Callable, Any, Number)]
        A directed acyclic graph representing the computations where each
        vertex represents a computational task
    schedule : [[(Hashable, Number, Number)]]
        Task schedule represented as a nested list of (task, start, finish)-
        tuples, one list per processor
    timeout : Number
        Timeout for receive tasks

    Returns
    -------
    List[List[(Callable, Any)]]
        A set of task lists
    List[List[Hashable]]
        A nested list for mapping execution results to task IDs
    """

    num_processes = len(schedule)

    queues = [[None for _ in range(num_processes)]
              for _ in range(num_processes)]
    for sink in range(num_processes):
        for source in range(sink):
            x = mp.Queue()
            y = mp.Queue()
            queues[source][sink] = x
            queues[sink][source] = y

    task_process = {slot[0]: process
                    for process in range(len(schedule))
                    for slot in schedule[process]}

    successor_processes = \
        {key: demux((task_process[task], task) for task in val
                    if not task_process[task] == task_process[key])
         for key, val in successor_graph(graph).items()}

    # Flatten the schedule and sort by finish time (reverse order)
    flat_schedule = sorted([add_process_index(t, p)
                            for p in range(len(schedule))
                            for t in schedule[p]],
                           key=lambda q: q[3],
                           reverse=True)

    task_lists = [[] for _ in range(num_processes)]
    task_ids = [[] for _ in range(num_processes)]
    receive_queue = [[] for _ in range(num_processes)]
    task_indices = [{} for _ in range(num_processes)]

    while len(flat_schedule) > 0:
        this_task, process, start, finish = flat_schedule.pop()

        # Add any queued receive tasks
        removals = []
        for k in range(len(receive_queue[process])):
            task_, process_, finish_, own_tasks = receive_queue[process][k]
            if finish_ < start:
                task_indices[process][task_] = len(task_lists[process])
                task_lists[process].append((receive,
                                            {"queue": queues[process_][process],
                                             "timeout": timeout}))
                task_ids[process].append(Communication(task_, own_tasks))
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
        for (p, ts) in successor_processes[this_task]:
            task_lists[process].append((send, {"queue": queues[process][p],
                                               "result": Dependency(task_index,
                                                                    None),
                                               "timeout": timeout}))
            task_ids[process].append(Communication(this_task, ts))
            receive_queue[p].append((this_task, process, finish, ts))

    return task_lists, task_ids


def multiplex_task_ids(task_ids, multiplexing_keys):
    """Multiplex task ids.

    Parameters
    ----------
    task_ids : List[List[Hashable]]
        A nested list for mapping execution results to task IDs
    multiplexing_keys : Dict[Hashable, List[Hashable]]
        A dict of multiplexing keys

    Returns
    -------
    List[List[Union[Hashable, Tuple[Hashable, ...]]]
        A nested list for mapping execution results to task IDs, including
        multiplexed tasks
    """

    task_ids_ = [[p for p in q] for q in task_ids]

    if multiplexing_keys == {}:
        return task_ids_

    for k in range(len(task_ids_)):
        for kk in range(len(task_ids_[k])):
            this_task_id = task_ids_[k][kk]
            if (not isinstance(this_task_id, Communication) and
                    this_task_id in multiplexing_keys):
                task_ids_[k][kk] = [this_task_id] + \
                                   multiplexing_keys[this_task_id]

    return task_ids_


def filter_task_ids(task_ids, output_tasks):
    """Filter task IDs, replacing the ID of non-output tasks with None.

    Parameters
    ----------
    task_ids : List[List[Union[Hashable, Tuple[Hashable, ...]]]
        A nested list for mapping execution results to task IDs
    output_tasks : List[Hashable]
        A list of output tasks

    Returns
    -------
    List[List[Union[Hashable, Tuple[Hashable, ...]]]
        A filtered,  nested list for mapping execution results to task IDs
    """
    if output_tasks is None:
        return task_ids

    filtered_task_ids = []
    for k, task_ids_k in enumerate(task_ids):
        filtered_task_ids.append([])
        for task_id in task_ids_k:
            if isinstance(task_id, Communication):
                if task_id.sender in output_tasks:
                    task_id_ = task_id
                elif any(x in output_tasks for x in task_id.recipients):
                    task_id_ = Communication(task_id.sender,
                                             [x for x in task_id.recipients
                                              if x in output_tasks])
                else:
                    task_id_ = None
            elif isinstance(task_id, tuple):
                task_id_ = tuple(id_ for id_ in task_id if id_ in output_tasks)
            else:
                task_id_ = task_id if task_id in output_tasks else None
            filtered_task_ids[k].append(task_id_)

    return filtered_task_ids


def idle_slots(schedule):
    """List the idle time slots in a single processor schedule.

    Parameters
    ----------
    schedule : List[(Hashable, float, float)]
        Single processor task schedule represented as a list of
        (task, start, finish)-tuples

    Returns
    -------
    List[(float, float)]
        List of (start, finish)-tuples
    """

    idle_schedule = []
    last = 0.0

    for slot in schedule:
        _, start, finish = slot
        length = start - last
        if length > 0.0:
            idle_schedule.append((last, start))
        elif length < 0.0:
            # TODO: investigate issue that can cause unsorted
            raise ValueError("Schedule must be sorted")
        last = finish

    idle_schedule.append((last, inf))

    return idle_schedule


def add_slot(task, start, finish, schedule):
    """Add a new slot to a single process schedule.

    Parameters
    ----------
    task : Hashable
        ID of the task
    start : float
        Starting time
    finish : float
        Finish time
    schedule : List[(Hashable, float, float)]
        Single processor task schedule represented as a list of
        (task, start, finish)-tuples

    Returns
    -------
    List[(Hashable, float, float)]
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


def available(min_length, earliest_time, schedule):
    """Find the earliest available time slot of some minimum length in a
    single processor schedule.

    Parameters
    ----------
    min_length : float
        The required length
    earliest_time : float
        The earliest starting time allowed
    schedule : [(Hashable, float, float)]
        Single processor task schedule represented as a list of
        (task, start, finish)-tuples

    Returns
    -------
    float
        Start of the earliest available, sufficiently long time slot
    """

    for time_slot in idle_slots(schedule):
        start_time, end_time = time_slot
        start_time_ = max(start_time, earliest_time)
        if end_time - start_time_ >= min_length:
            return start_time_

    raise ValueError("No available time slot found.")


def actual_finish_time(task, schedule):
    """Find the finish time and assigned processor for a given task.

    Parameters
    ----------
    task : Hashable
        Task ID
    schedule : [[(Hashable, float, float)]]
        Task schedule represented as a nested list of (task, start, finish)-
        tuples, one list per processor

    Returns
    -------
    float
        Task finish time
    int
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
    task : Hashable
        Task ID
    processor : int
        Index of the processor
    dependencies : List[Hashable]
        Predecessor tasks IDs
    computation_costs : Dict[Hashable, float]
        Computational costs per task
    communication_costs : Dict[Hashable, List[(Hashable, float)]
        Communication costs per task
    schedule : List[(Hashable, float, float)]

    Returns
    -------
    float
        Earliest start time
    """

    dependency_times = [0.0]

    for this_task in dependencies:
        (finish_time, this_processor) = actual_finish_time(this_task, schedule)

        if this_processor == processor:
            dependency_times.append(finish_time)
        else:
            communication_cost = [c for k, c in communication_costs[task] if
                                  k == this_task][0]
            dependency_times.append(finish_time + communication_cost)

    earliest_start = max(dependency_times)

    return available(computation_costs[task],
                     earliest_start,
                     schedule[processor])


# ## Preprocessing functions

def prefix(graph, prefix_key):
    """Prefix task IDs in a ``graph`` with some ``prefix_key``.

    Parameters
    ----------
    graph : Dict[Hashable, (Callable, Any, Number)]
        A directed acyclic graph representing the computations where each
        vertex represents a computational task. The graph is represented by a
        dict with task IDs as keys and tuples of (function, arguments,
        computational cost) as values. Edges are implied by ``Dependency``
        instances among arguments.
    prefix_key : Hashable

    Returns
    -------
    Dict[Hashable, (Callable, Any, Number)]
        A new graph with the task IDs prefixed.
    """
    prefixed_graph = {}
    for key, val in six.iteritems(graph):
        prefixed_graph[prefix_single(key, prefix_key)] = \
            prefix_dependencies(val, prefix_key)

    return prefixed_graph


def prefix_dependencies(args, prefix_key):
    """Prefix dependencies in task argument(s).

    Parameters
    ----------
    args : Any
        A single argument, an iterable of arguments or a dict of keyword
        arguments
    prefix_key : Hashable
        A dict of task reference replacements

    Returns
    -------
    Any
        A new dict or iterable of arguments
    """

    def f(x):
        if isinstance(x, Dependency):
            predecessor, key_, cost = x
            return Dependency(prefix_single(predecessor, prefix_key),
                              key_,
                              cost)
        return x

    return recursive_map(f, args)


def prefix_single(task_id, prefix_key):
    """Prefix a single task ID with some ``prefix_key``.

    Parameters
    ----------
    task_id : Hashable
    prefix_key: Hashable

    Returns
    -------
    Tuple[Hashable, ...]
    """
    if isinstance(task_id, tuple):
        return tuple([prefix_key] + list(task_id))
    else:
        return prefix_key, task_id


def remove_duplicates(graph):
    """Remove duplicate tasks from a ``graph``.

    This is done automatically by ``limp.earliest_finish_time``.

    Parameters
    ----------
    graph : Dict[Hashable, (Callable, Any, Number)]
        A directed acyclic graph representing the computations where each
        vertex represents a computational task. The graph is represented by a
        dict with task IDs as keys and tuples of (function, arguments,
        computational cost) as values. Edges are implied by ``Dependency``
        instances among arguments.

    Returns
    -------
    Dict[Hashable, (Callable, Any, Number)]
        An equivalent graph without duplicates.
    Dict[Hashable, List[Hashable]]
        A dict of multiplexing keys from the shortened to the original graph.
    """

    graph_ = graph.copy()

    successors = successor_graph(graph_)
    predecessors = predecessor_graph(graph_)

    reduced_graph = {}
    multiplexing_keys = {}
    this_tier = [key for key, p in predecessors.items() if not p]

    while len(this_tier) > 0:
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

def send(queue, result, timeout=None):
    """Send a result from one process to another.

    Parameters
    ----------
    queue : multiprocessing.Queue
    result : Any
    timeout : Optional[int]
        Optional timeout in seconds, default is None
    """

    queue.put(result, True, timeout)
    return None


def receive(queue, timeout=None):
    """Receive a result from another process.

    Parameters
    ----------
    queue : multiprocessing.Queue
    timeout : Optional[int]
        Optional timeout in seconds, default is None

    Returns
    -------
    Hashable
        Task id
    Any
        Results
    """

    return queue.get(True, timeout)


# ## Scheduling heuristics

def add_task_eft(task,
                 dependencies,
                 computation_costs,
                 communication_costs,
                 schedule):
    """Add a task to a ``schedule``.

    Parameters
    ----------
    task : Hashable
        Task ID
    dependencies : List[Hashable]
        Predecessor task IDs
    computation_costs : Dict[Hashable, float]
        Computational costs per task
    communication_costs : Dict[Hashable, List[(Hashable, float)]
        Communication costs per task
    schedule : List[List[(Hashable, float, float)]]
        Schedule

    Returns
    -------
    List[List[(Hashable, float, float)]]
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
    graph : Dict[Hashable, (Callable, Any, Number)]
        A directed acyclic graph representing the computations where each
        vertex represents a computational task and each edge indicates a
        successor task

    Returns
    -------
    Dict[Hashable, float]
        A dict containing the rank of each task
    """

    computation_costs, communication_costs = costs(graph)

    mean_communication_costs = {k: sum(c for _, c in t)/float(len(t)) if t
                                else 0 for k, t in communication_costs.items()}

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


def earliest_finish_time(graph, n_processes, output_tasks=None, timeout=None):
    """Generate a list of ``n_processes`` task lists from a ``graph``.

    Parameters
    ----------
    graph : Dict[Hashable, (Callable, Any, Number)]
        A directed acyclic graph representing the computations where each
        vertex represents a computational task. The graph is represented by a
        dict with task IDs as keys and tuples of (function, arguments,
        computational cost) as values. Edges are implied by ``Dependency``
        instances among arguments.
    n_processes : int
        Number of processes to use for execution.
    output_tasks : Optional[List[Hashable]]
        A list of output tasks. Default is None, which considers all tasks to
        be output tasks.
    timeout : Optional[Number]
        Optional timeout in seconds, default is None

    Returns
    -------
    List[List[(Callable, Any)]]
        A list of task lists.
    List[List[Union[Hashable, List[Hashable], Communication]]]
        A list of lists for mapping execution results to task IDs.
    """

    graph_, multiplexing_keys = remove_duplicates(graph)

    computation_costs, communication_costs = costs(graph_)

    predecessors = predecessor_graph(graph_)
    ranks = upward_rank(graph_)

    task_ids = [task for task in graph_.keys()]
    ranks_ = [ranks[task] for task in task_ids]

    # noinspection PyTypeChecker
    task_priority = [task_ids[k] for k in sorted(range(len(task_ids)),
                                                 key=ranks_.__getitem__)]

    schedule = [[] for _ in range(n_processes)]

    while len(task_priority) > 0:
        next_task = task_priority.pop()
        schedule = add_task_eft(next_task,
                                predecessors[next_task],
                                computation_costs,
                                communication_costs,
                                schedule)

    task_lists, task_ids_ = generate_task_lists(graph_, schedule, timeout)
    task_ids = filter_task_ids(multiplex_task_ids(task_ids_,
                                                  multiplexing_keys),
                               output_tasks)

    return task_lists, task_ids
