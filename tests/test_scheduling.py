# -*- coding: utf-8 -*-
"""Unit tests for functions in limp.scheduling.

Author: Stefan Peterson
"""

# import os
# import pytest

from .context import limp
from .helpers import (stats, square, sum_stats, normalize, diff)

inf = limp.scheduling.inf

# ## Test graphs and vectors
test_x = [[0, 6, 2, 6, 1, 2, 3, 7, 2, 3, 1, 5, 6, 2, 8],
          [1, 4, 5, 2, 3, 1, 4, 4, 3, 2, 5, 6, 3, 2, 1]]

test_graph_1 = {'stats_0': (stats, {'x': test_x[0]}, 13),
                'stats_1': (stats, {'x': test_x[1]}, 52),
                2: (square, (test_x[0],), 64),
                3: (square, {'x': test_x[1]}, 38),
                4: (sum_stats,
                    (limp.Dependency('stats_0', ('dummy', 'mu'), 5),
                     limp.Dependency('stats_1', ('dummy', 'mu'), 3),
                     limp.Dependency('stats_0', ('dummy', 'var')),
                     limp.Dependency('stats_1', ('dummy', 'var'))),
                    56),
                5: (normalize,
                    {'x': limp.Dependency(2, None, 13),
                     'mu': limp.Dependency(4, 0, 6),
                     'var': limp.Dependency(4, 1)},
                    75),
                6: (normalize,
                    {'x': limp.Dependency(3, None, 7),
                     'mu': limp.Dependency('stats_1', ('dummy', 'mu'), 8),
                     'var': limp.Dependency('stats_1', ('dummy', 'var'))},
                    75),
                'final': (diff,
                          {'x': limp.Dependency(5, 'y', 12),
                           'y': limp.Dependency(6, 'y', 10)},
                          42)}

computation_costs_1 = {'stats_0': 13,
                       'stats_1': 52,
                       2: 64,
                       3: 38,
                       4: 56,
                       5: 75,
                       6: 75,
                       'final': 42}

communication_costs_1 = {'stats_0': [],
                         'stats_1': [],
                         2: [],
                         3: [],
                         4: [('stats_0', 5), ('stats_1', 3)],
                         5: [(2, 13), (4, 6)],
                         6: [(3, 7), ('stats_1', 8)],
                         'final': [(5, 12), (6, 10)]}

successor_graph_1 = {'stats_0': [4],
                     'stats_1': [4, 6],
                     2: [5],
                     3: [6],
                     4: [5],
                     5: ['final'],
                     6: ['final'],
                     'final': []}

upward_rank_1 = {'stats_0': 210.5,
                 'stats_1': 385,
                 2: 201.5,
                 3: 173.5,
                 4: 197.5,
                 5: 137.5,
                 6: 135.5,
                 'final': 53}

task_lists_functions_only_1 = [[stats,
                                limp.scheduling.send,
                                limp.scheduling.receive,
                                sum_stats,
                                limp.scheduling.receive,
                                normalize,
                                limp.scheduling.receive,
                                diff],
                               [stats,
                                limp.scheduling.send,
                                limp.scheduling.receive,
                                limp.scheduling.receive,
                                normalize,
                                limp.scheduling.send],
                               [square,
                                limp.scheduling.send],
                               [square,
                                limp.scheduling.send]]

task_ids_1 = [['stats_1', None, None, 4, None, 5, None, 'final'],
              ['stats_0', None, None, None, 6, None],
              [2, None],
              [3, None]]

test_graph_2 = {0: (square, {'x': test_x[0]}, 13),
                1: (square, {'x': test_x[0]}, 16),
                2: (stats, {'x': limp.Dependency(0, None, 4)}, 28),
                3: (stats, {'x': limp.Dependency(1, None, 2)}, 21),
                4: (normalize,
                    {'x': test_x[0],
                     'mu': limp.Dependency(2, ('dummy', 'mu'), 5),
                     'var': limp.Dependency(2, ('dummy', 'var'), 5)}, 17),
                5: (normalize,
                    {'x': test_x[1],
                     'mu': limp.Dependency(3, ('dummy', 'mu'), 8),
                     'var': limp.Dependency(3, ('dummy', 'var'), 8)}, 22),
                6: (stats, {'x': limp.Dependency(1, None, 3)}, 15)}

reduced_graph_1 = {0: (square, {'x': test_x[0]}, 16),
                   2: (stats, {'x': limp.Dependency(0, None, 4)}, 28),
                   4: (normalize,
                       {'x': test_x[0],
                        'mu': limp.Dependency(2, ('dummy', 'mu'), 5),
                        'var': limp.Dependency(2, ('dummy', 'var'), 5)}, 17),
                   5: (normalize,
                       {'x': test_x[1],
                        'mu': limp.Dependency(2, ('dummy', 'mu'), 8),
                        'var': limp.Dependency(2, ('dummy', 'var'), 8)}, 22)}

multiplexing_keys_1 = {0: [1], 2: [3, 6]}


# ## Tests for helper functions

def test_overlaps_preceding():
    assert not limp.scheduling.overlaps((0, 23, 45.2), (1, 45.2, 67))


def test_overlaps_succeeding():
    assert not limp.scheduling.overlaps((0, 45.2, 67), (1, 23, 45.2))


def test_overlaps_overlapping_1():
    assert limp.scheduling.overlaps((0, 23, 45.2), (1, 39.1, 67))


def test_overlaps_overlapping_2():
    assert limp.scheduling.overlaps((0, 39.1, 67), (1, 23, 45.2))


def test_overlaps_overlapping_3():
    assert limp.scheduling.overlaps((0, 39.1, 45.2), (1, 23, 67))


def test_overlaps_overlapping_4():
    assert limp.scheduling.overlaps((0, 23, 67), (1, 39.1, 45.2))


def test_precedes_preceding_1():
    assert limp.scheduling.precedes((0, 23, 45.2), (1, 45.2, 67))


def test_precedes_preceding_2():
    assert limp.scheduling.precedes((0, 23, 45.2), (1, 48.6, 67))


def test_precedes_succeeding():
    assert not limp.scheduling.precedes((0, 45.2, 67), (1, 23, 45.2))


def test_precedes_overlapping_1():
    assert not limp.scheduling.precedes((0, 23, 45.2), (1, 39.1, 67))


def test_precedes_overlapping_2():
    assert not limp.scheduling.precedes((0, 39.1, 67), (1, 23, 45.2))


def test_precedes_overlapping_3():
    assert not limp.scheduling.precedes((0, 39.1, 45.2), (1, 23, 67))


def test_precedes_overlapping_4():
    assert not limp.scheduling.precedes((0, 23, 67), (1, 39.1, 45.2))


def test_list_dependencies_single_dependency_1():
    task_args = {'a': limp.Dependency(0, 'x')}
    assert limp.scheduling.list_dependencies(task_args) == [0]


def test_list_dependencies_single_dependency_2():
    task_args = {'a': limp.Dependency(0, 'x'),
                 'b': limp.Dependency(0, 'y')}
    assert limp.scheduling.list_dependencies(task_args) == [0]


def test_list_dependencies_multiple_dependencies():
    task_args = {'a': limp.Dependency(0, 'x'),
                 'b': limp.Dependency(1, 'y')}
    assert limp.scheduling.list_dependencies(task_args) == [0, 1]


def test_list_dependencies_no_dependencies():
    task_args = {'a': 6, 'b': 3.2}
    assert limp.scheduling.list_dependencies(task_args) == []


def test_list_dependencies_nested_dependencies():
    task_args = {'a': limp.Dependency(0, 'x'),
                 'b': {'c': limp.Dependency(1, 'y'),
                       'd': limp.Dependency(3, 'z')}}
    assert limp.scheduling.list_dependencies(task_args) == [0, 1, 3]


def test_successor_graph():
    assert limp.scheduling.successor_graph(test_graph_1) == successor_graph_1


def test_costs():
    x, y = limp.scheduling.costs(test_graph_1)
    assert x == computation_costs_1
    assert y == communication_costs_1


def test_idle_slots():
    schedule = [(0, 0, 56.1), (1, 72.3, 89.3)]
    assert limp.scheduling.idle_slots(schedule) == [(56.1, 72.3),
                                                    (89.3, inf)]


def test_idle_slots_empty_schedule():
    assert limp.scheduling.idle_slots([]) == [(0, inf)]


def test_add_slot_to_empty_schedule():
    assert limp.scheduling.add_slot('task_name',
                                    12.8,
                                    56.2,
                                    []) == [('task_name', 12.8, 56.2)]


def test_add_slot_immediately_following():
    schedule = [('task_name', 12.8, 56.2)]
    new_schedule = [('task_name', 12.8, 56.2), ('new_task_name', 56.2, 76.1)]
    assert limp.scheduling.add_slot('new_task_name',
                                    56.2,
                                    76.1,
                                    schedule) == new_schedule


def test_upward_rank():
    assert limp.scheduling.upward_rank(test_graph_1) == upward_rank_1


def test_relabel_dependencies_dict():
    args = {'a': limp.Dependency('task_0', 6)}
    labels = {'task_0': 0}
    new_args = {'a': limp.Dependency(0, 6)}
    assert limp.scheduling.relabel_dependencies(args, labels) == new_args


def test_relabel_dependencies_iterable():
    args = [limp.Dependency('task_0', 6)]
    labels = {'task_0': 0}
    new_args = [limp.Dependency(0, 6)]
    assert limp.scheduling.relabel_dependencies(args, labels) == new_args


def test_remove_duplicates():
    reduced_graph, multiplexing_keys = \
        limp.scheduling.remove_duplicates(test_graph_2)
    assert reduced_graph == reduced_graph_1
    assert multiplexing_keys == multiplexing_keys_1


def test_eft():
    task_lists, task_ids = limp.earliest_finish_time(test_graph_1, 4)

    task_lists_functions_only = [[f for f, _ in q] for q in task_lists]
    assert task_lists_functions_only == task_lists_functions_only_1
    assert task_ids == task_ids_1
