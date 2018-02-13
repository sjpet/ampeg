# -*- coding: utf-8 -*-
"""Unit tests for functions in limp.scheduling.

Author: Stefan Peterson
"""

import limp

from .helpers import square_one, add
from .data import (test_graph_1,
                   computation_costs_1,
                   communication_costs_1,
                   successor_graph_1,
                   upward_rank_1,
                   task_lists_functions_only_1,
                   task_ids_1,
                   test_graph_2,
                   reduced_graph_1,
                   multiplexing_keys_1)

inf = limp._scheduling.inf


# Helper function tests

def test_overlaps_preceding():
    assert not limp._scheduling.overlaps((0, 23, 45.2), (1, 45.2, 67))


def test_overlaps_succeeding():
    assert not limp._scheduling.overlaps((0, 45.2, 67), (1, 23, 45.2))


def test_overlaps_overlapping_1():
    assert limp._scheduling.overlaps((0, 23, 45.2), (1, 39.1, 67))


def test_overlaps_overlapping_2():
    assert limp._scheduling.overlaps((0, 39.1, 67), (1, 23, 45.2))


def test_overlaps_overlapping_3():
    assert limp._scheduling.overlaps((0, 39.1, 45.2), (1, 23, 67))


def test_overlaps_overlapping_4():
    assert limp._scheduling.overlaps((0, 23, 67), (1, 39.1, 45.2))


def test_precedes_preceding_1():
    assert limp._scheduling.precedes((0, 23, 45.2), (1, 45.2, 67))


def test_precedes_preceding_2():
    assert limp._scheduling.precedes((0, 23, 45.2), (1, 48.6, 67))


def test_precedes_succeeding():
    assert not limp._scheduling.precedes((0, 45.2, 67), (1, 23, 45.2))


def test_precedes_overlapping_1():
    assert not limp._scheduling.precedes((0, 23, 45.2), (1, 39.1, 67))


def test_precedes_overlapping_2():
    assert not limp._scheduling.precedes((0, 39.1, 67), (1, 23, 45.2))


def test_precedes_overlapping_3():
    assert not limp._scheduling.precedes((0, 39.1, 45.2), (1, 23, 67))


def test_precedes_overlapping_4():
    assert not limp._scheduling.precedes((0, 23, 67), (1, 39.1, 45.2))


def test_list_dependencies_single_dependency_1():
    task_args = {'a': limp.Dependency(0, 'x')}
    assert limp._scheduling.list_dependencies(task_args) == [0]


def test_list_dependencies_single_dependency_2():
    task_args = {'a': limp.Dependency(0, 'x'),
                 'b': limp.Dependency(0, 'y')}
    assert limp._scheduling.list_dependencies(task_args) == [0]


def test_list_dependencies_multiple_dependencies():
    task_args = {'a': limp.Dependency(0, 'x'),
                 'b': limp.Dependency(1, 'y')}
    assert limp._scheduling.list_dependencies(task_args) == [0, 1]


def test_list_dependencies_no_dependencies():
    task_args = {'a': 6, 'b': 3.2}
    assert limp._scheduling.list_dependencies(task_args) == []


def test_list_dependencies_nested_dependencies():
    task_args = {'a': limp.Dependency(0, 'x'),
                 'b': {'c': limp.Dependency(1, 'y'),
                       'd': limp.Dependency(3, 'z')}}
    assert limp._scheduling.list_dependencies(task_args) == [0, 1, 3]


def test_successor_graph():
    assert limp._scheduling.successor_graph(test_graph_1) == successor_graph_1


def test_successor_graph_nested_dependencies():
    test_graph = {('a', 0): (square_one, 3, 1),
                  ('b', 0): (square_one, 2, 1),
                  ('sums', 0): (sum,
                                ([limp.Dependency(('a', 0), None, 1),
                                  limp.Dependency(('b', 0), None, 1)],),
                                1)}
    successor_graph = {('a', 0): [('sums', 0)],
                       ('b', 0): [('sums', 0)],
                       ('sums', 0): []}
    assert limp._scheduling.successor_graph(test_graph) == successor_graph


def test_costs():
    x, y = limp._scheduling.costs(test_graph_1)
    assert x == computation_costs_1
    # assert y == communication_costs_1
    assert set(y.keys()) == set(communication_costs_1.keys())
    for key in y.keys():
        assert set(y[key]) == set(communication_costs_1[key])


def test_costs_single_dependency():
    test_graph = {0: (lambda x: x, [1, 2, 3], 4),
         1: (lambda x: x, limp.Dependency(0, 1, 3), 2)}
    x, y = limp._scheduling.costs(test_graph)
    assert x == {0: 4, 1: 2}
    assert y == {0: [], 1: [(0, 3)]}


def test_idle_slots():
    schedule = [(0, 0, 56.1), (1, 72.3, 89.3)]
    assert limp._scheduling.idle_slots(schedule) == [(56.1, 72.3),
                                                     (89.3, inf)]


def test_idle_slots_empty_schedule():
    assert limp._scheduling.idle_slots([]) == [(0, inf)]


def test_available_idle_slot_too_short():
    test_schedule = [(None, 0.0, 1.2), (None, 2.8, 3.6)]
    assert limp._scheduling.available(2.0, 0.0, test_schedule) == 3.6


def test_available_idle_slot_sufficient():
    test_schedule = [(None, 0.0, 1.2), (None, 3.3, 3.9)]
    assert limp._scheduling.available(2.0, 0.0, test_schedule) == 1.2


def test_available_idle_slot_too_early():
    test_schedule = [(None, 0.0, 1.2), (None, 3.3, 3.9)]
    assert limp._scheduling.available(2.0, 2.5, test_schedule) == 3.9


def test_add_slot_to_empty_schedule():
    assert limp._scheduling.add_slot('task_name',
                                     12.8,
                                     56.2,
                                     []) == [('task_name', 12.8, 56.2)]


def test_add_slot_immediately_following():
    schedule = [('task_name', 12.8, 56.2)]
    new_schedule = [('task_name', 12.8, 56.2), ('new_task_name', 56.2, 76.1)]
    assert limp._scheduling.add_slot('new_task_name',
                                     56.2,
                                     76.1,
                                     schedule) == new_schedule


def test_add_slot_in_idle():
    schedule = [('task_name', 12.8, 56.2)]
    new_schedule = [('new_task_name', 0.0, 10.3), ('task_name', 12.8, 56.2)]
    assert limp._scheduling.add_slot('new_task_name',
                                     0.0,
                                     10.3,
                                     schedule) == new_schedule


# Preprocessing tests

def test_relabel_dependencies_dict():
    args = {'a': limp.Dependency('task_0', 6)}
    labels = {'task_0': 0}
    new_args = {'a': limp.Dependency(0, 6)}
    assert limp._scheduling.relabel_dependencies(args, labels) == new_args


def test_relabel_dependencies_iterable():
    args = [limp.Dependency('task_0', 6)]
    labels = {'task_0': 0}
    new_args = [limp.Dependency(0, 6)]
    assert limp._scheduling.relabel_dependencies(args, labels) == new_args


def test_remove_duplicates():
    reduced_graph, multiplexing_keys = \
        limp._scheduling.remove_duplicates(test_graph_2)
    assert reduced_graph == reduced_graph_1
    assert multiplexing_keys == multiplexing_keys_1


def test_prefix():
    test_graph = {0: (square_one, 4, 2), 1: (add, (7, 5), 1)}
    assert limp.prefix(test_graph, 'a') == {('a', 0): (square_one, 4, 2),
                                            ('a', 1): (add, (7, 5), 1)}


def test_prefix_with_dependencies():
    test_graph = {0: (square_one, 4, 2),
                  1: (add, (limp.Dependency(0, None, 1), 5), 1)}
    test_graph_prefixed = {('a', 0): (square_one, 4, 2),
                           ('a', 1): (add,
                                      (limp.Dependency(('a', 0), None, 1), 5),
                                      1)}
    assert limp.prefix(test_graph, 'a') == test_graph_prefixed


def test_prefix_extend():
    test_graph = {('a', 0): (square_one, 4, 2),
                  1: (add, (7, 5), 1)}
    test_graph_prefixed = {('foo', 'a', 0): (square_one, 4, 2),
                           ('foo', 1): (add, (7, 5), 1)}
    assert limp.prefix(test_graph, 'foo') == test_graph_prefixed


# Scheduling tests

def test_upward_rank():
    assert limp._scheduling.upward_rank(test_graph_1) == upward_rank_1


def test_eft():
    task_lists, task_ids = limp.earliest_finish_time(test_graph_1, 4)

    task_lists_functions_only = [[f for f, _ in q] for q in task_lists]
    assert task_lists_functions_only == task_lists_functions_only_1
    assert task_ids == task_ids_1
