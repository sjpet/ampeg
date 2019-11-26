# -*- coding: utf-8 -*-
"""Unit tests for functions in ampeg._helpers.

Author: Stefan Peterson
"""

import ampeg

from .helpers import square_one
from .data import (test_graph_1, successor_graph_1)

inf = ampeg._scheduling.inf

# Helper function tests


def test_list_dependencies_single_dependency_1():
    task_args = {'a': ampeg.Dependency(0, 'x')}
    assert ampeg._helpers.list_dependencies(task_args) == [0]


def test_list_dependencies_single_dependency_2():
    task_args = {'a': ampeg.Dependency(0, 'x'),
                 'b': ampeg.Dependency(0, 'y')}
    assert ampeg._helpers.list_dependencies(task_args) == [0]


def test_list_dependencies_multiple_dependencies():
    task_args = {'a': ampeg.Dependency(0, 'x'),
                 'b': ampeg.Dependency(1, 'y')}
    assert ampeg._helpers.list_dependencies(task_args) == [0, 1]


def test_list_dependencies_no_dependencies():
    task_args = {'a': 6, 'b': 3.2}
    assert ampeg._helpers.list_dependencies(task_args) == []


def test_list_dependencies_nested_dependencies():
    task_args = {'a': ampeg.Dependency(0, 'x'),
                 'b': {'c': ampeg.Dependency(1, 'y'),
                       'd': ampeg.Dependency(3, 'z')}}
    assert ampeg._helpers.list_dependencies(task_args) == [0, 1, 3]


def test_successor_graph():
    found_successor_graph = ampeg._helpers.successor_graph(test_graph_1)
    assert set(found_successor_graph.keys()) == set(successor_graph_1.keys())
    for task_id in found_successor_graph:
        assert set(found_successor_graph[task_id]) == \
            set(successor_graph_1[task_id])


def test_successor_graph_nested_dependencies():
    test_graph = {('a', 0): (square_one, 3, 1),
                  ('b', 0): (square_one, 2, 1),
                  ('sums', 0): (sum,
                                ([ampeg.Dependency(('a', 0), None, 1),
                                  ampeg.Dependency(('b', 0), None, 1)],),
                                1)}
    successor_graph = {('a', 0): [('sums', 0)],
                       ('b', 0): [('sums', 0)],
                       ('sums', 0): []}
    found_successor_graph = ampeg._helpers.successor_graph(test_graph)
    assert set(found_successor_graph.keys()) == set(successor_graph.keys())
    for task_id in found_successor_graph:
        assert set(found_successor_graph[task_id]) == \
            set(successor_graph[task_id])

