# -*- coding: utf-8 -*-
"""Unit tests for functions in limp.scheduling.

Author: Stefan Peterson
"""

import limp

from .helpers import square_one
from .data import (test_graph_1, successor_graph_1)

inf = limp._scheduling.inf

# Helper function tests


def test_list_dependencies_single_dependency_1():
    task_args = {'a': limp.Dependency(0, 'x')}
    assert limp._helpers.list_dependencies(task_args) == [0]


def test_list_dependencies_single_dependency_2():
    task_args = {'a': limp.Dependency(0, 'x'),
                 'b': limp.Dependency(0, 'y')}
    assert limp._helpers.list_dependencies(task_args) == [0]


def test_list_dependencies_multiple_dependencies():
    task_args = {'a': limp.Dependency(0, 'x'),
                 'b': limp.Dependency(1, 'y')}
    assert limp._helpers.list_dependencies(task_args) == [0, 1]


def test_list_dependencies_no_dependencies():
    task_args = {'a': 6, 'b': 3.2}
    assert limp._helpers.list_dependencies(task_args) == []


def test_list_dependencies_nested_dependencies():
    task_args = {'a': limp.Dependency(0, 'x'),
                 'b': {'c': limp.Dependency(1, 'y'),
                       'd': limp.Dependency(3, 'z')}}
    assert limp._helpers.list_dependencies(task_args) == [0, 1, 3]


def test_successor_graph():
    assert limp._helpers.successor_graph(test_graph_1) == successor_graph_1


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
    assert limp._helpers.successor_graph(test_graph) == successor_graph

