# -*- coding: utf-8 -*-
"""Unit tests for functions in limp.execution.

Author: Stefan Peterson
"""

# import os
# import pytest

from .context import limp
from .helpers import (stats, square, sum_stats, normalize, diff)

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

stats_0 = stats(test_x[0])
stats_1 = stats(test_x[1])
squared_0 = square(test_x[0])
squared_1 = square(test_x[1])
summed_stats = sum_stats(stats_0['dummy']['mu'],
                         stats_1['dummy']['mu'],
                         stats_0['dummy']['var'],
                         stats_1['dummy']['var'])
norm_0 = normalize(squared_0, *summed_stats)
norm_1 = normalize(squared_1, stats_1['dummy']['mu'], stats_1['dummy']['var'])


results_1 = {'stats_0': stats_0,
             'stats_1': stats_1,
             2: squared_0,
             3: squared_1,
             4: summed_stats,
             5: norm_0,
             6: norm_1,
             'final': diff(norm_0['y'], norm_1['y'])}

costs_1 = {'stats_0': (None, {}),
           'stats_1': (None, {}),
           2: (None, {}),
           3: (None, {}),
           4: (None, {'stats_0': None}),
           5: (None, {2: None}),
           6: (None, {'stats_1': None, 3: None}),
           'final': (None, {6: None})}


# Dependency expansion tests

def test_expand_recursively_nested_iterable():
    results = [None, [5]]
    keys = (1, 0)
    assert limp._execution.expand_recursively(results, keys) == 5


def test_expand_recursively_nested_dict():
    results = {'a': {'b': 6, 'c': 7}}
    keys = ('a', 'b')
    assert limp._execution.expand_recursively(results, keys) == 6


def test_expand_args_single_result():
    results = [(None,), (None,), (7,)]
    args = (limp.Dependency(2, None),)
    assert limp._execution.expand_args(args, results) == (7,)


def test_expand_arg_dict_key():
    results = [(None,), (None,), ({'a': 8},)]
    args = (limp.Dependency(2, 'a'),)
    assert limp._execution.expand_args(args, results) == (8,)


def test_expand_arg_iterable_index():
    results = [(None,), (None,), ([6, 9, 2],)]
    args = (limp.Dependency(2, 1),)
    assert limp._execution.expand_args(args, results) == (9,)


def test_expand_arg_nested_keys():
    results = [(None,), (None,), ({'a': [8, 9, 10]},)]
    args = (limp.Dependency(2, ('a', 2)),)
    assert limp._execution.expand_args(args, results) == (10,)


def test_expand_arg_no_dependency():
    results = [(None,), (None,), (7,)]
    args = (42,)
    assert limp._execution.expand_args(args, results) == args


def test_expand_args_iterable_no_dependency():
    results = [(None,), (None,), (7,)]
    args = ([1, 2, 3, 4],)
    assert limp._execution.expand_args(args, results) == args


def test_expand_args_dict():
    results = [({'a': 6},), (None,), (None,), (8,)]
    args = {'x': limp.Dependency(0, 'a'), 'y': limp.Dependency(3, None)}
    assert limp._execution.expand_args(args, results) == {'x': 6, 'y': 8}


def test_expand_args_list():
    results = [({'a': 6},), (None,), (None,), (8,)]
    args = [limp.Dependency(0, 'a'), limp.Dependency(3, None)]
    assert limp._execution.expand_args(args, results) == [6, 8]


def test_expand_args_iterable_mixed():
    results = [({'a': 6},), (None,), (None,), (8,)]
    args = [limp.Dependency(0, 'a'), 63, (limp.Dependency(3, None), 5)]
    assert limp._execution.expand_args(args, results) == [6, 63, (8, 5)]


def test_expand_args_nested_dict():
    results = [({'a': 6},), (None,), (None,), (8,)]
    args = {'stage_0': {'x': limp.Dependency(0, 'a'),
                        'y': limp.Dependency(3, None)}}
    expanded_args = {'stage_0': {'x': 6, 'y': 8}}
    assert limp._execution.expand_args(args, results) == expanded_args


def test_expand_args_dict_no_dependencies():
    results = [({'a': 6},), (None,), (None,), (8,)]
    args = {'x': 4, 'y': 2}
    assert limp._execution.expand_args(args, results) == args


# Execution tests

def test_execution():
    task_lists, task_ids = limp.earliest_finish_time(test_graph_1, 4)
    results = limp.execute_task_lists(task_lists, task_ids)
    assert results == results_1


def test_execution_costs():
    task_lists, task_ids = limp.earliest_finish_time(test_graph_1, 4)
    results = limp.execute_task_lists(task_lists, task_ids, costs=True)
    costs = results.pop('costs')
    assert results == results_1
    assert set(costs.keys()) == set(costs_1.keys())
    for key in costs.keys():
        assert costs[key][0] >= 0.0
        assert set(costs[key][1].keys()) == set(costs_1[key][1].keys())
        for key_ in costs[key][1].keys():
            assert costs[key][1][key_] >= 0.0


def test_execution_multiplexing():
    task_lists, task_ids = limp.earliest_finish_time(test_graph_1, 4)
    task_ids[0][0] = [task_ids[0][0], 'stats_2']
    results_1_ = results_1.copy()
    results_1_['stats_2'] = results_1_['stats_1']
    results = limp.execute_task_lists(task_lists, task_ids)
    assert results == results_1_


def test_execution_multiplexing_costs():
    task_lists, task_ids = limp.earliest_finish_time(test_graph_1, 4)
    task_ids[0][0] = [task_ids[0][0], 'stats_2']
    results_1_ = results_1.copy()
    results_1_['stats_2'] = results_1_['stats_1']
    costs_1_ = costs_1.copy()
    costs_1_['stats_2'] = costs_1_['stats_1']
    results = limp.execute_task_lists(task_lists, task_ids, costs=True)
    costs = results.pop('costs')
    assert results == results_1_
    assert set(costs.keys()) == set(costs_1_.keys())
    for key in costs.keys():
        assert costs[key][0] >= 0.0
        assert set(costs[key][1].keys()) == set(costs_1_[key][1].keys())
        for key_ in costs[key][1].keys():
            assert costs[key][1][key_] >= 0.0
