# -*- coding: utf-8 -*-
"""Unit tests for functions in limp.execution.

Author: Stefan Peterson
"""

import sys

import pytest

from .context import limp
from .helpers import (id_,
                      div,
                      add,
                      square_one,
                      stats,
                      square,
                      sum_stats,
                      normalize,
                      diff)

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

results_1_filtered = {'stats_0': stats_0,
                      'stats_1': stats_1,
                      'final': diff(norm_0['y'], norm_1['y'])}

costs_1 = {'stats_0': (None, {}),
           'stats_1': (None, {}),
           2: (None, {}),
           3: (None, {}),
           4: (None, {'stats_0': None}),
           5: (None, {2: None}),
           6: (None, {'stats_1': None, 3: None}),
           'final': (None, {6: None})}

test_graph_2_a = {0: (id_, [[]], 1),
                  1: (sum, (limp.Dependency(0, None, 1),), 4),
                  2: (len, (limp.Dependency(0, None, 1),), 2),
                  3: (div, (limp.Dependency(1, None, 1),
                            limp.Dependency(2, None, 1)), 6),
                  4: (add, (limp.Dependency(1, None, 1),
                            limp.Dependency(2, None, 1)), 3),
                  5: (square_one, (limp.Dependency(3, None, 1),), 2),
                  6: (square_one, (limp.Dependency(4, None, 1),), 2)}

test_graph_2_b = {0: (id_, [[]], 1),
                  1: (sum, (limp.Dependency(0, None, 1),), 4),
                  2: (len, (limp.Dependency(0, None, 1),), 2),
                  3: (div, (limp.Dependency(1, None, 1),
                            limp.Dependency(2, None, 1)), 3),
                  4: (add, (limp.Dependency(1, None, 1),
                            limp.Dependency(2, None, 1)), 6),
                  5: (square_one, (limp.Dependency(3, None, 1),), 2),
                  6: (square_one, (limp.Dependency(4, None, 1),), 2)}

try:
    zero_div_error = sum([])/len([])
except ZeroDivisionError as e:
    zero_div_error = limp.Err(e)

results_2 = {0: [],
             1: 0,
             2: 0,
             3: zero_div_error,
             4: 0,
             5: limp.Err(limp.DependencyError.default(zero_div_error)),
             6: 0}

timeout_error = limp.Err(limp.TimeoutError.default(1))
recv_timeout_error = limp.Err(limp.TimeoutError.default(None))
dep_timeout_error = limp.Err(limp.DependencyError.default(recv_timeout_error))
results_2_timeout = {0: [],
                     1: 0,
                     2: timeout_error,
                     3: timeout_error,
                     4: dep_timeout_error,
                     5: timeout_error,
                     6: dep_timeout_error}


# DependencyError test

def test_dependency_error_default():
    err = limp.Err(Exception("An exception"))
    expected_message = "A dependency raised Exception with the message " \
                       "\"An exception\""
    assert limp.DependencyError.default(err).message == expected_message


#  Dependency expansion tests

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


def test_expand_args_dict_key():
    results = [(None,), (None,), ({'a': 8},)]
    args = (limp.Dependency(2, 'a'),)
    assert limp._execution.expand_args(args, results) == (8,)


def test_expand_args_iterable_index():
    results = [(None,), (None,), ([6, 9, 2],)]
    args = (limp.Dependency(2, 1),)
    assert limp._execution.expand_args(args, results) == (9,)


def test_expand_args_nested_keys():
    results = [(None,), (None,), ({'a': [8, 9, 10]},)]
    args = (limp.Dependency(2, ('a', 2)),)
    assert limp._execution.expand_args(args, results) == (10,)


def test_expand_args_no_dependency():
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


def test_expand_args_single_error():
    results = [(limp.Err(Exception("An exception")),)]
    args = limp.Dependency(0, None, 1)
    with pytest.raises(limp.DependencyError):
        limp._execution.expand_args(args, results)


def test_expand_args_dict_key_error():
    results = [(None,), (None,), ({'a': limp.Err(Exception("An exception"))},)]
    args = (limp.Dependency(2, 'a'),)
    with pytest.raises(limp.DependencyError):
        limp._execution.expand_args(args, results)


def test_expand_args_iterable_index_error():
    results = [(None,),
               (None,),
               ([6, limp.Err(Exception("An exception")), 2],)]
    args = (limp.Dependency(2, 1),)
    with pytest.raises(limp.DependencyError):
        limp._execution.expand_args(args, results)


def test_expand_args_nested_keys_error():
    results = [(None,),
               (None,),
               ({'a': [8, 9, limp.Err(Exception("An exception"))]},)]
    args = (limp.Dependency(2, ('a', 2)),)
    with pytest.raises(limp.DependencyError):
        limp._execution.expand_args(args, results)


def test_expand_args_dict_error_1():
    results = [({'a': limp.Err(Exception("An exception"))},),
               (None,),
               (None,),
               (8,)]
    args = {'x': limp.Dependency(0, 'a'), 'y': limp.Dependency(3, None)}
    with pytest.raises(limp.DependencyError):
        limp._execution.expand_args(args, results)


def test_expand_args_dict_error_2():
    results = [({'a': 6},),
               (None,),
               (None,),
               (limp.Err(Exception("An exception")),)]
    args = {'x': limp.Dependency(0, 'a'), 'y': limp.Dependency(3, None)}
    with pytest.raises(limp.DependencyError):
        limp._execution.expand_args(args, results)


def test_expand_args_list_error_1():
    results = [({'a': limp.Err(Exception("An exception"))},),
               (None,),
               (None,),
               (8,)]
    args = [limp.Dependency(0, 'a'), limp.Dependency(3, None)]
    with pytest.raises(limp.DependencyError):
        limp._execution.expand_args(args, results)


def test_expand_args_list_error_2():
    results = [({'a': 6},),
               (None,),
               (None,),
               (limp.Err(Exception("An exception")),)]
    args = [limp.Dependency(0, 'a'), limp.Dependency(3, None)]
    with pytest.raises(limp.DependencyError):
        limp._execution.expand_args(args, results)


def test_expand_args_iterable_mixed_error_1():
    results = [({'a': limp.Err(Exception("An exception"))},),
               (None,),
               (None,),
               (8,)]
    args = [limp.Dependency(0, 'a'), 63, (limp.Dependency(3, None), 5)]
    with pytest.raises(limp.DependencyError):
        limp._execution.expand_args(args, results)


def test_expand_args_iterable_mixed_error_2():
    results = [({'a': 6},),
               (None,),
               (None,),
               (limp.Err(Exception("An exception")),)]
    args = [limp.Dependency(0, 'a'), 63, (limp.Dependency(3, None), 5)]
    with pytest.raises(limp.DependencyError):
        limp._execution.expand_args(args, results)


def test_expand_args_nested_dict_error_1():
    results = [({'a': limp.Err(Exception("An exception"))},),
               (None,),
               (None,),
               (8,)]
    args = {'stage_0': {'x': limp.Dependency(0, 'a'),
                        'y': limp.Dependency(3, None)}}
    with pytest.raises(limp.DependencyError):
        limp._execution.expand_args(args, results)


def test_expand_args_nested_dict_error_2():
    results = [({'a': 6},),
               (None,),
               (None,),
               (limp.Err(Exception("An exception")),)]
    args = {'stage_0': {'x': limp.Dependency(0, 'a'),
                        'y': limp.Dependency(3, None)}}
    with pytest.raises(limp.DependencyError):
        limp._execution.expand_args(args, results)


# Execution tests

def test_execution():
    task_lists, task_ids = limp.earliest_finish_time(test_graph_1, 4)
    results = limp.execute_task_lists(task_lists, task_ids)
    assert results == results_1


def test_execution_filtered():
    task_lists, task_ids = limp.earliest_finish_time(test_graph_1,
                                                     4,
                                                     output_tasks=["stats_0",
                                                                   "stats_1",
                                                                   "final"])
    results = limp.execute_task_lists(task_lists, task_ids)
    assert results == results_1_filtered


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


def test_execution_handle_error_in_parent_process():
    task_lists, task_ids = limp.earliest_finish_time(test_graph_2_a, 2)
    results = limp.execute_task_lists(task_lists, task_ids)
    for key in results:
        assert results[key] == results_2[key]


def test_execution_handle_error_in_child_process():
    task_lists, task_ids = limp.earliest_finish_time(test_graph_2_b, 2)
    results = limp.execute_task_lists(task_lists, task_ids)
    for key in results:
        assert results[key] == results_2[key]


def test_execution_child_process_killed():
    task_lists, task_ids = limp.earliest_finish_time(test_graph_2_b,
                                                     2,
                                                     timeout=1)
    task_lists[1][1] = (sys.exit, [])
    results = limp.execute_task_lists(task_lists, task_ids, timeout=1)
    for key in results:
        assert results[key] == results_2_timeout[key]
