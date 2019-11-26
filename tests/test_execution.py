# -*- coding: utf-8 -*-
"""Unit tests for functions in ampeg._execution.

Author: Stefan Peterson
"""

import sys

import pytest
import six

import ampeg

from .data import (test_graph_1,
                   test_graph_1_nested,
                   results_1,
                   results_1_filtered,
                   results_1_nested,
                   costs_1,
                   costs_1_nested,
                   test_graph_2_a,
                   test_graph_2_b,
                   results_2,
                   results_2_timeout)


class ArbitraryObject(object):
    
    def __init__(self, attr1, attr2):
        self.attr1 = attr1
        self.attr2 = attr2


# DependencyError test

def test_dependency_error_default():
    err = ampeg.Err(Exception("An exception"))
    expected_message = "A dependency raised Exception with the message " \
                       "\"An exception\""
    assert str(ampeg.DependencyError.default(err)) == expected_message


#  Dependency expansion tests

def test_expand_recursively_nested_iterable():
    results = [None, [5]]
    keys = (1, 0)
    assert ampeg._execution.expand_recursively(results, keys) == 5


def test_expand_recursively_nested_iterable_object_attributes():
    results = [None, ArbitraryObject(4, 5)]
    keys = (1, "attr2")
    assert ampeg._execution.expand_recursively(results, keys) == 5


def test_expand_recursively_nested_iterable_object_attributes():
    results = ArbitraryObject([1, 3, 5, 7], "irrelevant")
    keys = ("attr1", 2)
    assert ampeg._execution.expand_recursively(results, keys) == 5


def test_expand_recursively_nested_dict():
    results = {'a': {'b': 6, 'c': 7}}
    keys = ('a', 'b')
    assert ampeg._execution.expand_recursively(results, keys) == 6


def test_expand_args_single_result():
    results = [(None,), (None,), (7,)]
    args = (ampeg.Dependency(2, None),)
    assert ampeg._execution.expand_args(args, results) == (7,)


def test_expand_args_dict_key():
    results = [(None,), (None,), ({'a': 8},)]
    args = (ampeg.Dependency(2, 'a'),)
    assert ampeg._execution.expand_args(args, results) == (8,)


def test_expand_args_iterable_index():
    results = [(None,), (None,), ([6, 9, 2],)]
    args = (ampeg.Dependency(2, 1),)
    assert ampeg._execution.expand_args(args, results) == (9,)


def test_expand_args_attribute():
    results = [(None,), (None,), (ArbitraryObject(9, -1,),)]
    args = (ampeg.Dependency(2, "attr1"),)
    assert ampeg._execution.expand_args(args, results) == (9,)


def test_expand_args_nested_keys():
    results = [(None,), (None,), ({'a': [8, 9, 10]},)]
    args = (ampeg.Dependency(2, ('a', 2)),)
    assert ampeg._execution.expand_args(args, results) == (10,)


def test_expand_args_nested_keys_with_attribute_last():
    results = [(None,), (None,), ({'a': ArbitraryObject(6, 10)},)]
    args = (ampeg.Dependency(2, ('a', "attr2")),)
    assert ampeg._execution.expand_args(args, results) == (10,)


def test_expand_args_nested_keys_with_attribute_first():
    results = [(None,), (None,), (ArbitraryObject({'a': 4}, 10),)]
    args = (ampeg.Dependency(2, ("attr1", 'a')),)
    assert ampeg._execution.expand_args(args, results) == (4,)


def test_expand_args_no_dependency():
    results = [(None,), (None,), (7,)]
    args = (42,)
    assert ampeg._execution.expand_args(args, results) == args


def test_expand_args_iterable_no_dependency():
    results = [(None,), (None,), (7,)]
    args = ([1, 2, 3, 4],)
    assert ampeg._execution.expand_args(args, results) == args


def test_expand_args_dict():
    results = [({'a': 6},), (None,), (None,), (8,)]
    args = {'x': ampeg.Dependency(0, 'a'), 'y': ampeg.Dependency(3, None)}
    assert ampeg._execution.expand_args(args, results) == {'x': 6, 'y': 8}


def test_expand_args_list():
    results = [({'a': 6},), (None,), (None,), (8,)]
    args = [ampeg.Dependency(0, 'a'), ampeg.Dependency(3, None)]
    assert ampeg._execution.expand_args(args, results) == [6, 8]


def test_expand_args_iterable_mixed():
    results = [({'a': 6},), (None,), (None,), (8,)]
    args = [ampeg.Dependency(0, 'a'), 63, (ampeg.Dependency(3, None), 5)]
    assert ampeg._execution.expand_args(args, results) == [6, 63, (8, 5)]


def test_expand_args_nested_dict():
    results = [({'a': 6},), (None,), (None,), (8,)]
    args = {'stage_0': {'x': ampeg.Dependency(0, 'a'),
                        'y': ampeg.Dependency(3, None)}}
    expanded_args = {'stage_0': {'x': 6, 'y': 8}}
    assert ampeg._execution.expand_args(args, results) == expanded_args


def test_expand_args_list_of_tuples():
    results = []


def test_expand_args_dict_no_dependencies():
    results = [({'a': 6},), (None,), (None,), (8,)]
    args = {'x': 4, 'y': 2}
    assert ampeg._execution.expand_args(args, results) == args


def test_expand_args_single_error():
    results = [(ampeg.Err(Exception("An exception")),)]
    args = ampeg.Dependency(0, None, 1)
    with pytest.raises(ampeg.DependencyError):
        ampeg._execution.expand_args(args, results)


def test_expand_args_dict_key_error():
    results = [(None,), (None,), ({'a': ampeg.Err(Exception("An exception"))},)]
    args = (ampeg.Dependency(2, 'a'),)
    with pytest.raises(ampeg.DependencyError):
        ampeg._execution.expand_args(args, results)


def test_expand_args_iterable_index_error():
    results = [(None,),
               (None,),
               ([6, ampeg.Err(Exception("An exception")), 2],)]
    args = (ampeg.Dependency(2, 1),)
    with pytest.raises(ampeg.DependencyError):
        ampeg._execution.expand_args(args, results)


def test_expand_args_nested_keys_error():
    results = [(None,),
               (None,),
               ({'a': [8, 9, ampeg.Err(Exception("An exception"))]},)]
    args = (ampeg.Dependency(2, ('a', 2)),)
    with pytest.raises(ampeg.DependencyError):
        ampeg._execution.expand_args(args, results)


def test_expand_args_dict_error_1():
    results = [({'a': ampeg.Err(Exception("An exception"))},),
               (None,),
               (None,),
               (8,)]
    args = {'x': ampeg.Dependency(0, 'a'), 'y': ampeg.Dependency(3, None)}
    with pytest.raises(ampeg.DependencyError):
        ampeg._execution.expand_args(args, results)


def test_expand_args_dict_error_2():
    results = [({'a': 6},),
               (None,),
               (None,),
               (ampeg.Err(Exception("An exception")),)]
    args = {'x': ampeg.Dependency(0, 'a'), 'y': ampeg.Dependency(3, None)}
    with pytest.raises(ampeg.DependencyError):
        ampeg._execution.expand_args(args, results)


def test_expand_args_list_error_1():
    results = [({'a': ampeg.Err(Exception("An exception"))},),
               (None,),
               (None,),
               (8,)]
    args = [ampeg.Dependency(0, 'a'), ampeg.Dependency(3, None)]
    with pytest.raises(ampeg.DependencyError):
        ampeg._execution.expand_args(args, results)


def test_expand_args_list_error_2():
    results = [({'a': 6},),
               (None,),
               (None,),
               (ampeg.Err(Exception("An exception")),)]
    args = [ampeg.Dependency(0, 'a'), ampeg.Dependency(3, None)]
    with pytest.raises(ampeg.DependencyError):
        ampeg._execution.expand_args(args, results)


def test_expand_args_iterable_mixed_error_1():
    results = [({'a': ampeg.Err(Exception("An exception"))},),
               (None,),
               (None,),
               (8,)]
    args = [ampeg.Dependency(0, 'a'), 63, (ampeg.Dependency(3, None), 5)]
    with pytest.raises(ampeg.DependencyError):
        ampeg._execution.expand_args(args, results)


def test_expand_args_iterable_mixed_error_2():
    results = [({'a': 6},),
               (None,),
               (None,),
               (ampeg.Err(Exception("An exception")),)]
    args = [ampeg.Dependency(0, 'a'), 63, (ampeg.Dependency(3, None), 5)]
    with pytest.raises(ampeg.DependencyError):
        ampeg._execution.expand_args(args, results)


def test_expand_args_nested_dict_error_1():
    results = [({'a': ampeg.Err(Exception("An exception"))},),
               (None,),
               (None,),
               (8,)]
    args = {'stage_0': {'x': ampeg.Dependency(0, 'a'),
                        'y': ampeg.Dependency(3, None)}}
    with pytest.raises(ampeg.DependencyError):
        ampeg._execution.expand_args(args, results)


def test_expand_args_nested_dict_error_2():
    results = [({'a': 6},),
               (None,),
               (None,),
               (ampeg.Err(Exception("An exception")),)]
    args = {'stage_0': {'x': ampeg.Dependency(0, 'a'),
                        'y': ampeg.Dependency(3, None)}}
    with pytest.raises(ampeg.DependencyError):
        ampeg._execution.expand_args(args, results)


# Collection tests

def test_collect():
    test_results = [[(4, 2), (6, 1), (2, 9)], [(1, 2), (-8, 5)]]
    test_task_ids = [['a', 'b', 'e'], ['c', 'd']]
    test_results_collected = {'a': 4, 'b': 6, 'c': 1, 'd': -8, 'e': 2}
    assert (ampeg._execution.collect_results(test_results, test_task_ids) ==
            test_results_collected)


def test_collect_tuple_keys():
    test_results = [[(4, 2), (6, 1), (2, 9)], [(1, 2), (-8, 5)]]
    test_task_ids = [[('a', 0), ('a', 1), 'd'], ['b', 'c']]
    test_results_collected = {('a', 0): 4,
                              ('a', 1): 6,
                              'b': 1,
                              'c': -8,
                              'd': 2}
    assert (ampeg._execution.collect_results(test_results, test_task_ids) ==
            test_results_collected)


def test_collect_no_task_ids():
    test_results = [[(4, 2), (6, 1), (2, 9)], [(1, 2), (-8, 5)]]
    test_results_collected = {(0, 0): 4,
                              (0, 1): 6,
                              (1, 0): 1,
                              (1, 1): -8,
                              (0, 2): 2}
    assert (ampeg._execution.collect_results(test_results, None) ==
            test_results_collected)


# Inflation tests

def test_inflate():
    test_results = {(0, 0): 4, (0, 1): 3, 1: 6}
    test_results_inflated = {0: {0: 4, 1: 3}, 1: 6}
    assert (ampeg._execution.inflate_results(test_results) ==
            test_results_inflated)


def test_inflate_do_not_recurse():
    test_results = {(0, (0, 0)): 4, (0, (0, 1)): 3, 1: 6}
    test_results_inflated = {0: {(0, 0): 4, (0, 1): 3}, 1: 6}
    assert (ampeg._execution.inflate_results(test_results) ==
            test_results_inflated)


# Execution tests

def test_execution():
    task_lists, task_ids = ampeg.earliest_finish_time(test_graph_1, 4)
    results = ampeg.execute_task_lists(task_lists, task_ids, 4)
    assert results == results_1


def test_execution_filtered():
    task_lists, task_ids = ampeg.earliest_finish_time(test_graph_1,
                                                      4,
                                                      output_tasks=["stats_0",
                                                                   "stats_1",
                                                                   "final"])
    results = ampeg.execute_task_lists(task_lists, task_ids, 4)
    assert results == results_1_filtered


def test_execution_costs():
    task_lists, task_ids = ampeg.earliest_finish_time(test_graph_1, 4)
    results = ampeg.execute_task_lists(task_lists, task_ids, costs=True)
    costs = results.pop('costs')
    assert results == results_1
    assert set(costs.keys()) == set(costs_1.keys())
    for key in costs.keys():
        assert costs[key][0] >= 0.0
        assert set(costs[key][1].keys()) == set(costs_1[key][1].keys())
        for key_ in costs[key][1].keys():
            assert costs[key][1][key_] >= 0.0


def test_execution_multiplexing():
    task_lists, task_ids = ampeg.earliest_finish_time(test_graph_1, 4)
    task_ids[0][0] = [task_ids[0][0], 'stats_2']
    results_1_ = results_1.copy()
    results_1_['stats_2'] = results_1_['stats_1']
    results = ampeg.execute_task_lists(task_lists, task_ids)
    assert results == results_1_


def test_execution_multiplexing_costs():
    task_lists, task_ids = ampeg.earliest_finish_time(test_graph_1, 4)
    task_ids[0][0] = [task_ids[0][0], 'stats_2']
    results_1_ = results_1.copy()
    results_1_['stats_2'] = results_1_['stats_1']
    costs_1_ = costs_1.copy()
    costs_1_['stats_2'] = costs_1_['stats_1']
    results = ampeg.execute_task_lists(task_lists, task_ids, costs=True)
    costs = results.pop('costs')
    assert results == results_1_
    assert set(costs.keys()) == set(costs_1_.keys())
    for key in costs.keys():
        assert costs[key][0] >= 0.0
        assert set(costs[key][1].keys()) == set(costs_1_[key][1].keys())
        for key_ in costs[key][1].keys():
            assert costs[key][1][key_] >= 0.0


def test_execution_handle_error_in_parent_process():
    task_lists, task_ids = ampeg.earliest_finish_time(test_graph_2_a, 2)
    results = ampeg.execute_task_lists(task_lists, task_ids)
    for key in results:
        if isinstance(results[key], ampeg.Err):
            assert results[key].err_type == results_2[key].err_type
            assert str(results[key]).startswith(str(results_2[key])[:-1])
        else:
            assert results[key] == results_2[key]


def test_execution_handle_error_in_child_process():
    task_lists, task_ids = ampeg.earliest_finish_time(test_graph_2_b, 2)
    results = ampeg.execute_task_lists(task_lists, task_ids)
    for key in results:
        if isinstance(results[key], ampeg.Err):
            assert results[key].err_type == results_2[key].err_type
            assert str(results[key]).startswith(str(results_2[key])[:-1])
        else:
            assert results[key] == results_2[key]


def test_execution_child_process_killed():
    task_lists, task_ids = ampeg.earliest_finish_time(test_graph_2_b,
                                                      2,
                                                      timeout=1)
    task_lists[1][1] = (sys.exit, [])
    results = ampeg.execute_task_lists(task_lists, task_ids, timeout=1)
    for key in results:
        if isinstance(results[key], ampeg.Err):
            assert results[key].err_type == results_2_timeout[key].err_type
            assert str(results[key]).startswith(
                str(results_2_timeout[key])[:-1])
        else:
            assert results[key] == results_2_timeout[key]


def test_execution_inflate():
    task_lists, task_ids = ampeg.earliest_finish_time(test_graph_1_nested, 4)
    results = ampeg.execute_task_lists(task_lists, task_ids, inflate=True)
    assert results == results_1_nested


def test_execution_multiplexing_costs_inflate():
    task_lists, task_ids = ampeg.earliest_finish_time(test_graph_1_nested, 4)
    task_ids[0][0] = [task_ids[0][0], ('stats', 2)]
    results_1_nested_ = results_1_nested.copy()
    results_1_nested_['stats'][2] = results_1_nested['stats'][1]
    costs_1_nested_ = costs_1_nested.copy()
    costs_1_nested_['stats'][2] = costs_1_nested_['stats'][1]
    results = ampeg.execute_task_lists(task_lists,
                                       task_ids,
                                       inflate=True,
                                       costs=True)
    costs = results.pop('costs')
    assert results == results_1_nested_
    assert set(costs.keys()) == set(costs_1_nested_.keys())
    for key, val in six.iteritems(costs):
        if isinstance(val, dict):
            for key_, val_ in six.iteritems(val):
                assert costs[key][key_][0] >= 0.0
                assert set(costs[key][key_][1].keys()) == set(
                    costs_1_nested_[key][key_][1].keys())
                for key__ in costs[key][key_][1].keys():
                    assert costs[key][key_][1][key__] >= 0.0
        else:
            assert costs[key][0] >= 0.0
            assert set(costs[key][1].keys()) == set(
                costs_1_nested_[key][1].keys())
            for key_ in costs[key][1].keys():
                assert costs[key][1][key_] >= 0.0
