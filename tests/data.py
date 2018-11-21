# -*- coding: utf-8 -*-
"""Common test data.

Author: Stefan Peterson
"""

import ampeg
from .helpers import (id_,
                      div,
                      add,
                      square_one,
                      stats,
                      square,
                      sum_stats,
                      normalize,
                      diff)


test_x = [[0, 6, 2, 6, 1, 2, 3, 7, 2, 3, 1, 5, 6, 2, 8],
          [1, 4, 5, 2, 3, 1, 4, 4, 3, 2, 5, 6, 3, 2, 1]]

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

# First test graph

test_graph_1 = {'stats_0': (stats, {'x': test_x[0]}, 13),
                'stats_1': (stats, {'x': test_x[1]}, 52),
                2: (square, (test_x[0],), 64),
                3: (square, {'x': test_x[1]}, 38),
                4: (sum_stats,
                    (ampeg.Dependency('stats_0', ('dummy', 'mu'), 5),
                     ampeg.Dependency('stats_1', ('dummy', 'mu'), 3),
                     ampeg.Dependency('stats_0', ('dummy', 'var')),
                     ampeg.Dependency('stats_1', ('dummy', 'var'))),
                    56),
                5: (normalize,
                    {'x': ampeg.Dependency(2, None, 13),
                     'mu': ampeg.Dependency(4, 0, 6),
                     'var': ampeg.Dependency(4, 1)},
                    75),
                6: (normalize,
                    {'x': ampeg.Dependency(3, None, 7),
                     'mu': ampeg.Dependency('stats_1', ('dummy', 'mu'), 8),
                     'var': ampeg.Dependency('stats_1', ('dummy', 'var'))},
                    75),
                'final': (diff,
                          {'x': ampeg.Dependency(5, 'y', 12),
                           'y': ampeg.Dependency(6, 'y', 10)},
                          42)}


test_graph_1_nested = {('stats', 0): (stats, {'x': test_x[0]}, 13),
                       ('stats', 1): (stats, {'x': test_x[1]}, 52),
                       ('square', 0): (square, (test_x[0],), 64),
                       ('square', 1): (square, {'x': test_x[1]}, 38),
                       4: (sum_stats,
                           (ampeg.Dependency(('stats', 0), ('dummy', 'mu'), 5),
                            ampeg.Dependency(('stats', 1), ('dummy', 'mu'), 3),
                            ampeg.Dependency(('stats', 0), ('dummy', 'var')),
                            ampeg.Dependency(('stats', 1), ('dummy', 'var'))),
                           56),
                       ('normalize', 0): (normalize,
                                          {'x': ampeg.Dependency(('square', 0),
                                                                 None,
                                                                 13),
                                           'mu': ampeg.Dependency(4, 0, 6),
                                           'var': ampeg.Dependency(4, 1)},
                                          75),
                       ('normalize', 1): (normalize,
                                          {'x': ampeg.Dependency(('square', 1),
                                                                 None,
                                                                 7),
                                           'mu': ampeg.Dependency(('stats', 1),
                                                                  ('dummy',
                                                                  'mu'),
                                                                  8),
                                           'var': ampeg.Dependency(('stats', 1),
                                                                   ('dummy',
                                                                   'var'))},
                                          75),
                       'final': (diff,
                                 {'x': ampeg.Dependency(('normalize', 0),
                                                       'y',
                                                        12),
                                  'y': ampeg.Dependency(('normalize', 1),
                                                       'y',
                                                        10)},
                                 42)}


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

results_1_nested = {'stats': {0: stats_0, 1: stats_1},
                    'square': {0: squared_0, 1: squared_1},
                    4: summed_stats,
                    'normalize': {0: norm_0, 1: norm_1},
                    'final': diff(norm_0['y'], norm_1['y'])}

costs_1 = {'stats_0': (None, {}),
           'stats_1': (None, {}),
           2: (None, {}),
           3: (None, {}),
           4: (None, {'stats_0': None}),
           5: (None, {2: None}),
           6: (None, {'stats_1': None, 3: None}),
           'final': (None, {6: None})}

costs_1_nested = {'stats': {0: (None, {}), 1: (None, {})},
                  'square': {0: (None, {}), 1: (None, {})},
                  4: (None, {('stats', 0): None}),
                  'normalize': {0: (None, {('square', 0): None}),
                                1: (None, {('stats', 1): None,
                                           ('square', 1): None})},
                  'final': (None, {('normalize', 1): None})}

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
                                ampeg._scheduling.send,
                                ampeg._scheduling.receive,
                                sum_stats,
                                ampeg._scheduling.receive,
                                normalize,
                                ampeg._scheduling.receive,
                                diff],
                               [stats,
                                ampeg._scheduling.send,
                                ampeg._scheduling.receive,
                                ampeg._scheduling.receive,
                                normalize,
                                ampeg._scheduling.send],
                               [square,
                                ampeg._scheduling.send],
                               [square,
                                ampeg._scheduling.send]]

task_ids_1_ = [['stats_1', None, None, 4, None, 5, None, 'final'],
               ['stats_0', None, None, None, 6, None],
               [2, None],
               [3, None]]

task_ids_1 = [['stats_1',
               ('stats_1', [6]),
               ('stats_0', [4]),
               4,
               (2, [5]),
               5,
               (6, ['final']),
               'final'],
              ['stats_0',
               ('stats_0', [4]),
               (3, [6]),
               ('stats_1', [6]),
               6,
               (6, ['final'])],
              [2,
               (2, [5])],
              [3,
               (3, [6])]]

reduced_graph_1 = {0: (square, {'x': test_x[0]}, 16),
                   2: (stats, {'x': ampeg.Dependency(0, None, 4)}, 28),
                   4: (normalize,
                       {'x': test_x[0],
                        'mu': ampeg.Dependency(2, ('dummy', 'mu'), 5),
                        'var': ampeg.Dependency(2, ('dummy', 'var'), 5)}, 17),
                   5: (normalize,
                       {'x': test_x[1],
                        'mu': ampeg.Dependency(2, ('dummy', 'mu'), 8),
                        'var': ampeg.Dependency(2, ('dummy', 'var'), 8)}, 22)}

multiplexing_keys_1 = {0: [1], 2: [3, 6]}

# Second test graph

test_graph_2_a = {0: (id_, [[]], 1),
                  1: (sum, (ampeg.Dependency(0, None, 1),), 4),
                  2: (len, (ampeg.Dependency(0, None, 1),), 2),
                  3: (div, (ampeg.Dependency(1, None, 1),
                            ampeg.Dependency(2, None, 1)), 6),
                  4: (add, (ampeg.Dependency(1, None, 1),
                            ampeg.Dependency(2, None, 1)), 3),
                  5: (square_one, (ampeg.Dependency(3, None, 1),), 2),
                  6: (square_one, (ampeg.Dependency(4, None, 1),), 2)}

test_graph_2_b = {0: (id_, [[]], 1),
                  1: (sum, (ampeg.Dependency(0, None, 1),), 4),
                  2: (len, (ampeg.Dependency(0, None, 1),), 2),
                  3: (div, (ampeg.Dependency(1, None, 1),
                            ampeg.Dependency(2, None, 1)), 3),
                  4: (add, (ampeg.Dependency(1, None, 1),
                            ampeg.Dependency(2, None, 1)), 6),
                  5: (square_one, (ampeg.Dependency(3, None, 1),), 2),
                  6: (square_one, (ampeg.Dependency(4, None, 1),), 2)}

try:
    zero_div_error = sum([])/len([])
except ZeroDivisionError as e:
    zero_div_error = ampeg.Err(e)

results_2 = {0: [],
             1: 0,
             2: 0,
             3: zero_div_error,
             4: 0,
             5: ampeg.Err(ampeg.DependencyError.default(zero_div_error)),
             6: 0}

timeout_error = ampeg.Err(ampeg.TaskTimeoutError.default(1))
recv_timeout_error = ampeg.Err(ampeg.TaskTimeoutError.default(None))
dep_timeout_error = ampeg.Err(ampeg.DependencyError.default(recv_timeout_error))

results_2_timeout = {0: [],
                     1: 0,
                     2: timeout_error,
                     3: timeout_error,
                     4: dep_timeout_error,
                     5: timeout_error,
                     6: dep_timeout_error}

# Third test graph

test_graph_2 = {0: (square, {'x': test_x[0]}, 13),
                1: (square, {'x': test_x[0]}, 16),
                2: (stats, {'x': ampeg.Dependency(0, None, 4)}, 28),
                3: (stats, {'x': ampeg.Dependency(1, None, 2)}, 21),
                4: (normalize,
                    {'x': test_x[0],
                     'mu': ampeg.Dependency(2, ('dummy', 'mu'), 5),
                     'var': ampeg.Dependency(2, ('dummy', 'var'), 5)}, 17),
                5: (normalize,
                    {'x': test_x[1],
                     'mu': ampeg.Dependency(3, ('dummy', 'mu'), 8),
                     'var': ampeg.Dependency(3, ('dummy', 'var'), 8)}, 22),
                6: (stats, {'x': ampeg.Dependency(1, None, 3)}, 15)}
