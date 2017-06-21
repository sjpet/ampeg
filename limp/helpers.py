#  -*- coding: utf-8 -*-
"""Helper functions for limp.

@author: Stefan Peterson
"""

try:
    from math import inf
except ImportError:     # For Python 2
    inf = float('inf')


def is_iterable(x):
    """Check if a variable is a non-string iterable.

    Parameters
    ----------
    x : any type

    Returns
    -------
    bool
    """

    return (not isinstance(x, str)) and hasattr(x, '__iter__')


def reverse_graph(graph):
    """Reverse a directed graph.

    Parameters
    ----------
    graph : dict
        A graph on the form {node: [successor]}

    Returns
    -------
    dict
        A graph with all directions reversed
    """
    graph_ = {vertex: [] for vertex in graph}
    for vertex, successors in graph.items():
        for vertex_ in successors:
            graph_[vertex_].append(vertex)

    return graph_


def equivalent_args(args_0, args_1):
    """Compare args in a way that handles problematic types such as numpy
    arrays.

    Parameters
    ----------
    args_0
    args_1

    Returns
    -------
    bool
    """

    if isinstance(args_0, dict):
        if isinstance(args_1, dict):
            for key in args_0:
                if key not in args_1:
                    return False
                elif equivalent_args(args_0[key], args_1[key]) is False:
                    return False
            for key in args_1:
                if key not in args_0:
                    return False
        else:
            return False

    elif is_iterable(args_0):
        if is_iterable(args_1):
            try:
                return args_0 == args_1
            except ValueError:
                return all(args_0 == args_1)
        else:
            return False

    else:
        print(args_0 == args_1)
        return args_0 == args_1

    return True
