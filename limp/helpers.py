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
