#  -*- coding: utf-8 -*-
"""Helper functions for limp.

@author: Stefan Peterson
"""

from types import GeneratorType

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

    return isinstance(x, (tuple, list, dict, GeneratorType))


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
                return (args_0 == args_1) is True
            except ValueError:
                return all(equivalent_args(a, b)
                           for a, b in zip(args_0, args_1))
        else:
            return False

    else:
        return (args_0 == args_1) is True

    return True


def demux(xs):
    """De-multiplex an iterable of tuples by their first value.

    Parameters
    ----------
    xs: Iterable of tuples

    Returns
    -------
    [tuple]
    """

    demux_dict = {}
    for x in xs:
        if x[0] in demux_dict:
            demux_dict[x[0]].append(x[1])
        else:
            demux_dict[x[0]] = [x[1]]

    return list(demux_dict.items())


def pretty_traceback(tb):
    """Format traceback information as a pretty string.
    
    Parameters
    ----------
    tb : List[(str, int, str, str)]
    
    Returns
    -------
    str
    """

    tb_string = "  File \"{fname}\", line {line_no}, in {module} \n    {expr}"
    return "Traceback (most recent call last):\n" + \
        "\n".join(tb_string.format(fname=level[0],
                                   line_no=level[1],
                                   module=level[2],
                                   expr=level[3]) for level in tb)
