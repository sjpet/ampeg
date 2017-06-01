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
