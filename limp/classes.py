#  -*- coding: utf-8 -*-
"""Classes for limp.

@author: Stefan Peterson
"""


class Dependency(tuple):
    """A simple (index, key, communication cost) tuple, implemented as a new
    subclass in order to eliminate ambiguity.
    """
    def __new__(cls, index, key, communication_cost=0):
        return tuple.__new__(cls, (index, key, communication_cost))
