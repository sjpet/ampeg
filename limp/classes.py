#  -*- coding: utf-8 -*-
"""Classes for limp.

@author: Stefan Peterson
"""


class Dependency(tuple):
    """A simple (index, key, communication cost) tuple, implemented as a new
    subclass in order to eliminate ambiguity and allow relaxed equivalence.
    """
    def __new__(cls, index, key, communication_cost=0):
        return tuple.__new__(cls, (index, key, communication_cost))

    def __eq__(self, other):
        return (self[0], self[1]) == (other[0], other[1])
