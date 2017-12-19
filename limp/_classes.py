#  -*- coding: utf-8 -*-
"""Classes for limp.

@author: Stefan Peterson
"""

from collections import namedtuple


class Dependency(namedtuple("Dependency",
                            ("index", "key", "communication_cost"))):

    def __new__(cls, index, key, communication_cost=0):
        return super(Dependency, cls).__new__(cls,
                                              index,
                                              key,
                                              communication_cost)

    def __eq__(self, other):
        return (self[0], self[1]) == (other[0], other[1])

Communication = namedtuple("Communication", ("sender", "recipients"))
