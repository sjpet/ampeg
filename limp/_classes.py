#  -*- coding: utf-8 -*-
"""Classes for limp.

@author: Stefan Peterson
"""

from collections import namedtuple


class Err(object):
    """A task result indicating that an exception was raised by the task."""

    def __init__(self, err):
        # Deconstruct the exception since Python 2 can't unpickle them
        self.err_type = type(err)
        self.message = str(err)

    def __eq__(self, other):
        if not isinstance(other, Err):
            return False
        else:
            return self.err_type == other.err_type and \
                   self.message == other.message

    def __repr__(self):
        return "Err<{t}(\"{m}\")>".format(t=self.err_type.__name__,
                                          m=self.message)


class Dependency(namedtuple("Dependency",
                            ("index", "key", "communication_cost"))):
    """A named tuple that defines a dependency on some other task in the
    same graph."""

    def __new__(cls, index, key, communication_cost=0):
        return super(Dependency, cls).__new__(cls,
                                              index,
                                              key,
                                              communication_cost)

    def __eq__(self, other):
        return (self[0], self[1]) == (other[0], other[1])

Communication = namedtuple("Communication", ("sender", "recipients"))
