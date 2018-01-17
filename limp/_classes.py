#  -*- coding: utf-8 -*-
"""Classes for limp.

@author: Stefan Peterson
"""

from collections import namedtuple


class Err(object):
    """A task result indicating that an exception was raised by the task."""

    def __init__(self, err, traceback=None):
        # Deconstruct the exception since Python 2 can't unpickle them
        self.err_type = type(err)
        self.message = str(err)
        self.traceback = traceback

    def __eq__(self, other):
        if not isinstance(other, Err):
            return False
        else:
            return self.err_type == other.err_type and \
                   self.message == other.message

    def __repr__(self):
        return "Err<{t}(\"{m}\")>".format(t=self.err_type.__name__,
                                          m=self.message)
    
    @property
    def message_with_traceback(self):
        return self.message + "\n" + self._pretty_traceback()

    def _pretty_traceback(self):
        """Format traceback information as a pretty string.

        Returns
        -------
        str
        """

        tb_string = "  File \"{fname}\", line {line_no}, in {module} \n" \
                    "    {expr}"
        return "Traceback (most recent call last):\n" + \
               "\n".join(tb_string.format(fname=level[0],
                                          line_no=level[1],
                                          module=level[2],
                                          expr=level[3])
                         for level in self.traceback)


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
