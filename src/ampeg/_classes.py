#  -*- coding: utf-8 -*-
"""Classes for ampeg.

@author: Stefan Peterson
"""

from collections import namedtuple
from warnings import warn


class Err(object):
    # noinspection PyUnresolvedReferences
    """A task result indicating that an exception was raised by the task.

    Attributes
    ----------
    err_type : Exception
        The type of exception that was raised
    message : str
        Message of the exception
    message_with_traceback : str
        Message of the exception along with traceback information
    """

    def __init__(self, err, traceback=None):
        # Deconstruct the exception since Python 2 can't unpickle them
        self.err_type = type(err)
        self.args = err.args
        self._traceback = traceback

    def __eq__(self, other):
        if not isinstance(other, Err):
            return False
        else:
            return self.err_type == other.err_type and \
                   self.args == other.args

    def __repr__(self):
        if self._traceback is not None:
            return "Err({e}, '{t}')".format(e=self.err_type(*self.args),
                                            t=self._traceback)
        else:
            return "Err({})".format(self.err_type(*self.args))

    def __str__(self):
        if len(self.args) == 0:
            return ""
        elif len(self.args) == 1:
            return str(self.args[0])
        else:
            return str(self.args)
    
    @property
    def message(self):
        warn(DeprecationWarning("Err.message has been deprecated as of 0.1.4"))
        return str(self)

    @property
    def message_with_traceback(self):
        if self._traceback is None:
            return str(self)
        else:
            return str(self) + "\n" + self._pretty_traceback()

    def _pretty_traceback(self):
        """Format traceback information as a pretty string.

        Returns
        -------
        str
        """

        if self._traceback is None:
            return ""
        else:
            tb_string = "  File \"{fname}\", line {line_no}, in {module} \n" \
                        "    {expr}"
            return "Traceback (most recent call last):\n" + \
                   "\n".join(tb_string.format(fname=level[0],
                                              line_no=level[1],
                                              module=level[2],
                                              expr=level[3])
                             for level in self._traceback)


class Dependency(namedtuple("Dependency",
                            ("task_id", "key", "communication_cost"))):
    """A triple of (task ID, key, communication cost).

    The key may be a single
    key, index or slice, or it may be an iterable of such types to be applied
    in sequence. For example, the key ``('values', 2)`` extracts the value 5
    from the dict ``{'values': [1, 3, 5]}``. The key may also be ``None``,
    indicating that the entire return value of the predecessor shall be used.

    Communication cost is optional and defaults to 0.

    Examples
    --------
    >>> import ampeg as ag
    >>> ag.Dependency("task_0", None) == \\
    ...     ag.Dependency("task_0", None, 0)
    True

    >>> _ = ag.Dependency("task_0",
    ...                   [("first", "second"),
    ...                    slice(2, 5)])
    """

    def __new__(cls, task_id, key, communication_cost=0):
        return super(Dependency, cls).__new__(cls,
                                              task_id,
                                              key,
                                              communication_cost)

    def __eq__(self, other):
        if not isinstance(other, Dependency):
            return False
        else:
            return (self[0], self[1]) == (other[0], other[1])


class Communication(namedtuple("Communication", ("sender", "recipients"))):
    """A pair of (sender, recipients).

    Sender is a single task ID and recipients is a list of task IDs. This named
    tuple is used in place of task ID for send- and receive tasks in task
    lists. Only advanced users wishing to estimate communication costs in
    handcrafted task list need worry about it.
    """
