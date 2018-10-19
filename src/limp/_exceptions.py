# -*- coding: utf-8 -*-
"""Custom exceptions for limp.

@author: Stefan Peterson
"""

from ._classes import Err


class DependencyError(Exception):
    """A task dependency raised an Error."""

    def __init__(self, message):
        self.message = message

    @staticmethod
    def default(err):
        if isinstance(err, Err):
            if err.err_type == DependencyError:
                message = err.message
            else:
                message = "A dependency raised {t} with the message \"{m}\""\
                    .format(t=err.err_type.__name__, m=err.message)
        else:
            message = "A wild DependencyError appeared!"
        return DependencyError(message)


class LimpTimeoutError(Exception):
    """A task timed out."""

    def __init__(self, message):
        self.message = message

    @staticmethod
    def default(process_index):
        if process_index is None:
            message = "Receive task timed out"
        else:
            message = "Timeout when collecting results from process {k}"\
                .format(k=process_index)
        return LimpTimeoutError(message)
