# -*- coding: utf-8 -*-
"""Custom exceptions for ampeg.

@author: Stefan Peterson
"""

from ._classes import Err


class DependencyError(Exception):
    """A task dependency raised an Error."""

    @staticmethod
    def default(err):
        if isinstance(err, Err):
            if err.err_type == DependencyError:
                return DependencyError(*err.args)
            else:
                message = "A dependency raised {t} with the message \"{m}\""\
                    .format(t=err.err_type.__name__,
                            m=err.message_with_traceback)
                return DependencyError(message)
        else:
            return DependencyError("A wild DependencyError appeared!")


class TaskTimeoutError(Exception):
    """A task timed out."""

    @staticmethod
    def default(process_index):
        if process_index is None:
            message = "Receive task timed out"
        else:
            message = "Timeout when collecting results from process {k}"\
                .format(k=process_index)
        return TaskTimeoutError(message)
