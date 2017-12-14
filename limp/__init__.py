# -*- coding: utf-8 -*-
from ._classes import Dependency, Communication
from ._scheduling import earliest_finish_time, remove_duplicates
from ._execution import execute_task_lists

__version__ = "0.3"
