===============
Advanced Topics
===============

This part of the tutorial covers some less common use cases.

Generating Graphs and Organizing Results
----------------------------------------

In this example, we have a class that holds two lists of points in the
euclidean plane.

>>> import ampeg as ag
...
>>> class MyClass(object):
...
...     def __init__(self, list_a, list_b):
...          self.list_a = list_a
...          self.list_b = list_b

We want to compute the combined squared distance to the origin for all possible
combinations of one point in ``list_a`` and one point in ``list_b``, and we
want to do this using :mod:`ampeg`. For this purpose, we write a function that
generates a computation graph for an instance of our class, using hashable
tuples as task IDs.

>>> def squared_distance(x, y):
...     return x**2 + y**2
...
>>> def graph(the_object):
...     g = {}
...     for k, point_a in enumerate(the_object.list_a):
...         g[("a", k)] = (squared_distance, point_a, 4)
...     for k, point_b in enumerate(the_object.list_b):
...         g[("b", k)] = (squared_distance, point_b, 4)
...     for k_a in range(len(the_object.list_a)):
...         for k_b in range(len(the_object.list_b)):
...             g[("sums", k_a, k_b)] = (
...                 sum,
...                 ([ag.Dependency(("a", k_a), None, 1),
...                   ag.Dependency(("b", k_b), None, 1)],),
...                 2)
...     return g

We want to view for some point in ``list_a``, the combined squared distance to
origin with all points in ``list_b``. To do this, we use the keyword argument
``inflate`` with :meth:`ampeg.execute_task_lists`. This instructs ampeg to
inflate tuple task IDs to keys in a nested dict.

>>> my_object = MyClass([(3, 2), (6, -1)],
...                     [(2, 4), (4, 2), (-4, -5)])
>>> my_distances = ag.execute_task_lists(
...     *ag.earliest_finish_time(graph(my_object), 2),
...     inflate=True)
>>> my_distances["sums"][1]
{0: 57, 1: 57, 2: 78}

To allow us to do the same for some point in ``list_b``, we make use of
:meth:`ampeg.earliest_finish_time`'s removal of duplicate tasks:

>>> def multiplexed_graph(the_object):
...     g = {}
...     for k, point_a in enumerate(the_object.list_a):
...         g[("a", k)] = (squared_distance, point_a, 4)
...     for k, point_b in enumerate(the_object.list_b):
...         g[("b", k)] = (squared_distance, point_b, 4)
...     for k_a in range(len(the_object.list_a)):
...         for k_b in range(len(the_object.list_b)):
...             g[("sums", k_a, k_b)] = (
...                 sum,
...                 ([ag.Dependency(("a", k_a), None, 1),
...                   ag.Dependency(("b", k_b), None, 1)],),
...                 2)
...             g[("sums b", k_b, k_a)] = g[("sums", k_a, k_b)]
...     return g
...
>>> my_multiplexed_distances = ag.execute_task_lists(
...     *ag.earliest_finish_time(multiplexed_graph(my_object), 2),
...     inflate=True)
>>> my_multiplexed_distances["sums b"][0]
{0: 33, 1: 57}


Joining Graphs
--------------

Extending our previous example, we want to process several instances of
``MyClass`` at the same time. Since ``generate`` will produce graphs with
identical task IDs, we cannot simply join graphs using ``dict.update``.
Fortunately, :mod:`ampeg` provides a function :meth:`ampeg.prefix` that applies
some prefix to all task IDs in a graph and updates any dependencies
accordingly. Combining this with the above techinques, we write a function to
process a list of instances of ``MyClass``:

>>> def compute_and_multiplex_many(list_of_objects):
...     combined_graph = {}
...     for k, this_object in enumerate(list_of_objects):
...          this_graph = ag.prefix(
...              multiplexed_graph(this_object), k)
...          combined_graph.update(this_graph)
...     results = ag.execute_task_lists(
...         *ag.earliest_finish_time(combined_graph, 2),
...         inflate=True)
...     return results
...
>>> my_other_object = MyClass([(3, -3), (2, -1), (0, 5)],
...                           [(0, 1), (-2, 3)])
>>> my_list_of_objects = [my_object, my_other_object]
>>> my_many_distances = compute_and_multiplex_many(
...     my_list_of_objects)
>>> my_many_distances[1]["sums"][2]
{0: 26, 1: 38}
>>> my_many_distances[0]["sums b"][2]
{0: 54, 1: 78}
