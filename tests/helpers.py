# -*- coding: utf-8 -*-
"""Helper functions for testing.

Author: Stefan Peterson
"""


def id_(x):
    return x


def div(x, y):
    return x/y


def add(x, y):
    return x + y


def square_one(x):
    return x**2


def stats(x):
    n = len(x)
    mu = sum(x)/n
    var = sum((y - mu)**2 for y in x)/n

    return {"dummy": {"mu": mu,
                      "var": var}}


def square(x):
    return [y**2 for y in x]


def sum_stats(mu_0, mu_1, var_0, var_1):
    return (mu_0 + mu_1), (var_0 + var_1)


def normalize(x, mu, var):
    return {"y": [(y - mu)/var for y in x]}


def diff(x, y):
    return {"z": [q - p for (p, q) in zip(x, y)]}