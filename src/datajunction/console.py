"""
DataJunction (DJ) is a metric repository.

Usage:
    dj c [REPO]
    dj t [METRIC] [-db DATABASE]
    dj g [METRIC] [-db DATABASE] [-f FORMAT]

Actions:
    c               Compile repository
    t               Translate a metric into SQL
    g               Get data for a given metric

Options:
    -db DATABASE    Specify a database
    -f FORMAT       Data format (JSON, CSV)

Released under the MIT license.
(c) 2018 Beto Dealmeida <roberto@dealmeida.net>
"""
