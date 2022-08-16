ibis-datasette
==============

|github| |pypi|

``ibis-datasette`` provides a datasette_ backend for ibis_. This lets you query
any ``datasette`` server using a familiar dataframe-like API (rather than SQL).


Installation
------------

``ibis-datasette`` is available on pypi_:

.. code-block::

    $ pip install ibis-datasette


Usage
-----

Once installed, you can connect to any ``datasette`` server using the
``ibis.datasette.connect`` function. This takes the `full URL to a database`_
For example, to connect to the `legislators database`_.

.. code-block:: python

    In [1]: import ibis

    In [2]: con = ibis.datasette.connect("https://congress-legislators.datasettes.com/legislators")


Once connected, you can interact with tables using ``ibis`` just as you would a
local ``sqlite`` database:

.. code-block:: python

    In [3]: ibis.options.interactive = True  # enable interactive mode

    In [4]: con.list_tables()
    Out[4]:
    ['executive_terms',
     'executives',
     'legislator_terms',
     'legislators',
     'offices',
     'social_media']

    In [5]: t = con.tables.legislators  # access the `legislators` table

    In [6]: t.name_first.topk(5)  # top 5 first names for legislators
    Out[6]:
      name_first  count
    0       John   1273
    1    William   1024
    2      James    721
    3     Thomas    457
    4    Charles    442


LICENSE
-------

New BSD. See the `License File`_.

.. |github| image:: https://github.com/jcrist/ibis-datasette/actions/workflows/ci.yml/badge.svg
   :target: https://github.com/jcrist/ibis-datasette/actions/workflows/ci.yml
.. |pypi| image:: https://img.shields.io/pypi/v/ibis-datasette.svg
   :target: https://pypi.org/project/ibis-datasette/

.. _pypi: https://pypi.org/project/ibis-datasette/
.. _ibis: https://ibis-project.org/
.. _datasette: https://datasette.io/
.. _full URL to a database: https://docs.datasette.io/en/stable/pages.html#database
.. _legislators database: https://congress-legislators.datasettes.com/legislators
.. _License File: https://github.com/jcrist/ibis-datasette/blob/main/LICENSE
