import functools
import operator
import random
import sqlite3
import string
import subprocess
import time

import pytest
import ibis
from ibis import _


def randstr():
    length = random.randint(4, 30)
    return "".join(random.choices(string.ascii_letters, k=length))


NAMES = [
    "alice",
    "brad",
    "caroline",
    "doug",
    "emily",
    "frank",
    "gwen",
    "harold",
    "isabel",
    "john",
    "katrina",
    "lou",
    "manny",
    "nora",
    "oren",
    "patty",
]


def randname():
    return random.choice(NAMES)


@pytest.fixture(scope="session")
def database(tmp_path_factory):
    path = str(tmp_path_factory.mktemp("databases") / "test.db")
    con = sqlite3.connect(path)
    con.execute("CREATE TABLE table1 (col1 text, col2 text, col3 int, col4 real)")
    con.execute("CREATE TABLE table2 (x text, y int)")

    with con:
        con.executemany(
            "INSERT INTO table1 VALUES (?, ?, ?, ?)",
            [
                (randstr(), randname(), random.randint(0, 100), random.random())
                for _ in range(5000)
            ],
        )

        con.executemany(
            "INSERT INTO table2 VALUES (?, ?)",
            [(randname(), random.randint(0, 20)) for _ in range(3000)],
        )

    con.close()
    return path


@pytest.fixture(scope="session")
def datasette(database):
    ds_proc = subprocess.Popen(
        ["datasette", "serve", "-p", "8041", database],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    # Give the server time to start
    time.sleep(1.5)
    # Check it started successfully
    assert not ds_proc.poll(), ds_proc.stdout.read().decode("utf-8")
    yield "http://127.0.0.1:8041"
    # Shut it down at the end of the pytest session
    ds_proc.terminate()


@pytest.fixture
def url(datasette):
    return datasette + "/test"


def test_connect_errors_not_database_url(datasette):
    with pytest.raises(ValueError, match="`connect` expects"):
        ibis.datasette.connect(datasette)


def test_connect_errors_invalid_url(url):
    with pytest.raises(ValueError, match="is not a valid datasette URL"):
        ibis.datasette.connect(url + "/missing")


def test_list_tables(url):
    con = ibis.datasette.connect(url)
    tables = sorted(con.list_tables())
    assert tables == ["table1", "table2"]


def test_access_table(url):
    con = ibis.datasette.connect(url)
    t1 = con.tables.table1
    assert t1.columns == ["col1", "col2", "col3", "col4"]
    schema = ibis.schema(
        [
            ("col1", "string"),
            ("col2", "string"),
            ("col3", "int32"),
            ("col4", "float64"),
        ]
    )
    assert t1.schema() == schema


def test_table_does_not_exist(url):
    con = ibis.datasette.connect(url)
    with pytest.raises(AttributeError):
        con.tables.missing


def test_table_query(url):
    con = ibis.datasette.connect(url)
    t1 = con.tables.table1
    query = t1.group_by(t1.col2).col3.mean()
    out = query.execute()
    assert len(out) == len(NAMES)


def test_table_limit(url):
    con = ibis.datasette.connect(url)
    t1 = con.tables.table1
    out = t1.limit(123).execute()
    assert len(out) == 123


@pytest.mark.parametrize("bound", [200, 200.5])
def test_numeric_query_parameters(url, bound):
    con = ibis.datasette.connect(url)
    t1 = con.tables.table1
    query = t1.group_by("col2").count().filter(lambda _: _["count"] > bound)
    out = query.execute()
    assert len(out)  # out is empty if bound interpreted as string


def test_many_query_parameters(url):
    con = ibis.datasette.connect(url)
    query = con.tables.table1.filter(
        functools.reduce(operator.or_, (_.col3 == x for x in range(30)))
    ).col3.nunique()
    out = query.execute()
    assert out == 30


def test_string_query_parameters(url):
    con = ibis.datasette.connect(url)
    t1 = con.tables.table1
    assert t1.filter(t1.col2 == "alice").count().execute()


def test_api_error_raised(url):
    con = ibis.datasette.connect(url)
    with pytest.raises(ValueError, match="missing"):
        con.raw_sql("SELECT * FROM missing")
