import random
import sqlite3
import string
import subprocess
import time

import ibis
import pytest


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
    yield "http://127.0.0.1:8041/test"
    # Shut it down at the end of the pytest session
    ds_proc.terminate()


def test_list_tables(datasette):
    con = ibis.datasette.connect(datasette)
    tables = sorted(con.list_tables())
    assert tables == ["table1", "table2"]


def test_access_table(datasette):
    con = ibis.datasette.connect(datasette)
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


def test_table_does_not_exist(datasette):
    con = ibis.datasette.connect(datasette)
    with pytest.raises(AttributeError):
        con.tables.missing


def test_table_query(datasette):
    con = ibis.datasette.connect(datasette)
    t1 = con.tables.table1
    query = t1.group_by(t1.col2).col3.mean()
    out = query.execute()
    assert len(out) == len(NAMES)


def test_table_limit(datasette):
    con = ibis.datasette.connect(datasette)
    t1 = con.tables.table1
    out = t1.limit(123).execute()
    assert len(out) == 123
