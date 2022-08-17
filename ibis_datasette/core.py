import functools
import urllib.parse
import contextvars
import contextlib
from urllib.parse import urlencode

import httpx
import sqlalchemy as sa
import sqlalchemy.engine.reflection
import sqlalchemy.dialects.sqlite.base
from ibis.backends.base.sql.alchemy import BaseAlchemyBackend
from ibis.backends.sqlite.compiler import SQLiteCompiler


_cacheable = contextvars.ContextVar("cacheable", default=False)


@contextlib.contextmanager
def cacheable():
    """A contextmanager for marking a request as cacheable"""
    t = _cacheable.set(True)
    try:
        yield
    finally:
        _cacheable.reset(t)


class _Client:
    def __init__(self, client, base_url):
        self._client = client
        self._base_url = base_url

    def _get(self, suffix):
        url = self._base_url + suffix
        resp = self._client.get(url)
        try:
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                raise ValueError(f"{url!r} is not a valid datasette URL") from exc
            raise
        return resp

    @functools.lru_cache(32)
    def _cached_get(self, url):
        return self._get(url)

    def get(self, url):
        if _cacheable.get():
            return self._cached_get(url)
        return self._get(url)


class Cursor:
    def __init__(self, conn):
        self._conn = conn
        self._rows = None
        self._description = None
        self._next = None

    @property
    def rowcount(self):
        return -1

    @property
    def description(self):
        return self._description

    def close(self):
        pass

    def _do_query(self, query_string):
        self._description = None
        self._next = None
        self._rows = None

        json = self._conn._get(query_string).json()

        self._description = [
            (col, None, None, None, None, None, None) for col in json["columns"]
        ]
        self._next = json.get("next", None)
        self._rows = iter(json.get("rows", []))

    def _next_row(self):
        if self._rows is not None:
            try:
                return next(self._rows)
            except StopIteration:
                self._rows = None

        if self._next is None:
            return None
        self._do_query(f"?_next={self._next}")
        if self._rows is not None:
            try:
                return next(self._rows)
            except StopIteration:
                self._rows = None

    def execute(self, statement, parameters=None):
        # Skip pragmas
        if statement.lstrip().startswith("PRAGMA"):
            raise NotImplementedError("PRAGMA operations aren't supported")

        query = {"sql": statement}

        if parameters:
            if isinstance(parameters, tuple):
                raise NotImplementedError(
                    "qmark (?) style parametrized queries are not supported"
                )
            else:
                query.update(parameters)

        self._do_query(f"?{urlencode(query)}")

    def executemany(self, statement, parameters=None):
        raise NotImplementedError(
            "executemany shouldn't be used for SELECT statements (which are the "
            "only thing datasette supports). This operation is not implemented."
        )

    def fetchone(self):
        return self._next_row()

    def fetchmany(self, size=None):
        out = []
        if size:
            for _ in range(size):
                if row := self._next_row():
                    out.append(row)
            return out
        return self.fetchall()

    def fetchall(self):
        out = []
        while row := self._next_row():
            out.append(row)
        return out


class Connection:
    def __init__(self, client, base_url):
        self._client = _Client(client, base_url)

    def _get(self, suffix):
        return self._client.get(suffix)

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        self.close()

    def cursor(self):
        return Cursor(self)

    def commit(self):
        pass

    def close(self):
        pass


class DBAPI:
    apilevel = "2.0"
    threadsafety = 2
    paramstyle = "named"
    sqlite_version_info = (3, 39, 0)

    Error = RuntimeError


class IbisDatasetteDialect(sa.dialects.sqlite.base.SQLiteDialect):
    name = "datasette"
    driver = "ibis_datasette"
    supports_statement_cache = True

    @functools.cached_property
    def httpx_client(self):
        return httpx.Client(follow_redirects=True)

    def connect(self, *args, **kwargs):
        return Connection(self.httpx_client, kwargs["url"])

    @classmethod
    def get_pool_class(cls, url):
        return sa.pool.SingletonThreadPool

    @staticmethod
    def dbapi():
        return DBAPI()

    def get_isolation_level(self, dbapi_conn):
        return "SERIALIZABLE"

    @sa.engine.reflection.cache
    def has_table(self, connection, table_name, schema=None, **kw):
        self._ensure_has_table_connection(connection)

        info = self._get_table_pragma(
            connection, "table_xinfo", table_name, schema=schema
        )
        return bool(info)

    @sa.engine.reflection.cache
    def _get_table_sql(self, connection, table_name, schema=None, **kw):
        qtable = self.identifier_preparer.quote_identifier(table_name)
        s = (
            f"SELECT sql FROM sqlite_master WHERE name = {qtable} "
            f"AND type in ('table', 'view')"
        )
        with cacheable():
            value = connection.exec_driver_sql(s).scalar()
        if value is None and not self._is_sys_table(table_name):
            raise sa.exc.NoSuchTableError(table_name)
        return value

    def _get_table_pragma(self, connection, pragma, table_name, schema=None):
        qtable = self.identifier_preparer.quote_identifier(table_name)
        s = f"SELECT * FROM pragma_{pragma}({qtable})"
        with cacheable():
            return connection.exec_driver_sql(s).fetchall()

    def _get_server_version_info(self, connection):
        # XXX: We can't know the sqlite version on the remote, assume it's
        # recent enough to use table_xinfo
        return (3, 39, 0)

    def do_rollback(self, connection):
        pass  # no-op

    def do_begin(self, connection):
        pass  # no-op


sa.dialects.registry.register(
    "ibisdatasette", "ibis_datasette.core", "IbisDatasetteDialect"
)


class Backend(BaseAlchemyBackend):
    name = "sqlite"
    compiler = SQLiteCompiler

    def __getstate__(self):
        r = super().__getstate__()
        r.update(
            dict(
                compiler=self.compiler,
                database_name=self.database_name,
                _con=None,  # clear connection on copy()
                _meta=None,
            )
        )
        return r

    def do_connect(self, url):
        parsed = urllib.parse.urlparse(url)
        if not parsed.path:
            raise ValueError(
                f"`connect` expects a datasette URL including the path for a "
                f"specific database, got {url!r}"
            )
        if not url.endswith(".json"):
            url += ".json"

        query = urllib.parse.urlencode({"url": url})
        engine = sa.create_engine(url=f"ibisdatasette://?{query}")

        with engine.dialect.connect(url=url) as con:
            resp = con._get("")
            json = resp.json()

            if not json.get("allow_execute_sql", False):
                raise ValueError(
                    "This datasette instance disallows custom SQL queries; "
                    "ibis-datasette cannot query it."
                )

        self.database_name = "main"
        super().do_connect(engine)
        self._meta = sa.MetaData(bind=self.con)

    def _get_sqla_table(self, name, schema=None, autoload=True):
        return sa.Table(
            name,
            self.meta,
            schema=schema or self.current_database,
            autoload=autoload,
        )

    def list_tables(self, like=None, database=None):
        """List the tables in the database.

        Parameters
        ----------
        like
            A pattern to use for listing tables.

        """
        with cacheable():
            return super().list_tables(like=like, database=database)

    def table(self, name):
        """Create a table expression from a table in the SQLite database.

        Parameters
        ----------
        name
            Table name

        Returns
        -------
        Table
            Table expression
        """
        alch_table = self._get_sqla_table(name)
        node = self.table_class(source=self, sqla_table=alch_table)
        return self.table_expr_class(node)

    def _table_from_schema(self, name, schema, database=None):
        columns = self._columns_from_schema(name, schema)
        return sa.Table(name, self.meta, schema=database, *columns)

    @property
    def _current_schema(self):
        return self.current_database
