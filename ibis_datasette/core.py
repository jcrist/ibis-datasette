import functools
import urllib.parse
from urllib.parse import urlencode

import httpx
import sqlalchemy as sa
import sqlalchemy.engine.reflection
import sqlalchemy.dialects.sqlite.base
from ibis.backends.base.sql.alchemy import BaseAlchemyBackend
from ibis.backends.sqlite.compiler import SQLiteCompiler


class _ReflectionCache:
    """The SQLiteDialect in sqlalchemy doesn't use the reflection cache
    for pragmas, which can result in excess queries. Since datasette databases
    are generally immutable, we use our own caching mechanism for reflection
    operations to further reduce queries"""

    @functools.lru_cache(32)
    def _execute(self, statement, scalar=True):
        res = self.connection.exec_driver_sql(statement)
        if scalar:
            return res.scalar()
        else:
            return res.fetchall()

    def execute(self, connection, statement, scalar=True):
        try:
            # Pass connection out-of-band so it doesn't affect
            # the lru cache
            self.connection = connection
            return self._execute(statement, scalar=scalar)
        finally:
            self.connection = None


_RCACHE = _ReflectionCache()


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

        resp = self._conn._client.get(query_string)
        resp.raise_for_status()
        json = resp.json()

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
    def __init__(self, base_url):
        self._client = httpx.Client(follow_redirects=True, base_url=base_url)

    def cursor(self):
        return Cursor(self)

    def commit(self):
        pass

    def close(self):
        self._client.close()


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

    def connect(self, *args, **kwargs):
        base_url = kwargs["url"]
        if not base_url.endswith(".json"):
            base_url += ".json"
        return Connection(base_url)

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
        value = _RCACHE.execute(connection, s, scalar=True)
        if value is None and not self._is_sys_table(table_name):
            raise sa.exc.NoSuchTableError(table_name)
        return value

    def _get_table_pragma(self, connection, pragma, table_name, schema=None):
        qtable = self.identifier_preparer.quote_identifier(table_name)
        s = f"SELECT * FROM pragma_{pragma}({qtable})"
        return _RCACHE.execute(connection, s, scalar=False)

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
        self.database_name = "main"
        query = urllib.parse.urlencode({"url": url})
        engine = sa.create_engine(url=f"ibisdatasette://?{query}")
        super().do_connect(engine)
        self._meta = sa.MetaData(bind=self.con)

    def _get_sqla_table(self, name, schema=None, autoload=True):
        return sa.Table(
            name,
            self.meta,
            schema=schema or self.current_database,
            autoload=autoload,
        )

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
