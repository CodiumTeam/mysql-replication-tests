import pathlib
from dataclasses import dataclass

import sqlalchemy
import sqlparse
from testcontainers.core.network import Network

MYSQL_IMAGE = "mysql:8.4.2"
MYSQL_NET = Network().create()

def resource_path(filename):
    return pathlib.Path(__file__).parent.parent.resolve() / filename

@dataclass
class Credentials:
    username: str
    password: str


@dataclass
class BinlogRef:
    filename: str
    position: int


class ReplicationSource:
    def __init__(self, conn: 'Connection', credentials: Credentials, binlog: BinlogRef):
        self.connection = conn
        self.credentials = credentials
        self.binlog = binlog

    @classmethod
    def from_source(cls, conn: 'Connection', credentials: Credentials):
        return cls(conn, credentials, conn.get_binlog_reference())

    def setup_target(self, target: 'Connection'):
        target.execute(f"""
            change replication source to
            GET_SOURCE_PUBLIC_KEY=1,
            SOURCE_HOST='{self.connection.get_host()}',
            SOURCE_PORT={self.connection.get_port()},
            SOURCE_USER='{self.credentials.username}',
            SOURCE_PASSWORD='{self.credentials.password}',
            SOURCE_LOG_FILE='{self.binlog.filename}',
            SOURCE_LOG_POS={self.binlog.position}
        """)


class Connection:
    def __init__(self, engine, container):
        self._engine = engine
        self._container = container

    def execute(self, text):
        with self._engine.begin() as connection:
            return connection.execute(sqlalchemy.text(text))

    def execute_from_file(self, path):
        with open(path) as file:
            statements = sqlparse.split(sqlparse.format(file.read()))
            for statement in statements:
                self.execute(statement)

    def get_host(self):
        return self._container._name

    def get_port(self):
        return self._container.port

    def show_variable(self, name):
        result = self.execute(f"show variables like '{name}'")
        name, value = result.fetchone()
        return value

    def get_binlog_reference(self):
        self.execute("flush tables with read lock")
        result = self.execute("show binary log status")
        file, position, *_ = result.fetchone()
        self.execute("unlock tables")

        return BinlogRef(filename=file, position=position)


def create_replication_user(connection: Connection, credentials: Credentials, host='%'):
    connection.execute(f"""
        create user '{credentials.username}'@'{host}'
        identified by '{credentials.password}'
    """)
    connection.execute(f"""
        grant replication slave on *.*
        to '{credentials.username}'@'{host}'
    """)
    connection.execute("flush privileges")


