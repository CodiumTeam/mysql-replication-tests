import pathlib
from dataclasses import dataclass

import sqlalchemy
import sqlparse


def resource_path(filename):
    return str(pathlib.Path(__file__).parent.parent.resolve() / filename)

@dataclass
class Credentials:
    username: str
    password: str


@dataclass
class BinlogRef:
    filename: str
    position: int


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

    def dump(self, database: str, output_path: str, dump_flags: str = ''):
        self._container.exec(f'mysqldump -u{self._container.username} -p{self._container.root_password} -r {output_path} {dump_flags} {database}')


class ReplicationSource:
    def __init__(self, conn: Connection, credentials: Credentials, binlog: BinlogRef):
        self.connection = conn
        self.credentials = credentials
        self.binlog = binlog

    @classmethod
    def from_source(cls, conn: Connection, credentials: Credentials):
        return cls(conn, credentials, conn.get_binlog_reference())

    def setup_credentials(self, host='%'):
        self.connection.execute(
            f"""
            create user '{self.credentials.username}'@'{host}'
            identified by '{self.credentials.password}'
        """
        )
        self.connection.execute(
            f"""
            grant replication slave on *.*
            to '{self.credentials.username}'@'{host}'
        """
        )
        self.connection.execute("flush privileges")

    def setup_target(self, target: Connection):
        target.execute(f"""
            change replication source to
            GET_SOURCE_PUBLIC_KEY=1,
            SOURCE_SSL=1,
            SOURCE_HOST='{self.connection.get_host()}',
            SOURCE_PORT={self.connection.get_port()},
            SOURCE_USER='{self.credentials.username}',
            SOURCE_PASSWORD='{self.credentials.password}',
            SOURCE_LOG_FILE='{self.binlog.filename}',
            SOURCE_LOG_POS={self.binlog.position}
        """)
