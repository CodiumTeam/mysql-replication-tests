import sqlalchemy

class Connection:
    def __init__(self, engine, container):
        self._engine = engine
        self._container = container

    def execute(self, text):
        with self._engine.begin() as connection:
            return connection.execute(sqlalchemy.text(text))

    def get_host(self):
        return self._container._name

    def get_port(self):
        return self._container.port

    def show_variable(self, name):
        result = self.execute(f"show variables like '{name}'")
        name, value = result.fetchone()
        return value


def get_binlog_reference(connection: Connection):
    connection.execute("flush tables with read lock")
    result = connection.execute("show binary log status")
    file, position, *_ = result.fetchone()
    connection.execute("unlock tables")

    return (file, position)


def create_replication_user(connection: Connection, username, password, host):
    connection.execute(f"""
        create user '{username}'@'{host}'
        identified by '{password}'
    """)
    connection.execute(f"""
        grant replication slave on *.*
        to '{username}'@'{host}'
    """)
    connection.execute("flush privileges")


def setup_replication(source: Connection, replica: Connection, username, password):
    file, position = get_binlog_reference(source)

    replica.execute(f"""
        change replication source to
        GET_SOURCE_PUBLIC_KEY=1,
        SOURCE_HOST='{source.get_host()}',
        SOURCE_PORT={source.get_port()},
        SOURCE_USER='{username}',
        SOURCE_PASSWORD='{password}',
        SOURCE_LOG_FILE='{file}',
        SOURCE_LOG_POS={position}
    """)
    replica.execute("start replica")
    return replica.execute("show replica status")
