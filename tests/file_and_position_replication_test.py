import time

import sqlalchemy
import pytest
from testcontainers.mysql import MySqlContainer
from testcontainers.core.network import Network

from replication import (
    Connection,
    resource_path,
    Credentials,
    ReplicationSource,
)


MYSQL_IMAGE = "mysql:8.4.2"
MYSQL_NET = Network().create()
REPLICATION_CREDENTIALS = Credentials('replicator', 'replipassword')


@pytest.fixture()
def source(request):
    """
    A database instance intended to be the source of a replication
    """
    yield from mysql_container_connection(
        name="source",
        config_dir=resource_path("configs/source/")
    )


@pytest.fixture()
def loaded_source(request, source):
    """
    A source database preloaded with the menagerie dataset
    """
    source.execute('create database example')
    source.execute('use example')
    source.execute_from_file(resource_path('seeds/menagerie.sql'))
    yield source


@pytest.fixture()
def replica(request):
    """
    A database instance intended to be the replica of a replication source
    """
    yield from mysql_container_connection(
        name="replica",
        config_dir=resource_path("configs/replica/")
    )


def mysql_container_connection(name, config_dir):
    container_factory = MySqlContainer(
        image=MYSQL_IMAGE,
        username="root",
        root_password="secret",
    ).with_network(MYSQL_NET)\
    .with_name(name)\
    .with_volume_mapping(
        config_dir,
        "/etc/mysql/conf.d/", "ro"
    )\
    .with_volume_mapping(
        resource_path('.out/'),
        resource_path('.out/'), "rw"
    )

    with container_factory as container:
        engine = sqlalchemy.create_engine(container.get_connection_url())
        yield Connection(engine, container)


def test_connect_a_replica(source: Connection, replica: Connection):
    assert source.show_variable('version').startswith('8.4.')
    assert replica.show_variable('version').startswith('8.4.')
    assert source.show_variable('server_id') == '1'
    assert replica.show_variable('server_id') == '2'
    replication_source = ReplicationSource.from_source(source, REPLICATION_CREDENTIALS)
    replication_source.setup_credentials()
    replication_source.setup_target(replica)
    replica.execute("start replica")
    source.execute('create database db')
    source.execute('use db')
    source.execute('create table example_table (example_column varchar(30))')
    source.execute('insert into example_table values ("one"), ("two")')
    time.sleep(.2)

    result = replica.execute('select * from db.example_table')

    assert result.fetchall() == [
        ("one",),
        ("two",),
    ]


def test_load_backup(loaded_source: Connection):
    result = loaded_source.execute('select 1 from example.pet where name="Fluffy"')

    assert result.fetchone() == (1,)


def test_setup_replication_from_source_with_existent_data(loaded_source: Connection, replica: Connection):
    replication_source = ReplicationSource.from_source(loaded_source, REPLICATION_CREDENTIALS)
    replication_source.setup_credentials()
    replication_source.setup_target(replica)
    backup_file = resource_path('.out/source.sql')
    loaded_source.dump('example', backup_file, '--single-transaction --master-data')
    replica.execute('create database example_replica')
    replica.execute('use example_replica')
    replica.execute_from_file(backup_file)
    replica.execute("start replica")

    result = replica.execute('select 1 from example_replica.pet where name="Fluffy"')

    assert result.fetchone() == (1,)
