import time

import sqlalchemy
import pytest
from testcontainers.mysql import MySqlContainer

from replication import (
    create_replication_user,
    Connection,
    MYSQL_IMAGE,
    MYSQL_NET,
    resource_path,
    Credentials,
    ReplicationSource,
)

@pytest.fixture()
def source(request):
    yield from mysql_container_connection(
        name="source",
        config_dir=resource_path("configs/source/")
    )


@pytest.fixture()
def replica(request):
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
     .with_volume_mapping(config_dir, "/etc/mysql/conf.d/", "ro")

    with container_factory as container:
        engine = sqlalchemy.create_engine(container.get_connection_url())
        yield Connection(engine, container)


def test_connect_a_replica(source: Connection, replica: Connection):
    assert source.show_variable('version').startswith('8.4.')
    assert replica.show_variable('version').startswith('8.4.')
    assert source.show_variable('server_id') == '1'
    assert replica.show_variable('server_id') == '2'

    credentials = Credentials('replicator', 'replipassword')
    create_replication_user(source, credentials)
    replication_source = ReplicationSource.from_source(source, credentials)
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

def test_load_backup(source: Connection):
    source.execute('create database example')
    source.execute('use example')
    source.execute_from_file(resource_path('seeds/menagerie.sql'))

    result = source.execute('select 1 from example.pet where name="Fluffy"')

    assert result.fetchone() == (1,)
