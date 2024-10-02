import pathlib
import time

import sqlalchemy
import pytest
from testcontainers.mysql import MySqlContainer
from testcontainers.core.network import Network

from replication import (
    create_replication_user,
    Connection,
    setup_replication,
)

def abspath(filename):
    return pathlib.Path(__file__).parent.parent.resolve() / filename


@pytest.fixture(scope="session")
def network():
    with Network() as replication_network:
        yield replication_network

@pytest.fixture()
def source(request, network):
    yield from mysql_container_connection(network, "source", abspath("configs/source/"))


@pytest.fixture()
def replica(request, network):
    yield from mysql_container_connection(network, "replica", abspath("configs/replica/"))


def mysql_container_connection(network, name, config_dir):
    container_factory = MySqlContainer(
        image="mysql:8.4.2",
        username="root",
        root_password="secret"
    ).with_network(network)\
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

    create_replication_user(source, 'replicator', 'replipassword', '%')
    setup_replication(source, replica, 'replicator', 'replipassword')

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
