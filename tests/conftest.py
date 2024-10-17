import sqlalchemy
import pytest
from testcontainers.mysql import MySqlContainer
from testcontainers.core.network import Network

from replication import (
    Connection,
    resource_path,
    Credentials,
)

MYSQL_IMAGE = "mysql:8.4.2"
MYSQL_NET = Network().create()


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
        password="secret",
    ).with_network(MYSQL_NET) \
        .with_name(name) \
        .with_volume_mapping(
        config_dir,
        "/etc/mysql/conf.d/", "ro"
    ) \
        .with_volume_mapping(
        resource_path('.out/'),
        resource_path('.out/'), "rw"
    )

    with container_factory as container:
        engine = sqlalchemy.create_engine(container.get_connection_url())
        yield Connection(engine, container)

