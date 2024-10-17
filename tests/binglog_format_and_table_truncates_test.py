import time

from replication import (
    Connection,
    ReplicationSource,
    Credentials,
    resource_path,
)

REPLICATION_CREDENTIALS = Credentials('replicator', 'replipassword')

def _prepare_replication_with_animals_table(source: Connection, replica: Connection):
    # Setup replication
    replication_source = ReplicationSource.from_source(source, REPLICATION_CREDENTIALS)
    replication_source.setup_credentials()
    replication_source.setup_target(replica)
    # Create schema
    source.execute('create database db')
    source.execute('use db')
    source.execute("""
        CREATE TABLE animals (
             id MEDIUMINT NOT NULL AUTO_INCREMENT,
             name CHAR(30) NOT NULL,
             PRIMARY KEY (id)
        )
    """)

def test_truncate_table_using_mixed_format(source: Connection, replica: Connection):
    _prepare_replication_with_animals_table(source, replica)
    # Create initial data, truncate and insert again
    source.execute('set global binlog_format = "MIXED"')
    source.execute('insert into animals (name) values ("dog"), ("cat")')
    source.execute('truncate table animals')
    source.execute('insert into animals (name) values ("bat")')
    replica.execute("start replica")
    time.sleep(.2)

    # Search for an entry added to the source after the backup was done
    result = replica.execute('select id,name from db.animals')

    assert result.fetchall() == [(1,'bat')]
    assert source.execute('select @@GLOBAL.binlog_format').fetchone() == ('MIXED',)


def test_truncate_table_using_row_format(source: Connection, replica: Connection):
    _prepare_replication_with_animals_table(source, replica)
    # Create initial data, truncate and insert again
    source.execute('set global binlog_format = "ROW"')
    source.execute('insert into animals (name) values ("dog"), ("cat")')
    source.execute('truncate table animals')
    source.execute('insert into animals (name) values ("bat")')
    replica.execute("start replica")
    time.sleep(.2)

    # Search for an entry added to the source after the backup was done
    result = replica.execute('select id,name from db.animals')

    assert result.fetchall() == [(1,'bat')]
    assert source.execute('select @@GLOBAL.binlog_format').fetchone() == ('ROW',)
