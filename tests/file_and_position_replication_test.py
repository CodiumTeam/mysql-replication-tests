import time

from replication import (
    Connection,
    ReplicationSource,
    resource_path,
    Credentials,
)

REPLICATION_CREDENTIALS = Credentials('replicator', 'replipassword')


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

def test_setup_replication_from_source_with_existent_data(loaded_source: Connection, replica: Connection):
    # Setup replication
    replication_source = ReplicationSource.from_source(loaded_source, REPLICATION_CREDENTIALS)
    replication_source.setup_credentials()
    replication_source.setup_target(replica)
    # Backup master data
    backup_file = resource_path('.out/source.sql')
    loaded_source.dump('example', backup_file, '--single-transaction --master-data')
    # Insert data in the master that will not be in the backup
    loaded_source.execute('use example')
    loaded_source.execute("insert into pet values ('Smity','Hugo','dog','m','2013-09-07',NULL)")
    # Load the backup in the slave
    replica.execute('create database example')
    replica.execute('use example')
    replica.execute_from_file(backup_file)
    # Start slave replication
    replica.execute("start replica")
    # Wait for replication to carry changes from source to replica
    time.sleep(.2)

    # Search for an entry added to the source after the backup was done
    result = replica.execute('select 1 from example.pet where name="Smity"')

    assert result.fetchone() == (1,)
