Based on the tutorial in

https://www.digitalocean.com/community/tutorials/how-to-set-up-replication-in-mysql

https://dev.mysql.com/doc/refman/8.4/en/change-replication-source-to.html


check conenction encryption status from the master:

    SELECT t.THREAD_ID,
        t.PROCESSLIST_USER,
        t.PROCESSLIST_HOST,
        t.CONNECTION_TYPE,
        sbt.VARIABLE_VALUE AS cipher
    FROM performance_schema.threads t
    LEFT JOIN performance_schema.status_by_thread sbt
        ON (t.THREAD_ID = sbt.THREAD_ID AND sbt.VARIABLE_NAME = 'Ssl_cipher')
    WHERE t.PROCESSLIST_USER IS NOT NULL;
