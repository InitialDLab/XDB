approXimate DataBase (XDB)
=========================

XDB integrates the wander join algorithm into PostgreSQL 9.4.2 to support
online aggregation queries. The main difference between the TODS version and
SIGMOD version is we use a standard B-tree instead of a counting B-tree to draw
random samples. Sampling in each join step is almost but not strictly uniform.

## Build

Building XDB is the same as building PostgreSQL.
See INSTALL for building and installation instructions.

## Syntax

        SELECT ONLINE <target_list>
        FROM rel1, rel2, ... reln
        WHERE   rel1.A op rel2.B
            AND rel2.C op rel3.D
            AND ...
            AND rel(n-1).X op reln.Y
            [AND <other quals>]
        WITHTIME <time in ms>
        CONFIDENCE <confidence in percentage>
        REPORTINTERVAL <report interval in ms>
		[GROUP BY col1, ..., colm]
        [INITSAMPLE <n>];
            
Other quals may not be allowed by the online aggregation planner if they are
general quals involving multiple relations.

Existence of quals of different columns of the same relation may cause some of
the samples to be rejected. 

If INITSAMPLE clause exists and n following it is greater than 0, all
reasonable plans will be run for n samples and the one with lowest rejection
rate will be selected for subsequent samples.

## Frontend

Due to the restriction of the psql frontend, it cannot report any aggregation
estimator until the time expries. We have a modified Apache Zeppelin (see
InitialD Lab Github) frontend which allows you to see the estimators as the
query is being executed. You can also write your own frontend using JDBC,
which also allows you to see results as the query is being executed if you se
the fetch size to 1.

Alternatively, run a single instance of backend, using the following command:

    postgres --single <dbname>

## Warm up I/O Buffer

PostgreSQL depends on system i/o buffer to hold disk pages in memory. We added
a sql statement ``SCAN <relation_name/index_name>'' to load relations and
indexes into main memory. Wander join performs a lot of random I/O's, so
having relations and indexes buffered in memory greatly improves the
performance.

## Sample Queries

A few sample queries for the TPC-H benchmark is under ``queries'' directory.

## License

Copyright and license information can be found in the COPYRIGHT file and
comments in the source code. XDB is based on PostgreSQL 9.4.2, which is
licensed under PostgreSQL license (see postgresql.COPYRIGHT). Any files that are
new to or modified from PostgreSQL 9.4.2 are covered by the COPYRIGHT file.

Please feel free to use XDB for non-commercial purpose. If you would like to
make commercial use of XDB, please contact the authors.

## Authors

Zhuoyue Zhao: zyzhao [at] cs [dot] utah [dot] edu (contact author) <br/>
Bin Wu: bwuac [at] cse [dot] ust [dot] hk <br/>
Feifei Li: lifeifei [at] cs [dot] utah [dot] edu <br/>
Ke Yi: yike [at] cse [dot] ust [dot] hk <br/>

