# trino-vertica
Trino JDBC connector to Vertica

This is an experiment based on trino-example-jdbc.  Currently it supports:

Data types: BOOLEAN, INT, DOUBLE, CHAR, VARCHAR, BINARY, VARBINARY, DATE, TIME, TIMESTAMP

Nothing else works, but I will work on predicates, aggregates, remaining primitives and complex data types over time.

### How to install

Out of the box, this connector works with Trino release version 419.

Download and unpack the Trino 419 tag from the official GitHub.

Download this repo and copy trino-vertica into the plugins directory

Import the project into IntelliJ IDEA.  Open the root pom.xml and add "plugin/trino-vertica" as a module in the modules list.

Reload Maven and wait for everything to settle.

Open the Maven panel and expand trino-vertica lifecycle.  Turn off tests, then run Clean, then Install.

Now go to the source tree into plugins/trino-vertica/target.  Copy the ZIP file to the plugins directory in your Trino 419 install.

Expand the ZIP and rename the directory to "vertica".

Add a minimal catalog file, e.g.:
```
$ cat etc/catalog/vertica.properties
connector.name=vertica
connection-url=jdbc:vertica://localhost:5433/xxx
connection-user=xxx
connection-password=xxx
```
Restart Trino.  You should be able to get something like this to work (outputs simplified here):
```
$ vsql -U trino
d2=> select * from trino.test ;
 i |  f   |     d      |             ts             |    v
---+------+------------+----------------------------+---------
 1 | 1.23 | 2023-06-07 | 2023-06-07 11:36:19.250644 | Vertica
[bryan@hpbox trino]$ ./trino
trino> show tables in vertica.trino;
 Table
-------
 test
(1 row)

trino> select * from vertica.trino.test;
 i |  f   |     d      |           ts            |    v
---+------+------------+-------------------------+---------
 1 | 1.23 | 2023-06-07 | 2023-06-07 11:36:19.251 | Vertica
(1 row)
```
