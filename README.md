# trino-vertica-connector
Trino JDBC connector to Vertica

This project is based on trino-vertica by bryanherger. You can find the original project here: https://github.com/bryanherger/trino-vertica.
The original was targeting Trino 425, but builds just fine for Trino 431 with just a version change in pom.xml,
but is broken for Trino 448.

This version updates the connector for Trino 448 and beyond

### How to install

The master branch of this connector works with Trino release version 448.  Tags and binary releases exist for several older branches.

INSTALL FROM BINARY RELEASE: Download the ZIP and unzip in your Trino plugins directory.  Rename the directory to "vertica".  Create a catalog  file as shown below.  Restart Trino.

INSTALL FROM GITHUB SOURCE:

Download and unpack the Trino 448 tag from the official GitHub.

Clone or download this repo and copy trino-vertica into the plugins directory

Import the project into IntelliJ IDEA.  Open the root pom.xml and add "plugin/trino-vertica" as a module in the modules list.

Reload Maven and wait for everything to settle.

Open the Maven panel and expand trino-vertica lifecycle.  Tests are implemented using TestContainers and can take a long time to run, so you might ant to skip tests.  Run Clean, then Install.

./mvnw clean install -DskipTests

Now go to the source tree into plugins/trino-vertica/target.  Copy the ZIP file to the plugins directory in your Trino install.

Expand the ZIP and rename the directory to "vertica".

CREATE A CATALOG FILE:

Add a minimal catalog file, e.g.:
```
$ cat etc/catalog/vertica.properties
connector.name=vertica
connection-url=jdbc:vertica://localhost:5433/xxx
connection-user=xxx
connection-password=xxx
# uncomment/set the following to EAGER to enable join pushdown
#join-pushdown.strategy=EAGER
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
