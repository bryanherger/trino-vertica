/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.vertica;

import org.intellij.lang.annotations.Language;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static java.lang.String.format;

public class TestingVerticaServer
        implements Closeable
{
    private static final String USER = "dbadmin";
    private static final String PASSWORD = "";
    private static final String DATABASE = "VMart";
    public static final java.lang.Integer VSQL_PORT = 5433;
    private final GenericContainer dockerContainer;

    public TestingVerticaServer()
    {
        dockerContainer = new GenericContainer(DockerImageName.parse("vertica/vertica-ce:latest"))
                .withExposedPorts(VSQL_PORT);
        dockerContainer.start();
        execute("CREATE SCHEMA trino;");
        execute("CREATE TABLE trino.test (i int,f float,d date,ts timestamp,v varchar(80)\n);");
        execute("INSERT INTO trino.test VALUES (1,1.23,current_date,current_timestamp,'Trino');");
        execute("INSERT INTO trino.test VALUES (2,2.34,current_date,current_timestamp,'Vertica');");
        execute("INSERT INTO trino.test SELECT i+1,f+1.1,d,ts,v from trino.test;");
        execute("INSERT INTO trino.test SELECT i+2,f+2.2,d,ts,v from trino.test;");
    }

    public void execute(@Language("SQL") String sql)
    {
        execute(getJdbcUrl(), getProperties(), sql);
    }

    private static void execute(String url, Properties properties, String sql)
    {
        try (Connection connection = DriverManager.getConnection(url, properties);
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public String getUser()
    {
        return USER;
    }

    public String getPassword()
    {
        return PASSWORD;
    }

    public Properties getProperties()
    {
        Properties properties = new Properties();
        properties.setProperty("user", USER);
        properties.setProperty("password", PASSWORD);
        return properties;
    }

    public String getJdbcUrl()
    {
        return format("jdbc:vertica://%s:%s/%s", dockerContainer.getHost(), dockerContainer.getMappedPort(VSQL_PORT), DATABASE);
    }

    @Override
    public void close()
    {
        dockerContainer.close();
    }
}
