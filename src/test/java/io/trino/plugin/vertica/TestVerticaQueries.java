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

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

public class TestVerticaQueries
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return VerticaQueryRunner.createQueryRunner();
    }

    @Test
    public void showTables()
    {
        assertQuery("SHOW SCHEMAS FROM vertica LIKE 't%'", "VALUES 'trino'");
        assertQuery("SHOW TABLES FROM vertica.trino LIKE 'test%'", "VALUES 'test'");
    }

    @Test
    public void createAndDropTable()
    {
        assertQuerySucceeds("create table vertica.trino.test2 (a int, b date, c varchar)");
        assertQuery("SHOW TABLES FROM vertica.trino LIKE 'test%'", "VALUES 'test','test2'");
        assertQuerySucceeds("drop table vertica.trino.test2");
        assertQuery("SHOW TABLES FROM vertica.trino LIKE 'test%'", "VALUES 'test'");
    }

    @Test
    public void typeTesting()
    {
        assertQuerySucceeds("create table vertica.trino.sometypes (d1 decimal(18,8), d2 decimal(12))");
        assertQuerySucceeds("INSERT INTO vertica.trino.sometypes VALUES (1.234,246)");
        assertQuery("SELECT d1 FROM vertica.trino.sometypes", "VALUES 1.234");
        assertQuery("SELECT d2 FROM vertica.trino.sometypes", "VALUES 246");
        assertQuerySucceeds("drop table vertica.trino.sometypes");
    }

    @Test
    public void selectFromTable()
    {
        assertQuery("SELECT DISTINCT v FROM vertica.trino.test", "VALUES 'Vertica', 'Trino'");
    }

    @Test
    public void selectTsFromTable()
    {
        assertQuerySucceeds("SELECT ts FROM vertica.trino.test WHERE ts > timestamp '2023-01-01 00:00:00'");
    }

    @Test
    public void aggregates()
    {
        assertQuerySucceeds("create table vertica.trino.unitAgg (a int, b double)");
        assertQuerySucceeds("INSERT INTO vertica.trino.unitAgg values (1,1.23)");
        assertQuerySucceeds("INSERT INTO vertica.trino.unitAgg values (2,2.23)");
        assertQuerySucceeds("INSERT INTO vertica.trino.unitAgg values (3,3.23)");
        assertQuery("SELECT SUM(a) FROM vertica.trino.unitAgg", "VALUES 6");
        assertQuery("SELECT AVG(a) FROM vertica.trino.unitAgg", "VALUES 2");
        assertQuery("SELECT SUM(b) FROM vertica.trino.unitAgg", "VALUES 6.69");
        assertQuery("SELECT AVG(b) FROM vertica.trino.unitAgg", "VALUES 2.23");
        assertQuery("SELECT COUNT(*) FROM vertica.trino.unitAgg", "VALUES 3");
        assertQuery("SELECT MIN(a) FROM vertica.trino.unitAgg", "VALUES 1");
        assertQuery("SELECT MAX(a) FROM vertica.trino.unitAgg", "VALUES 3");
        assertQuerySucceeds("drop table vertica.trino.unitAgg");
    }
}
