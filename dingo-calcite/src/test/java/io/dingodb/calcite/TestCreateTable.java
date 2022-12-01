/*
 * Copyright 2021 DataCanvas
 *
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

package io.dingodb.calcite;

import io.dingodb.calcite.grammar.ddl.DingoSqlCreateTable;
import io.dingodb.common.partition.DingoPartDetail;
import io.dingodb.common.partition.DingoTablePart;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.ddl.SqlKeyConstraint;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.dingo.DingoSqlParserImpl;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class TestCreateTable {

    public static void parseCreateTable(SqlCreateTable create) {
        List<String> keyList = null;
        for (SqlNode sqlNode : create.columnList) {
            if (sqlNode instanceof SqlKeyConstraint) {
                SqlKeyConstraint constraint = (SqlKeyConstraint) sqlNode;
                if (constraint.getOperator().getKind() == SqlKind.PRIMARY_KEY) {
                    // The 0th element is the name of the constraint
                    keyList = ((SqlNodeList) constraint.getOperandList().get(1)).getList().stream()
                        .map(t -> ((SqlIdentifier) Objects.requireNonNull(t)).getSimple())
                        .collect(Collectors.toList());
                    break;
                }
            }
        }

        for (String key : keyList) {
            System.out.println("---> primary key:" + key);
        }
    }

    @Test
    public void createTablePartMulCols() {
        String sql = "create table TEST_AA(name varchar(32) not null,age int, primary key(id,age)) with(ttl=86400, a=a) "
            + "partition by range(a, b) (values (11, 11),(20,20),(30,30), (40,40)) ";
        sql.toUpperCase();
        SqlParser.Config config = SqlParser.config().withParserFactory(DingoSqlParserImpl::new);
        SqlParser parser = SqlParser.create(sql, config);
        try {
            SqlNode sqlNode = parser.parseStmt();

            DingoSqlCreateTable dingoSqlCreateTable = (DingoSqlCreateTable) sqlNode;
            parseCreateTable(dingoSqlCreateTable);
            System.out.println("---------------->" + dingoSqlCreateTable.toString());
            System.out.println("---------------->" + dingoSqlCreateTable.getAttrMap());
            DingoTablePart part = dingoSqlCreateTable.getDingoTablePart();
            System.out.println("---------------->cols:" + part.getCols());
            System.out.println("---------------->partType:" + dingoSqlCreateTable.getPartType());


        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @Test
    public void createTablePartSingleCols() {
        String sql = "create table TEST_AA(name varchar(32) not null,age int, primary key(id,age)) with(ttl=86400) "
            + "partition by range(id) (values (11),(20),(30), (40)) ";
        SqlParser.Config config = SqlParser.config().withParserFactory(DingoSqlParserImpl::new);
        SqlParser parser = SqlParser.create(sql, config);
        try {
            SqlNode sqlNode = parser.parseStmt();

            DingoSqlCreateTable dingoSqlCreateTable = (DingoSqlCreateTable) sqlNode;
            parseCreateTable(dingoSqlCreateTable);
            System.out.println("---------------->" + dingoSqlCreateTable.toString());
            System.out.println("---------------->" + dingoSqlCreateTable.getAttrMap());
            DingoTablePart part = dingoSqlCreateTable.getDingoTablePart();
            System.out.println("---------------->cols:" + part.getCols());
            System.out.println("---------------->partType:" + dingoSqlCreateTable.getPartType());


        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @Test
    public void createUser() {
        String sql = "CREATE USER 'gj' IDENTIFIED BY 'abc'";
        SqlParser.Config config = SqlParser.config().withParserFactory(DingoSqlParserImpl::new);
        SqlParser parser = SqlParser.create(sql, config);
        try {
            SqlNode sqlNode = parser.parseStmt();
            System.out.println("---> sqlNode:" + sqlNode);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @Test
    public void createUserWithLocation() {
        String sql = "CREATE USER 'gj'@'192.168.22.22' IDENTIFIED BY 'abc'";
        SqlParser.Config config = SqlParser.config().withParserFactory(DingoSqlParserImpl::new);
        SqlParser parser = SqlParser.create(sql, config);
        try {
            SqlNode sqlNode = parser.parseStmt();
            System.out.println("---> sqlNode:" + sqlNode);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @Test
    public void dropUser() {
        String sql = "drop USER gj";
        SqlParser.Config config = SqlParser.config().withParserFactory(DingoSqlParserImpl::new);
        SqlParser parser = SqlParser.create(sql, config);
        try {
            SqlNode sqlNode = parser.parseStmt();
            System.out.println("---> sqlNode:" + sqlNode);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @Test
    public void grant() {
        String sql = "grant select,update on dingo.* to 'gjn'";
        SqlParser.Config config = SqlParser.config().withParserFactory(DingoSqlParserImpl::new);
        SqlParser parser = SqlParser.create(sql, config);
        try {
            SqlNode sqlNode = parser.parseStmt();
            System.out.println("---> sqlNode:" + sqlNode);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @Test
    public void grant2() {
        String sql = "grant select,update on *.* to 'gjn'";
        SqlParser.Config config = SqlParser.config().withParserFactory(DingoSqlParserImpl::new);
        SqlParser parser = SqlParser.create(sql, config);
        try {
            SqlNode sqlNode = parser.parseStmt();
            System.out.println("---> sqlNode:" + sqlNode);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @Test
    public void revoke() {
        String sql = "revoke select,update on dingo.userinfo from 'gjn'";
        SqlParser.Config config = SqlParser.config().withParserFactory(DingoSqlParserImpl::new);
        SqlParser parser = SqlParser.create(sql, config);
        try {
            SqlNode sqlNode = parser.parseStmt();
            System.out.println("---> sqlNode:" + sqlNode);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

}
