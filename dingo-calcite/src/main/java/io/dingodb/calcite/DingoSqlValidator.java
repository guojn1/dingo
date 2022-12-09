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

import io.dingodb.calcite.fun.DingoOperatorTable;
import io.dingodb.calcite.grammar.ddl.DingoSqlCreateTable;
import io.dingodb.calcite.grammar.ddl.SqlCreateUser;
import io.dingodb.calcite.grammar.ddl.SqlDropUser;
import io.dingodb.calcite.grammar.ddl.SqlFlushPrivileges;
import io.dingodb.calcite.grammar.ddl.SqlGrant;
import io.dingodb.calcite.grammar.ddl.SqlRevoke;
import io.dingodb.calcite.grammar.ddl.SqlSetPassword;
import io.dingodb.common.domain.Domain;
import io.dingodb.net.error.ApiTerminateException;
import io.dingodb.verify.privilege.PrivilegeType;
import io.dingodb.verify.privilege.PrivilegeVerify;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.checkerframework.checker.nullness.qual.Nullable;

public class DingoSqlValidator extends SqlValidatorImpl {
    private String user;
    private String host;

    public static Config CONFIG = Config.DEFAULT
        .withConformance(DingoParser.PARSER_CONFIG.conformance());

    public DingoSqlValidator(
        CalciteCatalogReader catalogReader,
        RelDataTypeFactory typeFactory,
        String user,
        String host
    ) {
        super(
            SqlOperatorTables.chain(
                SqlStdOperatorTable.instance(),
                DingoOperatorTable.instance(),
                catalogReader
            ),
            catalogReader,
            typeFactory,
            DingoSqlValidator.CONFIG
        );
        this.user = user;
        this.host = host;
    }

    @Override
    public void validateCall(SqlCall call, SqlValidatorScope scope) {
        super.validateCall(call, scope);

    }

    @Override
    public void validateQuery(SqlNode node, @Nullable SqlValidatorScope scope, RelDataType targetRowType) {
        super.validateQuery(node, scope, targetRowType);
        if (node instanceof SqlIdentifier) {
            verify((SqlIdentifier) node, "select");
        }
    }

    @Override
    public void validateUpdate(SqlUpdate call) {
        super.validateUpdate(call);
        SqlNode node = call.getTargetTable();
        if (node instanceof SqlIdentifier) {
            verify((SqlIdentifier) node, "update");
        }
    }

    @Override
    public void validateDelete(SqlDelete call) {
        super.validateDelete(call);
        SqlNode node = call.getTargetTable();
        if (node instanceof SqlIdentifier) {
            verify((SqlIdentifier) node, "delete");
        }
    }

    @Override
    public void validateInsert(SqlInsert insert) {
        super.validateInsert(insert);
        SqlNode node = insert.getTargetTable();
        if (node instanceof SqlIdentifier) {
            verify((SqlIdentifier) node, "insert");
        }
    }


    public void verify(SqlIdentifier node, String sqlAccessType) {
        String schema = "DINGO";
        String table;
        if (node.names.size() == 1) {
            table = node.names.get(0);
        } else {
            schema = node.names.get(0);
            table = node.names.get(1);
        }
        if (!PrivilegeVerify.verify(PrivilegeType.SQL, user,
            host, schema, table, sqlAccessType, Domain.INSTANCE.privilegeGatherMap.get(user))) {
            throw new RuntimeException(String.format("Access denied for user '%s'@'%s' to %s", user, host, schema));
        }
    }
}
