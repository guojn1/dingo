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
import io.dingodb.calcite.grammar.ddl.SqlCreateUser;
import io.dingodb.calcite.grammar.ddl.SqlDropUser;
import io.dingodb.calcite.grammar.ddl.SqlFlushPrivileges;
import io.dingodb.calcite.grammar.ddl.SqlGrant;
import io.dingodb.calcite.grammar.ddl.SqlRevoke;
import io.dingodb.calcite.grammar.ddl.SqlSetPassword;
import io.dingodb.calcite.grammar.ddl.SqlShowGrants;
import io.dingodb.calcite.grammar.ddl.SqlTruncate;
import io.dingodb.common.CommonId;
import io.dingodb.common.domain.Domain;
import io.dingodb.common.partition.PartitionDetailDefinition;
import io.dingodb.common.partition.PartitionDefinition;
import io.dingodb.common.privilege.PrivilegeDefinition;
import io.dingodb.common.privilege.PrivilegeDict;
import io.dingodb.common.privilege.PrivilegeGather;
import io.dingodb.common.privilege.PrivilegeList;
import io.dingodb.common.privilege.PrivilegeType;
import io.dingodb.common.privilege.SchemaPrivDefinition;
import io.dingodb.common.privilege.TablePrivDefinition;
import io.dingodb.common.privilege.UserDefinition;
import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.type.converter.StrParseConverter;
import io.dingodb.common.util.Optional;
import io.dingodb.common.util.Parameters;
import io.dingodb.meta.SysInfoService;
import io.dingodb.meta.SysInfoServiceProvider;
import io.dingodb.verify.plugin.AlgorithmPlugin;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.ContextSqlValidator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.server.DdlExecutorImpl;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.ddl.SqlDropTable;
import org.apache.calcite.sql.ddl.SqlKeyConstraint;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.calcite.util.Static.RESOURCE;

@Slf4j
public class DingoDdlExecutor extends DdlExecutorImpl {
    public static final DingoDdlExecutor INSTANCE = new DingoDdlExecutor();

    public SysInfoService sysInfoService;

    private DingoDdlExecutor() {
        this.sysInfoService = (SysInfoService) SysInfoServiceProvider.getRoot();
    }

    private static @Nullable ColumnDefinition fromSqlColumnDeclaration(
        @NonNull SqlColumnDeclaration scd,
        SqlValidator validator,
        List<String> pkSet
    ) {
        SqlDataTypeSpec typeSpec = scd.dataType;
        RelDataType dataType = typeSpec.deriveType(validator, true);
        SqlTypeName typeName = dataType.getSqlTypeName();
        int precision = typeName.allowsPrec() ? dataType.getPrecision() : RelDataType.PRECISION_NOT_SPECIFIED;
        if ((typeName == SqlTypeName.TIME || typeName == SqlTypeName.TIMESTAMP)) {
            if (precision > 3) {
                throw new RuntimeException("Precision " + precision + " is not support.");
            }
        }
        int scale = typeName.allowsScale() ? dataType.getScale() : RelDataType.SCALE_NOT_SPECIFIED;
        String defaultValue = null;
        ColumnStrategy strategy = scd.strategy;
        if (strategy == ColumnStrategy.DEFAULT) {
            SqlNode expr = scd.expression;
            if (expr != null) {
                defaultValue = expr.toSqlString(c -> c.withDialect(AnsiSqlDialect.DEFAULT)
                    .withAlwaysUseParentheses(false)
                    .withSelectListItemsOnSeparateLines(false)
                    .withUpdateSetListNewline(false)
                    .withIndentation(0)
                    .withQuoteAllIdentifiers(false)
                ).getSql();
            }
        }

        String name = scd.name.getSimple().toUpperCase();
        boolean isPrimary = (pkSet != null && pkSet.contains(name));
        RelDataType elementType = dataType.getComponentType();
        SqlTypeName elementTypeName = elementType != null ? elementType.getSqlTypeName() : null;
        return ColumnDefinition.builder()
            .name(name)
            .type(typeName.getName())
            .elementType(Optional.mapOrNull(elementTypeName, SqlTypeName::getName))
            .precision(precision)
            .scale(scale)
            .nullable(!isPrimary && dataType.isNullable())
            .primary(isPrimary)
            .defaultValue(defaultValue)
            .build();
    }

    private static @NonNull Pair<MutableSchema, String> getSchemaAndTableName(
        @NonNull SqlIdentifier id, CalcitePrepare.@NonNull Context context
    ) {
        CalciteSchema rootSchema = context.getMutableRootSchema();
        assert rootSchema != null : "No root schema.";
        final List<String> defaultSchemaPath = context.getDefaultSchemaPath();
        assert defaultSchemaPath.size() == 1 : "Assume that the schema path has only one level.";
        CalciteSchema defaultSchema = rootSchema.getSubSchema(defaultSchemaPath.get(0), false);
        if (defaultSchema == null) {
            defaultSchema = rootSchema;
        }
        List<String> names = new ArrayList<>(id.names);
        Schema schema;
        String tableName;
        if (names.size() == 1) {
            schema = defaultSchema.schema;
            tableName = names.get(0);
        } else {
            CalciteSchema subSchema = rootSchema.getSubSchema(names.get(0), false);
            if (subSchema != null) {
                schema = subSchema.schema;
                tableName = String.join(".", Util.skip(names));
            } else {
                schema = defaultSchema.schema;
                tableName = String.join(".", names);
            }
        }
        if (!(schema instanceof MutableSchema)) {
            throw new AssertionError("Schema must be mutable.");
        }
        return Pair.of((MutableSchema) schema, tableName.toUpperCase());
    }

    @SuppressWarnings({"unused"})
    public void execute(SqlCreateTable createT, CalcitePrepare.Context context) {
        DingoSqlCreateTable create = (DingoSqlCreateTable) createT;
        log.info("DDL execute: {}", create);
        final Pair<MutableSchema, String> schemaTableName
            = getSchemaAndTableName(create.name, context);
        SqlNodeList columnList = create.columnList;
        if (columnList == null) {
            throw SqlUtil.newContextException(create.name.getParserPosition(),
                RESOURCE.createTableRequiresColumnList());
        }

        // Get all primary key
        List<String> pks = create.columnList.stream()
            .filter(SqlKeyConstraint.class::isInstance)
            .map(SqlKeyConstraint.class::cast)
            .filter(constraint -> constraint.getOperator().getKind() == SqlKind.PRIMARY_KEY)
            // The 0th element is the name of the constraint
            .map(constraint -> (SqlNodeList) constraint.getOperandList().get(1))
            .findAny().map(sqlNodes -> sqlNodes.getList().stream()
                .filter(Objects::nonNull)
                .map(SqlIdentifier.class::cast)
                .map(SqlIdentifier::getSimple)
                .map(String::toUpperCase)
                .collect(Collectors.toCollection(ArrayList::new))
            ).filter(ks -> !ks.isEmpty())
            .orElseThrow(() -> new RuntimeException("Primary keys are required in table definition."));
        SqlValidator validator = new ContextSqlValidator(context, true);

        // Mapping, column node -> column definition
        List<ColumnDefinition> columns = create.columnList.stream()
            .filter(col -> col.getKind() == SqlKind.COLUMN_DECL)
            .map(col -> fromSqlColumnDeclaration((SqlColumnDeclaration) col, validator, pks))
            .collect(Collectors.toCollection(ArrayList::new));

        // Validate partition strategy
        Optional.ifPresent(create.getPartDefinition(), __ -> validatePartitionBy(pks, columns, __));

        final String tableName = Parameters.nonNull(schemaTableName.right, "table name");

        // Distinct column
        long distinctColCnt = columns.stream().map(ColumnDefinition::getName).distinct().count();
        long realColCnt = columns.size();
        if (distinctColCnt != realColCnt) {
            throw new RuntimeException("Duplicate column names are not allowed in table definition. Total: "
                + realColCnt + ", distinct: " + distinctColCnt);
        }

        final MutableSchema schema = Parameters.nonNull(schemaTableName.left, "table schema");

        // Check table exist
        if (schema.getTable(tableName) != null) {
            if (!create.ifNotExists) {
                // They did not specify IF NOT EXISTS, so give error.
                throw SqlUtil.newContextException(
                    create.name.getParserPosition(),
                    RESOURCE.tableExists(tableName)
                );
            } else {
                return;
            }
        }

        schema.createTable(tableName, new TableDefinition(
            tableName,
            columns,
            null,
            1,
            create.getTtl(),
            create.getPartDefinition(),
            create.getProperties()
        ));
    }

    @SuppressWarnings({"unused", "MethodMayBeStatic"})
    public void execute(SqlDropTable drop, CalcitePrepare.Context context) {
        log.info("DDL execute: {}", drop);
        final Pair<MutableSchema, String> schemaTableName
            = getSchemaAndTableName(drop.name, context);
        final MutableSchema schema = schemaTableName.left;
        final String tableName = schemaTableName.right;
        final boolean existed;
        assert schema != null;
        assert tableName != null;
        existed = schema.dropTable(tableName);
        if (!existed && !drop.ifExists) {
            throw SqlUtil.newContextException(
                drop.name.getParserPosition(),
                RESOURCE.tableNotFound(drop.name.toString())
            );
        }
        Domain.INSTANCE.tableIdMap.computeIfPresent(schema.metaService.id(), (k, v) -> {
            v.remove(tableName);
            return v;
        });
    }

    public void execute(@NonNull SqlTruncate truncate, CalcitePrepare.Context context) {
        SqlIdentifier name = (SqlIdentifier) truncate.getOperandList().get(0);
        final Pair<MutableSchema, String> schemaTableName
            = getSchemaAndTableName(name, context);
        final MutableSchema schema = Parameters.nonNull(schemaTableName.left, "table schema");
        final String tableName = Parameters.nonNull(schemaTableName.right, "table name");
        TableDefinition tableDefinition = schema.getMetaService().getTableDefinition(tableName.toUpperCase());
        if (tableDefinition == null) {
            throw SqlUtil.newContextException(
                name.getParserPosition(),
                RESOURCE.tableNotFound(name.toString()));
        }

        if (schema.dropTable(tableName)) {
            throw SqlUtil.newContextException(
                name.getParserPosition(),
                RESOURCE.tableNotFound(name.toString())
            );
        }
        schema.createTable(tableName, tableDefinition);

    }


    public void execute(@NonNull SqlGrant sqlGrant, CalcitePrepare.Context context) {
        log.info("DDL execute: {}", sqlGrant);
        if (!"*".equals(sqlGrant.table)) {
            SqlIdentifier name = sqlGrant.tableIdentifier;
            final Pair<MutableSchema, String> schemaTableName
                = getSchemaAndTableName(name, context);
            final String tableName = Parameters.nonNull(schemaTableName.right, "table name");
            final MutableSchema schema = Parameters.nonNull(schemaTableName.left, "table schema");
            if (schema.getTable(tableName) == null) {
                throw new RuntimeException("table doesn't exist");
            }
        }
        if (sysInfoService.existsUser(UserDefinition.builder().user(sqlGrant.user).host(sqlGrant.host).build())) {
            PrivilegeDefinition privilegeDefinition = getPrivilegeDefinition(sqlGrant);
            sysInfoService.grant(privilegeDefinition);
        } else {
            throw new RuntimeException("You are not allowed to create a user with GRANT");
        }
    }

    public void execute(@NonNull SqlRevoke sqlRevoke, CalcitePrepare.Context context) {
        log.info("DDL execute: {}", sqlRevoke);
        if (!"*".equals(sqlRevoke.table)) {
            SqlIdentifier name = sqlRevoke.tableIdentifier;
            final Pair<MutableSchema, String> schemaTableName
                = getSchemaAndTableName(name, context);
            final String tableName = Parameters.nonNull(schemaTableName.right, "table name");
            final MutableSchema schema = Parameters.nonNull(schemaTableName.left, "table schema");
            if (schema.getTable(tableName) == null) {
                throw new RuntimeException("table doesn't exist");
            }
        }
        if (sysInfoService.existsUser(UserDefinition.builder().user(sqlRevoke.user).host(sqlRevoke.host).build())) {
            PrivilegeDefinition privilegeDefinition = getPrivilegeDefinition(sqlRevoke);
            sysInfoService.revoke(privilegeDefinition);
        } else {
            throw new RuntimeException("You are not allowed to create a user with GRANT");
        }
    }

    public void execute(@NonNull SqlCreateUser sqlCreateUser, CalcitePrepare.Context context) {
        log.info("DDL execute: {}", sqlCreateUser);
        UserDefinition userDefinition = UserDefinition.builder().user(sqlCreateUser.user)
            .host(sqlCreateUser.host).build();
        if (sysInfoService.existsUser(userDefinition)) {
            throw new RuntimeException("user is exists");
        } else {
            userDefinition.setPlugin("mysql_native_password");
            String digestPwd = AlgorithmPlugin.digestAlgorithm(sqlCreateUser.password, userDefinition.getPlugin());
            userDefinition.setPassword(digestPwd);
            sysInfoService.createUser(userDefinition);
        }
    }

    public void execute(@NonNull SqlDropUser sqlDropUser, CalcitePrepare.Context context) {
        log.info("DDL execute: {}", sqlDropUser);
        UserDefinition userDefinition = UserDefinition.builder().user(sqlDropUser.name).host(sqlDropUser.host).build();
        sysInfoService.dropUser(userDefinition);
    }

    public void execute(@NonNull SqlFlushPrivileges dingoFlushPrivileges, CalcitePrepare.Context context) {
        sysInfoService.flushPrivileges();
    }

    public void execute(@NonNull SqlSetPassword sqlSetPassword, CalcitePrepare.Context context) {
        UserDefinition userDefinition = UserDefinition.builder()
            .user(sqlSetPassword.user)
            .host(sqlSetPassword.host)
            .build();
        if (sysInfoService.existsUser(userDefinition)) {
            userDefinition.setPassword(sqlSetPassword.password);
            sysInfoService.setPassword(userDefinition);
        } else {
            throw new RuntimeException("user is not exist");
        }
    }

    public void execute(@NonNull SqlShowGrants dingoSqlShowGrants, CalcitePrepare.Context context) {

    }

    public SqlGrant getUserGrant(@NonNull SqlShowGrants dingoSqlShowGrants, List<UserDefinition> userDefinitions) {
        List<Boolean> userPrivileges = Arrays.asList(userDefinitions.get(0).getPrivileges());
        long count = userPrivileges.stream()
            .filter(isPrivilege -> isPrivilege).count();

        if (count > 0) {
            boolean isAllPrivilege = false;
            List<String> privileges = null;
            if (count == PrivilegeList.privilegeMap.get(PrivilegeType.USER).size()) {
                isAllPrivilege = true;
            } else {
                List<Integer> indexs =  new ArrayList<>();
                Stream.iterate(0, i -> i + 1).limit(userPrivileges.size()).forEach(i -> {
                    if (userPrivileges.get(i)) {
                        indexs.add(i);
                    }
                });
                privileges = PrivilegeDict.getPrivilege(indexs);
            }
            SqlIdentifier subject = new SqlIdentifier(Arrays.asList("*", "*"), null, null,
                new ArrayList<SqlParserPos>());
            SqlGrant sqlGrant = new SqlGrant(null, isAllPrivilege, privileges, subject,
                dingoSqlShowGrants.user, dingoSqlShowGrants.host);
            log.info("user sqlGrant:" + sqlGrant.toString());
            return sqlGrant;
        }
        return null;
    }

    public void validatePartitionBy(
        @NonNull List<String> keyList,
        @NonNull List<ColumnDefinition> cols,
        @NonNull PartitionDefinition partDefinition
    ) {
        StrParseConverter converter = StrParseConverter.INSTANCE;
        cols = cols.stream().filter(col -> keyList.contains(col.getName())).collect(Collectors.toList());
        String strategy = partDefinition.getFuncName().toUpperCase();
        switch (strategy) {
            case "RANGE":
                if (partDefinition.getCols() == null || partDefinition.getCols().isEmpty()) {
                    partDefinition.setCols(keyList);
                } else {
                    partDefinition.setCols(
                        partDefinition.getCols().stream().map(String::toUpperCase).collect(Collectors.toList())
                    );
                }
                if (!keyList.equals(partDefinition.getCols())) {
                    throw new IllegalArgumentException(
                        "Partition columns must be equals primary key columns, but " + partDefinition.getCols()
                    );
                }

                for (PartitionDetailDefinition rangePart : partDefinition.getDetails()) {
                    List<Object> operand = rangePart.getOperand();
                    for (int i = 0; i < operand.size(); i++) {
                        operand.set(i, cols.get(i).getType().convertFrom(operand.get(i).toString(), converter));
                    }
                }
                break;
            default:
                throw new IllegalStateException("Unsupported " + strategy);
        }
    }

    @NonNull
    private PrivilegeDefinition getPrivilegeDefinition(@NonNull SqlGrant sqlGrant) {
        String table = sqlGrant.table;
        String schema = sqlGrant.schema;
        CommonId schemaId = null;
        PrivilegeDefinition privilegeDefinition = null;
        PrivilegeType privilegeType = null;
        if ("*".equals(table) && "*".equals(schema)) {
            privilegeDefinition = UserDefinition.builder()
                .build();
            privilegeType = PrivilegeType.USER;
        } else if ("*".equals(table)) {
            schemaId = sysInfoService.getSchemaIdByCache(schema);
            privilegeDefinition = SchemaPrivDefinition.builder()
                .schema(schemaId)
                .build();
            privilegeType = PrivilegeType.SCHEMA;
        } else {
            schemaId = sysInfoService.getSchemaIdByCache(schema);
            CommonId tableId = sysInfoService.getTableIdByCache(schemaId, table);
            privilegeDefinition = TablePrivDefinition.builder()
                .schema(schemaId)
                .table(tableId)
                .build();
            privilegeType = PrivilegeType.TABLE;
        }
        privilegeDefinition.setPrivilegeIndexs(sqlGrant.getPrivileges(privilegeType));
        privilegeDefinition.setUser(sqlGrant.user);
        privilegeDefinition.setHost(sqlGrant.host);
        return privilegeDefinition;
    }
}
