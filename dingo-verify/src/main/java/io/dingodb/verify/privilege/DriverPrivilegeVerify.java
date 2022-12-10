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

package io.dingodb.verify.privilege;

import io.dingodb.common.CommonId;
import io.dingodb.common.auth.DingoRole;
import io.dingodb.common.domain.Domain;
import io.dingodb.common.privilege.PrivilegeDict;
import io.dingodb.common.privilege.PrivilegeGather;
import io.dingodb.common.privilege.SchemaPrivDefinition;
import io.dingodb.common.privilege.TablePrivDefinition;
import io.dingodb.common.privilege.UserDefinition;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class DriverPrivilegeVerify extends PrivilegeVerify {
    public DriverPrivilegeVerify() {
    }

    @Override
    public boolean verify(String user, String host, CommonId schema, CommonId table,
                             String accessType, PrivilegeGather privilegeGather) {
        if (DingoRole.SQLLINE == Domain.role) {
            return true;
        }
        if (StringUtils.isBlank(user)) {
            return true;
        }

        Integer index = PrivilegeDict.privilegeIndexDict.get(accessType);

        UserDefinition userDef = privilegeGather.getUserDef();
        if (userDef != null && userDef.getPrivileges()[index]) {
            return true;
        } else {
            if (schema == null) {
                return false;
            }
            List<SchemaPrivDefinition> schemaPrivDefs = privilegeGather.getSchemaPrivDefMap().entrySet().stream()
                .filter(entry -> match(entry, host, schema))
                .map(Map.Entry::getValue)
                .collect(Collectors.toList());
            SchemaPrivDefinition schemaPrivDef = null;
            if (schemaPrivDefs.size() > 0) {
                schemaPrivDef = schemaPrivDefs.get(0);
            }
            if (schemaPrivDef != null) {
                if (schemaPrivDef.getPrivileges()[index]) {
                    return true;
                } else {
                    log.info("schema verify failed, schema privilege:" + schemaPrivDef);
                    return false;
                }
            } else {
                List<TablePrivDefinition> tablePrivDefs = privilegeGather.getTablePrivDefMap().entrySet().stream()
                    .filter(entry -> match(entry, host, schema, table))
                    .map(Map.Entry::getValue)
                    .collect(Collectors.toList());
                TablePrivDefinition tablePrivDef = null;
                if (tablePrivDefs.size() > 0) {
                    tablePrivDef = tablePrivDefs.get(0);
                }
                if (tablePrivDef != null && tablePrivDef.getPrivileges()[index]) {
                    return true;
                } else {
                    log.info("table verify failed, table privilege:" + tablePrivDef);
                    return false;
                }
            }
        }
    }

    public boolean match(Map.Entry<CommonId, SchemaPrivDefinition> entry, String host, CommonId schema) {
        if (("%".equals(entry.getValue().getHost())
            || host.equals(entry.getValue().getHost())) && schema.equals(entry.getKey())) {
            return true;
        } else {
            return false;
        }
    }

    public boolean match(Map.Entry<CommonId, TablePrivDefinition> entry, String host,
                         CommonId schema, CommonId table) {
        if (("%".equals(entry.getValue().getHost())
            || host.equals(entry.getValue().getHost())) && schema.equals(entry.getValue().getSchema())
            && table.equals(entry.getKey())) {
            return true;
        } else {
            return false;
        }
    }

}
