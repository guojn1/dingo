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

import io.dingodb.common.privilege.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DriverPrivilegeVerify extends PrivilegeVerify {
    public DriverPrivilegeVerify() {
    }

    @Override
    public boolean verify(Object... param) {
        String host = (String) param[1];
        String schema = (String) param[2];
        String table = (String) param[3];
        String privilege = (String) param[4];
        PrivilegeGather privilegeGather = (PrivilegeGather) param[5];
        Integer index = PrivilegeDict.privilegeIndexDict.get(privilege);

        List<UserDefinition> userDefs = privilegeGather.getUserDefMap().stream()
            .filter(userDefinition -> matchHost(userDefinition, host)).collect(Collectors.toList());

        UserDefinition userDef = null;
        if (userDefs.size() > 0) {
            userDef = userDefs.get(0);
        }

        if (userDef != null && userDef.getPrivileges()[index]) {
            return true;
        } else {
            List<SchemaPrivDefinition> schemaPrivDefs = privilegeGather.getSchemaPrivDefMap().stream()
                .filter(schemaPrivDefinition -> match(schemaPrivDefinition, host, schema))
                .collect(Collectors.toList());
            SchemaPrivDefinition schemaPrivDef = null;
            if (schemaPrivDefs.size() > 0) {
                schemaPrivDef = schemaPrivDefs.get(0);
            }
            if (schemaPrivDef != null) {
                if (schemaPrivDef.getPrivileges()[index]) {
                    return true;
                } else {
                    return false;
                }
            } else {
                List<TablePrivDefinition> tablePrivDefs = privilegeGather.getTablePrivDefMap().stream()
                    .filter(tablePrivDefinition -> match(tablePrivDefinition, host, schema, table))
                    .collect(Collectors.toList());
                TablePrivDefinition tablePrivDef = null;
                if (tablePrivDefs.size() > 0) {
                    tablePrivDef = tablePrivDefs.get(0);
                }
                if (tablePrivDef != null && tablePrivDef.getPrivileges()[index]) {
                    return true;
                } else {
                    return false;
                }
            }
        }
    }

    public boolean match(SchemaPrivDefinition schemaPrivDefinition, String host, String schema) {
        if (("%".equals(schemaPrivDefinition.getHost())
            || host.equals(schemaPrivDefinition.getHost())) && schema.equals(schemaPrivDefinition.getSchema())) {
            return true;
        } else {
            return false;
        }
    }

    public boolean match(TablePrivDefinition tablePrivDefinition, String host, String schema, String table) {
        if (("%".equals(tablePrivDefinition.getHost())
            || host.equals(tablePrivDefinition.getHost())) && schema.equals(tablePrivDefinition.getSchema())
            && table.equals(tablePrivDefinition.getTable())) {
            return true;
        } else {
            return false;
        }
    }

}
