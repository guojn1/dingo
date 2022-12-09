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
import io.dingodb.common.privilege.PrivilegeDict;
import io.dingodb.common.privilege.PrivilegeGather;
import io.dingodb.common.privilege.SchemaPrivDefinition;
import io.dingodb.common.privilege.TablePrivDefinition;
import io.dingodb.common.privilege.UserDefinition;

public class ApiPrivilegeVerify extends PrivilegeVerify {
    @Override
    public boolean verify(String user, String host, String schema, String table, String accessType,
                          PrivilegeGather privilegeGather) {
        return false;
    }

    @Override
    public boolean apiVerify(String user, String host, CommonId schema, CommonId table,
                             String accessType, PrivilegeGather privilegeGather) {
        // Get index
        Integer index = PrivilegeDict.privilegeIndexDict.get(accessType);

        // User verify
        UserDefinition userDef = privilegeGather.getUserDef();
        if (userDef.getPrivileges()[index]) {
            return true;
        }

        // Schema verify
        SchemaPrivDefinition schemaDef = privilegeGather.getSchemaPrivDefMap().get(schema);
        if (schemaDef.getPrivileges()[index]) {
            return true;
        }

        // Table verify
        TablePrivDefinition tableDef = privilegeGather.getTablePrivDefMap().get(table);
        if (tableDef.getPrivileges()[index]) {
            return true;
        }

        return false;
    }
}
