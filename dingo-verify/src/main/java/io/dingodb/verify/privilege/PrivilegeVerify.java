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

import io.dingodb.common.privilege.PrivilegeDefinition;
import io.dingodb.common.privilege.PrivilegeGather;
import io.dingodb.common.privilege.UserDefinition;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PrivilegeVerify {

    private static Map<PrivilegeType, PrivilegeVerify> privilegeVerifyMap = new HashMap<>();

    static {
        privilegeVerifyMap.put(PrivilegeType.SQL, new DriverPrivilegeVerify());
        privilegeVerifyMap.put(PrivilegeType.SDK, new SdkClientPrivilegeVerify());
        privilegeVerifyMap.put(PrivilegeType.API, new ApiPrivilegeVerify());
    }

    public static String getUserHost() {
        return "";
    }

    public static boolean isVerify = false;

    public boolean matchHost(PrivilegeDefinition privilegeDefinition, String host) {
        if ("%".equals(privilegeDefinition.getHost()) || host.equals(privilegeDefinition.getHost())) {
            return true;
        } else {
            return false;
        }
    }

    public PrivilegeVerify() {
        this(true);
    }

    public PrivilegeVerify(boolean isVerify) {
        this.isVerify = isVerify;
    }

    public UserDefinition matchUser(String host, PrivilegeGather privilegeGather) {
        return matchUser(host, privilegeGather.getUserDefMap());
    }

    public UserDefinition matchUser(String host, List<UserDefinition> userDefList) {
        List<UserDefinition> userDefs = userDefList.stream()
            .filter(userDefinition -> matchHost(userDefinition, host)).collect(Collectors.toList());

        UserDefinition userDef = null;
        if (userDefs.size() > 0) {
            userDef = userDefs.get(0);
        }
        return userDef;
    }

    public boolean verify(String user, String host, String schema, String table,
                          String accessType, PrivilegeGather privilegeGather) {
        return false;
    }

    /**
     * privilege verify.
     * @param verifyType driver/sdk/api
     * @return true/false
     */
    public boolean verify(PrivilegeType verifyType, String user, String host, String schema, String table,
                          String accessType, PrivilegeGather privilegeGather) {
        if (isVerify) {
            PrivilegeVerify privilegeVerify = privilegeVerifyMap.get(verifyType);
            return privilegeVerify.verify(user, host, schema, table, accessType, privilegeGather);
        } else {
            return true;
        }
    }
}
