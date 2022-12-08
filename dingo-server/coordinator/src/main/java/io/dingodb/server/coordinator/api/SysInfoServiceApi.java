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

package io.dingodb.server.coordinator.api;

import io.dingodb.common.CommonId;
import io.dingodb.common.privilege.PrivilegeDefinition;
import io.dingodb.common.privilege.PrivilegeGather;
import io.dingodb.common.privilege.PrivilegeType;
import io.dingodb.common.privilege.SchemaPrivDefinition;
import io.dingodb.common.privilege.TablePrivDefinition;
import io.dingodb.common.privilege.UserDefinition;
import io.dingodb.net.Channel;
import io.dingodb.net.api.ApiRegistry;
import io.dingodb.server.coordinator.meta.adaptor.impl.PrivilegeAdaptor;
import io.dingodb.server.coordinator.meta.adaptor.impl.PrivilegeDictAdaptor;
import io.dingodb.server.coordinator.meta.adaptor.impl.SchemaPrivAdaptor;
import io.dingodb.server.coordinator.meta.adaptor.impl.TablePrivAdaptor;
import io.dingodb.server.coordinator.meta.adaptor.impl.UserAdaptor;
import io.dingodb.server.protocol.meta.Privilege;
import io.dingodb.server.protocol.meta.PrivilegeDict;
import io.dingodb.server.protocol.meta.SchemaPriv;
import io.dingodb.server.protocol.meta.TablePriv;
import io.dingodb.server.protocol.meta.User;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.dingodb.server.coordinator.meta.adaptor.MetaAdaptorRegistry.getMetaAdaptor;

@Slf4j
public class SysInfoServiceApi implements io.dingodb.server.api.SysInfoServiceApi {
    public SysInfoServiceApi() {
        ApiRegistry.getDefault().register(io.dingodb.server.api.SysInfoServiceApi.class, this);
    }

    public boolean existsUser(UserDefinition userDefinition) {
        return ((UserAdaptor) getMetaAdaptor(User.class)).isExist(userDefinition);
    }

    @Override
    public void createUser(UserDefinition userDefinition) {
        ((UserAdaptor) getMetaAdaptor(User.class)).create(userDefinition);
    }

    @Override
    public void dropUser(UserDefinition userDefinition) {
        ((UserAdaptor) getMetaAdaptor(User.class)).delete(userDefinition);
    }

    @Override
    public void setPassword(UserDefinition userDefinition) {
        ((UserAdaptor) getMetaAdaptor(User.class)).setPassword(userDefinition);
    }

    @Override
    public void grant(PrivilegeDefinition privilegeDefinition) {
        CommonId commonId = null;
        if (privilegeDefinition instanceof UserDefinition) {
            commonId = ((UserAdaptor)getMetaAdaptor(User.class))
                .create((UserDefinition) privilegeDefinition);
        } else if (privilegeDefinition instanceof SchemaPrivDefinition) {
            commonId = ((SchemaPrivAdaptor)getMetaAdaptor(SchemaPriv.class))
                .create((SchemaPrivDefinition) privilegeDefinition);
        } else if (privilegeDefinition instanceof TablePrivDefinition) {
            commonId = ((TablePrivAdaptor)getMetaAdaptor(TablePriv.class))
                .create((TablePrivDefinition) privilegeDefinition);
        }
        ((PrivilegeAdaptor)getMetaAdaptor(Privilege.class)).create(privilegeDefinition, commonId);
    }

    @Override
    public void revoke(PrivilegeDefinition privilegeDefinition) {
        CommonId subjectId = null;
        if (privilegeDefinition instanceof UserDefinition) {
            subjectId = ((UserAdaptor)getMetaAdaptor(User.class))
                .create((UserDefinition) privilegeDefinition);
        } else if (privilegeDefinition instanceof SchemaPrivDefinition) {
            subjectId = ((SchemaPrivAdaptor)getMetaAdaptor(SchemaPriv.class))
                .create((SchemaPrivDefinition) privilegeDefinition);
        } else if (privilegeDefinition instanceof TablePrivDefinition) {
            subjectId = ((TablePrivAdaptor)getMetaAdaptor(TablePriv.class))
                .create((TablePrivDefinition) privilegeDefinition);
        }
        boolean deleteAll = ((PrivilegeAdaptor)getMetaAdaptor(Privilege.class)).delete(privilegeDefinition, subjectId);
        if (deleteAll) {
            if (privilegeDefinition instanceof UserDefinition) {
                ((UserAdaptor)getMetaAdaptor(User.class))
                    .delete((UserDefinition) privilegeDefinition);
            } else if (privilegeDefinition instanceof SchemaPrivDefinition) {
                ((SchemaPrivAdaptor)getMetaAdaptor(SchemaPriv.class))
                    .delete((SchemaPrivDefinition) privilegeDefinition);
            } else if (privilegeDefinition instanceof TablePrivDefinition) {
                ((TablePrivAdaptor)getMetaAdaptor(TablePriv.class))
                    .delete((TablePrivDefinition) privilegeDefinition);
            }
        }
    }

    @Override
    public PrivilegeGather getPrivilegeDef(Channel channel, String user) {
        PrivilegeAdaptor privilegeAdaptor = getMetaAdaptor(Privilege.class);
        if (channel != null) {
            // driver or executor: verify identity req coordinator for flush privileges
            if (!privilegeAdaptor.channels.contains(channel)) {
                privilegeAdaptor.channels.add(channel);
            }
        }
        PrivilegeGather privilegeDefinition = privilegeAdaptor.getPrivilegeGather(user);
        return privilegeDefinition;
    }

    @Override
    public List<UserDefinition> getUserDefinition(String user) {
        return ((UserAdaptor) getMetaAdaptor(User.class)).getUserDefinition(user);
    }

    public void saveRootPrivilege(String userName) {
        List<User> userList = ((UserAdaptor) getMetaAdaptor(User.class)).getUser(userName);
        if (userList.size() == 1) {
            PrivilegeAdaptor privilegeAdaptor = (PrivilegeAdaptor) getMetaAdaptor(Privilege.class);
            if (privilegeAdaptor.getAll().isEmpty()) {
                // Get root user
                User user = userList.get(0);
                // Get dict
                Map<String, CommonId> privilegeDict =
                    ((PrivilegeDictAdaptor) getMetaAdaptor(PrivilegeDict.class)).getPrivilegeDict();
                // Save to table privilege
                for (Map.Entry<String, CommonId> entry : privilegeDict.entrySet()) {
                    Privilege privilege = Privilege.builder()
                        .id(user.getId())
                        .privilegeType(PrivilegeType.USER)
                        .user(userName)
                        .subjectId(((PrivilegeDictAdaptor) getMetaAdaptor(PrivilegeDict.class)).privilegeDictId)
                        .host(user.getHost())
                        .privilegeIndex(entry.getValue().seq())
                        .build();
                    privilegeAdaptor.save(privilege);
                }
            }
        }
    }

    public List<String> getAllPrivilegeDict() {
        Map<String, CommonId> privilegeDict =
            ((PrivilegeDictAdaptor) getMetaAdaptor(PrivilegeDict.class)).getPrivilegeDict();
        return privilegeDict.entrySet().stream().map(this::mappingPrivilegeIndex).collect(Collectors.toList());
    }

    public String mappingPrivilegeIndex(Map.Entry<String, CommonId> entry) {
        return new StringBuilder(entry.getKey()).append("#").append(entry.getValue().seq()).toString();
    }

}
