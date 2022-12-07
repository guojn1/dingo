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

package io.dingodb.server.coordinator.meta.adaptor.impl;

import com.google.auto.service.AutoService;
import io.dingodb.common.CommonId;
import io.dingodb.common.privilege.PrivilegeDefinition;
import io.dingodb.common.privilege.PrivilegeType;
import io.dingodb.common.privilege.SchemaPrivDefinition;
import io.dingodb.common.privilege.TablePrivDefinition;
import io.dingodb.common.privilege.UserDefinition;
import io.dingodb.common.util.Optional;
import io.dingodb.server.coordinator.meta.adaptor.MetaAdaptorRegistry;
import io.dingodb.server.coordinator.store.MetaStore;
import io.dingodb.server.protocol.meta.Privilege;
import io.dingodb.server.protocol.meta.SchemaPriv;
import io.dingodb.server.protocol.meta.TablePriv;
import io.dingodb.server.protocol.meta.User;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static io.dingodb.server.protocol.CommonIdConstant.ID_TYPE;
import static io.dingodb.server.protocol.CommonIdConstant.PRIVILEGE_IDENTIFIER;

@Slf4j
public class PrivilegeAdaptor extends BaseAdaptor<Privilege> {

    public static final CommonId META_ID = CommonId.prefix(ID_TYPE.privilege, PRIVILEGE_IDENTIFIER.privilege);

    protected final Map<CommonId, List<Privilege>> privilegeMap;

    public PrivilegeAdaptor(MetaStore metaStore) {
        super(metaStore);
        MetaAdaptorRegistry.register(Privilege.class, this);
        privilegeMap = new ConcurrentHashMap<>();

        metaMap.forEach((k, v) -> privilegeMap.computeIfAbsent(v.getSubjectId(), p -> new ArrayList<>()).add(v));
    }

    @AutoService(BaseAdaptor.Creator.class)
    public static class Creator implements BaseAdaptor.Creator<Privilege, PrivilegeAdaptor> {
        @Override
        public PrivilegeAdaptor create(MetaStore metaStore) {
            return new PrivilegeAdaptor(metaStore);
        }
    }

    @Override
    protected CommonId newId(Privilege privilege) {
        return new CommonId(
            META_ID.type(),
            privilege.identifier(), privilege.getSubjectId().seq(),
            metaStore.generateSeq(CommonId.prefix(META_ID.type(), META_ID.identifier()).encode())
        );
    }


    @Override
    public CommonId metaId() {
        return META_ID;
    }

    @Override
    protected void doSave(Privilege privilege) {
        super.doSave(privilege);
        privilegeMap.computeIfAbsent(privilege.getSubjectId(), k -> new ArrayList<>()).add(privilege);
    }

    @Override
    public CommonId save(Privilege privilege) {
        CommonId id = super.save(privilege);
        privilegeMap.computeIfAbsent(privilege.getSubjectId(), k -> new ArrayList<>()).add(privilege);
        return id;
    }

    @Override
    protected void doDelete(Privilege meta) {
        super.doDelete(meta);
    }

    public boolean delete(PrivilegeDefinition definition, CommonId subjectId) {
        List<Privilege> privileges = this.privilegeMap.computeIfPresent(subjectId, (k, v) -> {
            Iterator iterator = v.iterator();
            while (iterator.hasNext()) {
                Privilege privilege = (Privilege) iterator.next();
                if (definition.getPrivilegeIndexs().contains(privilege.getPrivilegeIndex())) {
                    iterator.remove();
                    this.doDelete(privilege);
                }
            }
            if (v.size() == 0) {
                return null;
            }
            return v;
        });
        return privileges == null ? true : false;
    }

    public void create(PrivilegeDefinition definition, CommonId id) {
        delete(definition, id);
        definition.getPrivilegeIndexs().forEach(k -> {
            Privilege privilege = Privilege.builder()
                .host(definition.getHost())
                .user(definition.getUser())
                .privilegeIndex(k)
                .build();
            if (definition instanceof UserDefinition) {
                privilege.setPrivilegeType(PrivilegeType.USER);
            } else if (definition instanceof SchemaPrivDefinition) {
                privilege.setSchema(((SchemaPrivDefinition) definition).getSchema());
                privilege.setPrivilegeType(PrivilegeType.SCHEMA);
            } else if (definition instanceof TablePrivDefinition) {
                privilege.setSchema(((TablePrivDefinition) definition).getSchema());
                privilege.setTable(((TablePrivDefinition) definition).getTable());
                privilege.setPrivilegeType(PrivilegeType.TABLE);
            }
            privilege.setSubjectId(id);
            privilege.setId(newId(privilege));
            this.doSave(privilege);
        });
        log.info("privilege map:" + privilegeMap);
    }

    public List<UserDefinition> userDefinitions(List<User> users) {
        return users.stream().map(this :: metaToDefinition).collect(Collectors.toList());
    }

    public List<SchemaPrivDefinition> schemaPrivDefinitions(List<SchemaPriv> schemaPrivs) {
        return schemaPrivs.stream().map(this :: metaToDefinition).collect(Collectors.toList());
    }

    public List<TablePrivDefinition> tablePrivDefinitions(List<TablePriv> tablePrivs) {
        return tablePrivs.stream().map(this :: metaToDefinition).collect(Collectors.toList());
    }

    public UserDefinition metaToDefinition(User user) {
        UserDefinition userDefinition = UserDefinition.builder().user(user.getUser())
            .host(user.getHost())
            .build();
        Boolean[] privilegeIndexs = new Boolean[34];

        if (!"root".equalsIgnoreCase(user.getUser())) {
            for (int i = 0; i < privilegeIndexs.length; i ++) {
                privilegeIndexs[i] = false;
            }
            Optional.ofNullable(privilegeMap.get(user.getId())).ifPresent(privileges -> {
                privileges.forEach(privilege -> privilegeIndexs[privilege.getPrivilegeIndex()] = true);
            });
            userDefinition.setPrivileges(privilegeIndexs);
            return userDefinition;
        } else {
            for (int i = 0; i < privilegeIndexs.length; i ++) {
                privilegeIndexs[i] = true;
            }
            userDefinition.setPrivileges(privilegeIndexs);
            return userDefinition;
        }
    }

    public SchemaPrivDefinition metaToDefinition(SchemaPriv schemaPriv) {
        SchemaPrivDefinition schemaPrivDefinition = SchemaPrivDefinition.builder()
            .user(schemaPriv.getUser())
            .host(schemaPriv.getHost())
            .schema(schemaPriv.getSchema())
            .build();
        Boolean[] privilegeIndexs = new Boolean[34];
        for (int i = 0; i < privilegeIndexs.length; i ++) {
            privilegeIndexs[i] = false;
        }
        Optional.ofNullable(privilegeMap.get(schemaPriv.getId())).ifPresent(privileges -> {
            privileges.forEach(privilege -> privilegeIndexs[privilege.getPrivilegeIndex()] = true);
        });
        //privilegeMap.get(schemaPriv.getId())
        //    .forEach(privilege -> privilegeIndexs[privilege.getPrivilegeIndex()] = true);
        schemaPrivDefinition.setPrivileges(privilegeIndexs);
        return schemaPrivDefinition;
    }

    public TablePrivDefinition metaToDefinition(TablePriv tablePriv) {
        TablePrivDefinition tablePrivDefinition = TablePrivDefinition.builder()
            .user(tablePriv.getUser())
            .host(tablePriv.getHost())
            .schema(tablePriv.getSchema())
            .table(tablePriv.getTable())
            .build();
        Boolean[] privilegeIndexs = new Boolean[34];
        for (int i = 0; i < privilegeIndexs.length; i ++) {
            privilegeIndexs[i] = false;
        }
        Optional.ofNullable(privilegeMap.get(tablePriv.getId())).ifPresent(privileges -> {
            privileges.forEach(privilege -> privilegeIndexs[privilege.getPrivilegeIndex()] = true);
        });
        //privilegeMap.get(tablePriv.getId()).forEach(privilege -> privilegeIndexs[privilege.getPrivilegeIndex()] = true);
        tablePrivDefinition.setPrivileges(privilegeIndexs);
        return tablePrivDefinition;
    }
}
