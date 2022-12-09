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

package io.dingodb.driver.auth.service;

import com.google.auto.service.AutoService;
import io.dingodb.common.auth.Authentication;
import io.dingodb.common.auth.Certificate;
import io.dingodb.common.domain.Domain;

import io.dingodb.common.privilege.PrivilegeGather;
import io.dingodb.common.privilege.UserDefinition;
import io.dingodb.meta.SysInfoService;
import io.dingodb.meta.SysInfoServiceProvider;
import io.dingodb.net.service.AuthService;
import io.dingodb.verify.plugin.AlgorithmPlugin;
import io.dingodb.verify.privilege.PrivilegeType;
import io.dingodb.verify.privilege.PrivilegeVerify;
import io.dingodb.verify.token.TokenManager;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class JDBCAuthService implements AuthService<Authentication> {

    PrivilegeVerify privilegeVerify = PrivilegeVerify.getPrivilegeVerify(PrivilegeType.SQL);

    private SysInfoService sysInfoService;

    private static final AuthService INSTANCE = new JDBCAuthService();

    @AutoService(AuthService.Provider.class)
    public static class JDBCAuthServiceProvider implements AuthService.Provider {

        @Override
        public <C> AuthService<C> get() {
            return INSTANCE;
        }
    }

    @Override
    public String tag() {
        return "identity";
    }

    @Override
    public Authentication createAuthentication() {
        Domain domain = Domain.INSTANCE;
        String user = (String) domain.getInfo("user");
        String host = (String) domain.getInfo("host");
        String password = (String) domain.getInfo("password");
        if (user == null && host == null) {
            return null;
        } else {
            Authentication authentication = Authentication.builder()
                .username(user)
                .host(host)
                .password(password).role(domain.role).build();
            return authentication;
        }
    }

    @Override
    public Object auth(Authentication authentication) throws Exception {
        if (authentication == null) {
            return Certificate.builder().code(200).build();
        }
        if (sysInfoService == null) {
            sysInfoService = SysInfoServiceProvider.getRoot();
        }
        String user = authentication.getUsername();
        String host = authentication.getHost();
        String clientPassword = authentication.getPassword();

        List<UserDefinition> userDefinitionList = sysInfoService.getUserDefinition(user);
        UserDefinition userDef = privilegeVerify.matchUser(host, userDefinitionList);

        if (userDef == null) {
            throw new Exception(String.format("Access denied for user '%s'@'%s'", user, host));
        }
        String plugin = userDef.getPlugin();
        String password = userDef.getPassword();
        String digestPwd = AlgorithmPlugin.digestAlgorithm(clientPassword, plugin);
        Certificate certificate = Certificate.builder().code(100).build();
        log.info("digestPwd:" + digestPwd + ", login pwd:" + password);
        if (digestPwd.equals(password)) {
            log.info("cache privileges:" + Domain.INSTANCE.privilegeGatherMap);
            PrivilegeGather privilegeGather = Domain.INSTANCE.privilegeGatherMap.computeIfAbsent(user,
                k -> sysInfoService.getPrivilegeDef(null, user));

            Map<String, Object> clientInfo = new HashMap<>();
            clientInfo.put("username", user);
            clientInfo.put("host", host);
            TokenManager tokenManager = TokenManager.getInstance("0123456789");
            String token = tokenManager.createToken(clientInfo);
            clientInfo.put("token", token);

            certificate.setToken(token);
            certificate.setPrivilegeGather(privilegeGather);
            certificate.setInfo(clientInfo);
        } else {
            throw new Exception(String.format("Access denied for user '%s'@'%s'", user, host));
        }
        return certificate;
    }
}
