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

package io.dingodb.server.executor.auth.service;

import com.google.auto.service.AutoService;
import io.dingodb.common.auth.Authentication;
import io.dingodb.common.auth.Certificate;
import io.dingodb.common.auth.DingoRole;
import io.dingodb.common.domain.Domain;
import io.dingodb.common.privilege.PrivilegeGather;
import io.dingodb.meta.SysInfoService;
import io.dingodb.meta.SysInfoServiceProvider;
import io.dingodb.net.NetService;
import io.dingodb.net.NetServiceProvider;
import io.dingodb.net.service.AuthService;
import io.dingodb.verify.token.TokenManager;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

@Slf4j
public class TokenAuthService implements AuthService<Authentication> {

    private static final AuthService INSTANCE = new TokenAuthService();

    private final NetService netService = ServiceLoader.load(NetServiceProvider.class).iterator().next().get();

    @AutoService(AuthService.Provider.class)
    public static class TokenAuthServiceProvider implements AuthService.Provider {

        @Override
        public <C> AuthService<C> get() {
            return INSTANCE;
        }
    }

    private SysInfoService sysInfoService;

    public TokenAuthService() {
    }

    @Override
    public String tag() {
        return "token";
    }

    @Override
    public Authentication createAuthentication() {
        TokenManager tokenManager = TokenManager.getInstance("0123456789");
        Map<String, Object> map = new HashMap<>();
        map.put("inner", "dingo");
        String token =  tokenManager.createToken(map);
        return Authentication.builder().role(Domain.role).token(token).build();
    }

    @Override
    public Object auth(Authentication authentication) throws Exception {
        try {
            if (authentication == null) {
                return Certificate.builder().code(200).build();
            }
            if (sysInfoService == null) {
                sysInfoService = SysInfoServiceProvider.getRoot();
            }
            String token = authentication.getToken();
            Map<String, Object> clientInfo = verifyToken(token);
            if (clientInfo == null) {
                throw new Exception("auth token error");
            }
            Certificate certificate = Certificate.builder().code(100).build();
            DingoRole clientRole = authentication.getRole();
            DingoRole role = Domain.role;
            if (clientRole == DingoRole.SDK_CLIENT && role == DingoRole.EXECUTOR) {
                String host = (String) clientInfo.get("host");
                String user = (String) clientInfo.get("user");
                if (host == null || host == null) {
                    log.error("sdk to executor token auth fail");
                }

                Domain.INSTANCE.privilegeGatherMap
                    .computeIfAbsent(user + "#" + host,
                    k -> sysInfoService.getPrivilegeDef(null, user, host));
                log.info("sdk to executor token auth success: user:" + user + ", host:" + host + ", privileges:"
                    + Domain.INSTANCE.privilegeGatherMap);
                //certificate.setPrivilegeGather(privilegeGather);
            }

            return certificate;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw e;
        }
    }

    private Map<String, Object> verifyToken(String token) {
        TokenManager tokenManager = TokenManager.getInstance("0123456789");
        Map<String, Object> claims = tokenManager.certificateToken(token);
        return claims;
    }
}
