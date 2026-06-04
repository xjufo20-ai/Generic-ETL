package com.generic.etl.api.security;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.web.util.matcher.AntPathRequestMatcher;
import org.springframework.security.web.util.matcher.OrRequestMatcher;
import org.springframework.security.web.util.matcher.RequestMatcher;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class ApiKeyAuthFilter extends OncePerRequestFilter {

    private static final String HEADER = "X-API-Key";
    private final Map<String, String> keyToRole;
    private final RequestMatcher publicPaths;

    public ApiKeyAuthFilter(Map<String, String> keyToRole, String[] publicPathPatterns) {
        this.keyToRole = keyToRole;
        this.publicPaths = new OrRequestMatcher(
            java.util.Arrays.stream(publicPathPatterns)
                .map(AntPathRequestMatcher::new)
                .toArray(AntPathRequestMatcher[]::new)
        );
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        return publicPaths.matches(request);
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request,
                                    HttpServletResponse response,
                                    FilterChain chain) throws ServletException, IOException {
        String apiKey = request.getHeader(HEADER);

        if (apiKey != null && keyToRole.containsKey(apiKey)) {
            String role = "ROLE_" + keyToRole.get(apiKey).toUpperCase();
            var auth = new UsernamePasswordAuthenticationToken(
                    apiKey, null,
                    List.of(new SimpleGrantedAuthority(role))
            );
            SecurityContextHolder.getContext().setAuthentication(auth);
        }

        chain.doFilter(request, response);
    }
}
