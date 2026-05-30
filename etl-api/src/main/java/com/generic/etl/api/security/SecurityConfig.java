package com.generic.etl.api.security;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Configuration
@EnableMethodSecurity
public class SecurityConfig {

    /** Comma-separated key:role pairs, e.g. "sk-admin-123:ADMIN,sk-op-456:OPERATOR,sk-view-789:VIEWER" */
    @Value("${etl.api-keys:sk-admin-123:ADMIN,sk-op-456:OPERATOR,sk-view-789:VIEWER}")
    private List<String> apiKeyEntries;

    @Bean
    public Map<String, String> keyToRole() {
        return apiKeyEntries.stream()
                .map(entry -> entry.split(":", 2))
                .filter(parts -> parts.length == 2)
                .collect(Collectors.toMap(parts -> parts[0].trim(), parts -> parts[1].trim().toUpperCase()));
    }

    @Bean
    public SecurityFilterChain filterChain(HttpSecurity http, Map<String, String> keyToRole) throws Exception {
        http
            .csrf(AbstractHttpConfigurer::disable)
            .sessionManagement(s -> s.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
            .authorizeHttpRequests(auth -> auth
                // Swagger & Actuator — no auth needed
                .requestMatchers("/swagger-ui/**", "/v3/api-docs/**", "/actuator/health").permitAll()
                // Everything else requires authentication
                .anyRequest().authenticated()
            )
            .addFilterBefore(new ApiKeyAuthFilter(keyToRole), UsernamePasswordAuthenticationFilter.class);

        return http.build();
    }
}
