package com.generic.etl.api.security;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
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

    @Value("${etl.api-keys}")
    private List<String> apiKeyEntries;

    private static final String[] PUBLIC_PATHS = {
        "/swagger-ui/**", "/swagger-ui.html", "/v3/api-docs/**", "/swagger-resources/**", "/webjars/**",
        "/hawtio/**", "/jolokia/**",
        "/actuator/**",
        "/dashboard/**", "/", "/css/**", "/js/**", "/img/**", "/favicon.ico",
        "/error"
    };

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
                .requestMatchers(PUBLIC_PATHS).permitAll()
                .anyRequest().authenticated()
            )
            .addFilterBefore(new ApiKeyAuthFilter(keyToRole, PUBLIC_PATHS),
                UsernamePasswordAuthenticationFilter.class);

        return http.build();
    }
}
