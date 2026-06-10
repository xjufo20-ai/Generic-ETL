package com.generic.etl.api.config;

import com.generic.etl.common.dto.ApiResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.Set;

@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Set<String> DEV_PROFILES = Set.of("dev", "local");

    @Value("${spring.profiles.active:dev}")
    private String activeProfile;

    @ExceptionHandler(IllegalArgumentException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public ApiResponse<?> handleBadRequest(IllegalArgumentException e) {
        log.warn("Bad request: {}", e.getMessage());
        return ApiResponse.error(e.getMessage());
    }

    @ExceptionHandler(AccessDeniedException.class)
    @ResponseStatus(HttpStatus.FORBIDDEN)
    public ApiResponse<?> handleForbidden(AccessDeniedException e) {
        log.warn("Access denied: {}", e.getMessage());
        return ApiResponse.error("Access denied");
    }

    @ExceptionHandler(Exception.class)
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public ApiResponse<?> handleInternal(Exception e) {
        log.error("Internal error", e);
        if (DEV_PROFILES.contains(activeProfile)) {
            return ApiResponse.error(e.getMessage());
        }
        return ApiResponse.error("Internal server error");
    }
}
