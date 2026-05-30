package com.generic.etl.api.security;

public final class Roles {
    public static final String ADMIN = "ADMIN";
    public static final String OPERATOR = "OPERATOR";
    public static final String VIEWER = "VIEWER";

    // Convenience SpEL expressions
    public static final String IS_ADMIN = "hasRole('ADMIN')";
    public static final String IS_ADMIN_OR_OPERATOR = "hasAnyRole('ADMIN','OPERATOR')";
    public static final String IS_AUTHENTICATED = "hasAnyRole('ADMIN','OPERATOR','VIEWER')";

    private Roles() {}
}
