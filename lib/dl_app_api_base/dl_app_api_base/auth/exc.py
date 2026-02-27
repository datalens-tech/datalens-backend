class AuthError(Exception):
    ...


class NoApplicableAuthCheckersError(AuthError):
    ...


class AuthFailureError(AuthError):
    ...


class UserAuthProviderFactoryError(AuthError):
    ...
