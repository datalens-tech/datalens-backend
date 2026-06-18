class I18nError(Exception):
    pass


class UnknownLocaleError(I18nError):
    pass


class UnknownDomainError(I18nError):
    pass


class InvalidLocaleConfigError(I18nError):
    pass
