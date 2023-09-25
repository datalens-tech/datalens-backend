class I18nException(Exception):
    pass


class UnknownLocale(I18nException):
    pass


class UnknownDomain(I18nException):
    pass


class InvalidLocaleConfig(I18nException):
    pass
