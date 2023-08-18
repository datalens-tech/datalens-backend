# coding: utf8
"""
...
"""


class UserError(Exception):
    """ Raise an error to the user in an HTTP response """

    def __init__(self, detail, status_code=400):
        assert isinstance(detail, dict)
        self.detail = detail
        self.status_code = status_code
        super().__init__(detail.get('message') or detail)


class NotFound(Exception):
    """ A requested entity was not found """


class MultipleObjectsReturned(Exception):
    """ Requested one object, found more """


class LimitReached(Exception):
    """ ... """
