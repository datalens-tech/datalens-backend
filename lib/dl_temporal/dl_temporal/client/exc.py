import temporalio.service


class TemporalClientError(Exception):
    pass


class PermissionDeniedError(TemporalClientError):
    pass


class AlreadyExistsError(TemporalClientError):
    pass


class NotFoundError(TemporalClientError):
    pass


def wrap_temporal_error(exc: Exception) -> TemporalClientError:
    if isinstance(exc, temporalio.service.RPCError):
        if exc.status == temporalio.service.RPCStatusCode.PERMISSION_DENIED:
            raise PermissionDeniedError(exc.message) from exc
        if exc.status == temporalio.service.RPCStatusCode.ALREADY_EXISTS:
            raise AlreadyExistsError(exc.message) from exc
        if exc.status == temporalio.service.RPCStatusCode.NOT_FOUND:
            raise NotFoundError(exc.message) from exc

    raise exc
