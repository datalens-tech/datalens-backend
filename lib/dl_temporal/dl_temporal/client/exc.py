import temporalio.service


class TemporalClientError(Exception):
    pass


class PermissionDenied(TemporalClientError):
    pass


class AlreadyExists(TemporalClientError):
    pass


def wrap_temporal_error(exc: Exception) -> TemporalClientError:
    if isinstance(exc, temporalio.service.RPCError):
        if exc.status == temporalio.service.RPCStatusCode.PERMISSION_DENIED:
            raise PermissionDenied(exc.message) from exc
        if exc.status == temporalio.service.RPCStatusCode.ALREADY_EXISTS:
            raise AlreadyExists(exc.message) from exc

    raise exc
