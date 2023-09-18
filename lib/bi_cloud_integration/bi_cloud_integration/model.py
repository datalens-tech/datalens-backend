from __future__ import annotations

import datetime
import enum
import time
from typing import (
    Any,
    Dict,
    Optional,
    Sequence,
)
import uuid

import attr


@attr.s
class ServiceAccountData:
    id: str = attr.ib()
    name: str = attr.ib()
    description: str = attr.ib()


@attr.s
class ServiceAccountKeyData:
    id: str = attr.ib()
    svc_acct_id: str = attr.ib()
    description: str = attr.ib()
    created_at: datetime.datetime = attr.ib()
    public_key: str = attr.ib(repr=False)


@attr.s
class ServiceAccountKeyWithPrivateKeyData:
    svc_acct_key_data: ServiceAccountKeyData = attr.ib()
    private_key: str = attr.ib(repr=False)


class AccessBindingAction(enum.Enum):
    ADD = enum.auto()
    REMOVE = enum.auto()


@attr.s
class OperationError:
    # See https://cloud.google.com/tasks/docs/reference/rpc/google.rpc#google.rpc.Status
    code: int = attr.ib()
    message: str = attr.ib()
    details: Sequence[Any] = attr.ib()


@attr.s(frozen=True)
class Operation:
    # See https://bb.yandex-team.ru/projects/CLOUD/repos/cloud-go/browse/private-api/yandex/cloud/priv/operation/operation.proto
    id: str = attr.ib()
    description: str = attr.ib()
    done: bool = attr.ib()
    error: Optional[OperationError] = attr.ib(default=None)
    response: Any = attr.ib(default=None)

    def __attrs_post_init__(self) -> None:
        assert self.error is None or self.response is None, "Response and error could not be both not-None"


@attr.s
class Metric:
    folder_id: str = attr.ib()
    version: str = attr.ib()
    source_id: str = attr.ib()
    schema: str = attr.ib()

    @attr.s
    class Usage:
        start: int = attr.ib()
        finish: int = attr.ib()
        type: str = attr.ib()
        quantity: float = attr.ib()
        unit: str = attr.ib()

    usage: Usage = attr.ib()

    source_wt: int = attr.ib(factory=lambda: int(time.time()))
    tags: Dict = attr.ib(factory=dict)
    id: str = attr.ib(factory=lambda: str(uuid.uuid4()))
    sku_id: str = attr.ib(default=None)


@attr.s
class IAMResource:
    type: str = attr.ib()
    id: str = attr.ib()

    @classmethod
    def folder(cls, folder_id: str) -> IAMResource:
        return cls(type="resource-manager.folder", id=folder_id)

    @classmethod
    def cloud(cls, cloud_id: str) -> IAMResource:
        return cls(type="resource-manager.cloud", id=cloud_id)

    @classmethod
    def billing_account(cls, billing_account_id: str) -> IAMResource:
        return cls(type="billing.account", id=billing_account_id)

    @classmethod
    def service_account(cls, service_account_id: str) -> IAMResource:
        return cls(type="iam.serviceAccount", id=service_account_id)

    @classmethod
    def organization(cls, organization_id: str) -> IAMResource:
        return cls(type="organization-manager.organization", id=organization_id)


@attr.s
class IAMAccount:
    id: str = attr.ib()


@attr.s
class IAMUserAccount(IAMAccount):
    federation_id: str = attr.ib()


@attr.s
class IAMServiceAccount(IAMAccount):
    folder_id: str = attr.ib()
