from __future__ import annotations

from enum import Enum


class AlertType(Enum):
    threshold = 'threshold'
    absrelative = 'absrelative'


class NotificationTransportType(Enum):
    email = 'email'
