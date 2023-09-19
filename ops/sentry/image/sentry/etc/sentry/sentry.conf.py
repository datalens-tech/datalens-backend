from __future__ import annotations

import granular_settings
from sentry.conf.server import *


CONF_ROOT = os.path.dirname(__file__)

locals().update(granular_settings.get(path="/etc/sentry/settings"))
