import inspect
import os
import re
from typing import Optional

import attr
import yaml


_CONTAINER_HOST_FROM_COMPOSE_MARKER = "from_compose"
_APPLIB_PATH_RE = re.compile(r".+\/(?:app|lib|site-packages)\/[^\/]+\/")


@attr.s(frozen=True)
class HostPort:
    host: str = attr.ib()
    port: int = attr.ib()

    def as_pair(self) -> str:
        return f"{self.host}:{self.port}"


def _get_docker_compose_file_path(filename: Optional[str] = None) -> str | None:
    filename = filename or "docker-compose.yml"
    assert filename is not None
    base_filename: str
    stack = inspect.stack()
    for frame in stack:
        if "/dl_testing/" not in frame.filename:
            base_filename = frame.filename
            break
    else:
        raise Exception("No test dir found in stack")

    re_match = _APPLIB_PATH_RE.search(base_filename)
    if not re_match:
        return None
    applib_path = re_match.group(0)
    return os.path.join(applib_path, filename)


def get_test_container_hostport(
    service_key: str,
    original_port: Optional[int] = None,
    fallback_port: Optional[int] = None,  # likely redundant
    dc_filename: Optional[str] = None,
) -> HostPort:
    # TODO: Turn this into a method of a class with dc_filename in self
    host: str
    port: int

    file_path = _get_docker_compose_file_path(filename=dc_filename)
    if file_path is None:
        if fallback_port is not None:
            return HostPort(host="127.0.0.1", port=fallback_port)
        raise FileNotFoundError("Docker compose file not found")
    else:
        try:
            with open(file_path) as dcyml:
                docker_compose_yml = yaml.safe_load(dcyml)
        except FileNotFoundError:
            if fallback_port is not None:
                return HostPort(host="127.0.0.1", port=fallback_port)
            else:
                raise

    ports = docker_compose_yml["services"][service_key]["ports"]
    if original_port is not None:
        port_pair = [pp.split(":") for pp in ports if int(pp.split(":")[1]) == original_port][0]
    else:
        port_pair = ports[0].split(":")

    if (container_host := os.environ.get("TEST_CONTAINER_HOST")) is not None:
        if container_host == _CONTAINER_HOST_FROM_COMPOSE_MARKER:
            # github actions env, taking host from docker-compose and original port
            host = service_key
            port = int(port_pair[1])
        else:
            # local test run, with remote docker containers
            host = container_host
            port = int(port_pair[0])
    else:
        # remote test run, with containers on the same host
        host = "127.0.0.1"
        port = int(port_pair[0])

    return HostPort(host=host, port=port)
