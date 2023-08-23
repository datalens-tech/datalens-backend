from functools import partial
from pathlib import Path

import clize
import yaml


def remove_ports_from_docker_compose(src: Path, dst: Path) -> None:
    with open(src, 'r') as file:
        data = yaml.safe_load(file)

    for service in data.get('services', {}).values():
        if 'ports' in service:
            del service['ports']

    with open(dst, 'w') as file:
        yaml.safe_dump(data, file)


cmd = partial(clize.run, remove_ports_from_docker_compose)
