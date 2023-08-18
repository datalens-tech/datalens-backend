#!/usr/bin/env python3
"""
Modify the environment variables in all `ig-config.yaml` in the current folder (recursively).

Edit the `ADD_ENV` in the code as needed.

Variables with `None` value will be deleted.

To quickly redeploy a single instance group with the updated vars:

    PROFILE=dlb-preprod  # target cloud-folder profile name
    pip install yq==2.12.0 && \
    IGID="$(yq -r .INSTANCE_GROUP_ID deploy-settings.yaml)" && \
    IMAGE_ID="$(
        yc \
            --profile="$PROFILE" \
            compute instance-group get \
            --id="$IGID" --format=json \
        | jq -r .instance_template.boot_disk_spec.disk_spec.image_id
    )" && \
    ../../../../tools/add_env_var.py && \
    sed "s/__IMAGE_ID__/$IMAGE_ID/g" ig-config.yaml > ig-config-current.yaml && \
    yc \
        --profile="$PROFILE" \
        compute instance-group update \
        --id="$IGID" --file=ig-config-current.yaml && \
    rm ig-config-current.yaml

after which an additional in-container command might be necessary, e.g.:

    yc-pssh run -p 1 "sudo update_env_vars && sleep 300" C@cloud_preprod_datalens-back-preprod_dls-tasks

(or e.g. `C@cloud_preprod_datalens-back-preprod` for all IGs)
which updates the `/etc/kubelet.d/app.yaml` which triggers a restart of the docker container.
"""

from __future__ import annotations

import os
import io
import ruamel.yaml  # parser that can preserve comments


ADD_ENV = dict(
    # BI_FORCE_CLOUD_LOGBROKER=None,
    # YENV_TYPE="production",
)

def process_data(filepath, data):
    if os.path.basename(filepath) != 'ig-config.yaml':
        return

    user_data_s = data['instance_template']['metadata']['user-data']
    ryaml = ruamel.yaml.YAML()
    user_data = ryaml.load(user_data_s)
    for key, value in ADD_ENV.items():
        if value is None:
            user_data['env_vars'].pop(key, None)
        else:
            user_data['env_vars'][str(key)] = str(value)
    fobj = io.StringIO()
    ryaml.dump(user_data, fobj)
    user_data_s = fobj.getvalue()
    data['instance_template']['metadata']['user-data'] = user_data_s
    return data


def main():
    ryaml = ruamel.yaml.YAML()
    for dir_name, _, filenames in os.walk('.'):
        for filename in filenames:
            if not filename.endswith('.yaml'):
                continue
            filepath = os.path.join(dir_name, filename)
            with open(filepath) as fobj:
                data = ryaml.load(fobj)
            data = process_data(filepath, data)
            if not data:
                continue
            with open(filepath, 'w') as fobj:
                ryaml.dump(data, fobj)


if __name__ == '__main__':
    main()
