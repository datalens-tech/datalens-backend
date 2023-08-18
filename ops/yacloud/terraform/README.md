# Datalens Terraform

Datalens Terraform

## Development

### Global dependencies

- [yacloud-cli](https://cloud.yandex.ru/docs/cli/quickstart#install)
- [yacloud-private-cli](https://wiki.yandex-team.ru/cloud/devel/platform-team/dev/ycp/#install)

#### Setup configs
[configs in wiki](https://wiki.yandex-team.ru/datalens/backend/devops/konfig-dlja-yc-ycp/)

or

Setup yc config by adding next to ~/.config/yandex-cloud/config.yaml

```text
current: int-prod
profiles:
  default: {}
  int-prod:
    token: AQAD-<...>
    endpoint: gw.db.yandex-team.ru:443
    cloud-id: footcgneu8tgopda7vqu
    folder-id: foohfkkb5s0vc4a9ui3g
  yc-preprod:
    federation-id: yc.yandex-team.federation
    endpoint: api.cloud-preprod.yandex.net:443
    federation-endpoint: console-preprod.cloud.yandex.ru
  yc-prod:
    federation-id: yc.yandex-team.federation
    endpoint: api.cloud.yandex.net:443
    federation-endpoint: console.cloud.yandex.ru
  israel:
    federation-id: yc.yandex-team.federation
    federation-endpoint: auth.cloudil.co.il:443
    endpoint: api.cloudil.com:443
    cloud-id: yc.datalens.backend-service-cloud
    folder-id: yc.datalens.backend-service-folder
```

Setup ycp config by adding next to ~/.config/ycp/config.yaml

```text
current-profile: prod
profiles:
  yc-prod:
    user: yc-fed
    environment: prod
    cloud-id: "b1g08s4su5tgce7cpeo5"
    folder-id: "b1g77mbejmj4m6flq848"
  yc-preprod:
    user: yc-fed
    environment: preprod
    cloud-id: "aoee4gvsepbo0ah4i2j6"
    folder-id: "aoevv1b69su5144mlro3"
  israel:
    user: yc-fed
    environment: israel
    cloud-id: ""
    folder-id: ""

users:
  yc-fed:
    federation-id: yc.yandex-team.federation

environments:
  my-env:
    based-on: preprod
    platform:
      alb:
        endpoint:
          address: localhost:8080
          plaintext: true
        v1:
          endpoint:
            address: localhost:4443
            insecure: true
```

### Environment

Activate environment:

- `source ../ycdl_profile.sh yc-preprod` - yc-preprod
- `source ../ycdl_profile.sh israel` - israel

Deactivate:

```shell
ycdl_reset
```

### Makefile commands

- `make init` - Initialize repository
- `make lint` - Lint repository
- `make lint-fix` - Auto-fix repository
- `make clean` - Clean up repository
- `make clean-plugin-cache` - Clean up system terraform cache
- `make yc-preprod-init` - Init all yc-preprod terraform layers
- `make yc-preprod-validate` - Validate all yc-preprod terraform layers
- `make yc-preprod-plan` - Plan all yc-preprod terraform layers
- `make yc-preprod-apply` - Apply all yc-preprod terraform layers
- `make yc-preprod-refresh` - Refresh all yc-preprod terraform layers

#### Troubleshooting
* `zsh: killed` or similar.
You should move ~/arcadia/datalens/backend/ops/yacloud to outside of ~/arcadia,
because you OS forbid run binaries in ~/arcadia
* `Error: error configuring S3 Backend ... Error: SSOProviderInvalidToken: the SSO session has expired or is invalid`
or similar. You should ask the leader of your team to give some rights.

### Scripts

#### terraform_all

```shell
.scripts/terraform_all [-env {environment_name}] [-layer {layer_name}] [COMMAND] [OPTIONS]
```

Applies `.shims/terraform` command to all layers (or selected) of all environment (or selected), where:

- `{environment_name}` - optional environment name
- `{layer_name}` - optional layer name
