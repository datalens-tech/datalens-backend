## How to init project

Check ```~/.terraformrc```
```
$ cat ~/.terraformrc
provider_installation {
  network_mirror {
    url = "https://terraform-mirror.yandexcloud.net/"
    include = ["registry.terraform.io/*/*"]
  }
  direct {
    exclude = ["registry.terraform.io/*/*"]
  }
}
```

```TF_VAR_iam_token=`yc iam create-token --profile yc-preprod` AWS_ACCESS_KEY=YCBFB0-... AWS_SECRET_KEY=YCMK7... terraform init```
use secrets from https://yav.yandex-team.ru/secret/sec-01dc24ppx88wd8wjawzp3zwxcg/explore/version/ver-01g6qywbrg60n3gq0h02sfw1vn, terraform-state-sa-access-key

### Terraform provider lock example
```terraform providers lock -net-mirror=https://terraform-mirror.yandexcloud.net -platform=linux_amd64 -platform=darwin_arm64 -platform=darwin_amd64 yandex-cloud/yandex```
