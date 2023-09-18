import os
from pathlib import Path

REPLACE_MAP = {
    "registry.yandex.net/statinfra/redis@sha256:3127620da977815556439a9dc347fff89432a79b6bb6e93a16f20ac4a34ce337": "bitnami/redis:5.0.8@sha256:3127620da977815556439a9dc347fff89432a79b6bb6e93a16f20ac4a34ce337",
    "registry.yandex.net/statinfra/zookeeper@sha256:ad460bd234a3166cb37e14a1526d677052f7c42e1d1dccd040ca3e24ed1bec1f": "zookeeper:3.4@sha256:d2b1ea8db9241d31daed9b1b90e22b62d9ffb14e29d355d0d0c8d0a87819d929",
    "registry.yandex.net/statinfra/mssql-server-linux@sha256:6522290393006d93b88f63a295c5137010e4e0fea548d3fce9892c07262f7a1a": "mcr.microsoft.com/mssql/server:2017-CU12@sha256:19b9392f035fc9f82b77f6833d1490bca8cb041b445cd451de0d1f1f3efe70e8",
    "registry.yandex.net/yandex-docker-local-ydb@sha256:882755b316b72490702e372e82c84df770b046fd3ecdd77163fc088a82c043a1": "cr.yandex/crpsufj53lhl0via9iam/yandex-docker-local-ydb:latest",
    # "registry.yandex.net/statinfra/mysql@sha256:d8e4032005f53a774f2361281ebf61fa3d7635d5dacf9c58bc54e823ddcf9f1d": "cr.yandex/crpsufj53lhl0via9iam/mysql:5.6",
    "registry.yandex.net/statinfra/mysql@sha256:d8e4032005f53a774f2361281ebf61fa3d7635d5dacf9c58bc54e823ddcf9f1d": "mysql:5.6@sha256:897086d07d1efa876224b147397ea8d3147e61dd84dce963aace1d5e9dc2802d",
    # "registry.yandex.net/statinfra/mysql@sha256:574bf8a61e3276788bcaa9a9e226977ea3037f439295e2a07b624b8aaebd66d4": "cr.yandex/crpsufj53lhl0via9iam/mysql:8.0.12",
    "registry.yandex.net/statinfra/mysql@sha256:574bf8a61e3276788bcaa9a9e226977ea3037f439295e2a07b624b8aaebd66d4": "mysql:8.0.12@sha256:8fdc47e9ccb8112a62148032ae70484e3453b628ab6fe02bccf159e2966b750e",
    "registry.yandex.net/statinfra/postgres@sha256:094358a1a64da927d5c26dcac9dad022cf0db840b6b627b143e5e4fd9adf982b": "postgres:9.3-alpine@sha256:028cb3bc3792c307a490864b5deecfaec9027cef4aad16d33882e32003052756",
    # "registry.yandex.net/statinfra/postgres@sha256:b3ae2dad6f153711444221849919b98c0503e6eef57be18300713cbef7ada4bc": "cr.yandex/crpsufj53lhl0via9iam/postgres:9_4",
    "registry.yandex.net/statinfra/postgres@sha256:b3ae2dad6f153711444221849919b98c0503e6eef57be18300713cbef7ada4bc": "postgres:9.4.26-alpine@sha256:b3ae2dad6f153711444221849919b98c0503e6eef57be18300713cbef7ada4bc",
    "registry.yandex.net/statinfra/postgres@sha256:3335d0494b62ae52f0c18a1e4176a83991c9d3727fe67d8b1907b569de2f6175": "postgres:13-alpine@sha256:b9f66c57932510574fb17bccd175776535cec9abcfe7ba306315af2f0b7bfbb4",
    "registry.yandex.net/data-ui/united-storage:latest": "cr.yandex/crpsufj53lhl0via9iam/united-storage:latest",
    "registry.yandex.net/statinfra/base/bi/initdb:latest": "cr.yandex/crpsufj53lhl0via9iam/initdb:latest",
    "registry.yandex.net/statinfra/base/bi/initdb@sha256:41477e01d5e1017d31c09776ded1b135ce7e58add715a024fc294c0490b95c44": "cr.yandex/crpsufj53lhl0via9iam/initdb:41477",
    "registry.yandex.net/statinfra/clickhouse-server:22.11": "cr.yandex/crpsufj53lhl0via9iam/clickhouse-server:22.11",
    "registry.yandex.net/statinfra/clickhouse-server@sha256:74e094253baa15b18ec1ea3a79fb4015451de3bb23c40c99dcc37d1f85c1ac09": "cr.yandex/crpsufj53lhl0via9iam/clickhouse-server:22.3",
    "registry.yandex.net/statinfra/clickhouse-server@sha256:1c4b87bfec402ff477a9490752c908d2e3e920f74a0d10f1c904c4fac5f62dd8": "cr.yandex/crpsufj53lhl0via9iam/clickhouse-server:19.3",
    "registry.yandex.net/statinfra/clickhouse-server@sha256:22603e0c2121142197610c387e472e22cdb0209e66abe5ad57776e686a9f5659": "yandex/clickhouse-server:21.8.7.22",
    "registry.yandex.net/statinfra/s3cloudserver@sha256:b53e57829cf7df357323e60a19c9f98d2218f1b7ccb1d7cea5761a5a227a9ee3": "cr.yandex/crpsufj53lhl0via9iam/s3cloudserver",
    "registry.yandex.net/statinfra/node-exporter@sha256:4f30c76de420097cdc9915c439f5c5f725ccb834a98d0631b3fac1ccb39f96f6": "prom/node-exporter:v1.2.2@sha256:4f30c76de420097cdc9915c439f5c5f725ccb834a98d0631b3fac1ccb39f96f6",
    "registry.yandex.net/statinfra/prometheus@sha256:ca8ba947c1fb52346cb0e398eefef07927de4ae42e2270f9627edfc8efbdc4f0": "prom/prometheus:v2.29.1@sha256:ca8ba947c1fb52346cb0e398eefef07927de4ae42e2270f9627edfc8efbdc4f0",
    #  "cr.yandex/crpsufj53lhl0via9iam/"
}


def main():
    fix_cr_images()


def fix_in_root(root: Path):
    for path, dirs, items in os.walk(root):
        for name in items:
            if name.startswith(("Dockerfile", "docker-compose.")):
                fn = Path(path) / name
                changed = False
                content = open(fn).read()
                for k in REPLACE_MAP.keys():
                    if k in content:
                        print(f"Detected {k} in {fn}")
                        content = content.replace(k, REPLACE_MAP[k])
                        changed = True
                if changed:
                    # print(f"New content: {content}")
                    with open(fn, "w") as fh:
                        fh.write(content)


def fix_cr_images():
    root = Path(__file__).parent.parent.parent
    fix_in_root(root / "lib")
    fix_in_root(root / "mainrepo/lib")
    fix_in_root(root / "app")


if __name__ == "__main__":
    main()
