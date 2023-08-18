#!/usr/bin/env python3
import os

import requests


YAV_ENDPOINT = 'https://vault-api.passport.yandex.net'

YC_PROFILE = os.environ['YC_PROFILE']
YAV_TOKEN = os.environ['YAV_TOKEN']
IAM_TOKEN = os.environ['IAM_TOKEN']


# lockbox secret can have up to 32 records, so we have to split our ext-testing secret into separate secrets
SECRETS_YC_PREPROD = {
    'bi-backend-common': {
        'lockbox_id': 'fc34lbiuk6r92c80qnc2',
        'values': [
            {
                'yav_id': 'sec-01dc24ppx88wd8wjawzp3zwxcg',
                'keys': [
                    'CRYPTO_KEY_2', 'LOGS_CH_PASSWORD', 'US_MASTER_TOKEN', 'EXT_QUERY_EXECUTER_SECRET_KEY',
                    'COMPONENTS_CERT_CHAIN', 'COMPONENTS_CERT_KEY',
                    'CRYPTO_KEY_3', 'IAM_INTERACTION_SERVICE_ACCOUNT_KEY_DATA',
                ],
            },
            {
                'yav_id': 'sec-01dv8x3s847wpz1mjtcgmfzgv7',
                'keys': ['client_secret', 'TVM_INFO'],
                'remap': {'client_secret': 'TVM_SECRET'}
            }
        ]
    },
    'bi-backend-bi-api': {
        'lockbox_id': 'fc3h34voqset5mm4u10m',
        'values': [
            {
                'yav_id': 'sec-01dc24ppx88wd8wjawzp3zwxcg',
                'keys': [
                    'REDIS_CACHES_PASSWORD', 'DLS_API_KEY',
                    'US_PUBLIC_API_TOKEN', 'PUBLIC_MASTER_TOKEN', 'REDIS_MISC_PASSWORD',
                    'REDIS_RQE_CACHES_PASSWORD',
                ],
                'remap': {'PUBLIC_MASTER_TOKEN': 'PUBLIC_API_KEY'}
            }
        ]
    },
    'bi-backend-service-connectors': {
        'lockbox_id': 'fc3sphf760bfqfto4rjp',
        'values': [
            {
                'yav_id': 'sec-01dc24ppx88wd8wjawzp3zwxcg',
                'keys': [
                    'YA_MUSIC_PODCAST_STATS_PASSWORD', 'MOYSKLAD_DB_USER_PASSWORD', 'MOYSKLAD_PARTNER_KEYS',
                    'BITRIX_DB_USER_PASSWORD', 'BITRIX_PARTNER_KEYS', 'CH_SCHOOLBOOK_PASSWORD',
                    'YC_BILLING_ANALYTICS_PASSWORD'
                ]
            },
            {
                'yav_id': 'sec-01gr616qr799xq3ftc925a8wkm',
                'keys': ['PARTNER_KEYS', 'pw'],
                'remap': {'PARTNER_KEYS': 'EQUEO_PARTNER_KEYS', 'pw': 'EQUEO_DB_USER_PASSWORD'}
            },
            {
                'yav_id': 'sec-01gwpgpc4sep4fe5etwy3tw0ek',
                'keys': ['PARTNER_KEYS', 'ch-password'],
                'remap': {'PARTNER_KEYS': 'KONTUR_MARKET_PARTNER_KEYS', 'ch-password': 'KONTUR_MARKET_DB_USER_PASSWORD'}
            },
            {
                'yav_id': 'sec-01g18jp2yaq1xj1sfkjsh9h9fd',
                'keys': ['password'],
                'remap': {'password': 'CH_SMB_HEATMAPS_PASSWORD'}
            },
            {
                'yav_id': 'sec-01g79fq9e0fycdqwsmpeka7hwn',
                'keys': ['datalens'],
                'remap': {'datalens': 'CH_MARKET_COURIERS_PASSWORD'}
            },
            {
                'yav_id': 'sec-01gbacteyrx00x1gcjkdj8h10m',
                'keys': ['datalens'],
                'remap': {'datalens': 'USAGE_TRACKING_PASSWORD'}
            }
        ]
    },
    'bi-backend-frozen-connectors': {
        'lockbox_id': 'fc3vd9dr3ccmp196m9b3',
        'values': [
            {
                'yav_id': 'sec-01f2482eyh1xqwfpg65452rzz8',
                'keys': ['bumpy_roads_user_ro'],
                'remap': {'bumpy_roads_user_ro': 'CH_FROZEN_BUMPY_ROADS_PASSWORD'}
            },
            {
                'yav_id': 'sec-01dpjw0f855743k8we1bw849gb',
                'keys': ['covid_public'],
                'remap': {'covid_public': 'CH_FROZEN_COVID_PASSWORD'}
            },
            {
                'yav_id': 'sec-01dpjw0f855743k8we1bw849gb',
                'keys': ['samples_user-password'],
                'remap': {'samples_user-password': 'CH_FROZEN_DEMO_PASSWORD'}
            },
            {
                'yav_id': 'sec-01f8qnpcw9kwxyabyfad9kb2wp',
                'keys': ['dtp_mos_user_ro'],
                'remap': {'dtp_mos_user_ro': 'CH_FROZEN_DTP_PASSWORD'}
            },
            {
                'yav_id': 'sec-01f01hcbagr13zwvmhgvxgvh5d',
                'keys': ['gkh_user'],
                'remap': {'gkh_user': 'CH_FROZEN_GKH_PASSWORD'}
            },
            {
                'yav_id': 'sec-01dpjw0f855743k8we1bw849gb',
                'keys': ['samples_user-password'],
                'remap': {'samples_user-password': 'CH_FROZEN_SAMPLES_PASSWORD'}
            },
            {
                'yav_id': 'sec-01exscqjzy4764m3nwhrw1e8hr',
                'keys': ['transparency_report_user_ro'],
                'remap': {'transparency_report_user_ro': 'CH_FROZEN_TRANSPARENCY_PASSWORD'}
            },
            {
                'yav_id': 'sec-01dnvz294hkp2g4s4jycpzad5m',
                'keys': ['dl_user_password'],
                'remap': {'dl_user_password': 'CH_FROZEN_WEATHER_PASSWORD'}
            },
            {
                'yav_id': 'sec-01f01hcbagr13zwvmhgvxgvh5d',
                'keys': ['horeca_user'],
                'remap': {'horeca_user': 'CH_FROZEN_HORECA_PASSWORD'}
            }
        ]
    },
    'bi-backend-dls': {
        'lockbox_id': 'fc3e8uh7mrm767b02vjp',
        'values': [
            {
                'yav_id': 'sec-01dc24ppx88wd8wjawzp3zwxcg',
                'keys': ['DLS_DB_PASSWORD', 'DLS_SENTRY_DSN']
            },
            {
                'yav_id': 'sec-01ekmt2r5p50hp5qw875dewfgw',
                'keys': ['solomon_token'],
                'remap': {'solomon_token': 'DLS_SOLOMON_OAUTH'}
            }
        ]
    },
    'bi-backend-service-accounts': {
        'lockbox_id': 'fc37dn5m5c15m07koh7a',
        'values': [
            {
                'yav_id': 'sec-01dc24ppx88wd8wjawzp3zwxcg',
                'keys': ['bi_api_sa_key_data'],
            }
        ]
    },
    'bi-backend-file-uploader': {
        'lockbox_id': 'fc3e3ehpnqt7821d6dep',
        'values': [
            {
                'yav_id': 'sec-01dc24ppx88wd8wjawzp3zwxcg',
                'keys': [
                    'CLOUD_S3_ACCESS_KEY_ID',
                    'CLOUD_S3_SECRET_ACCESS_KEY',
                    'FILE_UPLOADER_MASTER_TOKEN',
                    'CONN_FILE_CH_PASSWORD',
                ],
            },
            {
                'yav_id': 'sec-01cramk07fz80xzkj80f4nkd82',
                'keys': ['testing'],
                'remap': {'testing': 'CSRF_SECRET'}
            },
            {
                'yav_id': 'sec-01f1cq5ydyc94x7w4v2vsnztxs',
                'keys': [
                    'gsheets_yc_preprod_api_key',
                    'gsheets_yc_preprod_oauth_client_id',
                    'gsheets_yc_preprod_oauth_client_secret',
                ],
                'remap': {
                    'gsheets_yc_preprod_api_key': 'GSHEETS_APP_API_KEY',
                    'gsheets_yc_preprod_oauth_client_id': 'GSHEETS_APP_CLIENT_ID',
                    'gsheets_yc_preprod_oauth_client_secret': 'GSHEETS_APP_CLIENT_SECRET',
                },
            },
        ],
    },
}

SECRETS_YC_PROD = {}

SECRETS_ISRAEL = {
    'bi-backend-common': {
        'lockbox_id': 'bcn6rmoh9fiaqfq2egja',
        'values': [
            {
                'yav_id': 'sec-01g7f9bepxj47d3n90s9qysvv3',
                'keys': [
                    'CRYPTO_KEY_1', 'LOGS_CH_PASSWORD', 'EXT_QUERY_EXECUTER_SECRET_KEY',
                    'IAM_SERVICE_ACCOUNT_KEY_DATA', 'COMPONENTS_CERT_CHAIN', 'COMPONENTS_CERT_KEY',
                    'IAM_INTERACTION_SERVICE_ACCOUNT_KEY_DATA',
                    'TVM_INFO', 'TVM_SECRET'  # TODO: get rid of it
                ],
            },
            {
                'yav_id': 'sec-01csc2x33km5dc7x8p51hfh0nj',
                'keys': ['israel-production'],
                'remap': {'israel-production': 'US_MASTER_TOKEN'}
            },
        ]
    },
    'bi-backend-bi-api': {
        'lockbox_id': 'bcn5439joer3stmh09ga',
        'values': [
            {
                'yav_id': 'sec-01g7f9bepxj47d3n90s9qysvv3',
                'keys': [
                    'REDIS_CACHES_PASSWORD', 'DLS_API_KEY',
                    'US_PUBLIC_API_TOKEN', 'PUBLIC_API_KEY', 'REDIS_MISC_PASSWORD',
                    'REDIS_RQE_CACHES_PASSWORD',
                ],
            }
        ]
    },
    'bi-backend-service-accounts': {
        'lockbox_id': 'bcncskkau760m3opb5s5',
        'values': [
            {
                'yav_id': 'sec-01g7f9bepxj47d3n90s9qysvv3',
                'keys': ['bi_api_sa_key_data'],
            }
        ]
    },
    'bi-backend-frozen-connectors': {
        'lockbox_id': 'bcn8trhg5vvbqnbrjsnq',
        'values': [
            {
                'yav_id': 'sec-01gfxc071akjdwpr5c9yqa2qdk',
                'keys': ['samples_user_ro'],
                'remap': {'samples_user_ro': 'CH_FROZEN_DEMO_PASSWORD'}
            }
        ]
    },
    'bi-backend-file-uploader': {
        'lockbox_id': 'bcnpm5t679ier3a24apk',
        'values': [
            {
                'yav_id': 'sec-01g7f9bepxj47d3n90s9qysvv3',
                'keys': [
                    'CLOUD_S3_ACCESS_KEY_ID',
                    'CLOUD_S3_SECRET_ACCESS_KEY',
                    'FILE_UPLOADER_MASTER_TOKEN',
                    'CONN_FILE_CH_PASSWORD',
                ],
            },
            {
                'yav_id': 'sec-01gd0k04by1eyx8q5ym64r682v',
                'keys': ['production'],
                'remap': {'production': 'CSRF_SECRET'}
            },
            {
                'yav_id': 'sec-01f1cq5ydyc94x7w4v2vsnztxs',
                'keys': [
                    'gsheets_israel_api_key',
                    'gsheets_israel_oauth_client_id',
                    'gsheets_israel_oauth_client_secret',
                ],
                'remap': {
                    'gsheets_israel_api_key': 'GSHEETS_APP_API_KEY',
                    'gsheets_israel_oauth_client_id': 'GSHEETS_APP_CLIENT_ID',
                    'gsheets_israel_oauth_client_secret': 'GSHEETS_APP_CLIENT_SECRET',
                },
            },
        ],
    },
    'bi-backend-service-connectors': {
        'lockbox_id': 'bcnvhn3ndn9cljln5m20',
        'values': [],
    },
}


def get_yav_secret(yav_sec_id: str) -> dict[str, str]:
    resp = requests.get(
        f'{YAV_ENDPOINT}/1/versions/{yav_sec_id}',
        headers={'Authorization': f'OAuth {YAV_TOKEN}'}
    )
    resp.raise_for_status()
    yav_rec = resp.json()
    value_list = yav_rec['version']['value']
    values = {item['key']: item['value'] for item in value_list}
    return values


def main():

    secrets = {
        'yc-preprod': SECRETS_YC_PREPROD,
        'yc-prod': SECRETS_YC_PROD,
        'israel': SECRETS_ISRAEL,
    }[YC_PROFILE]

    lockbox_endpoint = {
        'yc-preprod': 'https://lockbox.api.cloud-preprod.yandex.net',
        'yc-prod': 'https://lockbox.api.cloud.yandex.net',
        'israel': 'https://cpl.lockbox.api.cloudil.com'
    }[YC_PROFILE]

    for lockbox_sec_name, sec_info in secrets.items():
        print(f'Going to sync {lockbox_sec_name}')
        entries = []
        for yav_info in sec_info['values']:
            print(f'Syncing yav {yav_info["yav_id"]}, keys: {str(yav_info["keys"])[:20]}...')
            values = get_yav_secret(yav_info['yav_id'])
            for key in yav_info['keys']:
                remapper = (lambda k: yav_info['remap'].get(k, k)) if 'remap' in yav_info else (lambda k: k)
                if key not in values:
                    print(f'ACHTUNG!!!!!!!!!!!!: key {key} not found in {yav_info["yav_id"]}')
                    continue
                entries.append({
                    'key': remapper(key),
                    'textValue': values[key]
                })

        print(f'Sending new version of {lockbox_sec_name} to lockbox, keys: {[e["key"] for e in entries]}')
        resp = requests.post(
            f'{lockbox_endpoint}/lockbox/v1/secrets/{sec_info["lockbox_id"]}:addVersion',
            headers={'Authorization': f'Bearer {IAM_TOKEN}'},
            json={
                'payloadEntries': entries
            }
        )
        resp.raise_for_status()
        print('Done')

    print('All secrets have been synced')


if __name__ == '__main__':
    main()
