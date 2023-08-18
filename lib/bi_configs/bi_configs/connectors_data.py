from __future__ import annotations

import abc
from typing import ClassVar, Optional

from bi_constants.enums import RawSQLLevel


CH_DATALENS_EXT_DATA_HOSTS: tuple[str, ...] = (
    # https://yc.yandex-team.ru/folders/foohfkkb5s0vc4a9ui3g/managed-clickhouse/cluster/mdbd60phvp3hvq7d3sq6
    "sas-t2pl126yki67ztly.db.yandex.net",
    "vla-1zwdkyy37cy8iz7f.db.yandex.net",
)


CH_EXT_DATA_HOSTS: tuple[str, ...] = (
    # https://console.cloud.yandex.ru/folders/b1gm5rgsft916g8kjhnd/managed-clickhouse/cluster/c9qte8l5lhb4vuadou1u
    "rc1a-lcdz5pcu5f40kcas.mdb.yandexcloud.net",
    "rc1b-tbmlr5cfyd96efi5.mdb.yandexcloud.net",
)


CH_SAMPLES_HOSTS: tuple[str, ...] = (
    # https://yc.yandex-team.ru/folders/foohfkkb5s0vc4a9ui3g/managed-clickhouse/cluster/mdb636es44gm87hucoip/view
    "sas-gvwzxfe1s83fmwex.db.yandex.net",
    "vla-wwc7qtot5u6hhcqc.db.yandex.net",
)


def normalize_sql_query(sql_query: str) -> str:
    # Only normalize newlines for now:
    sql_query = '\n{}\n'.format(sql_query.strip('\n'))
    return sql_query


SQL_YA_MUSIC_PODCASTS_STATS_STREAMS = normalize_sql_query(r'''
SELECT
    streams_t.age_segment as age_segment,
    streams_t.album_id as album_id,
    streams_t.album_name as album_name,
    streams_t.auth as auth,
    streams_t.city as city,
    streams_t.client_type as client_type,
    streams_t.country as country,
    streams_t.stream_date as stream_date,
    streams_t.stream_datetime as stream_datetime,
    streams_t.gender as gender,
    streams_t.longplay as longplay,
    streams_t.new_puid as new_puid,
    streams_t.new_uid as new_uid,
    streams_t.platform as platform,
    streams_t.play_percent as play_percent,
    streams_t.play_time as play_time,
    streams_t.publications_date as publications_date,
    streams_t.region as region,
    streams_t.track_id as track_id,
    streams_t.track_name as track_name,

    :passport_user_id as author_puid
FROM
    music.Streams_Table streams_t
WHERE streams_t.album_id IN (
    SELECT album_id
    FROM music.authors_podcasts
    WHERE author_puid = :passport_user_id
)
SETTINGS join_use_nulls = 1
''')


SQL_YA_MUSIC_PODCASTS_STATS_SUBSCRIBERS = normalize_sql_query(r'''
SELECT
    subs.age_segment as age_segment,
    subs.album_id as album_id,
    subs.album_name as album_name,
    subs.city as city,
    subs.country as country,
    subs.`date` as `date`,
    subs.gender as gender,
    subs.likes_growth as likes_growth,
    subs.likes_total as likes_total,
    subs.region as region,

    :passport_user_id as author_puid
FROM
    music.Subscribers_Table subs
WHERE subs.album_id IN (
    SELECT album_id
    FROM music.authors_podcasts
    WHERE author_puid = :passport_user_id
)
SETTINGS join_use_nulls = 1
''')


SQL_BILLING_REPORTD = normalize_sql_query(r'''
SELECT
    dimensions.billing_account_name AS billing_account_name,
    dimensions.billing_account_name_id AS billing_account_name_id,
    report.billing_account_id AS billing_account_id,
    report.usage_date AS usage_date,
    report.resource_id AS resource_id,
    report.folder_id AS folder_id,
    dimensions.folder_name AS folder_name,
    dimensions.folder_name_id AS folder_name_id,
    report.cloud_id AS cloud_id,
    dimensions.cloud_name AS cloud_name,
    dimensions.cloud_name_id AS cloud_name_id,
    report.cost AS cost,
    report.credit AS credit,
    report.total AS total,
    report.monetary_grant_credit AS monetary_grant_credit,
    report.cud_credit AS cud_credit,
    report.misc_credit AS misc_credit,
    report.volume_incentive_credit AS volume_incentive_credit,
    report.pricing_quantity AS pricing_quantity,
    report.pricing_unit AS pricing_unit,
    report.cud_compensated_pricing_quantity AS cud_compensated_pricing_quantity,
    report.currency AS currency,
    report.labels_hash AS labels_hash,
    report.sku_id AS sku_id,
    report.sku_name_ru AS sku_name_ru,
    report.sku_name_en AS sku_name_en,
    report.sku_service_id AS sku_service_id,
    report.sku_service_name AS sku_service_name
FROM billing.billing_reportd AS report
LEFT JOIN
(
    SELECT
        account.billing_account_id AS billing_account_id,
        cloud.cloud_id AS cloud_id,
        folder.folder_id AS folder_id,
        account.billing_account_name AS billing_account_name,
        account.billing_account_name_id AS billing_account_name_id,
        folder.folder_name AS folder_name,
        folder.folder_name_id AS folder_name_id,
        cloud.cloud_name AS cloud_name,
        cloud.cloud_name_id AS cloud_name_id
    FROM
    (
        SELECT
            billing_account_id,
            anyLast(billing_account_name) AS billing_account_name,
            anyLast(billing_account_name_id) AS billing_account_name_id
        FROM billing.billing_account_name_d AS account
        PREWHERE billing_account_id IN :billing_account_id_list
        GROUP BY billing_account_id
    ) AS account
    LEFT JOIN
    (
        SELECT
            billing_account_id,
            cloud_id,
            anyLast(cloud_name) AS cloud_name,
            anyLast(cloud_name_id) AS cloud_name_id
        FROM billing.cloud_name_d AS cloud
        PREWHERE billing_account_id IN :billing_account_id_list
        GROUP BY billing_account_id, cloud_id
    ) AS cloud ON account.billing_account_id = cloud.billing_account_id
    LEFT JOIN
    (
        SELECT
            billing_account_id,
            cloud_id,
            folder_id,
            anyLast(folder_name) AS folder_name,
            anyLast(folder_name_id) AS folder_name_id
        FROM billing.folder_name_d AS folder
        PREWHERE billing_account_id IN :billing_account_id_list
        GROUP BY billing_account_id, cloud_id, folder_id
    ) AS folder ON account.billing_account_id = folder.billing_account_id AND cloud.cloud_id = folder.cloud_id
) AS dimensions USING (billing_account_id, cloud_id, folder_id)
PREWHERE billing_account_id IN :billing_account_id_list
SETTINGS join_use_nulls = 1, distributed_product_mode = 'local'
''')


SQL_BILLING_LABELS_MAPD = normalize_sql_query(r'''
SELECT
  report_with_dimensions.*,
  label_key,
  label_value
FROM (
  SELECT
    dimensions.billing_account_name AS billing_account_name,
    dimensions.billing_account_name_id AS billing_account_name_id,
    report.billing_account_id AS billing_account_id,
    report.usage_date AS usage_date,
    report.resource_id AS resource_id,
    report.folder_id AS folder_id,
    dimensions.folder_name AS folder_name,
    dimensions.folder_name_id AS folder_name_id,
    report.cloud_id AS cloud_id,
    dimensions.cloud_name AS cloud_name,
    dimensions.cloud_name_id AS cloud_name_id,
    report.cost AS cost,
    report.credit AS credit,
    report.total AS total,
    report.monetary_grant_credit AS monetary_grant_credit,
    report.cud_credit AS cud_credit,
    report.misc_credit AS misc_credit,
    report.volume_incentive_credit AS volume_incentive_credit,
    report.pricing_quantity AS pricing_quantity,
    report.pricing_unit AS pricing_unit,
    report.cud_compensated_pricing_quantity AS cud_compensated_pricing_quantity,
    report.currency AS currency,
    report.labels_hash AS labels_hash,
    report.sku_id AS sku_id,
    report.sku_name_ru AS sku_name_ru,
    report.sku_name_en AS sku_name_en,
    report.sku_service_id AS sku_service_id,
    report.sku_service_name AS sku_service_name
FROM billing.billing_reportd AS report
LEFT JOIN
(
    SELECT
        account.billing_account_id AS billing_account_id,
        cloud.cloud_id AS cloud_id,
        folder.folder_id AS folder_id,
        account.billing_account_name AS billing_account_name,
        account.billing_account_name_id AS billing_account_name_id,
        folder.folder_name AS folder_name,
        folder.folder_name_id AS folder_name_id,
        cloud.cloud_name AS cloud_name,
        cloud.cloud_name_id AS cloud_name_id
    FROM
    (
        SELECT
            billing_account_id,
            anyLast(billing_account_name) AS billing_account_name,
            anyLast(billing_account_name_id) AS billing_account_name_id
        FROM billing.billing_account_name_d AS account
        PREWHERE billing_account_id IN :billing_account_id_list
        GROUP BY billing_account_id
    ) AS account
    LEFT JOIN
    (
        SELECT
            billing_account_id,
            cloud_id,
            anyLast(cloud_name) AS cloud_name,
            anyLast(cloud_name_id) AS cloud_name_id
        FROM billing.cloud_name_d AS cloud
        PREWHERE billing_account_id IN :billing_account_id_list
        GROUP BY billing_account_id, cloud_id
    ) AS cloud ON account.billing_account_id = cloud.billing_account_id
    LEFT JOIN
    (
        SELECT
            billing_account_id,
            cloud_id,
            folder_id,
            anyLast(folder_name) AS folder_name,
            anyLast(folder_name_id) AS folder_name_id
        FROM billing.folder_name_d AS folder
        PREWHERE billing_account_id IN :billing_account_id_list
        GROUP BY billing_account_id, cloud_id, folder_id
    ) AS folder ON account.billing_account_id = folder.billing_account_id AND cloud.cloud_id = folder.cloud_id
) AS dimensions USING (billing_account_id, cloud_id, folder_id)
PREWHERE billing_account_id IN :billing_account_id_list
) as report_with_dimensions
LEFT JOIN (
  SELECT DISTINCT *
  FROM billing.labels_mapd as l
  PREWHERE billing_account_id IN :billing_account_id_list
) as labels
USING (billing_account_id, usage_date, labels_hash)
SETTINGS join_use_nulls = 1, distributed_product_mode = 'local'
''')


SQL_MARKET_COURIERS_COUR_RATING = normalize_sql_query(r'''
SELECT
    bench_canceled,
    bench_perenos,
    cancelled_client,
    cancelled_mk,
    capacity,
    car,
    cnt_fl_perenos_client,
    cnt_orders_accepted,
    cnt_orders_accepted_no_client,
    cnt_orders_dt,
    cnt_perenos_mk,
    company_id,
    company_name,
    courier_name,
    fact_dt,
    fst_shift_date,
    id_cou,
    last_shift_date,
    not_accepted,
    on_time,
    order_dost_chisl,
    order_vikup_chisl,
    order_znam,
    parcel_volume,
    region_detail,
    reit,
    service_name,
    sorting_center_id,
    task_accepted_no_client,
    task_actually_on_time,
    task_cancelled_by_client,
    task_cancelled_by_courier,
    task_cancelled_by_courier_support,
    task_cancelled_by_system,
    task_on_time,
    task_perenos_by_client,
    task_perenos_by_client_no_contact,
    task_perenos_by_client_refused,
    task_perenos_by_client_wrong,
    task_perenos_by_courier,
    task_perenos_by_courier_support,
    task_perenos_by_system,
    user_shift_id,
    user_shift_number
FROM tpl_partner_info.cour_rating AS tbl
JOIN tpl_partner_info.partner_uid_mapping AS mapping
    ON mapping.id = tbl.company_id
WHERE mapping.uid = :passport_user_id
''')


SQL_MARKET_COURIERS_KPI_PARTNER = normalize_sql_query(r'''
SELECT
    cancelled_client,
    cancelled_mk,
    cnt_fl_perenos_client,
    cnt_orders_accepted,
    cnt_orders_accepted_no_client,
    cnt_orders_dt,
    cnt_perenos_mk,
    cnt_shifts,
    cnt_yes,
    company_id,
    company_name,
    extra,
    fact_dt,
    not_accepted,
    on_time,
    penalty,
    service_name,
    sorting_center_id,
    task_accepted_no_client,
    task_cancelled_by_courier,
    task_on_time,
    task_perenos_by_courier,
    total,
    totalReward
FROM tpl_partner_info.kpi_partner AS tbl
JOIN tpl_partner_info.partner_uid_mapping AS mapping
    ON mapping.id = tbl.company_id
WHERE mapping.uid = :passport_user_id
''')


SQL_MARKET_COURIERS_NEVIHOD = normalize_sql_query(r'''
SELECT
    company_id,
    company_name,
    fake_flag,
    flag_no_shift,
    main_task_status,
    name,
    shift_date,
    shift_id,
    shift_status,
    sorting_center_id,
    sorting_center_name,
    transport_capacity,
    transport_name,
    transport_type,
    transport_type_id,
    user_id,
    user_shift_id,
    user_shift_status,
    user_status,
    user_status_change_time,
    warehouse_area_leave_time
FROM tpl_partner_info.nevihod AS tbl
JOIN tpl_partner_info.partner_uid_mapping AS mapping
    ON mapping.id = tbl.company_id
WHERE mapping.uid = :passport_user_id
''')


SQL_MARKET_COURIERS_OBYCHAEMOST = normalize_sql_query(r'''
SELECT
    account_created,
    company_id,
    company_name,
    courier,
    course,
    course_id,
    firstshift,
    kiosk_id,
    lastshift,
    maxstage,
    passed,
    routing_vehicle_type,
    sc,
    sc_id,
    score,
    session_id,
    session_status,
    status,
    transport_type_id,
    updated,
    user_id
FROM tpl_partner_info.obychaemost AS tbl
JOIN tpl_partner_info.partner_uid_mapping AS mapping
    ON mapping.id = tbl.company_id
WHERE mapping.uid = :passport_user_id
''')


SQL_MARKET_COURIERS_DROPOFF_ORDERS = normalize_sql_query(r'''
SELECT
    DO_130,
    courier_company_id,
    courier_company_name,
    courier_name,
    courier_shift_date,
    courier_task_finished_at,
    courier_task_status,
    delivery_service_name,
    dropoff_address,
    dropoff_name,
    dropoff_phone_number,
    item_id,
    item_name,
    item_price,
    merchand_partner_id,
    merchant_shipment_date,
    order_creation_date,
    order_id,
    sc_110_chp,
    sc_name,
    transport_type_name,
    vehicle_type
FROM tpl_partner_info.dropoff_orders AS tbl
JOIN tpl_partner_info.partner_uid_mapping AS mapping
    ON mapping.id = tbl.courier_company_id
WHERE mapping.uid = :passport_user_id
''')


SQL_SCHOOLBOOK_STATS_TESTING = normalize_sql_query(r'''
SELECT
    *
FROM
    pelican_db_prod_db.test_view_4
WHERE teacher_puid = :passport_user_id
''')


SQL_SCHOOLBOOK_STATS = normalize_sql_query(r'''
SELECT
    *
FROM
    pelican_db_prod_db.inf_journal
WHERE teacher_puid = :passport_user_id
''')


SQL_SMB_HEATMAPS = normalize_sql_query(r'''
SELECT
    base.city_ru,
    base.coords,
    base.district_name,
    base.polygon_name,
    demand_and_supply.polygon_id,
    demand_and_supply.polygon_type,
    demand_and_supply.rubric_name,
    demand_and_supply.demand,
    demand_and_supply.demand_to_supply_to_city,
    demand_and_supply.supply,
    demand_and_supply.top_companies,
    demand_and_supply.top_queries
FROM
    smb_geo_heat_maps.polygon_base AS base
INNER JOIN
    smb_geo_heat_maps.geoadv_geoproduct_intermediate_demand_and_supply AS demand_and_supply
USING (polygon_id, polygon_type)
WHERE :passport_user_id IN (select passport_uid from smb_geo_heat_maps.client_map)
SETTINGS join_use_nulls = 1
''')

SQL_USAGE_TRACKING = normalize_sql_query(r'''
SELECT
    *
FROM usage_tracking.usage_tracking
WHERE folder_id = :tenant_id
''')

SQL_USAGE_TRACKING_YA_TEAM = normalize_sql_query(r'''
SELECT
    event_time,
    request_id,
    connection_id,
    dataset_id,
    chart_id,
    dash_id,
    dash_tab_id,
    chart_kind,
    entries_conn.display_key AS connection_title,
    entries_dataset.display_key AS dataset_title,
    entries_chart.display_key AS chart_title,
    entries_dash.display_key AS dash_title,
    response_status_code,
    err_code,
    query,
    source,
    dataset_mode,
    username,
    user_id,
    toUInt8(startsWith(ifNull(username,
 ''),
 'robot') OR startsWith(ifNull(username,
 ''),
 'zomb-')) AS is_robot,
    execution_time,
    department,
    status,
    error,
    ifNull(connection_type,
 '') AS connection_type,
    host,
    cluster,
    clique_alias,
    if(startsWith(connection_type,
 'ch_over_yt'),
 toUInt8(ifNull(clique_alias,
 '') = '*ch_datalens'),
 NULL) AS clique_is_public,
    event_date,
    cache_full_hit,
    endpoint_code,
    query_type,
    dictGetHierarchy('staff_departments',
 dictGetUInt64('staff_users',
 'department',
 tuple(coalesce(username,
 ''))))[-1] AS department_level1,
    dictGetHierarchy('staff_departments',
 dictGetUInt64('staff_users',
 'department',
 tuple(coalesce(username,
 ''))))[-2] AS department_level2,
    dictGetHierarchy('staff_departments',
 dictGetUInt64('staff_users',
 'department',
 tuple(coalesce(username,
 ''))))[-3] AS department_level3,
    dictGetHierarchy('staff_departments',
 dictGetUInt64('staff_users',
 'department',
 tuple(coalesce(username,
 ''))))[-4] AS department_level4,
    dictGetHierarchy('staff_departments',
 dictGetUInt64('staff_users',
 'department',
 tuple(coalesce(username,
 ''))))[-5] AS department_level5,
    dictGetStringOrDefault('staff_departments',
 'name',
 department,
 'Остальные') AS department_name,
    dictGetStringOrDefault('staff_departments',
 'name',
 department_level1,
 'Остальные') AS department_level1_name,
    dictGetStringOrDefault('staff_departments',
 'name',
 department_level2,
 'Остальные') AS department_level2_name,
    dictGetStringOrDefault('staff_departments',
 'name',
 department_level3,
 'Остальные') AS department_level3_name,
    dictGetStringOrDefault('staff_departments',
 'name',
 department_level4,
 'Остальные') AS department_level4_name,
    dictGetStringOrDefault('staff_departments',
 'name',
 department_level5,
 'Остальные') AS department_level5_name,
    dictGetHierarchy('staff_departments',
 dictGetUInt64('staff_users',
 'department',
 tuple(coalesce(username,
 ''))))[-6] AS department_level6,
    dictGetHierarchy('staff_departments',
 dictGetUInt64('staff_users',
 'department',
 tuple(coalesce(username,
 ''))))[-7] AS department_level7,
    dictGetHierarchy('staff_departments',
 dictGetUInt64('staff_users',
 'department',
 tuple(coalesce(username,
 ''))))[-8] AS department_level8,
    dictGetStringOrDefault('staff_departments',
 'name',
 department_level6,
 'Остальные') AS department_level6_name,
    dictGetStringOrDefault('staff_departments',
 'name',
 department_level7,
 'Остальные') AS department_level7_name,
    dictGetStringOrDefault('staff_departments',
 'name',
 department_level8,
 'Остальные') AS department_level8_name
FROM dataset_profile.requests_v2_int_production_short AS requests
LEFT JOIN dataset_profile.us_entries_int_production AS entries_conn ON requests.connection_id = entries_conn.encoded_entry_id
LEFT JOIN dataset_profile.us_entries_int_production AS entries_dataset ON requests.dataset_id = entries_dataset.encoded_entry_id
LEFT JOIN dataset_profile.us_entries_int_production AS entries_chart ON requests.chart_id = entries_chart.encoded_entry_id
LEFT JOIN dataset_profile.us_entries_int_production AS entries_dash ON requests.dash_id = entries_dash.encoded_entry_id
WHERE source_entry_id in (SELECT entry_id FROM dataset_profile.dls_admins_int_production WHERE puid = :user_id)
SETTINGS join_use_nulls = 1
''')

SQL_USAGE_TRACKING_YA_TEAM_DASH_STATS = normalize_sql_query(r'''
SELECT
    datetime AS event_time,
    dashId AS dash_id,
    dashTabId AS dash_tab_id,
    entries_dash.display_key AS dash_title,
    login AS username,
    toUInt8(startsWith(ifNull(username,
 ''),
 'robot') OR startsWith(ifNull(username,
 ''),
 'zomb-')) AS is_robot,
    dictGetUInt64('staff_users',
 'department',
 tuple(coalesce(username,
 ''))) AS department,
    dictGetHierarchy('staff_departments',
 dictGetUInt64('staff_users',
 'department',
 tuple(coalesce(username,
 ''))))[-1] AS department_level1,
    dictGetHierarchy('staff_departments',
 dictGetUInt64('staff_users',
 'department',
 tuple(coalesce(username,
 ''))))[-2] AS department_level2,
    dictGetHierarchy('staff_departments',
 dictGetUInt64('staff_users',
 'department',
 tuple(coalesce(username,
 ''))))[-3] AS department_level3,
    dictGetHierarchy('staff_departments',
 dictGetUInt64('staff_users',
 'department',
 tuple(coalesce(username,
 ''))))[-4] AS department_level4,
    dictGetHierarchy('staff_departments',
 dictGetUInt64('staff_users',
 'department',
 tuple(coalesce(username,
 ''))))[-5] AS department_level5,
    dictGetStringOrDefault('staff_departments',
 'name',
 department,
 'Остальные') AS department_name,
    dictGetStringOrDefault('staff_departments',
 'name',
 department_level1,
 'Остальные') AS department_level1_name,
    dictGetStringOrDefault('staff_departments',
 'name',
 department_level2,
 'Остальные') AS department_level2_name,
    dictGetStringOrDefault('staff_departments',
 'name',
 department_level3,
 'Остальные') AS department_level3_name,
    dictGetStringOrDefault('staff_departments',
 'name',
 department_level4,
 'Остальные') AS department_level4_name,
    dictGetStringOrDefault('staff_departments',
 'name',
 department_level5,
 'Остальные') AS department_level5_name,
    dictGetHierarchy('staff_departments',
 dictGetUInt64('staff_users',
 'department',
 tuple(coalesce(username,
 ''))))[-6] AS department_level6,
    dictGetHierarchy('staff_departments',
 dictGetUInt64('staff_users',
 'department',
 tuple(coalesce(username,
 ''))))[-7] AS department_level7,
    dictGetHierarchy('staff_departments',
 dictGetUInt64('staff_users',
 'department',
 tuple(coalesce(username,
 ''))))[-8] AS department_level8,
    dictGetStringOrDefault('staff_departments',
 'name',
 department_level6,
 'Остальные') AS department_level6_name,
    dictGetStringOrDefault('staff_departments',
 'name',
 department_level7,
 'Остальные') AS department_level7_name,
    dictGetStringOrDefault('staff_departments',
 'name',
 department_level8,
 'Остальные') AS department_level8_name
FROM dataset_profile.dashStats AS requests
LEFT JOIN dataset_profile.us_entries_int_production AS entries_dash ON requests.dashId = entries_dash.encoded_entry_id
WHERE requests.dashId in (SELECT entry_id FROM dataset_profile.dls_dash_admins_int_production WHERE puid = :user_id)
SETTINGS join_use_nulls = 1
''')

SQL_USAGE_TRACKING_YA_TEAM_AGGREGATED = normalize_sql_query(r'''
SELECT
    event_date,
    request_count,
    cache_hits,
    errors_count,
    connection_id,
    dataset_id,
    chart_id,
    dash_id,
    dash_tab_id,
    chart_kind,
    entries_conn.display_key AS connection_title,
    entries_dataset.display_key AS dataset_title,
    entries_chart.display_key AS chart_title,
    entries_dash.display_key AS dash_title,
    username,
    user_id,
    toUInt8(startsWith(ifNull(username,
 ''),
 'robot') OR startsWith(ifNull(username,
 ''),
 'zomb-')) AS is_robot,
    ifNull(connection_type,
 '') AS connection_type,
    host,
    clique_alias,
    if(startsWith(connection_type,
 'ch_over_yt'),
 toUInt8(ifNull(clique_alias,
 '') = '*ch_datalens'),
 NULL) AS clique_is_public,
    dictGetUInt64('staff_users',
 'department',
 tuple(coalesce(username,
 ''))) AS department,
    dictGetHierarchy('staff_departments',
 dictGetUInt64('staff_users',
 'department',
 tuple(coalesce(username,
 ''))))[-1] AS department_level1,
    dictGetHierarchy('staff_departments',
 dictGetUInt64('staff_users',
 'department',
 tuple(coalesce(username,
 ''))))[-2] AS department_level2,
    dictGetHierarchy('staff_departments',
 dictGetUInt64('staff_users',
 'department',
 tuple(coalesce(username,
 ''))))[-3] AS department_level3,
    dictGetHierarchy('staff_departments',
 dictGetUInt64('staff_users',
 'department',
 tuple(coalesce(username,
 ''))))[-4] AS department_level4,
    dictGetHierarchy('staff_departments',
 dictGetUInt64('staff_users',
 'department',
 tuple(coalesce(username,
 ''))))[-5] AS department_level5,
    dictGetStringOrDefault('staff_departments',
 'name',
 department,
 'Остальные') AS department_name,
    dictGetStringOrDefault('staff_departments',
 'name',
 department_level1,
 'Остальные') AS department_level1_name,
    dictGetStringOrDefault('staff_departments',
 'name',
 department_level2,
 'Остальные') AS department_level2_name,
    dictGetStringOrDefault('staff_departments',
 'name',
 department_level3,
 'Остальные') AS department_level3_name,
    dictGetStringOrDefault('staff_departments',
 'name',
 department_level4,
 'Остальные') AS department_level4_name,
    dictGetStringOrDefault('staff_departments',
 'name',
 department_level5,
 'Остальные') AS department_level5_name,
    dictGetHierarchy('staff_departments',
 dictGetUInt64('staff_users',
 'department',
 tuple(coalesce(username,
 ''))))[-6] AS department_level6,
    dictGetHierarchy('staff_departments',
 dictGetUInt64('staff_users',
 'department',
 tuple(coalesce(username,
 ''))))[-7] AS department_level7,
    dictGetHierarchy('staff_departments',
 dictGetUInt64('staff_users',
 'department',
 tuple(coalesce(username,
 ''))))[-8] AS department_level8,
    dictGetStringOrDefault('staff_departments',
 'name',
 department_level6,
 'Остальные') AS department_level6_name,
    dictGetStringOrDefault('staff_departments',
 'name',
 department_level7,
 'Остальные') AS department_level7_name,
    dictGetStringOrDefault('staff_departments',
 'name',
 department_level8,
 'Остальные') AS department_level8_name
FROM dataset_profile.requests_v2_int_production_agg AS requests
LEFT JOIN dataset_profile.us_entries_int_production AS entries_conn ON requests.connection_id = entries_conn.encoded_entry_id
LEFT JOIN dataset_profile.us_entries_int_production AS entries_dataset ON requests.dataset_id = entries_dataset.encoded_entry_id
LEFT JOIN dataset_profile.us_entries_int_production AS entries_chart ON requests.chart_id = entries_chart.encoded_entry_id
LEFT JOIN dataset_profile.us_entries_int_production AS entries_dash ON requests.dash_id = entries_dash.encoded_entry_id
WHERE dataset_id in (SELECT entry_id FROM dataset_profile.dls_admins_int_production WHERE puid = :user_id)
SETTINGS join_use_nulls = 1
''')


class ConnectorsDataBase(metaclass=abc.ABCMeta):
    @classmethod
    @abc.abstractmethod
    def connector_name(cls) -> str:
        """
        TODO: BI-4359 just remove it after migration to new scheme
        """
        raise NotImplementedError


class ConnectorsDataCHFrozenBumpyRoadsBase(ConnectorsDataBase):
    CONN_CH_FROZEN_BUMPY_ROADS_HOST: ClassVar[Optional[str]] = None
    CONN_CH_FROZEN_BUMPY_ROADS_PORT: ClassVar[Optional[int]] = None
    CONN_CH_FROZEN_BUMPY_ROADS_DB_MAME: ClassVar[Optional[str]] = None
    CONN_CH_FROZEN_BUMPY_ROADS_USERNAME: ClassVar[Optional[str]] = None
    CONN_CH_FROZEN_BUMPY_ROADS_USE_MANAGED_NETWORK: ClassVar[Optional[bool]] = None
    CONN_CH_FROZEN_BUMPY_ROADS_ALLOWED_TABLES: ClassVar[Optional[list[str]]] = None
    CONN_CH_FROZEN_BUMPY_ROADS_SUBSELECT_TEMPLATES: ClassVar[Optional[tuple[dict[str, str], ...]]] = None

    @classmethod
    def connector_name(cls) -> str:
        return 'CH_FROZEN_BUMPY_ROADS'


class ConnectorsDataCHFrozenCovidBase(ConnectorsDataBase):
    CONN_CH_FROZEN_COVID_HOST: ClassVar[Optional[str]] = None
    CONN_CH_FROZEN_COVID_PORT: ClassVar[Optional[int]] = None
    CONN_CH_FROZEN_COVID_DB_MAME: ClassVar[Optional[str]] = None
    CONN_CH_FROZEN_COVID_USERNAME: ClassVar[Optional[str]] = None
    CONN_CH_FROZEN_COVID_USE_MANAGED_NETWORK: ClassVar[Optional[bool]] = None
    CONN_CH_FROZEN_COVID_ALLOWED_TABLES: ClassVar[Optional[list[str]]] = None
    CONN_CH_FROZEN_COVID_SUBSELECT_TEMPLATES: ClassVar[Optional[tuple[dict[str, str], ...]]] = None

    @classmethod
    def connector_name(cls) -> str:
        return 'CH_FROZEN_COVID'


class ConnectorsDataCHFrozenDemoBase(ConnectorsDataBase):
    CONN_CH_FROZEN_DEMO_HOST: ClassVar[Optional[str]] = None
    CONN_CH_FROZEN_DEMO_PORT: ClassVar[Optional[int]] = None
    CONN_CH_FROZEN_DEMO_DB_MAME: ClassVar[Optional[str]] = None
    CONN_CH_FROZEN_DEMO_USERNAME: ClassVar[Optional[str]] = None
    CONN_CH_FROZEN_DEMO_USE_MANAGED_NETWORK: ClassVar[Optional[bool]] = None
    CONN_CH_FROZEN_DEMO_ALLOWED_TABLES: ClassVar[Optional[list[str]]] = None
    CONN_CH_FROZEN_DEMO_SUBSELECT_TEMPLATES: ClassVar[Optional[tuple[dict[str, str], ...]]] = None
    CONN_CH_FROZEN_DEMO_RAW_SQL_LEVEL: ClassVar[Optional[tuple[dict[str, str], ...]]] = None
    CONN_CH_FROZEN_DEMO_PASS_DB_QUERY_TO_USER: ClassVar[Optional[bool]] = None

    @classmethod
    def connector_name(cls) -> str:
        return 'CH_FROZEN_DEMO'


class ConnectorsDataCHFrozenDTPBase(ConnectorsDataBase):
    CONN_CH_FROZEN_DTP_HOST: ClassVar[Optional[str]] = None
    CONN_CH_FROZEN_DTP_PORT: ClassVar[Optional[int]] = None
    CONN_CH_FROZEN_DTP_DB_MAME: ClassVar[Optional[str]] = None
    CONN_CH_FROZEN_DTP_USERNAME: ClassVar[Optional[str]] = None
    CONN_CH_FROZEN_DTP_USE_MANAGED_NETWORK: ClassVar[Optional[bool]] = None
    CONN_CH_FROZEN_DTP_ALLOWED_TABLES: ClassVar[Optional[list[str]]] = None
    CONN_CH_FROZEN_DTP_SUBSELECT_TEMPLATES: ClassVar[Optional[tuple[dict[str, str], ...]]] = None

    @classmethod
    def connector_name(cls) -> str:
        return 'CH_FROZEN_DTP'


class ConnectorsDataCHFrozenGKHBase(ConnectorsDataBase):
    CONN_CH_FROZEN_GKH_HOST: ClassVar[Optional[str]] = None
    CONN_CH_FROZEN_GKH_PORT: ClassVar[Optional[int]] = None
    CONN_CH_FROZEN_GKH_DB_MAME: ClassVar[Optional[str]] = None
    CONN_CH_FROZEN_GKH_USERNAME: ClassVar[Optional[str]] = None
    CONN_CH_FROZEN_GKH_USE_MANAGED_NETWORK: ClassVar[Optional[bool]] = None
    CONN_CH_FROZEN_GKH_ALLOWED_TABLES: ClassVar[Optional[list[str]]] = None
    CONN_CH_FROZEN_GKH_SUBSELECT_TEMPLATES: ClassVar[Optional[tuple[dict[str, str], ...]]] = None

    @classmethod
    def connector_name(cls) -> str:
        return 'CH_FROZEN_GKH'


class ConnectorsDataCHFrozenSamplesBase(ConnectorsDataBase):
    CONN_CH_FROZEN_SAMPLES_HOST: ClassVar[Optional[str]] = None
    CONN_CH_FROZEN_SAMPLES_PORT: ClassVar[Optional[int]] = None
    CONN_CH_FROZEN_SAMPLES_DB_MAME: ClassVar[Optional[str]] = None
    CONN_CH_FROZEN_SAMPLES_USERNAME: ClassVar[Optional[str]] = None
    CONN_CH_FROZEN_SAMPLES_USE_MANAGED_NETWORK: ClassVar[Optional[bool]] = None
    CONN_CH_FROZEN_SAMPLES_ALLOWED_TABLES: ClassVar[Optional[list[str]]] = None
    CONN_CH_FROZEN_SAMPLES_SUBSELECT_TEMPLATES: ClassVar[Optional[tuple[dict[str, str], ...]]] = None

    @classmethod
    def connector_name(cls) -> str:
        return 'CH_FROZEN_SAMPLES'


class ConnectorsDataCHFrozenTransparencyBase(ConnectorsDataBase):
    CONN_CH_FROZEN_TRANSPARENCY_HOST: ClassVar[Optional[str]] = None
    CONN_CH_FROZEN_TRANSPARENCY_PORT: ClassVar[Optional[int]] = None
    CONN_CH_FROZEN_TRANSPARENCY_DB_MAME: ClassVar[Optional[str]] = None
    CONN_CH_FROZEN_TRANSPARENCY_USERNAME: ClassVar[Optional[str]] = None
    CONN_CH_FROZEN_TRANSPARENCY_USE_MANAGED_NETWORK: ClassVar[Optional[bool]] = None
    CONN_CH_FROZEN_TRANSPARENCY_ALLOWED_TABLES: ClassVar[Optional[list[str]]] = None
    CONN_CH_FROZEN_TRANSPARENCY_SUBSELECT_TEMPLATES: ClassVar[Optional[tuple[dict[str, str], ...]]] = None

    @classmethod
    def connector_name(cls) -> str:
        return 'CH_FROZEN_TRANSPARENCY'


class ConnectorsDataCHFrozenWeatherBase(ConnectorsDataBase):
    CONN_CH_FROZEN_WEATHER_HOST: ClassVar[Optional[str]] = None
    CONN_CH_FROZEN_WEATHER_PORT: ClassVar[Optional[int]] = None
    CONN_CH_FROZEN_WEATHER_DB_MAME: ClassVar[Optional[str]] = None
    CONN_CH_FROZEN_WEATHER_USERNAME: ClassVar[Optional[str]] = None
    CONN_CH_FROZEN_WEATHER_USE_MANAGED_NETWORK: ClassVar[Optional[bool]] = None
    CONN_CH_FROZEN_WEATHER_ALLOWED_TABLES: ClassVar[Optional[list[str]]] = None
    CONN_CH_FROZEN_WEATHER_SUBSELECT_TEMPLATES: ClassVar[Optional[tuple[dict[str, str], ...]]] = None

    @classmethod
    def connector_name(cls) -> str:
        return 'CH_FROZEN_WEATHER'


class ConnectorsDataCHFrozenHorecaBase(ConnectorsDataBase):
    CONN_CH_FROZEN_HORECA_HOST: ClassVar[Optional[str]] = None
    CONN_CH_FROZEN_HORECA_PORT: ClassVar[Optional[int]] = None
    CONN_CH_FROZEN_HORECA_DB_MAME: ClassVar[Optional[str]] = None
    CONN_CH_FROZEN_HORECA_USERNAME: ClassVar[Optional[str]] = None
    CONN_CH_FROZEN_HORECA_USE_MANAGED_NETWORK: ClassVar[Optional[bool]] = None
    CONN_CH_FROZEN_HORECA_ALLOWED_TABLES: ClassVar[Optional[list[str]]] = None
    CONN_CH_FROZEN_HORECA_SUBSELECT_TEMPLATES: ClassVar[Optional[tuple[dict[str, str], ...]]] = None

    @classmethod
    def connector_name(cls) -> str:
        return 'CH_FROZEN_HORECA'


class ConnectorsDataCHYTBase(ConnectorsDataBase):
    CONN_CHYT_PUBLIC_CLIQUES: ClassVar[Optional[tuple[str]]] = None
    CONN_CHYT_FORBIDDEN_CLIQUES: ClassVar[Optional[tuple[str]]] = None
    CONN_CHYT_DEFAULT_CLIQUE: ClassVar[Optional[str]] = None

    @classmethod
    def connector_name(cls) -> str:
        return 'CHYT'


class ConnectorsDataYQBase(ConnectorsDataBase):
    CONN_YQ_HOST: ClassVar[Optional[str]] = None
    CONN_YQ_PORT: ClassVar[Optional[int]] = None
    CONN_YQ_DB_NAME: ClassVar[Optional[str]] = None

    @classmethod
    def connector_name(cls) -> str:
        return 'YQ'


class ConnectorsDataMusicBase(ConnectorsDataBase):
    CONN_MUSIC_HOST: ClassVar[Optional[str]] = None
    CONN_MUSIC_PORT: ClassVar[Optional[int]] = None
    CONN_MUSIC_DB_MAME: ClassVar[Optional[str]] = None
    CONN_MUSIC_USERNAME: ClassVar[Optional[str]] = None
    conn_music_use_managed_network: ClassVar[Optional[bool]] = None
    conn_music_allowed_tables: ClassVar[Optional[list[str]]] = None
    conn_music_subselect_templates: ClassVar[Optional[tuple[dict[str, str], ...]]] = None

    @classmethod
    def connector_name(cls) -> str:
        return 'CH_YA_MUSIC_PODCAST_STATS'


class ConnectorsDataBillingBase(ConnectorsDataBase):
    CONN_BILLING_HOST: ClassVar[Optional[str]] = None
    CONN_BILLING_PORT: ClassVar[Optional[int]] = None
    CONN_BILLING_DB_MAME: ClassVar[Optional[str]] = None
    CONN_BILLING_USERNAME: ClassVar[Optional[str]] = None
    CONN_BILLING_USE_MANAGED_NETWORK: ClassVar[Optional[bool]] = None
    CONN_BILLING_ALLOWED_TABLES: ClassVar[Optional[list[str]]] = None
    CONN_BILLING_SUBSELECT_TEMPLATES: ClassVar[Optional[tuple[dict[str, str], ...]]] = None

    @classmethod
    def connector_name(cls) -> str:
        return 'CH_BILLING_ANALYTICS'


class ConnectorsDataFileBase(ConnectorsDataBase):
    CONN_FILE_CH_HOST: ClassVar[Optional[str]] = None
    CONN_FILE_CH_PORT: ClassVar[int] = 8443
    CONN_FILE_CH_USERNAME: ClassVar[Optional[str]] = None

    @classmethod
    def connector_name(cls) -> str:
        return 'FILE'


class ConnectorsDataMarketCouriersBase(ConnectorsDataBase):
    CONN_MARKET_COURIERS_HOST: ClassVar[Optional[str]] = None
    CONN_MARKET_COURIERS_PORT: ClassVar[Optional[int]] = None
    CONN_MARKET_COURIERS_DB_MAME: ClassVar[Optional[str]] = None
    CONN_MARKET_COURIERS_USERNAME: ClassVar[Optional[str]] = None
    CONN_MARKET_COURIERS_USE_MANAGED_NETWORK: ClassVar[Optional[bool]] = None
    CONN_MARKET_COURIERS_ALLOWED_TABLES: ClassVar[Optional[list[str]]] = None
    CONN_MARKET_COURIERS_SUBSELECT_TEMPLATES: ClassVar[Optional[tuple[dict[str, str], ...]]] = None

    @classmethod
    def connector_name(cls) -> str:
        return 'MARKET_COURIERS'


class ConnectorsDataSchoolbookBase(ConnectorsDataBase):
    CONN_SCHOOLBOOK_HOST: ClassVar[Optional[str]] = None
    CONN_SCHOOLBOOK_PORT: ClassVar[Optional[int]] = None
    CONN_SCHOOLBOOK_DB_MAME: ClassVar[Optional[str]] = None
    CONN_SCHOOLBOOK_USERNAME: ClassVar[Optional[str]] = None
    CONN_SCHOOLBOOK_USE_MANAGED_NETWORK: ClassVar[Optional[bool]] = None
    CONN_SCHOOLBOOK_ALLOWED_TABLES: ClassVar[Optional[list[str]]] = None
    CONN_SCHOOLBOOK_SUBSELECT_TEMPLATES: ClassVar[Optional[tuple[dict[str, str], ...]]] = None

    @classmethod
    def connector_name(cls) -> str:
        return 'SCHOOLBOOK'


class ConnectorsDataSMBHeatmapsBase(ConnectorsDataBase):
    CONN_SMB_HEATMAPS_HOST: ClassVar[Optional[str]] = None
    CONN_SMB_HEATMAPS_PORT: ClassVar[Optional[int]] = None
    CONN_SMB_HEATMAPS_DB_MAME: ClassVar[Optional[str]] = None
    CONN_SMB_HEATMAPS_USERNAME: ClassVar[Optional[str]] = None
    CONN_SMB_HEATMAPS_USE_MANAGED_NETWORK: ClassVar[Optional[bool]] = None
    CONN_SMB_HEATMAPS_ALLOWED_TABLES: ClassVar[Optional[list[str]]] = None
    CONN_SMB_HEATMAPS_SUBSELECT_TEMPLATES: ClassVar[Optional[tuple[dict[str, str], ...]]] = None

    @classmethod
    def connector_name(cls) -> str:
        return 'SMB_HEATMAPS'


class ConnectorsDataMoyskladBase(ConnectorsDataBase):
    CONN_MOYSKLAD_HOST: ClassVar[Optional[str]] = None
    CONN_MOYSKLAD_PORT: ClassVar[Optional[int]] = None
    CONN_MOYSKLAD_USERNAME: ClassVar[Optional[str]] = None
    CONN_MOYSKLAD_USE_MANAGED_NETWORK: ClassVar[Optional[bool]] = None

    @classmethod
    def connector_name(cls) -> str:
        return 'MOYSKLAD'


class ConnectorsDataEqueoBase(ConnectorsDataBase):
    CONN_EQUEO_HOST: ClassVar[Optional[str]] = None
    CONN_EQUEO_PORT: ClassVar[Optional[int]] = None
    CONN_EQUEO_USERNAME: ClassVar[Optional[str]] = None
    CONN_EQUEO_USE_MANAGED_NETWORK: ClassVar[Optional[bool]] = None

    @classmethod
    def connector_name(cls) -> str:
        return 'EQUEO'


class ConnectorsDataKonturMarketBase(ConnectorsDataBase):
    CONN_KONTUR_MARKET_HOST: ClassVar[Optional[str]] = None
    CONN_KONTUR_MARKET_PORT: ClassVar[Optional[int]] = None
    CONN_KONTUR_MARKET_USERNAME: ClassVar[Optional[str]] = None
    CONN_KONTUR_MARKET_USE_MANAGED_NETWORK: ClassVar[Optional[bool]] = None

    @classmethod
    def connector_name(cls) -> str:
        return 'KONTUR_MARKET'


class ConnectorsDataBitrixBase(ConnectorsDataBase):
    CONN_BITRIX_HOST: ClassVar[Optional[str]] = None
    CONN_BITRIX_PORT: ClassVar[Optional[int]] = None
    CONN_BITRIX_USERNAME: ClassVar[Optional[str]] = None
    CONN_BITRIX_USE_MANAGED_NETWORK: ClassVar[Optional[bool]] = None

    @classmethod
    def connector_name(cls) -> str:
        return 'BITRIX'


class ConnectorsDataMonitoringBase(ConnectorsDataBase):
    CONN_MONITORING_HOST: ClassVar[Optional[str]] = None

    @classmethod
    def connector_name(cls) -> str:
        return 'MONITORING'


class ConnectorsDataCHFrozenBumpyRoadsExtProduction(ConnectorsDataCHFrozenBumpyRoadsBase):
    CONN_CH_FROZEN_BUMPY_ROADS_HOST: ClassVar[Optional[str]] = ','.join(CH_EXT_DATA_HOSTS)
    CONN_CH_FROZEN_BUMPY_ROADS_PORT: ClassVar[Optional[int]] = 8443
    CONN_CH_FROZEN_BUMPY_ROADS_DB_MAME: ClassVar[Optional[str]] = 'bumpy_roads'
    CONN_CH_FROZEN_BUMPY_ROADS_USERNAME: ClassVar[Optional[str]] = 'bumpy_roads_user_ro'
    CONN_CH_FROZEN_BUMPY_ROADS_USE_MANAGED_NETWORK: ClassVar[Optional[bool]] = False
    CONN_CH_FROZEN_BUMPY_ROADS_ALLOWED_TABLES: ClassVar[Optional[list[str]]] = ['slow_polygons']
    CONN_CH_FROZEN_BUMPY_ROADS_SUBSELECT_TEMPLATES: ClassVar[Optional[tuple[dict[str, str], ...]]] = ()


class ConnectorsDataCHFrozenCovidExtProduction(ConnectorsDataCHFrozenCovidBase):
    CONN_CH_FROZEN_COVID_HOST: ClassVar[Optional[str]] = ','.join(CH_SAMPLES_HOSTS)
    CONN_CH_FROZEN_COVID_PORT: ClassVar[Optional[int]] = 8443
    CONN_CH_FROZEN_COVID_DB_MAME: ClassVar[Optional[str]] = 'covid_public'
    CONN_CH_FROZEN_COVID_USERNAME: ClassVar[Optional[str]] = 'covid_public'
    CONN_CH_FROZEN_COVID_USE_MANAGED_NETWORK: ClassVar[Optional[bool]] = False
    CONN_CH_FROZEN_COVID_ALLOWED_TABLES: ClassVar[Optional[list[str]]] = [
        'iso_data_daily',
        'iso_data_hourly_with_history',
        'marker_share_by_region',
        'marker_share_by_region_stat',
        'russia_stat',
        'world_stat',
    ]
    CONN_CH_FROZEN_COVID_SUBSELECT_TEMPLATES: ClassVar[Optional[tuple[dict[str, str], ...]]] = ()


class ConnectorsDataCHFrozenDemoExtProduction(ConnectorsDataCHFrozenDemoBase):
    CONN_CH_FROZEN_DEMO_HOST: ClassVar[Optional[str]] = ','.join(CH_SAMPLES_HOSTS)
    CONN_CH_FROZEN_DEMO_PORT: ClassVar[Optional[int]] = 8443
    CONN_CH_FROZEN_DEMO_DB_MAME: ClassVar[Optional[str]] = 'samples'
    CONN_CH_FROZEN_DEMO_USERNAME: ClassVar[Optional[str]] = 'samples_user'
    CONN_CH_FROZEN_DEMO_USE_MANAGED_NETWORK: ClassVar[Optional[bool]] = False
    CONN_CH_FROZEN_DEMO_ALLOWED_TABLES: ClassVar[Optional[list[str]]] = [
        'bike_routes',
        'MS_Clients',
        'MS_MOSCOW_GEO',
        'MS_Products',
        'MS_SalesFacts_up',
        'MS_Shops',
        'run_routes',
        'TreeSample',
    ]
    CONN_CH_FROZEN_DEMO_SUBSELECT_TEMPLATES: ClassVar[Optional[tuple[dict[str, str], ...]]] = ()
    CONN_CH_FROZEN_DEMO_RAW_SQL_LEVEL: ClassVar[RawSQLLevel] = RawSQLLevel.dashsql
    CONN_CH_FROZEN_DEMO_PASS_DB_QUERY_TO_USER: ClassVar[bool] = True


class ConnectorsDataCHFrozenDTPExtProduction(ConnectorsDataCHFrozenDTPBase):
    CONN_CH_FROZEN_DTP_HOST: ClassVar[Optional[str]] = ','.join(CH_EXT_DATA_HOSTS)
    CONN_CH_FROZEN_DTP_PORT: ClassVar[Optional[int]] = 8443
    CONN_CH_FROZEN_DTP_DB_MAME: ClassVar[Optional[str]] = 'dtp_mos'
    CONN_CH_FROZEN_DTP_USERNAME: ClassVar[Optional[str]] = 'dtp_mos_user_ro'
    CONN_CH_FROZEN_DTP_USE_MANAGED_NETWORK: ClassVar[Optional[bool]] = False
    CONN_CH_FROZEN_DTP_ALLOWED_TABLES: ClassVar[Optional[list[str]]] = ['points', 'regions']
    CONN_CH_FROZEN_DTP_SUBSELECT_TEMPLATES: ClassVar[Optional[tuple[dict[str, str], ...]]] = ()


class ConnectorsDataCHFrozenGKHExtProduction(ConnectorsDataCHFrozenGKHBase):
    CONN_CH_FROZEN_GKH_HOST: ClassVar[Optional[str]] = ','.join(CH_DATALENS_EXT_DATA_HOSTS)
    CONN_CH_FROZEN_GKH_PORT: ClassVar[Optional[int]] = 8443
    CONN_CH_FROZEN_GKH_DB_MAME: ClassVar[Optional[str]] = 'gkh'
    CONN_CH_FROZEN_GKH_USERNAME: ClassVar[Optional[str]] = 'gkh_user'
    CONN_CH_FROZEN_GKH_USE_MANAGED_NETWORK: ClassVar[Optional[bool]] = False
    CONN_CH_FROZEN_GKH_ALLOWED_TABLES: ClassVar[Optional[list[str]]] = ['GKH_Alarm']
    CONN_CH_FROZEN_GKH_SUBSELECT_TEMPLATES: ClassVar[Optional[tuple[dict[str, str], ...]]] = ()


class ConnectorsDataCHFrozenSamplesExtProduction(ConnectorsDataCHFrozenSamplesBase):
    CONN_CH_FROZEN_SAMPLES_HOST: ClassVar[Optional[str]] = ','.join(CH_SAMPLES_HOSTS)
    CONN_CH_FROZEN_SAMPLES_PORT: ClassVar[Optional[int]] = 8443
    CONN_CH_FROZEN_SAMPLES_DB_MAME: ClassVar[Optional[str]] = 'samples'
    CONN_CH_FROZEN_SAMPLES_USERNAME: ClassVar[Optional[str]] = 'samples_user'
    CONN_CH_FROZEN_SAMPLES_USE_MANAGED_NETWORK: ClassVar[Optional[bool]] = False
    CONN_CH_FROZEN_SAMPLES_ALLOWED_TABLES: ClassVar[Optional[list[str]]] = ['SampleSuperstore']
    CONN_CH_FROZEN_SAMPLES_SUBSELECT_TEMPLATES: ClassVar[Optional[tuple[dict[str, str], ...]]] = ()


class ConnectorsDataCHFrozenTransparencyExtProduction(ConnectorsDataCHFrozenTransparencyBase):
    CONN_CH_FROZEN_TRANSPARENCY_HOST: ClassVar[Optional[str]] = ','.join(CH_EXT_DATA_HOSTS)
    CONN_CH_FROZEN_TRANSPARENCY_PORT: ClassVar[Optional[int]] = 8443
    CONN_CH_FROZEN_TRANSPARENCY_DB_MAME: ClassVar[Optional[str]] = 'transparency_report'
    CONN_CH_FROZEN_TRANSPARENCY_USERNAME: ClassVar[Optional[str]] = 'transparency_report_user_ro'
    CONN_CH_FROZEN_TRANSPARENCY_USE_MANAGED_NETWORK: ClassVar[Optional[bool]] = False
    CONN_CH_FROZEN_TRANSPARENCY_ALLOWED_TABLES: ClassVar[Optional[list[str]]] = [
        'summary_stats_direct_transparency_report',
        'charity_mono_direct_transparency_report',
    ]
    CONN_CH_FROZEN_TRANSPARENCY_SUBSELECT_TEMPLATES: ClassVar[Optional[tuple[dict[str, str], ...]]] = ()


class ConnectorsDataCHFrozenWeatherExtProduction(ConnectorsDataCHFrozenWeatherBase):
    CONN_CH_FROZEN_WEATHER_HOST: ClassVar[Optional[str]] = ','.join(CH_DATALENS_EXT_DATA_HOSTS)
    CONN_CH_FROZEN_WEATHER_PORT: ClassVar[Optional[int]] = 8443
    CONN_CH_FROZEN_WEATHER_DB_MAME: ClassVar[Optional[str]] = 'weather'
    CONN_CH_FROZEN_WEATHER_USERNAME: ClassVar[Optional[str]] = 'dl_user'
    CONN_CH_FROZEN_WEATHER_USE_MANAGED_NETWORK: ClassVar[Optional[bool]] = False
    CONN_CH_FROZEN_WEATHER_ALLOWED_TABLES: ClassVar[Optional[list[str]]] = ['weather_stats']
    CONN_CH_FROZEN_WEATHER_SUBSELECT_TEMPLATES: ClassVar[Optional[tuple[dict[str, str], ...]]] = ()


class ConnectorsDataCHFrozenHorecaExtProduction(ConnectorsDataCHFrozenHorecaBase):
    CONN_CH_FROZEN_HORECA_HOST: ClassVar[Optional[str]] = ','.join(CH_DATALENS_EXT_DATA_HOSTS)
    CONN_CH_FROZEN_HORECA_PORT: ClassVar[Optional[int]] = 8443
    CONN_CH_FROZEN_HORECA_DB_MAME: ClassVar[Optional[str]] = 'horeca'
    CONN_CH_FROZEN_HORECA_USERNAME: ClassVar[Optional[str]] = 'horeca_user'
    CONN_CH_FROZEN_HORECA_USE_MANAGED_NETWORK: ClassVar[Optional[bool]] = False
    CONN_CH_FROZEN_HORECA_ALLOWED_TABLES: ClassVar[Optional[list[str]]] = ['MosRest']
    CONN_CH_FROZEN_HORECA_SUBSELECT_TEMPLATES: ClassVar[Optional[tuple[dict[str, str], ...]]] = ()


class ConnectorsDataCHYTInternalInstallation(ConnectorsDataCHYTBase):
    CONN_CHYT_PUBLIC_CLIQUES = ('*ch_datalens',)
    CONN_CHYT_FORBIDDEN_CLIQUES = ('*ch_public',)
    CONN_CHYT_DEFAULT_CLIQUE = None


class ConnectorsDataCHYTExternalInstallation(ConnectorsDataCHYTBase):
    CONN_CHYT_PUBLIC_CLIQUES = tuple()
    CONN_CHYT_FORBIDDEN_CLIQUES = tuple()
    CONN_CHYT_DEFAULT_CLIQUE = '*ch_public'


class ConnectorsDataYQInternalInstallation(ConnectorsDataYQBase):
    CONN_YQ_HOST: ClassVar[str] = 'grpcs://grpc.yandex-query.cloud-preprod.yandex.net'
    CONN_YQ_PORT: ClassVar[int] = 2135
    CONN_YQ_DB_NAME: ClassVar[str] = '/root/default'


class ConnectorsDataFileIntTesting(ConnectorsDataFileBase):
    CONN_FILE_CH_HOST: ClassVar[str] = ','.join((
        'sas-ghdocmjr80kgah7o.db.yandex.net',
        'vla-pndkiq7cur9e2gkl.db.yandex.net',
    ))
    CONN_FILE_CH_USERNAME: ClassVar[str] = 'user1'


class ConnectorsDataFileIntProduction(ConnectorsDataFileBase):
    CONN_FILE_CH_HOST: ClassVar[str] = ','.join((
        'vla-7j0q86rpm2ub4031.db.yandex.net',
        'sas-ht1ld2emok8n2gl4.db.yandex.net',
    ))
    CONN_FILE_CH_USERNAME: ClassVar[str] = 'dl_ch_over_s3'


class ConnectorsDataYQExternalInstallation(ConnectorsDataYQBase):
    CONN_YQ_PORT: ClassVar[int] = 2135
    CONN_YQ_DB_NAME: ClassVar[str] = '/root/default'


class ConnectorsDataMarketCouriersExternalInstallation(ConnectorsDataMarketCouriersBase):
    CONN_MARKET_COURIERS_HOST: ClassVar[Optional[str]] = 'rc1b-vsqgf0n3qa54stsp.mdb.yandexcloud.net'
    CONN_MARKET_COURIERS_PORT: ClassVar[Optional[int]] = 8443
    CONN_MARKET_COURIERS_DB_MAME: ClassVar[Optional[str]] = 'tpl_partner_info'
    CONN_MARKET_COURIERS_USERNAME: ClassVar[Optional[str]] = 'datalens'
    CONN_MARKET_COURIERS_USE_MANAGED_NETWORK: ClassVar[Optional[bool]] = True
    CONN_MARKET_COURIERS_ALLOWED_TABLES: ClassVar[Optional[list[str]]] = []
    CONN_MARKET_COURIERS_SUBSELECT_TEMPLATES: ClassVar[tuple[dict[str, str], ...]] = (
        dict(title='Courier Rating', sql_query=SQL_MARKET_COURIERS_COUR_RATING),
        dict(title='KPI Partner', sql_query=SQL_MARKET_COURIERS_KPI_PARTNER),
        dict(title='Nevyhod', sql_query=SQL_MARKET_COURIERS_NEVIHOD),
        dict(title='Obuchaemost', sql_query=SQL_MARKET_COURIERS_OBYCHAEMOST),
        dict(title='Dropoff Orders', sql_query=SQL_MARKET_COURIERS_DROPOFF_ORDERS),
    )


class ConnectorsDataMusicExternalInstallation(ConnectorsDataMusicBase):
    CONN_MUSIC_HOST: ClassVar[str] = ','.join((
        'rc1a-2pq5rc6042voc1qz.mdb.yandexcloud.net',
        'rc1b-bxkl9g3xyl0vqh5q.mdb.yandexcloud.net',
        'rc1c-4fvx578tidghynm0.mdb.yandexcloud.net',
    ))
    CONN_MUSIC_PORT: ClassVar[int] = 8443
    CONN_MUSIC_DB_MAME: ClassVar[str] = 'music'
    CONN_MUSIC_USERNAME: ClassVar[str] = 'datalens_podcast_connection_user'
    CONN_MUSIC_USE_MANAGED_NETWORK: ClassVar[bool] = True
    CONN_MUSIC_ALLOWED_TABLES: ClassVar[Optional[list[str]]] = []
    CONN_MUSIC_SUBSELECT_TEMPLATES: ClassVar[tuple[dict[str, str], ...]] = (
        dict(title='Streams', sql_query=SQL_YA_MUSIC_PODCASTS_STATS_STREAMS),
        dict(title='Subscribers', sql_query=SQL_YA_MUSIC_PODCASTS_STATS_SUBSCRIBERS),
    )


class ConnectorsDataBillingExternalInstallation(ConnectorsDataBillingBase):
    CONN_BILLING_PORT: ClassVar[Optional[int]] = 8443
    CONN_BILLING_DB_MAME: ClassVar[str] = 'billing'
    CONN_BILLING_USERNAME: ClassVar[str] = 'datalens'
    CONN_BILLING_USE_MANAGED_NETWORK: ClassVar[Optional[bool]] = True
    CONN_BILLING_ALLOWED_TABLES: ClassVar[Optional[list[str]]] = []
    CONN_BILLING_SUBSELECT_TEMPLATES: ClassVar[tuple[dict[str, str], ...]] = (
        dict(title='billing_report', sql_query=SQL_BILLING_REPORTD),
        dict(title='billing_report_with_labels', sql_query=SQL_BILLING_LABELS_MAPD),
    )


class ConnectorsDataSMBHeatmapsExternalInstallation(ConnectorsDataSMBHeatmapsBase):
    CONN_SMB_HEATMAPS_HOST: ClassVar[Optional[str]] = 'rc1b-hm51k5geb4dwn2uk.mdb.yandexcloud.net'
    CONN_SMB_HEATMAPS_PORT: ClassVar[Optional[int]] = 8443
    CONN_SMB_HEATMAPS_DB_MAME: ClassVar[Optional[str]] = 'smb_autorecommendation'
    CONN_SMB_HEATMAPS_USERNAME: ClassVar[Optional[str]] = 'datalens-autorecom'
    CONN_SMB_HEATMAPS_USE_MANAGED_NETWORK: ClassVar[Optional[bool]] = True
    CONN_SMB_HEATMAPS_ALLOWED_TABLES: ClassVar[Optional[list[str]]] = []
    CONN_SMB_HEATMAPS_SUBSELECT_TEMPLATES: ClassVar[Optional[tuple[dict[str, str], ...]]] = (
        dict(title='Heatmaps', sql_query=SQL_SMB_HEATMAPS),
    )


class ConnectorsDataMoyskladExternalInstallation(ConnectorsDataMoyskladBase):
    CONN_MOYSKLAD_USE_MANAGED_NETWORK: ClassVar[Optional[bool]] = True
    CONN_MOYSKLAD_PORT: ClassVar[Optional[int]] = 8443


class ConnectorsDataEqueoExternalInstallation(ConnectorsDataEqueoBase):
    CONN_EQUEO_HOST: ClassVar[Optional[str]] = 'rc1a-0k8s9imssp6h7vmi.mdb.yandexcloud.net'
    CONN_EQUEO_PORT: ClassVar[Optional[int]] = 8443
    CONN_EQUEO_USERNAME: ClassVar[Optional[str]] = 'datalens'
    CONN_EQUEO_USE_MANAGED_NETWORK: ClassVar[Optional[bool]] = True


class ConnectorsDataKonturMarketExternalInstallation(ConnectorsDataKonturMarketBase):
    CONN_KONTUR_MARKET_HOST: ClassVar[Optional[str]] = 'rc1a-b1poaasd36s83dqp.mdb.yandexcloud.net'
    CONN_KONTUR_MARKET_PORT: ClassVar[Optional[int]] = 8443
    CONN_KONTUR_MARKET_USERNAME: ClassVar[Optional[str]] = 'datalens'
    CONN_KONTUR_MARKET_USE_MANAGED_NETWORK: ClassVar[Optional[bool]] = True


class ConnectorsDataBitrixExternalInstallation(ConnectorsDataBitrixBase):
    CONN_BITRIX_HOST: ClassVar[Optional[str]] = 'rc1a-fwit5p613vrgracz.mdb.yandexcloud.net'
    CONN_BITRIX_USERNAME: ClassVar[Optional[str]] = 'datalens'
    CONN_BITRIX_PORT: ClassVar[Optional[int]] = 8443


class ConnectorsDataYQExtTesting(ConnectorsDataYQExternalInstallation):
    CONN_YQ_HOST: ClassVar[str] = 'grpcs://grpc.yandex-query.cloud-preprod.yandex.net'


class ConnectorsDataMusicExtTesting(ConnectorsDataMusicExternalInstallation):
    CONN_MUSIC_USE_MANAGED_NETWORK: ClassVar[bool] = False


class ConnectorsDataFileExtTesting(ConnectorsDataFileBase):
    # materialization ch cluster
    # https://console-preprod.cloud.yandex.ru/folders/aoevv1b69su5144mlro3/managed-clickhouse/cluster/e4umim6f3jr14ois49i9
    CONN_FILE_CH_HOST: ClassVar[str] = ','.join((
        'rc1a-qr19go7vdcaukxsd.mdb.cloud-preprod.yandex.net',
        'rc1c-8afsacczmoytzlo5.mdb.cloud-preprod.yandex.net',
    ))
    CONN_FILE_CH_USERNAME: ClassVar[str] = 'dl_file_conn'


class ConnectorsDataBillingExtTesting(ConnectorsDataBillingExternalInstallation):
    CONN_BILLING_HOST: ClassVar[Optional[str]] = ','.join((
        'rc1a-rvwp0jt22z68ybre.mdb.cloud-preprod.yandex.net',
        'rc1b-6kjlsmsl0l7u5308.mdb.cloud-preprod.yandex.net',
        'rc1b-xtsv1236xrz6gd20.mdb.cloud-preprod.yandex.net',
        'rc1c-ct03ung4rmxwqg7f.mdb.cloud-preprod.yandex.net',
    ))


class ConnectorsDataMarketCouriersExtTesting(ConnectorsDataMarketCouriersExternalInstallation):
    CONN_MARKET_COURIERS_USE_MANAGED_NETWORK: ClassVar[Optional[bool]] = False


class ConnectorsDataSMBHeatmapsExtTesting(ConnectorsDataSMBHeatmapsExternalInstallation):
    CONN_SMB_HEATMAPS_USE_MANAGED_NETWORK: ClassVar[Optional[bool]] = False


class ConnectorsDataSchoolbookExtTesting(ConnectorsDataSchoolbookBase):
    CONN_SCHOOLBOOK_HOST: ClassVar[Optional[str]] = ','.join((
        'vla-ws8isxcg383rpugb.db.yandex.net',
        'sas-0vu6ols4s7prltlm.db.yandex.net',
        'man-c4i8hlp8hp0qkmfo.db.yandex.net',
    ))
    CONN_SCHOOLBOOK_PORT: ClassVar[Optional[int]] = 8443
    CONN_SCHOOLBOOK_DB_MAME: ClassVar[Optional[str]] = 'pelican_db_prod_db'
    CONN_SCHOOLBOOK_USERNAME: ClassVar[Optional[str]] = 'datalens'
    CONN_SCHOOLBOOK_USE_MANAGED_NETWORK: ClassVar[Optional[bool]] = False
    CONN_SCHOOLBOOK_ALLOWED_TABLES: ClassVar[Optional[list[str]]] = []
    CONN_SCHOOLBOOK_SUBSELECT_TEMPLATES: ClassVar[Optional[tuple[dict[str, str], ...]]] = (
        dict(title='Stats', sql_query=SQL_SCHOOLBOOK_STATS_TESTING),
    )


class ConnectorsDataMoyskladExtTesting(ConnectorsDataMoyskladExternalInstallation):
    # https://console-preprod.cloud.yandex.ru/folders/aoen5db923vq956hvb91/managed-clickhouse/cluster/e4usmt8rjpinn0ot4kac
    CONN_MOYSKLAD_HOST: ClassVar[Optional[str]] = 'rc1a-247svhto6jy5iymw.mdb.cloud-preprod.yandex.net'
    CONN_MOYSKLAD_USERNAME: ClassVar[Optional[str]] = 'moysklad_test_db_user'


class ConnectorsDataBitrixExtTesting(ConnectorsDataBitrixExternalInstallation):
    CONN_BITRIX_USE_MANAGED_NETWORK: ClassVar[Optional[bool]] = False


class ConnectorsDataMonitoringExtTesting(ConnectorsDataMonitoringBase):
    CONN_MONITORING_HOST: ClassVar[Optional[str]] = 'monitoring.api.cloud-preprod.yandex.net'


class ConnectorsDataFileExtProduction(ConnectorsDataFileBase):
    CONN_FILE_CH_HOST: ClassVar[str] = ','.join((
        # https://console.cloud.yandex.ru/folders/b1g77mbejmj4m6flq848/managed-clickhouse/cluster/c9q7c5ibbkf7vl65h94t/view
        'rc1a-4kva30nfjn716tn1.mdb.yandexcloud.net',
        # https://console.cloud.yandex.ru/folders/b1g77mbejmj4m6flq848/managed-clickhouse/cluster/c9q1bsfsrd05c1i23nld/view
        'rc1b-bbj8ne2tk7sq512l.mdb.yandexcloud.net',
    ))
    CONN_FILE_CH_USERNAME: ClassVar[str] = 'dl_file_conn'


class ConnectorsDataYQExtProduction(ConnectorsDataYQExternalInstallation):
    CONN_YQ_HOST: ClassVar[str] = 'grpcs://prod-cp.yandex-query.cloud.yandex.net'


class ConnectorsDataBillingExtProduction(ConnectorsDataBillingExternalInstallation):
    CONN_BILLING_HOST: ClassVar[Optional[str]] = ','.join((
        'rc1a-2etxhdvhga970fqf.mdb.yandexcloud.net',
        'rc1a-ak3xqdczt9a1dd2w.mdb.yandexcloud.net',
        'rc1a-nibcwhd6jyrtizeb.mdb.yandexcloud.net',
        'rc1b-3q8jpatuw5oogiic.mdb.yandexcloud.net',
        'rc1b-jw2chwulz19i81nw.mdb.yandexcloud.net',
        'rc1b-p5o8bp5utgh67zsv.mdb.yandexcloud.net',
        'rc1c-ms0l2ch9tugpncrp.mdb.yandexcloud.net',
        'rc1c-o1njlzbp0g3db8aw.mdb.yandexcloud.net',
        'rc1c-sdayayk5iyhkrz9d.mdb.yandexcloud.net',
    ))


class ConnectorsDataSchoolbookExtProduction(ConnectorsDataSchoolbookBase):
    CONN_SCHOOLBOOK_HOST: ClassVar[Optional[str]] = ','.join((
        'rc1a-qrtjactj2uo743xg.mdb.yandexcloud.net',
        'rc1b-clt0m3lvav6zff2e.mdb.yandexcloud.net',
        'rc1c-xpdfwkqnchatdu1w.mdb.yandexcloud.net',
    ))
    CONN_SCHOOLBOOK_PORT: ClassVar[Optional[int]] = 8443
    CONN_SCHOOLBOOK_DB_MAME: ClassVar[Optional[str]] = 'pelican_db_prod_db'
    CONN_SCHOOLBOOK_USERNAME: ClassVar[Optional[str]] = 'user_prod'
    CONN_SCHOOLBOOK_USE_MANAGED_NETWORK: ClassVar[Optional[bool]] = True
    CONN_SCHOOLBOOK_ALLOWED_TABLES: ClassVar[Optional[list[str]]] = []
    CONN_SCHOOLBOOK_SUBSELECT_TEMPLATES: ClassVar[Optional[tuple[dict[str, str], ...]]] = (
        dict(title='Stats', sql_query=SQL_SCHOOLBOOK_STATS),
    )


class ConnectorsDataMoyskladExtProduction(ConnectorsDataMoyskladExternalInstallation):
    CONN_MOYSKLAD_HOST: ClassVar[Optional[str]] = 'rc1a-ttnir71yo4sxu88h.mdb.yandexcloud.net'
    CONN_MOYSKLAD_USERNAME: ClassVar[Optional[str]] = 'moysklad_readonly'


class ConnectorsDataBitrixExtProduction(ConnectorsDataBitrixExternalInstallation):
    CONN_BITRIX_USE_MANAGED_NETWORK: ClassVar[Optional[bool]] = True


class ConnectorsDataMonitoringExtProduction(ConnectorsDataMonitoringBase):
    CONN_MONITORING_HOST: ClassVar[Optional[str]] = 'monitoring.api.cloud.yandex.net'


class ConnectorsDataUsageTrackingBase(ConnectorsDataBase):
    CONN_USAGE_TRACKING_HOST: ClassVar[Optional[str]] = None
    CONN_USAGE_TRACKING_PORT: ClassVar[Optional[int]] = None
    CONN_USAGE_TRACKING_DB_NAME: ClassVar[Optional[str]] = None
    CONN_USAGE_TRACKING_USERNAME: ClassVar[Optional[str]] = None
    CONN_USAGE_TRACKING_USE_MANAGED_NETWORK: ClassVar[Optional[bool]] = None
    CONN_USAGE_TRACKING_ALLOWED_TABLES: ClassVar[Optional[list[str]]] = None
    CONN_USAGE_TRACKING_SUBSELECT_TEMPLATES: ClassVar[Optional[tuple[dict[str, str], ...]]] = None
    CONN_USAGE_TRACKING_REQUIRED_IAM_ROLE: ClassVar[Optional[str]] = None

    @classmethod
    def connector_name(self) -> str:
        return 'USAGE_TRACKING'


class ConnectorsDataUsageTrackingExternalInstallation(ConnectorsDataUsageTrackingBase):
    CONN_USAGE_TRACKING_PORT: ClassVar[Optional[int]] = 8443
    CONN_USAGE_TRACKING_DB_NAME: ClassVar[str] = 'usage_tracking'
    CONN_USAGE_TRACKING_USERNAME: ClassVar[str] = 'datalens'
    CONN_USAGE_TRACKING_USE_MANAGED_NETWORK: ClassVar[Optional[bool]] = True
    CONN_USAGE_TRACKING_ALLOWED_TABLES: ClassVar[Optional[list[str]]] = []
    CONN_USAGE_TRACKING_SUBSELECT_TEMPLATES: ClassVar[tuple[dict[str, str], ...]] = (
        dict(title='Usage Tracking', sql_query=SQL_USAGE_TRACKING),
    )
    CONN_USAGE_TRACKING_REQUIRED_IAM_ROLE: ClassVar[str] = 'datalens.instances.admin'


class ConnectorsDataUsageTrackingExtProduction(ConnectorsDataUsageTrackingExternalInstallation):
    CONN_USAGE_TRACKING_HOST = ','.join((
        'rc1a-96iufcb8j58s5nrv.mdb.yandexcloud.net',
        'rc1c-5oxkvtparu51mfno.mdb.yandexcloud.net',
    ))


class ConnectorsDataUsageTrackingExtTesting(ConnectorsDataUsageTrackingExternalInstallation):
    CONN_USAGE_TRACKING_HOST = ','.join((
        'rc1a-sj4j1pnhotgffpn1.mdb.cloud-preprod.yandex.net',
        'rc1c-2g6441917n15umos.mdb.cloud-preprod.yandex.net',
    ))


class ConnectorsDataUsageTrackingYaTeamBase(ConnectorsDataBase):
    CONN_USAGE_TRACKING_YA_TEAM_HOST: ClassVar[Optional[str]] = None
    CONN_USAGE_TRACKING_YA_TEAM_PORT: ClassVar[Optional[int]] = None
    CONN_USAGE_TRACKING_YA_TEAM_DB_NAME: ClassVar[Optional[str]] = None
    CONN_USAGE_TRACKING_YA_TEAM_USERNAME: ClassVar[Optional[str]] = None
    CONN_USAGE_TRACKING_YA_TEAM_USE_MANAGED_NETWORK: ClassVar[Optional[bool]] = None
    CONN_USAGE_TRACKING_YA_TEAM_ALLOWED_TABLES: ClassVar[Optional[list[str]]] = None
    CONN_USAGE_TRACKING_YA_TEAM_SUBSELECT_TEMPLATES: ClassVar[Optional[tuple[dict[str, str], ...]]] = None
    CONN_USAGE_TRACKING_YA_TEAM_MAX_EXECUTION_TIME: ClassVar[Optional[int]] = None

    @classmethod
    def connector_name(self) -> str:
        return 'USAGE_TRACKING_YA_TEAM'


class ConnectorsDataUsageTrackingYaTeamInternalInstallation(ConnectorsDataUsageTrackingYaTeamBase):
    CONN_USAGE_TRACKING_YA_TEAM_PORT: ClassVar[Optional[int]] = 8443
    CONN_USAGE_TRACKING_YA_TEAM_DB_NAME: ClassVar[str] = 'dataset_profile'
    CONN_USAGE_TRACKING_YA_TEAM_USERNAME: ClassVar[str] = 'ut_connector_ro'
    CONN_USAGE_TRACKING_YA_TEAM_USE_MANAGED_NETWORK: ClassVar[Optional[bool]] = True
    CONN_USAGE_TRACKING_YA_TEAM_HOST = ','.join((
        'sas-51oltegx36iwaj67.db.yandex.net',
        'vla-tlhpfl8ilqz1hchu.db.yandex.net',
    ))
    CONN_USAGE_TRACKING_YA_TEAM_ALLOWED_TABLES: ClassVar[Optional[list[str]]] = []
    CONN_USAGE_TRACKING_YA_TEAM_SUBSELECT_TEMPLATES: ClassVar[tuple[dict[str, str], ...]] = (
        dict(title='Usage Tracking', sql_query=SQL_USAGE_TRACKING_YA_TEAM),
        dict(title='Usage Tracking light', sql_query=SQL_USAGE_TRACKING_YA_TEAM_AGGREGATED),
        dict(title='Dash Stats', sql_query=SQL_USAGE_TRACKING_YA_TEAM_DASH_STATS),
    )
    CONN_USAGE_TRACKING_YA_TEAM_MAX_EXECUTION_TIME: ClassVar[Optional[int]] = 45
