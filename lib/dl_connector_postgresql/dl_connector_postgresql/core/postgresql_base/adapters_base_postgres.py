from __future__ import annotations

from typing import (
    Any,
    Dict,
    Optional,
)

import attr
import sqlalchemy.dialects.postgresql as sa_pg

from dl_core.connectors.ssl_common.adapter import BaseSSLCertAdapter
from dl_core.db.native_type import SATypeSpec

from dl_connector_postgresql.core.postgresql.constants import CONNECTION_TYPE_POSTGRES
from dl_connector_postgresql.core.postgresql_base.constants import PGEnforceCollateMode
from dl_connector_postgresql.core.postgresql_base.target_dto import PostgresConnTargetDTO


# One way to obtain this data:
# docker exec bi-api_db-postgres_1 psql -U datalens -d  bi_db -A -t -F, -c 'select oid, typname from pg_type order by oid'
OID_KNOWLEDGE_RAW = """
16,bool
17,bytea
18,char
19,name
20,int8
21,int2
22,int2vector
23,int4
24,regproc
25,text
26,oid
27,tid
28,xid
29,cid
30,oidvector
71,pg_type
75,pg_attribute
81,pg_proc
83,pg_class
114,json
142,xml
143,_xml
194,pg_node_tree
199,_json
210,smgr
600,point
601,lseg
602,path
603,box
604,polygon
628,line
629,_line
650,cidr
651,_cidr
700,float4
701,float8
702,abstime
703,reltime
704,tinterval
705,unknown
718,circle
719,_circle
790,money
791,_money
829,macaddr
869,inet
1000,_bool
1001,_bytea
1002,_char
1003,_name
1005,_int2
1006,_int2vector
1007,_int4
1008,_regproc
1009,_text
1010,_tid
1011,_xid
1012,_cid
1013,_oidvector
1014,_bpchar
1015,_varchar
1016,_int8
1017,_point
1018,_lseg
1019,_path
1020,_box
1021,_float4
1022,_float8
1023,_abstime
1024,_reltime
1025,_tinterval
1027,_polygon
1028,_oid
1033,aclitem
1034,_aclitem
1040,_macaddr
1041,_inet
1042,bpchar
1043,varchar
1082,date
1083,time
1114,timestamp
1115,_timestamp
1182,_date
1183,_time
1184,timestamptz
1185,_timestamptz
1186,interval
1187,_interval
1231,_numeric
1248,pg_database
1263,_cstring
1266,timetz
1270,_timetz
1560,bit
1561,_bit
1562,varbit
1563,_varbit
1700,numeric
1790,refcursor
2201,_refcursor
2202,regprocedure
2203,regoper
2204,regoperator
2205,regclass
2206,regtype
2207,_regprocedure
2208,_regoper
2209,_regoperator
2210,_regclass
2211,_regtype
2249,record
2275,cstring
2276,any
2277,anyarray
2278,void
2279,trigger
2280,language_handler
2281,internal
2282,opaque
2283,anyelement
2287,_record
2776,anynonarray
2842,pg_authid
2843,pg_auth_members
2949,_txid_snapshot
2950,uuid
2951,_uuid
2970,txid_snapshot
3115,fdw_handler
3500,anyenum
3614,tsvector
3615,tsquery
3642,gtsvector
3643,_tsvector
3644,_gtsvector
3645,_tsquery
3734,regconfig
3735,_regconfig
3769,regdictionary
3770,_regdictionary
3831,anyrange
3838,event_trigger
3904,int4range
3905,_int4range
3906,numrange
3907,_numrange
3908,tsrange
3909,_tsrange
3910,tstzrange
3911,_tstzrange
3912,daterange
3913,_daterange
3926,int8range
3927,_int8range
10000,pg_attrdef
10001,pg_constraint
10002,pg_inherits
10003,pg_index
10004,pg_operator
10005,pg_opfamily
10006,pg_opclass
10117,pg_am
10118,pg_amop
10522,pg_amproc
10814,pg_language
10815,pg_largeobject_metadata
10816,pg_largeobject
10817,pg_aggregate
10818,pg_statistic
10819,pg_rewrite
10820,pg_trigger
10821,pg_event_trigger
10822,pg_description
10823,pg_cast
11020,pg_enum
11021,pg_namespace
11022,pg_conversion
11023,pg_depend
11024,pg_db_role_setting
11025,pg_tablespace
11026,pg_pltemplate
11027,pg_shdepend
11028,pg_shdescription
11029,pg_ts_config
11030,pg_ts_config_map
11031,pg_ts_dict
11032,pg_ts_parser
11033,pg_ts_template
11034,pg_extension
11035,pg_foreign_data_wrapper
11036,pg_foreign_server
11037,pg_user_mapping
11038,pg_foreign_table
11039,pg_default_acl
11040,pg_seclabel
11041,pg_shseclabel
11042,pg_collation
11043,pg_range
11055,pg_roles
11058,pg_shadow
11061,pg_group
11064,pg_user
11067,pg_rules
11071,pg_views
11075,pg_tables
11079,pg_matviews
11083,pg_indexes
11087,pg_stats
11091,pg_locks
11094,pg_cursors
11097,pg_available_extensions
11100,pg_available_extension_versions
11103,pg_prepared_xacts
11107,pg_prepared_statements
11110,pg_seclabels
11114,pg_settings
11119,pg_timezone_abbrevs
11122,pg_timezone_names
11125,pg_stat_all_tables
11129,pg_stat_xact_all_tables
11133,pg_stat_sys_tables
11137,pg_stat_xact_sys_tables
11140,pg_stat_user_tables
11144,pg_stat_xact_user_tables
11147,pg_statio_all_tables
11151,pg_statio_sys_tables
11154,pg_statio_user_tables
11157,pg_stat_all_indexes
11161,pg_stat_sys_indexes
11164,pg_stat_user_indexes
11167,pg_statio_all_indexes
11171,pg_statio_sys_indexes
11174,pg_statio_user_indexes
11177,pg_statio_all_sequences
11181,pg_statio_sys_sequences
11184,pg_statio_user_sequences
11187,pg_stat_activity
11190,pg_stat_replication
11193,pg_stat_database
11196,pg_stat_database_conflicts
11199,pg_stat_user_functions
11203,pg_stat_xact_user_functions
11207,pg_stat_bgwriter
11210,pg_user_mappings
11496,cardinal_number
11498,character_data
11499,sql_identifier
11501,information_schema_catalog_name
11503,time_stamp
11504,yes_or_no
11507,applicable_roles
11511,administrable_role_authorizations
11514,attributes
11518,character_sets
11522,check_constraint_routine_usage
11526,check_constraints
11530,collations
11533,collation_character_set_applicability
11536,column_domain_usage
11540,column_privileges
11544,column_udt_usage
11548,columns
11552,constraint_column_usage
11556,constraint_table_usage
11560,domain_constraints
11564,domain_udt_usage
11567,domains
11571,enabled_roles
11574,key_column_usage
11578,parameters
11582,referential_constraints
11586,role_column_grants
11589,routine_privileges
11593,role_routine_grants
11596,routines
11600,schemata
11603,sequences
11607,sql_features
11612,sql_implementation_info
11617,sql_languages
11622,sql_packages
11627,sql_parts
11632,sql_sizing
11637,sql_sizing_profiles
11642,table_constraints
11646,table_privileges
11650,role_table_grants
11653,tables
11657,triggered_update_columns
11661,triggers
11665,udt_privileges
11669,role_udt_grants
11672,usage_privileges
11676,role_usage_grants
11679,user_defined_types
11683,view_column_usage
11687,view_routine_usage
11691,view_table_usage
11695,views
11699,data_type_privileges
11703,element_types
11707,_pg_foreign_table_columns
11711,column_options
11714,_pg_foreign_data_wrappers
11717,foreign_data_wrapper_options
11720,foreign_data_wrappers
11723,_pg_foreign_servers
11727,foreign_server_options
11730,foreign_servers
11733,_pg_foreign_tables
11737,foreign_table_options
11740,foreign_tables
11743,_pg_user_mappings
11746,user_mapping_options
11750,user_mappings
"""
OID_KNOWLEDGE = {  # type: ignore  # TODO: fix
    int(oid): typname for oid, typname in (item.split(",", 1) for item in OID_KNOWLEDGE_RAW.strip().splitlines())
}

# `relkind`:
# https://www.postgresql.org/docs/12/catalog-pg-class.html
# r = ordinary table, i = index, S = sequence, t = TOAST table, v = view, m =
# materialized view, c = composite type, f = foreign table, p = partitioned
# table, I = partitioned index
# The `nspname` filter matches sqlalchemy's `get_schema_names` method.
# TODO?: skip `information_schema` tables here already?
PG_LIST_SOURCES_ALL_SCHEMAS_SQL = """
SELECT
    pg_namespace.nspname as schema,
    pg_class.relname as name
FROM
    pg_class
    JOIN pg_namespace
    ON pg_namespace.oid = pg_class.relnamespace
WHERE
    pg_namespace.nspname not like 'pg_%'
    AND pg_namespace.nspname != 'information_schema'
    AND pg_class.relkind in ('m', 'p', 'r', 'v')
    AND NOT COALESCE((row_to_json(pg_class)->>'relispartition')::boolean, false)
ORDER BY schema, name
""".strip().replace(
    "\n", "  "
)
# NOTE: pg_class.relispartition` field exists only for postgresql>=10, so for postgresql<10 support json is used here
PG_LIST_TABLE_NAMES = """
SELECT c.relname FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = :schema  AND c.relkind in ('r', 'p')
AND NOT COALESCE((row_to_json(c)->>'relispartition')::boolean, false);
"""


@attr.s(cmp=False)
class BasePostgresAdapter(BaseSSLCertAdapter):
    conn_type = CONNECTION_TYPE_POSTGRES

    # The cursor description returns type `oid` values.
    # These values can be found in `pg_type.oid` system table,
    # and mapped to `pg_type.typname`.
    # They can also be found in `psycopg2.extensions.string_types` mapping
    # (which maps oid to a string-to-python-value conversion function).
    # It is potentially possible to plug into the sqlalchemy logic
    # (with a few extra raw sql queries),
    # but for now, a more simple mapping is preferred.
    _type_code_to_sa: Optional[Dict[Any, SATypeSpec]] = {
        16: sa_pg.BOOLEAN,
        17: sa_pg.BYTEA,  # unsupported
        # 19: 'name',
        20: sa_pg.BIGINT,
        21: sa_pg.SMALLINT,
        23: sa_pg.INTEGER,
        25: sa_pg.TEXT,
        26: sa_pg.OID,  # unsupported
        114: sa_pg.JSON,  # unsupported
        700: sa_pg.REAL,
        701: sa_pg.DOUBLE_PRECISION,
        1005: sa_pg.ARRAY(sa_pg.SMALLINT),
        1007: sa_pg.ARRAY(sa_pg.INTEGER),
        1014: sa_pg.ARRAY(sa_pg.CHAR),
        1015: sa_pg.ARRAY(sa_pg.VARCHAR),
        1016: sa_pg.ARRAY(sa_pg.BIGINT),
        1021: sa_pg.ARRAY(sa_pg.REAL),
        1022: sa_pg.ARRAY(sa_pg.DOUBLE_PRECISION),
        1025: sa_pg.ARRAY(sa_pg.TEXT),
        1031: sa_pg.ARRAY(sa_pg.NUMERIC),
        1042: sa_pg.CHAR,
        1043: sa_pg.VARCHAR,
        1082: sa_pg.DATE,
        1083: sa_pg.TIME,  # unsupported
        1114: sa_pg.TIMESTAMP,
        1184: sa_pg.TIMESTAMP(timezone=True),
        1186: sa_pg.INTERVAL,  # unsupported
        1700: sa_pg.NUMERIC,  # untested
        2950: sa_pg.UUID,  # untested
        11020: sa_pg.ENUM,  # untested
    }

    @staticmethod
    def _convert_bytea(value: memoryview) -> Optional[str]:
        str_value = bytes(value[:110]).decode("utf-8", errors="replace")
        if len(value) > 110 or len(str_value) > 100:
            str_value = str_value[:100]
            str_value = str_value + "…"
        return str_value

    # it's static because of typing hell in target_dto
    @staticmethod
    def _get_enforce_collate(target_dto: PostgresConnTargetDTO) -> Optional[str]:
        if target_dto.enforce_collate == PGEnforceCollateMode.auto:
            raise Exception("Resolution of PGEnforceCollateMode.auto is not allowed at this point")
        if target_dto.enforce_collate == PGEnforceCollateMode.on:
            return "en_US"
        return None
