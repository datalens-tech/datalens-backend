# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: yandex/cloud/priv/mdb/greenplum/v1/perf_diag.proto
"""Generated protocol buffer code."""
from google.protobuf.internal import enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from google.protobuf import timestamp_pb2 as google_dot_protobuf_dot_timestamp__pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n2yandex/cloud/priv/mdb/greenplum/v1/perf_diag.proto\x12\"yandex.cloud.priv.mdb.greenplum.v1\x1a\x1fgoogle/protobuf/timestamp.proto\"\x9a\x03\n\x0cSessionState\x12(\n\x04time\x18\x01 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12\x0c\n\x04host\x18\x02 \x01(\t\x12\x0b\n\x03pid\x18\x03 \x01(\x03\x12\x10\n\x08\x64\x61tabase\x18\x04 \x01(\t\x12\x0c\n\x04user\x18\x05 \x01(\t\x12\x18\n\x10\x61pplication_name\x18\x06 \x01(\t\x12\x31\n\rbackend_start\x18\x07 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12.\n\nxact_start\x18\x08 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12/\n\x0bquery_start\x18\t \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12\x30\n\x0cstate_change\x18\n \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12\x16\n\x0ewaiting_reason\x18\x0b \x01(\t\x12\x0f\n\x07waiting\x18\x0c \x01(\x08\x12\r\n\x05state\x18\r \x01(\t\x12\r\n\x05query\x18\x0e \x01(\t\"h\n\x08Interval\x12.\n\nstart_time\x18\x01 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12,\n\x08\x65nd_time\x18\x02 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\"\x99\x01\n\x13SessionFieldWrapper\x12\x44\n\nfield_name\x18\x01 \x01(\x0e\x32\x30.yandex.cloud.priv.mdb.greenplum.v1.SessionField\x12<\n\x05order\x18\x02 \x01(\x0e\x32-.yandex.cloud.priv.mdb.greenplum.v1.SortOrder\"d\n\rSessionFilter\x12\x44\n\nfield_name\x18\x01 \x01(\x0e\x32\x30.yandex.cloud.priv.mdb.greenplum.v1.SessionField\x12\r\n\x05value\x18\x02 \x01(\t*:\n\tSortOrder\x12\x1a\n\x16SORT_ORDER_UNSPECIFIED\x10\x00\x12\x07\n\x03\x41SC\x10\x01\x12\x08\n\x04\x44\x45SC\x10\x02*\x98\x03\n\x0cSessionField\x12\x1d\n\x19SESSION_FIELD_UNSPECIFIED\x10\x00\x12\x10\n\x0cSESSION_TIME\x10\x01\x12\x10\n\x0cSESSION_HOST\x10\x02\x12\x0f\n\x0bSESSION_PID\x10\x03\x12\x14\n\x10SESSION_DATABASE\x10\x04\x12\x10\n\x0cSESSION_USER\x10\x05\x12\x1c\n\x18SESSION_APPLICATION_NAME\x10\x06\x12\x19\n\x15SESSION_BACKEND_START\x10\x07\x12\x16\n\x12SESSION_XACT_START\x10\x08\x12\x17\n\x13SESSION_QUERY_START\x10\t\x12\x18\n\x14SESSION_STATE_CHANGE\x10\n\x12\x1a\n\x16SESSION_WAITING_REASON\x10\x0b\x12\x13\n\x0fSESSION_WAITING\x10\x0c\x12\x11\n\rSESSION_STATE\x10\r\x12\x11\n\rSESSION_QUERY\x10\x0e\x12\x17\n\x13SESSION_BACKEND_XID\x10\x13\x12\x18\n\x14SESSION_BACKEND_XMIN\x10\x14*X\n\x10\x43olumnFilterEnum\x12\"\n\x1e\x43OLUMN_FILTER_ENUM_UNSPECIFIED\x10\x00\x12\x08\n\x04HOST\x10\x01\x12\x08\n\x04USER\x10\x02\x12\x0c\n\x08\x44\x41TABASE\x10\x03\x42\x64\x42\x07GPPDIAGZYa.yandex-team.ru/cloud/bitbucket/private-api/yandex/cloud/priv/mdb/greenplum/v1;greenplumb\x06proto3')

_SORTORDER = DESCRIPTOR.enum_types_by_name['SortOrder']
SortOrder = enum_type_wrapper.EnumTypeWrapper(_SORTORDER)
_SESSIONFIELD = DESCRIPTOR.enum_types_by_name['SessionField']
SessionField = enum_type_wrapper.EnumTypeWrapper(_SESSIONFIELD)
_COLUMNFILTERENUM = DESCRIPTOR.enum_types_by_name['ColumnFilterEnum']
ColumnFilterEnum = enum_type_wrapper.EnumTypeWrapper(_COLUMNFILTERENUM)
SORT_ORDER_UNSPECIFIED = 0
ASC = 1
DESC = 2
SESSION_FIELD_UNSPECIFIED = 0
SESSION_TIME = 1
SESSION_HOST = 2
SESSION_PID = 3
SESSION_DATABASE = 4
SESSION_USER = 5
SESSION_APPLICATION_NAME = 6
SESSION_BACKEND_START = 7
SESSION_XACT_START = 8
SESSION_QUERY_START = 9
SESSION_STATE_CHANGE = 10
SESSION_WAITING_REASON = 11
SESSION_WAITING = 12
SESSION_STATE = 13
SESSION_QUERY = 14
SESSION_BACKEND_XID = 19
SESSION_BACKEND_XMIN = 20
COLUMN_FILTER_ENUM_UNSPECIFIED = 0
HOST = 1
USER = 2
DATABASE = 3


_SESSIONSTATE = DESCRIPTOR.message_types_by_name['SessionState']
_INTERVAL = DESCRIPTOR.message_types_by_name['Interval']
_SESSIONFIELDWRAPPER = DESCRIPTOR.message_types_by_name['SessionFieldWrapper']
_SESSIONFILTER = DESCRIPTOR.message_types_by_name['SessionFilter']
SessionState = _reflection.GeneratedProtocolMessageType('SessionState', (_message.Message,), {
  'DESCRIPTOR' : _SESSIONSTATE,
  '__module__' : 'yandex.cloud.priv.mdb.greenplum.v1.perf_diag_pb2'
  # @@protoc_insertion_point(class_scope:yandex.cloud.priv.mdb.greenplum.v1.SessionState)
  })
_sym_db.RegisterMessage(SessionState)

Interval = _reflection.GeneratedProtocolMessageType('Interval', (_message.Message,), {
  'DESCRIPTOR' : _INTERVAL,
  '__module__' : 'yandex.cloud.priv.mdb.greenplum.v1.perf_diag_pb2'
  # @@protoc_insertion_point(class_scope:yandex.cloud.priv.mdb.greenplum.v1.Interval)
  })
_sym_db.RegisterMessage(Interval)

SessionFieldWrapper = _reflection.GeneratedProtocolMessageType('SessionFieldWrapper', (_message.Message,), {
  'DESCRIPTOR' : _SESSIONFIELDWRAPPER,
  '__module__' : 'yandex.cloud.priv.mdb.greenplum.v1.perf_diag_pb2'
  # @@protoc_insertion_point(class_scope:yandex.cloud.priv.mdb.greenplum.v1.SessionFieldWrapper)
  })
_sym_db.RegisterMessage(SessionFieldWrapper)

SessionFilter = _reflection.GeneratedProtocolMessageType('SessionFilter', (_message.Message,), {
  'DESCRIPTOR' : _SESSIONFILTER,
  '__module__' : 'yandex.cloud.priv.mdb.greenplum.v1.perf_diag_pb2'
  # @@protoc_insertion_point(class_scope:yandex.cloud.priv.mdb.greenplum.v1.SessionFilter)
  })
_sym_db.RegisterMessage(SessionFilter)

if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  DESCRIPTOR._serialized_options = b'B\007GPPDIAGZYa.yandex-team.ru/cloud/bitbucket/private-api/yandex/cloud/priv/mdb/greenplum/v1;greenplum'
  _SORTORDER._serialized_start=900
  _SORTORDER._serialized_end=958
  _SESSIONFIELD._serialized_start=961
  _SESSIONFIELD._serialized_end=1369
  _COLUMNFILTERENUM._serialized_start=1371
  _COLUMNFILTERENUM._serialized_end=1459
  _SESSIONSTATE._serialized_start=124
  _SESSIONSTATE._serialized_end=534
  _INTERVAL._serialized_start=536
  _INTERVAL._serialized_end=640
  _SESSIONFIELDWRAPPER._serialized_start=643
  _SESSIONFIELDWRAPPER._serialized_end=796
  _SESSIONFILTER._serialized_start=798
  _SESSIONFILTER._serialized_end=898
# @@protoc_insertion_point(module_scope)
