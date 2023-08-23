# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: yandex/cloud/priv/mdb/greenplum/v1/hba_rule_service.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from google.api import annotations_pb2 as google_dot_api_dot_annotations__pb2
from yandex.cloud.api import operation_pb2 as yandex_dot_cloud_dot_api_dot_operation__pb2
from yandex.cloud.priv.operation import operation_pb2 as yandex_dot_cloud_dot_priv_dot_operation_dot_operation__pb2
from yandex.cloud.priv import validation_pb2 as yandex_dot_cloud_dot_priv_dot_validation__pb2
from yandex.cloud.priv.mdb.greenplum.v1 import hba_rule_pb2 as yandex_dot_cloud_dot_priv_dot_mdb_dot_greenplum_dot_v1_dot_hba__rule__pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n9yandex/cloud/priv/mdb/greenplum/v1/hba_rule_service.proto\x12\"yandex.cloud.priv.mdb.greenplum.v1\x1a\x1cgoogle/api/annotations.proto\x1a yandex/cloud/api/operation.proto\x1a+yandex/cloud/priv/operation/operation.proto\x1a\"yandex/cloud/priv/validation.proto\x1a\x31yandex/cloud/priv/mdb/greenplum/v1/hba_rule.proto\"z\n\x11\x41\x64\x64HBARuleRequest\x12 \n\ncluster_id\x18\x01 \x01(\tB\x0c\xa8\x89\x31\x01\xca\x89\x31\x04<=50\x12\x43\n\x08hba_rule\x18\x02 \x01(\x0b\x32+.yandex.cloud.priv.mdb.greenplum.v1.HBARuleB\x04\xa8\x89\x31\x01\"}\n\x14UpdateHBARuleRequest\x12 \n\ncluster_id\x18\x01 \x01(\tB\x0c\xa8\x89\x31\x01\xca\x89\x31\x04<=50\x12\x43\n\x08hba_rule\x18\x02 \x01(\x0b\x32+.yandex.cloud.priv.mdb.greenplum.v1.HBARuleB\x04\xa8\x89\x31\x01\"V\n\x14\x44\x65leteHBARuleRequest\x12 \n\ncluster_id\x18\x01 \x01(\tB\x0c\xa8\x89\x31\x01\xca\x89\x31\x04<=50\x12\x1c\n\x08priority\x18\x02 \x01(\x03\x42\n\xba\x89\x31\x06\x30-1000\"7\n\x13ListHBARulesRequest\x12 \n\ncluster_id\x18\x01 \x01(\tB\x0c\xa8\x89\x31\x01\xca\x89\x31\x04<=50\"[\n\x1dListHBARulesAtRevisionRequest\x12 \n\ncluster_id\x18\x01 \x01(\tB\x0c\xa8\x89\x31\x01\xca\x89\x31\x04<=50\x12\x18\n\x08revision\x18\x02 \x01(\x03\x42\x06\xba\x89\x31\x02>0\"V\n\x14ListHBARulesResponse\x12>\n\thba_rules\x18\x01 \x03(\x0b\x32+.yandex.cloud.priv.mdb.greenplum.v1.HBARule\"\x86\x01\n\x1a\x42\x61tchUpdateHBARulesRequest\x12 \n\ncluster_id\x18\x01 \x01(\tB\x0c\xa8\x89\x31\x01\xca\x89\x31\x04<=50\x12\x46\n\thba_rules\x18\x02 \x03(\x0b\x32+.yandex.cloud.priv.mdb.greenplum.v1.HBARuleB\x06\xc2\x89\x31\x02>0\"&\n\x10HBARulesMetadata\x12\x12\n\ncluster_id\x18\x01 \x01(\t2\x8f\n\n\x0eHBARuleService\x12\xb3\x01\n\x04List\x12\x37.yandex.cloud.priv.mdb.greenplum.v1.ListHBARulesRequest\x1a\x38.yandex.cloud.priv.mdb.greenplum.v1.ListHBARulesResponse\"8\x82\xd3\xe4\x93\x02\x32\x12\x30/mdb/greenplum/v1/clusters/{cluster_id}/hbaRules\x12\xd2\x01\n\x0eListAtRevision\x12\x41.yandex.cloud.priv.mdb.greenplum.v1.ListHBARulesAtRevisionRequest\x1a\x38.yandex.cloud.priv.mdb.greenplum.v1.ListHBARulesResponse\"C\x82\xd3\xe4\x93\x02=\x12;/mdb/greenplum/v1/clusters/{cluster_id}/hbaRules/atRevision\x12\xce\x01\n\x03\x41\x64\x64\x12\x35.yandex.cloud.priv.mdb.greenplum.v1.AddHBARuleRequest\x1a&.yandex.cloud.priv.operation.Operation\"h\x82\xd3\xe4\x93\x02\x35\"0/mdb/greenplum/v1/clusters/{cluster_id}/hbaRules:\x01*\xb2\xd2*)\n\x10HBARulesMetadata\x12\x15google.protobuf.Empty\x12\xd4\x01\n\x06Update\x12\x38.yandex.cloud.priv.mdb.greenplum.v1.UpdateHBARuleRequest\x1a&.yandex.cloud.priv.operation.Operation\"h\x82\xd3\xe4\x93\x02\x35\x32\x30/mdb/greenplum/v1/clusters/{cluster_id}/hbaRules:\x01*\xb2\xd2*)\n\x10HBARulesMetadata\x12\x15google.protobuf.Empty\x12\xdb\x01\n\x06\x44\x65lete\x12\x38.yandex.cloud.priv.mdb.greenplum.v1.DeleteHBARuleRequest\x1a&.yandex.cloud.priv.operation.Operation\"o\x82\xd3\xe4\x93\x02<*:/mdb/greenplum/v1/clusters/{cluster_id}/hbaRule/{priority}\xb2\xd2*)\n\x10HBARulesMetadata\x12\x15google.protobuf.Empty\x12\xeb\x01\n\x0b\x42\x61tchUpdate\x12>.yandex.cloud.priv.mdb.greenplum.v1.BatchUpdateHBARulesRequest\x1a&.yandex.cloud.priv.operation.Operation\"t\x82\xd3\xe4\x93\x02\x41\"</mdb/greenplum/v1/clusters/{cluster_id}/hbaRules:batchUpdate:\x01*\xb2\xd2*)\n\x10HBARulesMetadata\x12\x15google.protobuf.EmptyBaB\x04GPHSZYa.yandex-team.ru/cloud/bitbucket/private-api/yandex/cloud/priv/mdb/greenplum/v1;greenplumb\x06proto3')



_ADDHBARULEREQUEST = DESCRIPTOR.message_types_by_name['AddHBARuleRequest']
_UPDATEHBARULEREQUEST = DESCRIPTOR.message_types_by_name['UpdateHBARuleRequest']
_DELETEHBARULEREQUEST = DESCRIPTOR.message_types_by_name['DeleteHBARuleRequest']
_LISTHBARULESREQUEST = DESCRIPTOR.message_types_by_name['ListHBARulesRequest']
_LISTHBARULESATREVISIONREQUEST = DESCRIPTOR.message_types_by_name['ListHBARulesAtRevisionRequest']
_LISTHBARULESRESPONSE = DESCRIPTOR.message_types_by_name['ListHBARulesResponse']
_BATCHUPDATEHBARULESREQUEST = DESCRIPTOR.message_types_by_name['BatchUpdateHBARulesRequest']
_HBARULESMETADATA = DESCRIPTOR.message_types_by_name['HBARulesMetadata']
AddHBARuleRequest = _reflection.GeneratedProtocolMessageType('AddHBARuleRequest', (_message.Message,), {
  'DESCRIPTOR' : _ADDHBARULEREQUEST,
  '__module__' : 'yandex.cloud.priv.mdb.greenplum.v1.hba_rule_service_pb2'
  # @@protoc_insertion_point(class_scope:yandex.cloud.priv.mdb.greenplum.v1.AddHBARuleRequest)
  })
_sym_db.RegisterMessage(AddHBARuleRequest)

UpdateHBARuleRequest = _reflection.GeneratedProtocolMessageType('UpdateHBARuleRequest', (_message.Message,), {
  'DESCRIPTOR' : _UPDATEHBARULEREQUEST,
  '__module__' : 'yandex.cloud.priv.mdb.greenplum.v1.hba_rule_service_pb2'
  # @@protoc_insertion_point(class_scope:yandex.cloud.priv.mdb.greenplum.v1.UpdateHBARuleRequest)
  })
_sym_db.RegisterMessage(UpdateHBARuleRequest)

DeleteHBARuleRequest = _reflection.GeneratedProtocolMessageType('DeleteHBARuleRequest', (_message.Message,), {
  'DESCRIPTOR' : _DELETEHBARULEREQUEST,
  '__module__' : 'yandex.cloud.priv.mdb.greenplum.v1.hba_rule_service_pb2'
  # @@protoc_insertion_point(class_scope:yandex.cloud.priv.mdb.greenplum.v1.DeleteHBARuleRequest)
  })
_sym_db.RegisterMessage(DeleteHBARuleRequest)

ListHBARulesRequest = _reflection.GeneratedProtocolMessageType('ListHBARulesRequest', (_message.Message,), {
  'DESCRIPTOR' : _LISTHBARULESREQUEST,
  '__module__' : 'yandex.cloud.priv.mdb.greenplum.v1.hba_rule_service_pb2'
  # @@protoc_insertion_point(class_scope:yandex.cloud.priv.mdb.greenplum.v1.ListHBARulesRequest)
  })
_sym_db.RegisterMessage(ListHBARulesRequest)

ListHBARulesAtRevisionRequest = _reflection.GeneratedProtocolMessageType('ListHBARulesAtRevisionRequest', (_message.Message,), {
  'DESCRIPTOR' : _LISTHBARULESATREVISIONREQUEST,
  '__module__' : 'yandex.cloud.priv.mdb.greenplum.v1.hba_rule_service_pb2'
  # @@protoc_insertion_point(class_scope:yandex.cloud.priv.mdb.greenplum.v1.ListHBARulesAtRevisionRequest)
  })
_sym_db.RegisterMessage(ListHBARulesAtRevisionRequest)

ListHBARulesResponse = _reflection.GeneratedProtocolMessageType('ListHBARulesResponse', (_message.Message,), {
  'DESCRIPTOR' : _LISTHBARULESRESPONSE,
  '__module__' : 'yandex.cloud.priv.mdb.greenplum.v1.hba_rule_service_pb2'
  # @@protoc_insertion_point(class_scope:yandex.cloud.priv.mdb.greenplum.v1.ListHBARulesResponse)
  })
_sym_db.RegisterMessage(ListHBARulesResponse)

BatchUpdateHBARulesRequest = _reflection.GeneratedProtocolMessageType('BatchUpdateHBARulesRequest', (_message.Message,), {
  'DESCRIPTOR' : _BATCHUPDATEHBARULESREQUEST,
  '__module__' : 'yandex.cloud.priv.mdb.greenplum.v1.hba_rule_service_pb2'
  # @@protoc_insertion_point(class_scope:yandex.cloud.priv.mdb.greenplum.v1.BatchUpdateHBARulesRequest)
  })
_sym_db.RegisterMessage(BatchUpdateHBARulesRequest)

HBARulesMetadata = _reflection.GeneratedProtocolMessageType('HBARulesMetadata', (_message.Message,), {
  'DESCRIPTOR' : _HBARULESMETADATA,
  '__module__' : 'yandex.cloud.priv.mdb.greenplum.v1.hba_rule_service_pb2'
  # @@protoc_insertion_point(class_scope:yandex.cloud.priv.mdb.greenplum.v1.HBARulesMetadata)
  })
_sym_db.RegisterMessage(HBARulesMetadata)

_HBARULESERVICE = DESCRIPTOR.services_by_name['HBARuleService']
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  DESCRIPTOR._serialized_options = b'B\004GPHSZYa.yandex-team.ru/cloud/bitbucket/private-api/yandex/cloud/priv/mdb/greenplum/v1;greenplum'
  _ADDHBARULEREQUEST.fields_by_name['cluster_id']._options = None
  _ADDHBARULEREQUEST.fields_by_name['cluster_id']._serialized_options = b'\250\2111\001\312\2111\004<=50'
  _ADDHBARULEREQUEST.fields_by_name['hba_rule']._options = None
  _ADDHBARULEREQUEST.fields_by_name['hba_rule']._serialized_options = b'\250\2111\001'
  _UPDATEHBARULEREQUEST.fields_by_name['cluster_id']._options = None
  _UPDATEHBARULEREQUEST.fields_by_name['cluster_id']._serialized_options = b'\250\2111\001\312\2111\004<=50'
  _UPDATEHBARULEREQUEST.fields_by_name['hba_rule']._options = None
  _UPDATEHBARULEREQUEST.fields_by_name['hba_rule']._serialized_options = b'\250\2111\001'
  _DELETEHBARULEREQUEST.fields_by_name['cluster_id']._options = None
  _DELETEHBARULEREQUEST.fields_by_name['cluster_id']._serialized_options = b'\250\2111\001\312\2111\004<=50'
  _DELETEHBARULEREQUEST.fields_by_name['priority']._options = None
  _DELETEHBARULEREQUEST.fields_by_name['priority']._serialized_options = b'\272\2111\0060-1000'
  _LISTHBARULESREQUEST.fields_by_name['cluster_id']._options = None
  _LISTHBARULESREQUEST.fields_by_name['cluster_id']._serialized_options = b'\250\2111\001\312\2111\004<=50'
  _LISTHBARULESATREVISIONREQUEST.fields_by_name['cluster_id']._options = None
  _LISTHBARULESATREVISIONREQUEST.fields_by_name['cluster_id']._serialized_options = b'\250\2111\001\312\2111\004<=50'
  _LISTHBARULESATREVISIONREQUEST.fields_by_name['revision']._options = None
  _LISTHBARULESATREVISIONREQUEST.fields_by_name['revision']._serialized_options = b'\272\2111\002>0'
  _BATCHUPDATEHBARULESREQUEST.fields_by_name['cluster_id']._options = None
  _BATCHUPDATEHBARULESREQUEST.fields_by_name['cluster_id']._serialized_options = b'\250\2111\001\312\2111\004<=50'
  _BATCHUPDATEHBARULESREQUEST.fields_by_name['hba_rules']._options = None
  _BATCHUPDATEHBARULESREQUEST.fields_by_name['hba_rules']._serialized_options = b'\302\2111\002>0'
  _HBARULESERVICE.methods_by_name['List']._options = None
  _HBARULESERVICE.methods_by_name['List']._serialized_options = b'\202\323\344\223\0022\0220/mdb/greenplum/v1/clusters/{cluster_id}/hbaRules'
  _HBARULESERVICE.methods_by_name['ListAtRevision']._options = None
  _HBARULESERVICE.methods_by_name['ListAtRevision']._serialized_options = b'\202\323\344\223\002=\022;/mdb/greenplum/v1/clusters/{cluster_id}/hbaRules/atRevision'
  _HBARULESERVICE.methods_by_name['Add']._options = None
  _HBARULESERVICE.methods_by_name['Add']._serialized_options = b'\202\323\344\223\0025\"0/mdb/greenplum/v1/clusters/{cluster_id}/hbaRules:\001*\262\322*)\n\020HBARulesMetadata\022\025google.protobuf.Empty'
  _HBARULESERVICE.methods_by_name['Update']._options = None
  _HBARULESERVICE.methods_by_name['Update']._serialized_options = b'\202\323\344\223\002520/mdb/greenplum/v1/clusters/{cluster_id}/hbaRules:\001*\262\322*)\n\020HBARulesMetadata\022\025google.protobuf.Empty'
  _HBARULESERVICE.methods_by_name['Delete']._options = None
  _HBARULESERVICE.methods_by_name['Delete']._serialized_options = b'\202\323\344\223\002<*:/mdb/greenplum/v1/clusters/{cluster_id}/hbaRule/{priority}\262\322*)\n\020HBARulesMetadata\022\025google.protobuf.Empty'
  _HBARULESERVICE.methods_by_name['BatchUpdate']._options = None
  _HBARULESERVICE.methods_by_name['BatchUpdate']._serialized_options = b'\202\323\344\223\002A\"</mdb/greenplum/v1/clusters/{cluster_id}/hbaRules:batchUpdate:\001*\262\322*)\n\020HBARulesMetadata\022\025google.protobuf.Empty'
  _ADDHBARULEREQUEST._serialized_start=293
  _ADDHBARULEREQUEST._serialized_end=415
  _UPDATEHBARULEREQUEST._serialized_start=417
  _UPDATEHBARULEREQUEST._serialized_end=542
  _DELETEHBARULEREQUEST._serialized_start=544
  _DELETEHBARULEREQUEST._serialized_end=630
  _LISTHBARULESREQUEST._serialized_start=632
  _LISTHBARULESREQUEST._serialized_end=687
  _LISTHBARULESATREVISIONREQUEST._serialized_start=689
  _LISTHBARULESATREVISIONREQUEST._serialized_end=780
  _LISTHBARULESRESPONSE._serialized_start=782
  _LISTHBARULESRESPONSE._serialized_end=868
  _BATCHUPDATEHBARULESREQUEST._serialized_start=871
  _BATCHUPDATEHBARULESREQUEST._serialized_end=1005
  _HBARULESMETADATA._serialized_start=1007
  _HBARULESMETADATA._serialized_end=1045
  _HBARULESERVICE._serialized_start=1048
  _HBARULESERVICE._serialized_end=2343
# @@protoc_insertion_point(module_scope)
