# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: yandex/cloud/priv/iam/v1/service_control.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from google.protobuf import timestamp_pb2 as google_dot_protobuf_dot_timestamp__pb2
from yandex.cloud.priv import validation_pb2 as yandex_dot_cloud_dot_priv_dot_validation__pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n.yandex/cloud/priv/iam/v1/service_control.proto\x12\x18yandex.cloud.priv.iam.v1\x1a\x1fgoogle/protobuf/timestamp.proto\x1a\"yandex/cloud/priv/validation.proto\"\xac\x02\n\x07Service\x12\x12\n\nservice_id\x18\x01 \x01(\t\x12\x13\n\x0b\x64\x65scription\x18\x02 \x01(\t\x12\x34\n\x08resource\x18\x03 \x01(\x0b\x32\".yandex.cloud.priv.iam.v1.Resource\x12.\n\nupdated_at\x18\x04 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12\x38\n\x06status\x18\x05 \x01(\x0e\x32(.yandex.cloud.priv.iam.v1.Service.Status\"X\n\x06Status\x12\x16\n\x12STATUS_UNSPECIFIED\x10\x00\x12\x0b\n\x07\x45NABLED\x10\x01\x12\x0c\n\x08\x44ISABLED\x10\x02\x12\x0c\n\x08\x45NABLING\x10\x03\x12\r\n\tDISABLING\x10\x04\"@\n\x08Resource\x12\x18\n\x02id\x18\x01 \x01(\tB\x0c\xa8\x89\x31\x01\xca\x89\x31\x04<=50\x12\x1a\n\x04type\x18\x02 \x01(\tB\x0c\xa8\x89\x31\x01\xca\x89\x31\x04<=50\"?\n\x0cSystemFolder\x12\n\n\x02id\x18\x01 \x01(\t\x12\x11\n\tfolder_id\x18\x02 \x01(\t\x12\x10\n\x08\x63loud_id\x18\x03 \x01(\tBPB\x03PSCZIa.yandex-team.ru/cloud/bitbucket/private-api/yandex/cloud/priv/iam/v1;iamb\x06proto3')



_SERVICE = DESCRIPTOR.message_types_by_name['Service']
_RESOURCE = DESCRIPTOR.message_types_by_name['Resource']
_SYSTEMFOLDER = DESCRIPTOR.message_types_by_name['SystemFolder']
_SERVICE_STATUS = _SERVICE.enum_types_by_name['Status']
Service = _reflection.GeneratedProtocolMessageType('Service', (_message.Message,), {
  'DESCRIPTOR' : _SERVICE,
  '__module__' : 'yandex.cloud.priv.iam.v1.service_control_pb2'
  # @@protoc_insertion_point(class_scope:yandex.cloud.priv.iam.v1.Service)
  })
_sym_db.RegisterMessage(Service)

Resource = _reflection.GeneratedProtocolMessageType('Resource', (_message.Message,), {
  'DESCRIPTOR' : _RESOURCE,
  '__module__' : 'yandex.cloud.priv.iam.v1.service_control_pb2'
  # @@protoc_insertion_point(class_scope:yandex.cloud.priv.iam.v1.Resource)
  })
_sym_db.RegisterMessage(Resource)

SystemFolder = _reflection.GeneratedProtocolMessageType('SystemFolder', (_message.Message,), {
  'DESCRIPTOR' : _SYSTEMFOLDER,
  '__module__' : 'yandex.cloud.priv.iam.v1.service_control_pb2'
  # @@protoc_insertion_point(class_scope:yandex.cloud.priv.iam.v1.SystemFolder)
  })
_sym_db.RegisterMessage(SystemFolder)

if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  DESCRIPTOR._serialized_options = b'B\003PSCZIa.yandex-team.ru/cloud/bitbucket/private-api/yandex/cloud/priv/iam/v1;iam'
  _RESOURCE.fields_by_name['id']._options = None
  _RESOURCE.fields_by_name['id']._serialized_options = b'\250\2111\001\312\2111\004<=50'
  _RESOURCE.fields_by_name['type']._options = None
  _RESOURCE.fields_by_name['type']._serialized_options = b'\250\2111\001\312\2111\004<=50'
  _SERVICE._serialized_start=146
  _SERVICE._serialized_end=446
  _SERVICE_STATUS._serialized_start=358
  _SERVICE_STATUS._serialized_end=446
  _RESOURCE._serialized_start=448
  _RESOURCE._serialized_end=512
  _SYSTEMFOLDER._serialized_start=514
  _SYSTEMFOLDER._serialized_end=577
# @@protoc_insertion_point(module_scope)
