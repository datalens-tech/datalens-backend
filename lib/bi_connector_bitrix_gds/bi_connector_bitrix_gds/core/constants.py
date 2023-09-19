from enum import (
    Enum,
    unique,
)

from dl_constants.enums import (
    ConnectionType,
    CreateDSFrom,
    SourceBackendType,
)


BACKEND_TYPE_BITRIX_GDS = SourceBackendType.declare("BITRIX_GDS")
CONNECTION_TYPE_BITRIX24 = ConnectionType.declare("bitrix24")
SOURCE_TYPE_BITRIX_GDS = CreateDSFrom.declare("BITRIX_GDS")

DEFAULT_DB = "default"


@unique
class BitrixGDSTableType(Enum):
    crm_deal = "crm_deal"
    crm_lead = "crm_lead"
    crm_company = "crm_company"
    crm_contact = "crm_contact"
    crm_deal_stage_history = "crm_deal_stage_history"
    crm_lead_status_history = "crm_lead_status_history"
    socialnetwork_group = "socialnetwork_group"
    telephony_call = "telephony_call"
    crm_activity = "crm_activity"
    crm_lead_uf = "crm_lead_uf"
    crm_deal_uf = "crm_deal_uf"
    crm_lead_product_row = "crm_lead_product_row"
    crm_deal_product_row = "crm_deal_product_row"
    crm_dynamic_items = "crm_dynamic_items"
    user = "user"
    crm_company_uf = "crm_company_uf"
    crm_contact_uf = "crm_contact_uf"
