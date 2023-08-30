import enum


class UserErrorMessages(enum.Enum):
    no_authentication_data_provided = "No authentication data provided"
    no_tenant_specified = "Neither organization ID nor folder ID provided"
    no_dc_tenant_specified = "Project ID is not specified"
    both_folder_and_org_specified = "Organization ID and folder ID can not be specified simultaneously"
    org_or_folder_mixed_with_dl_tenant = "Datalens tenant can not be mixed with folder ID or organization ID"
    # TODO FIX: Remove when authorization for organizations will be ready
    organizations_is_not_yet_supported = "Organizations is not yet supported"
    user_unauthenticated = "Unauthenticated"
    user_unauthorized = "Unauthorized"
