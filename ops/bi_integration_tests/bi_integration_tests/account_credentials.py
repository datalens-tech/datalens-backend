import attr

from bi_testing_ya.cloud_tokens import (
    AccountCredentials,
    CloudCredentialsConverter,
)


@attr.s(auto_attribs=True, frozen=True)
class TestCredentialsConverter:
    yc_credentials_converter: CloudCredentialsConverter

    def convert(
        self,
        account_data: dict,
    ) -> AccountCredentials:
        token = self._get_account_token(account_data)
        is_intranet_user = "token" in account_data
        return AccountCredentials(
            user_id=account_data["id"],
            token=token,
            is_intranet_user=is_intranet_user,
            user_name=account_data["name"],
            is_sa=not is_intranet_user,
        )

    def _get_account_token(self, account_data):
        if "token" in account_data:
            return account_data["token"]
        return self.yc_credentials_converter.get_service_account_iam_token(
            service_account_id=account_data["id"],
            key_id=account_data["key_id"],
            private_key=account_data["private_key"],
        )
