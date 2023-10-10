import requests


class YaDocumentsClient:
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    def get_spreadsheet_public(self, link):
        resp = requests.get(
            f"https://cloud-api.yandex.net/v1/disk/public/resources/download/?public_key={link}",
            headers=self.headers,
        )
        return resp.json()["href"]

    def get_spreadsheet_public_meta(self, link):
        resp = requests.get(
            f"https://cloud-api.yandex.net/v1/disk/public/resources/?public_key={link}",
            headers=self.headers,
        )
        return resp.json()

    def get_spreadsheet_private(self, path, token):
        headers_with_token = self._create_headers_with_token(token)
        resp = requests.get(
            f"https://cloud-api.yandex.net/v1/disk/resources/download/?path={path}",
            headers=headers_with_token,
        )
        return resp.json()["href"]

    def get_spreadsheet_private_meta(self, path, token):
        headers_with_token = self._create_headers_with_token(token)
        resp = requests.get(
            f"https://cloud-api.yandex.net/v1/disk/resources/?path={path}",
            headers=headers_with_token,
        )
        return resp.json()

    def _create_headers_with_token(self, token: str):
        headers_with_token = self.headers.copy()
        headers_with_token.update({"Authorization": "OAuth " + token})
        return headers_with_token
