import contextlib
import logging
import typing
import os
import uuid

import attr

import bi_configs.utils as bi_config_utils


@attr.s()
class BaseSSLCertAdapter:
    __context_counter: int = 0
    __file_name: str | None = None

    def __get_filename(self) -> str:
        if self.__file_name is None:
            self.__file_name = f"{uuid.uuid4()}.crt"

        return self.__file_name

    def get_ssl_cert_path(self, ssl_ca: typing.Optional[str]) -> str:
        if ssl_ca is None:
            return bi_config_utils.get_root_certificates_path()

        return os.path.join(
            bi_config_utils.get_temp_root_certificates_folder_path(),
            self.__get_filename(),
        )

    @contextlib.contextmanager
    def ssl_cert_context(self, ssl_ca: typing.Optional[str]) -> typing.Generator[None, None, None]:
        assert ssl_ca is not None, "Root CA can't be None for ssl connection context"

        crt_path = self.get_ssl_cert_path(ssl_ca=ssl_ca)

        if self.__context_counter == 0:
            with open(crt_path, 'x', encoding="utf-8") as f:
                logging.debug(f"Writing root CA to {crt_path}")
                f.write(ssl_ca)
        self.__context_counter += 1

        try:
            yield
        finally:
            self.__context_counter -= 1
            if self.__context_counter == 0:
                logging.debug(f"Removing root CA from {crt_path}")
                os.remove(crt_path)
