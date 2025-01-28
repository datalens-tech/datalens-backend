import unittest.mock as mock

import pytest
import pytest_mock

import dl_auth_native


@pytest.fixture(name="token_decoder")
def fixture_token_decoder(mocker: pytest_mock.MockerFixture) -> mock.Mock:
    decoder = mocker.Mock(spec=dl_auth_native.DecoderProtocol)
    return decoder
