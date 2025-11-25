from typing import ClassVar

import attr
import pytest
import sqlalchemy as sa

from dl_formula_testing.evaluator import DbEvaluator
from dl_formula_testing.testcases.base import FormulaConnectorTestBase


@attr.s(auto_attribs=True, frozen=True)
class HashFunctionSupport:
    md5: bool = False
    sha1: bool = False
    sha256: bool = False
    murmurhash2_64: bool = False
    siphash64: bool = False
    inthash64: bool = False
    cityhash64: bool = False


class DefaultHashFunctionFormulaConnectorTestSuite(FormulaConnectorTestBase):
    hash_function_support: ClassVar[HashFunctionSupport] = HashFunctionSupport()

    def test_md5(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        if not self.hash_function_support.md5:
            pytest.skip("MD5 is not supported by this connector")
        assert dbe.eval("MD5('DataLens')") == "C1FD5D9E4189FB89C1021A72F7E06C00"
        assert (
            dbe.eval("MD5([str_value])", from_=data_table, order_by=["[str_value]"])
            == "7694F4A66316E53C8CDD9D9954BD611D"
        )

    def test_sha1(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        if not self.hash_function_support.sha1:
            pytest.skip("SHA1 is not supported by this connector")
        assert dbe.eval("SHA1('DataLens')") == "F4EA6F8285E57FC18D8CA03672703B52C302231A"
        assert (
            dbe.eval("SHA1([str_value])", from_=data_table, order_by=["[str_value]"])
            == "22EA1C649C82946AA6E479E1FFD321E4A318B1B0"
        )

    def test_sha256(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        if not self.hash_function_support.sha256:
            pytest.skip("SHA256 is not supported by this connector")
        assert dbe.eval("SHA256('DataLens')") == "7466C4153CEB3184D0FDABEA1C7EAE86B24E184191C197845B96E1D8B3817F98"
        assert (
            dbe.eval("SHA256([str_value])", from_=data_table, order_by=["[str_value]"])
            == "8E35C2CD3BF6641BDB0E2050B76932CBB2E6034A0DDACC1D9BEA82A6BA57F7CF"
        )

    def test_murmurhash2_64(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        if not self.hash_function_support.murmurhash2_64:
            pytest.skip("MurmurHash2_64 is not supported by this connector")
        assert dbe.eval("MurmurHash2_64('DataLens')") == 12204402796868507663
        assert (
            dbe.eval("MurmurHash2_64([str_value])", from_=data_table, order_by=["[str_value]"]) == 8861071527689086543
        )

    def test_siphash64(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        if not self.hash_function_support.siphash64:
            pytest.skip("SipHash64 is not supported by this connector")
        assert dbe.eval("SipHash64('DataLens')") == 6283456972272785891
        assert dbe.eval("SipHash64([str_value])", from_=data_table, order_by=["[str_value]"]) == 17688157251436176611

    def test_inthash64(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        if not self.hash_function_support.inthash64:
            pytest.skip("IntHash64 is not supported by this connector")
        assert dbe.eval("IntHash64(12345)") == 16722121143744093920
        assert dbe.eval("IntHash64([int_value])", from_=data_table, order_by=["[int_value]"]) == 4761183170873013810

    def test_cityhash64(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        if not self.hash_function_support.cityhash64:
            pytest.skip("CityHash64 is not supported by this connector")
        assert dbe.eval("CityHash64('DataLens')") == 1276466053635395874
        assert dbe.eval("CityHash64([str_value])", from_=data_table, order_by=["[str_value]"]) == 17372780029233160351
