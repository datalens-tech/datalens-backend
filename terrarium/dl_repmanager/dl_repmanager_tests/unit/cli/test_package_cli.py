import pytest

from dl_repmanager.primitives import PackageInfo
from dl_repmanager_tests.unit.base import (
    RepmanagerCliTestBase,
    Repo,
)
from dl_repmanager_tests.unit.tool_runner import (
    PackageCliRunner,
    RepoCliRunner,
)


class TestPackageCli(RepmanagerCliTestBase):
    @pytest.fixture(scope="class")
    def package_cli(self, package: PackageInfo) -> PackageCliRunner:
        return self.get_package_cli(package_path=package.abs_path)

    def test_list_i18n_domains(self, repo: Repo, package: PackageInfo, package_cli: RepoCliRunner) -> None:
        with self.toml_writer(repo=repo, toml_path=package.toml_path) as toml_writer:
            section_name = "datalens.i18n.domains"
            toml_writer.add_section(section_name)
            toml_writer.set_array_value(
                section_name=section_name,
                key="first_domain",
                value=[{"path": "first_path"}],
            )

        cli_result = package_cli.run_with_args(["list-i18n-domains"])
        assert "first_domain=first_path" in cli_result.stdout
