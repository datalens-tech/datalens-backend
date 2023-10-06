from dl_repmanager_tests.unit.base import RepmanagerCliTestBase


class TestRepoCli(RepmanagerCliTestBase):
    def test_init(self, repo, repo_cli) -> None:
        package_name = self.generate_package_name()
        repo_cli.run_with_args(["init", "--package-type", "lib", "--package-name", package_name])
        repo = repo.reload()
        package_info = repo.package_index.get_package_info_from_module_name(package_name)
        assert package_info.package_type == "lib"
        assert package_info.single_module_name == package_name
        assert package_info.single_test_dir == f"{package_name}_tests"
        assert package_info.package_reg_name == package_name.replace("_", "-")
