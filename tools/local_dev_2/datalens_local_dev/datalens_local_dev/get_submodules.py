from pathlib import Path

from datalens_local_dev.pkg_ref import ProjectRef


def get_ci_project() -> ProjectRef:
    script_dir = Path(__file__).resolve().parent
    ci_dir = (script_dir / ".." / ".." / ".." / ".." / "ops" / "ci").resolve()
    pyproject_file = ci_dir / "pyproject.toml"

    if pyproject_file.exists():
        poetry = ProjectRef(pyproject_file)
        poetry.load()
        return poetry

    else:
        raise RuntimeError("Missing ci pyproject")


def rel_path_list_for_pycham_upload() -> list[str]:
    prj = get_ci_project()
    return sorted(prj.get_all_dependencies_rel_path())


def print_project_dependencies() -> None:
    print("Local dependencies:")
    for path in rel_path_list_for_pycham_upload():
        print(f"{path}")


if __name__ == "__main__":
    print_project_dependencies()
