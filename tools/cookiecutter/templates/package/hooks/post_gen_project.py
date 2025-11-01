import os

import tomlkit
import tomlkit.items


def get_metapkg_file() -> str:
    result = os.getenv("METAPKG_FILE")
    if not result:
        raise ValueError("METAPKG_FILE is not set")
    return result


def add_package_to_metapkg(
    package_name: str,
    package_slug: str,
) -> None:
    package_table = tomlkit.inline_table()
    package_table.add("develop", True)
    package_table.add("path", tomlkit.string(f"../lib/{package_slug}"))

    metapkg_file = get_metapkg_file()
    with open(metapkg_file, "r") as f:
        metapkg = tomlkit.load(f)

    dependencies = metapkg["tool"]["poetry"]["group"]["ci"]["dependencies"]
    assert isinstance(dependencies, tomlkit.items.Table)

    dependencies.append(tomlkit.string(package_name), package_table)
    with open(metapkg_file, "w") as f:
        tomlkit.dump(metapkg, f)


if __name__ == "__main__":
    add_package_to_metapkg(
        package_name="{{ cookiecutter.package_name }}",
        package_slug="{{ cookiecutter.package_slug }}",
    )
