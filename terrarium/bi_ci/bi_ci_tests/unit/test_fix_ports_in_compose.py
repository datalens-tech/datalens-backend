from pathlib import Path
from shutil import copyfile

from bi_ci.fix_ports_in_compose import remove_ports_from_docker_compose


def test_remove_ports_from_docker_compose(tmpdir, sample_compose_src, sample_compose_expected):
    tmp_src = Path(tmpdir) / "docker-compose.yml"
    tmp_dst = Path(tmpdir) / "docker-compose-modified.yml"

    copyfile(sample_compose_src, tmp_src)

    remove_ports_from_docker_compose(tmp_src, tmp_dst)

    with open(tmp_dst, "r") as fh:
        output_data = fh.read()

    with open(sample_compose_expected, "r") as fh:
        expected_output_data = fh.read()

    assert output_data == expected_output_data
