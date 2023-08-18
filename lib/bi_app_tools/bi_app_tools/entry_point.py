#!/usr/bin/env python3

from __future__ import annotations

import os
import sys
from typing import Any, Optional, Callable

from .utils import register_sa_dialects


class BIEntryPoint:
    """
    Arcadia tier0 entry-point manager.

    Structure:
       main()
         -> common_setup()
         -> get_subcommands()
         -> main_⋯()

    """

    # ### Overridables ###

    # Names of commands that should be available in `/usr/local/bin`
    # These names will have `_` -> `-` replacement, for consistency.
    subcommands_global: tuple[str, ...] = (
        "uwsgi", "gunicorn", "celery",
        "bi_core_rqe_async", "setup_iptables",
        "ipython", "pyexec", "flask_shell",
    )

    # Can"t do `def main_-c(self):` without extra horrors.
    special_subcommands: dict[str, str] = {"-c": "pyexec"}

    subcommand_method_prefix: str = "main_"
    global_subcommands_path: str = "/usr/local/bin"
    do_register_sa_dialects: bool = True

    def common_setup(self, **kwargs: Any) -> None:
        """
        Overridable method for preparation common to all entry points.

        When overriding, make sure to call super().
        """
        # as a replacement for the entry-points-based registering.
        if self.do_register_sa_dialects:
            register_sa_dialects()

    # ### Subcommands ###

    # ## Serving ##

    @staticmethod
    def main_uwsgi() -> Any:
        import pyuwsgi  # type: ignore
        return pyuwsgi.run()

    @staticmethod
    def main_gunicorn() -> Any:
        import gunicorn.app.wsgiapp  # type: ignore
        return gunicorn.app.wsgiapp.run()

    @staticmethod
    def main_celery() -> Any:
        # # Newer celery, perhaps?:
        # import celery.__main__
        # return celery.__main__.main()
        # # https://a.yandex-team.ru/arc/trunk/arcadia/contrib/python/celery/bin/__main__.py?rev=3328957#L13
        from celery import maybe_patch_concurrency  # type: ignore
        if "multi" not in sys.argv:
            maybe_patch_concurrency()
        from celery.bin.celery import main as _main  # type: ignore
        return _main()

    @staticmethod
    def main_bi_core_rqe_async() -> Any:
        from bi_core.connection_executors.remote_query_executor.app_async import async_qe_main  # type: ignore
        return async_qe_main()

    # ## Tooling ##

    @staticmethod
    def main_setup_iptables() -> Any:
        from bi_core.bin.setup_iptables import main  # type: ignore
        return main()

    # ## Debug ##

    @staticmethod
    def main_ipython() -> Any:
        import IPython
        # see also: IPython.embed()
        return IPython.start_ipython()

    @staticmethod
    def main_pyexec() -> Any:
        """ `python -c ⋯` equivalent """
        cmd = sys.argv[1]
        return exec(cmd)

    @staticmethod
    def main_flask_shell() -> Any:
        import flask_shell_ipython  # type: ignore
        return flask_shell_ipython.shell()

    # ## Entry-poing tooling ##

    def main_subcommands_list(self) -> None:
        """ List known subcommands; useful for debug """
        for cmd in self.subcommands:
            sys.stdout.write(cmd)
            sys.stdout.write("\n")
        sys.stdout.flush()

    def main_subcommands_to_bin(self) -> None:
        """ Symlink subcommands for convenient execution """
        path_main = os.path.realpath(sys.argv[0])
        path_base = self.global_subcommands_path
        for cmd in self.subcommands_global:
            cmd_dash = cmd.replace("_", "-")
            path_bin = f"{path_base}/{cmd_dash}"
            if os.path.exists(path_bin):
                sys.stderr.write(f"Exists: {path_bin}\n")
                continue
            sys.stderr.write(f"Creating: {path_bin}\n")
            os.symlink(src=path_main, dst=path_bin)

    # ### Tooling ###

    @property
    def name(self) -> str:
        base = sys.argv[0]
        base = os.path.realpath(base)
        base = os.path.basename(base)
        return base

    @property
    def subcommands(self) -> tuple[str, ...]:
        prefix = self.subcommand_method_prefix
        return tuple(
            name[len(prefix):]
            for name in dir(self)
            if name.startswith(prefix))

    @property
    def usage(self) -> str:
        name = self.name
        subcommands = ", ".join(self.subcommands)
        argv = sys.argv
        return (
            f"Usage: `{name} <subcommand> <subcommand_args>`, "
            f"or `<subcommand> <subcommand_args>`; "
            f"subcommands: {subcommands}; "
            f"Got: {argv}."
        )

    def get_subcommand_name(self, argv: list[str], subcommands: tuple[str, ...]) -> str:
        # Option #1: symlinked form
        # Note: the binary"s name, not the first argument.
        bin_name = argv[0]  # if it"s `sys.argv`, it *really* shouldn"t be empty.
        bin_name = os.path.basename(bin_name)  # e.g. "/usr/local/bin/ipython" -> "ipython"
        subcommand = bin_name.replace("-", "_")  # python-method-compatible normalize.
        if subcommand in subcommands:
            return subcommand

        # Option #2: first argument.
        try:
            subcommand = argv[1]
        except IndexError:  # sys.argv == ["/code/app/bi_⋯_app"]
            raise Exception(self.usage)

        subcommand = self.special_subcommands.get(subcommand, subcommand)

        if subcommand in self.subcommands:
            # ["/code/app/bi_⋯_app", "uwsgi", "--ini", ⋯] -> ["/code/app/bi_⋯_app", "--ini", ⋯]
            # i.e. ready to run a library"s main.
            argv.pop(1)  # Mutates in-place
            return subcommand

        raise Exception(
            "Unknown subcommand",
            dict(subcommand=subcommand, available=subcommands))

    def get_subcommand_main(self, name: str) -> Callable:
        return getattr(
            self,
            self.subcommand_method_prefix + name)

    def main(self, argv: Optional[list[str]] = None) -> Any:
        # Note: might mutate `sys.argv` later, regardless of this.
        if argv is None:
            argv = sys.argv
        else:
            argv = list(argv)

        subcommands = self.subcommands
        # Mutates the `argv`:
        subcommand_name = self.get_subcommand_name(argv=argv, subcommands=subcommands)
        subcommand_main = self.get_subcommand_main(subcommand_name)

        self.common_setup(
            subcommands=subcommands,
            subcommand_name=subcommand_name,
            subcommand_main=subcommand_main,
            argv=argv,
        )

        # Make sure the main is started with the altered argv.
        if sys.argv != argv:
            sys.argv[:] = argv

        return subcommand_main()
