#!/usr/bin/env python3

# get token by https://oauth.yandex-team.ru/authorize?response_type=token&client_id=5f671d781aca402ab7460fde4050267b
# and put it in ~/.startrek/token

from __future__ import annotations

import datetime
import json
import logging
import os
import re
import subprocess

import click


LOGGER = logging.getLogger("changelog_gatherer")


class ChangelogGatherer:
    history_depth = 100  # how far to look for the commits
    prj_common_prefix = "datalens/backend/"
    shorten_prefixes = (
        "library/python/",
        "contrib/python/",
        "datalens/backend/",
    )
    # flatten_sep = '     '
    flatten_sep = "\n      "
    release_message_tpl = "releasing version {version}"
    changelog_prj_tpl = "\n\n === {name_short} ===\n\n<{{changes\n\n"
    changelog_prj_end_tpl = "}}>\n"
    changelog_message_tpl = "* (({url} {rev}))  {author}@  {message_flat}\n"
    strip_message_review = True

    # state (debug)
    history = None
    commit_from = None
    commit_to = None
    rev_spec = None
    peerdirs = None
    all_histories = None

    def __init__(self, v_from, v_to=None, arc_path=None, prj=None):
        self.v_from = v_from

        if v_to is None:
            try:
                fobj_cm = open("changelog.md")
            except OSError:
                raise Exception("Could not auto-determine the v_to")
            with fobj_cm as fobj:
                version = fobj.readline().strip()
            # if len(version.split('.')) != 3:
            #     raise Exception("Does not look like a version from changelog", dict(verison=version))
            v_to = version
            LOGGER.info("v_to=%r", v_to)

        self.v_to = v_to

        if arc_path is None or prj is None:
            assert arc_path is None and prj is None, "both should be empty for auto-detect"
            cwd = os.path.realpath(".")
            cwd_pieces = cwd.split("/" + self.prj_common_prefix)
            if len(cwd_pieces) != 2 or not cwd_pieces[-1]:
                raise Exception("Could not auto-determine the arc+project paths", dict(cwd=cwd))
            arc_path, prj = cwd_pieces
            prj = os.path.join(self.prj_common_prefix, prj)
            LOGGER.info("arc_path=%r, prj=%r", arc_path, prj)

        self.arc_path = arc_path
        self.prj = prj

    @property
    def prj_short(self):
        return self.prj.strip("/").rsplit("/", 1)[-1]

    @property
    def prj_path(self):
        return os.path.join(self.arc_path, self.prj)

    def get_output(self, cmd, cwd, require=True):
        if not cwd.startswith("/"):
            cwd = os.path.join(self.arc_path, cwd)
        assert cwd.startswith("/")
        popen = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=cwd,
        )
        output = []
        err = []
        while popen.poll() is None:
            output += popen.stdout.readlines()
            err += popen.stderr.readlines()
        ret = popen.poll()
        output += popen.stdout.readlines()
        output = [line.decode("utf-8") for line in output]
        if require:
            if ret:
                raise Exception("Non-zero exit", dict(cmd=cmd, ret=ret, err=err))
            return output
        return output, ret

    def get_peerdirs(self, prj):
        """simple non-robust non-recursive peerdirs"""
        ya_make_path = os.path.join(self.arc_path, prj, "ya.make")
        with open(ya_make_path) as fobj:
            ya_make_text = fobj.read()
        ya_make_text = re.sub(r" *#[^\n]*\n", "\n", ya_make_text)
        pieces = ya_make_text.split("PEERDIR(", 1)
        if len(pieces) != 2:
            return []
        peerdirs = pieces[-1].split(")", 1)[0].splitlines()
        peerdirs = [line.strip() for line in peerdirs]
        peerdirs = [line for line in peerdirs if line]
        return peerdirs

    def get_peerdirs_recursive(self, prj, recurse_filter=None):
        """A broken prototype. Not very useful anyway."""
        queue = [prj]
        processed = set()
        result = []
        result_set = {prj}
        while queue:
            item = queue.pop(0)

            if item in processed:
                continue
            processed.add(item)

            try:
                item_peers = self.get_peerdirs(item)
            except OSError:
                LOGGER.error("Could not get peers for %r", item)
                continue

            item_peers_new = [peer for peer in item_peers if peer not in result_set]
            LOGGER.debug("new peerdirs for %r: %r", item, item_peers_new)
            result.extend(item_peers_new)
            result_set.update(item_peers_new)

            queue_new = [
                rec_item
                for rec_item in item_peers_new
                if rec_item not in processed
                and (rec_item.startswith(recurse_filter) if recurse_filter is not None else True)
            ]
            if queue_new:
                LOGGER.debug(
                    "peerdirs recurse queue size %r -> %r: %r", len(queue), len(queue) + len(queue_new), queue_new
                )
                queue.extend(queue_new)

        return result

    def get_history(self, prj, *args):
        history = self.get_output(
            ["arc", "log", "--json"] + list(args) + ["."],
            cwd=prj,
        )
        history = json.loads("".join(history))
        return history

    def find_version(self, version, history):
        message_piece = self.release_message_tpl.format(version=version)
        for item in history:
            if item["message"].strip().startswith(message_piece):
                return item
        return None

    # Straight out of `gitchronicler`

    def create_log_link(self, log):
        revision = log.get("revision")
        if revision is not None:
            return self.create_arc_revision_link(revision)
        return self.create_arc_vcs_commit_link(log["commit"])

    @staticmethod
    def create_arc_revision_link(revision):
        return "https://a.yandex-team.ru/arc/commit/{}".format(revision)

    @staticmethod
    def create_arc_vcs_commit_link(commit):
        return "https://a.yandex-team.ru/arc_vcs/commit/{}".format(commit)

    # ...

    @staticmethod
    def _commit_rev(commit_data):
        rev = commit_data.get("revision")
        if rev:
            return f"r{rev}"
        return commit_data["commit"]

    def get_rev_spec(self):
        LOGGER.debug("Finding version-commits...")
        history = self.get_history(self.prj, f"-n{self.history_depth}")
        self.history = history

        commit_from = self.find_version(self.v_from, history)
        if not commit_from:
            raise Exception(f"Could not find commit for from-version {self.v_from} in `arc log .`")
        self.commit_from = commit_from

        commit_to = self.find_version(self.v_to, history)
        if not commit_to:
            raise Exception(f"Could not find commit for to-version {self.v_to} in `arc log .`")
        self.commit_to = commit_to

        rev_from = self._commit_rev(commit_from)
        rev_to = self._commit_rev(commit_to)
        rev_spec = f"{rev_from}..{rev_to}"
        self.rev_spec = rev_spec
        return rev_spec

    def get_histories(self, peerdirs, rev_spec):
        all_histories = {}
        self.all_histories = all_histories

        for pd in [self.prj] + peerdirs:
            LOGGER.debug("Gathering history for %r...", pd)
            pd_hist = self.get_history(pd, rev_spec)
            all_histories[pd] = pd_hist

        return all_histories

    def shorten_name(self, name):
        for pfx in self.shorten_prefixes:
            if name.startswith(pfx):
                return name[len(pfx) :]
        return name

    def format_changes(self, all_histories):
        results = []
        for name, logs in all_histories.items():
            if not logs:
                continue
            tpl_data_prj = dict(
                name=name,
                name_short=self.shorten_name(name),
            )
            results.append(self.changelog_prj_tpl.format(**tpl_data_prj))
            for entry in logs:
                # # Example entry:
                # {
                #     'commit': 'bd17071c917040b4c4b5e3fca172c1aa4e9ef083',
                #     'parents': ['f6cc30ed5cda3479e44c8f7d6429b1596a4443fb'],
                #     'author': 'hhell',
                #     'date': '2020-08-03T16:47:55+03:00',
                #     'revision': 7170013,
                #     'message': 'tier1 venv-preparer, commonized all-local-libs installation\n\nREVIEW: 1372209',
                #     'path': 'datalens/backend/clickhouse-sqlalchemy',
                #     'attributes': {
                #         'pr.id': '1372209',
                #          'svn.txn.date': '2020-08-03T16:47:54+03:00',
                #         'svn.txn.id': '7170012-4yr77',
                #     },
                # }
                tpl_data = {}
                tpl_data.update(entry)
                tpl_data.update(tpl_data_prj)

                message = entry["message"]
                message = message.strip()
                message = re.sub(r"\n+", "\n", message)
                if self.strip_message_review:
                    message = re.sub(r"REVIEW: [0-9]+$", "", message)
                    message = message.strip()
                message = re.sub(r"((?:IGNIETFERRO|SUBBOTNIK)-[0-9]+)", r"%%\1%%", message)

                tpl_data.update(
                    message=message,
                    url=self.create_log_link(entry),
                    rev=self._commit_rev(entry),
                    message_flat=message.replace("\n", self.flatten_sep),
                )
                results.append(self.changelog_message_tpl.format(**tpl_data))

            results.append(self.changelog_prj_end_tpl.format(**tpl_data_prj))

        return "".join(results)

    def main(self):
        rev_spec = self.get_rev_spec()

        peerdirs = self.get_peerdirs_recursive(
            self.prj,
            recurse_filter=self.prj_common_prefix,
        )
        self.peerdirs = peerdirs

        all_histories = self.get_histories(
            peerdirs=peerdirs,
            rev_spec=rev_spec,
        )
        result = self.format_changes(all_histories)
        return result

    st_ua = "arcadia/datalens/backend/tools/changelog_gather.py"

    def st_ticket(self, changelog, env_name):
        from startrek_client import Startrek

        token = open(os.path.expanduser("~/.startrek/token")).read().strip()
        stc = Startrek(
            useragent=self.st_ua,
            token=token,
        )
        today = datetime.date.today().isoformat()
        info = dict(
            queue="BI",
            type="release",
            summary=f"Release {today}: {env_name} {self.prj_short} {self.v_from} -> {self.v_to}",
            description=changelog,
            tags=[env_name, self.prj_short],
        )
        new_issue = stc.issues.create(**info)
        new_issue_url = f"https://st.yandex-team.ru/{new_issue.key}"
        return new_issue_url

    def run_releaser(self, env, st_ticket=None):
        cmd = [
            "releaser",
            "deploy",
            "--environment",
            env,
            "--version",
            self.v_to,
            "--target-state",
            "committed",
        ]
        if st_ticket:
            cmd.extend(
                [
                    "--deploy-comment-format",
                    "%s {version}  %s\n\n{changelog}" % (self.prj_short, st_ticket),
                ]
            )

        return subprocess.run(cmd, cwd=self.prj_path, check=True)


@click.command()
@click.option("-v", "--verbose", count=True)
@click.option("--st-ticket-int", is_flag=True, default=False)
@click.option("--st-ticket-ext", is_flag=True, default=False)
@click.option("--st-ticket-dc", is_flag=True, default=False)
@click.option("--st-ticket-il", is_flag=True, default=False)
@click.option("--release-env", default=None)
@click.argument("v_from", nargs=1)
@click.argument("v_to", required=False, default=None)
def main(
    v_from,
    v_to,
    release_env=None,
    st_ticket_int=False,
    st_ticket_ext=False,
    st_ticket_dc=False,
    st_ticket_il=False,
    verbose=0,
):
    logging.basicConfig(level=logging.DEBUG if verbose else logging.INFO)
    logging.getLogger("yandex_tracker_client.objects").setLevel(logging.INFO)  # floody

    worker = ChangelogGatherer(v_from=v_from, v_to=v_to)
    result = worker.main()
    click.echo(result)

    st_urls = ""
    if st_ticket_int:
        st_url = worker.st_ticket(changelog=result, env_name="int-production")
        click.echo(st_url)
        st_urls += " " + st_url
    if st_ticket_ext:
        st_url = worker.st_ticket(changelog=result, env_name="ext-production")
        click.echo(st_url)
        st_urls += " " + st_url
    if st_ticket_dc:
        st_url = worker.st_ticket(changelog=result, env_name="dc-production")
        click.echo(st_url)
        st_urls += " " + st_url
    if st_ticket_il:
        st_url = worker.st_ticket(changelog=result, env_name="nebius-israel")
        click.echo(st_url)
        st_urls += " " + st_url

    if release_env:
        worker.run_releaser(env=release_env, st_ticket=st_url)

    return result


if __name__ == "__main__":
    main()  # noqa
