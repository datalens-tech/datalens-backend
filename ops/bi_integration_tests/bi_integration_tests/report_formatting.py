from __future__ import annotations

import contextlib
from typing import Iterable, Sequence
import sys

import attr
from bi_testing_ya.api_wrappers import Resp, Req

from bi_integration_tests.steps import StepExecutionException, StepExecutionReport, ScenarioStep


@attr.s
class ReportFormatter:
    _level: int = attr.ib(init=False, default=0)

    @contextlib.contextmanager
    def section(self, text: str) -> Iterable[None]:
        self._level += 1
        print(f"\n{'=' * (self._level + 2)}{text}", file=sys.stderr)
        try:
            yield
        finally:
            self._level -= 1

    def row(self, text: str):
        print(f"{' ' * self._level}{text}", file=sys.stderr)

    def send_step_exc_report(self, exc: StepExecutionException, err_section_name: str):
        with self.section(err_section_name):
            for exec_rpt in exc.done_steps:
                self.row(exec_rpt.to_short_string())

            with self.section("Failed step"):
                self.row(f"{exc.step.method} {exc.step.url}")

    @contextlib.contextmanager
    def step_exc_handler(self, err_section_name: str) -> Iterable[None]:
        try:
            yield
        except StepExecutionException as exc:
            self.send_step_exc_report(exc, err_section_name)
            raise

    def log_request(self, response: Resp, request: Req):
        report = StepExecutionReport(
            response=response, step=ScenarioStep(method=request.method, url=request.url),
            request=request, exception=None
        )
        self.row(report.to_short_string())

    def send_steps_exec_results(self, section_name: str, steps_exec_results: Sequence[StepExecutionReport]):
        with self.section(section_name):
            for exec_rpt in steps_exec_results:
                self.row(exec_rpt.to_short_string())
