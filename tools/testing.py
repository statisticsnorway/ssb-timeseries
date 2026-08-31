"""Testing tools for the Marimo notebooks used to generate documentation."""

import sys
from collections.abc import Callable
from typing import TypeAlias

TestResult: TypeAlias = tuple[str, str, BaseException | None]
# TestReport: TypeAlias = tuple[SystemExit, list[TestResult]]
TestReport: TypeAlias = None | Exception


# def run_and_report(tests: list[Callable]) -> TestReport:
def run_and_report(tests: list[Callable]) -> SystemExit:
    """For a list of tests, run and return whether they all passed plus the their individual results."""
    results = []

    for test in tests:
        result = run_test(test)
        results.append(result)

    # if all(status == "PASSED" for _, status, _ in results):
    #     exitcode = sys.exit(0)
    # else:
    #     exitcode = sys.exit(1)
    # return (exitcode, results)
    for _, status, exc in results:
        if status != "PASSED":
            sys.exit(1)
    sys.exit(0)


def run_test(test: Callable) -> TestResult:
    """Run a single test."""
    try:
        test_result = test()
    except AssertionError as exc:
        report_item = (test.__name__, "FAILED", exc)
    except Exception as exc:
        return test.__name__, "ERROR", exc
    else:
        return test.__name__, "PASSED", None
