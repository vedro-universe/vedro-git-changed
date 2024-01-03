from datetime import datetime
from time import time
from unittest.mock import Mock, call, patch

import pytest
from baby_steps import given, then, when
from pytest import raises
from vedro.core import Dispatcher
from vedro.core import MonotonicScenarioScheduler as ScenarioScheduler

from ._utils import (
    dispatcher,
    fire_arg_parsed_event,
    fire_cleanup_event,
    fire_startup_event,
    git_changed_plugin,
    git_repo_,
    local_storage_,
    make_scenarios_path,
    make_vscenario,
)

__all__ = ("git_changed_plugin", "dispatcher", "git_repo_", "local_storage_",)  # fixtures


@pytest.mark.usefixtures(git_changed_plugin.__name__)
async def test_plugin_arg_parsed_error(*, dispatcher: Dispatcher):
    with given:
        changed_against_branch = "main"
        changed_fetch_cache = -1

    with when, raises(Exception) as exception:
        await fire_arg_parsed_event(dispatcher,
                                    changed_against_branch=changed_against_branch,
                                    changed_fetch_cache=changed_fetch_cache)

    with then:
        assert exception.type is ValueError
        assert str(exception.value) == (
            "Cache duration must be non-negative. "
            "Please provide a valid value for '--changed-fetch-cache'."
        )


@pytest.mark.usefixtures(git_changed_plugin.__name__)
async def test_plugin_startup_no_changes(*, dispatcher: Dispatcher,
                                         git_repo_: Mock, local_storage_: Mock):
    with given:
        await fire_arg_parsed_event(dispatcher, changed_against_branch=(branch_name := "main"))

        scenario1, scenario2 = make_vscenario("scenario1.py"), make_vscenario("scenario2.py")
        scheduler = ScenarioScheduler(scenarios=[scenario1, scenario2])

    with when:
        await fire_startup_event(dispatcher, scheduler=scheduler)

    with then:
        assert local_storage_.mock_calls == [
            call.get("last_fetched")
        ]
        assert git_repo_.mock_calls == [
            call.fetch(),
            call.get_changed_files(branch_name, make_scenarios_path())
        ]
        assert list(scheduler.scheduled) == []


@pytest.mark.usefixtures(git_changed_plugin.__name__)
async def test_plugin_startup(*, dispatcher: Dispatcher, git_repo_: Mock, local_storage_: Mock):
    with given:
        await fire_arg_parsed_event(dispatcher, changed_against_branch=(branch_name := "main"))

        scenario1, scenario2 = make_vscenario("scenario1.py"), make_vscenario("scenario2.py")
        scheduler = ScenarioScheduler(scenarios=[scenario1, scenario2])

        git_repo_.get_changed_files.return_value = [
            make_scenarios_path() / "scenario1.py"
        ]

    with when:
        await fire_startup_event(dispatcher, scheduler=scheduler)

    with then:
        assert local_storage_.mock_calls == [
            call.get("last_fetched")
        ]
        assert git_repo_.mock_calls == [
            call.fetch(),
            call.get_changed_files(branch_name, make_scenarios_path())
        ]
        assert list(scheduler.scheduled) == [scenario1]


@pytest.mark.usefixtures(git_changed_plugin.__name__)
async def test_plugin_startup_no_fetch(*, dispatcher: Dispatcher,
                                       git_repo_: Mock, local_storage_: Mock):
    with given:
        await fire_arg_parsed_event(dispatcher, changed_against_branch=(branch_name := "main"))

        local_storage_.get.return_value = int(time())

    with when:
        await fire_startup_event(dispatcher)

    with then:
        assert local_storage_.mock_calls == [
            call.get("last_fetched")
        ]
        assert git_repo_.mock_calls == [
            call.get_changed_files(branch_name, make_scenarios_path())
        ]


@pytest.mark.usefixtures(git_changed_plugin.__name__)
async def test_plugin_cleanup_no_changes(*, dispatcher: Dispatcher,
                                         git_repo_: Mock, local_storage_: Mock):
    with given:
        await fire_arg_parsed_event(dispatcher, changed_against_branch="main")

        with patch("time.time", return_value=(now := 12345)):
            await fire_startup_event(dispatcher)

        local_storage_.reset_mock()
        git_repo_.reset_mock()

    with when:
        report = await fire_cleanup_event(dispatcher)

    with then:
        at = datetime.fromtimestamp(now).strftime("%Y-%m-%d %H:%M:%S")
        assert report.summary == [
            f"No scenarios have changed relative to the 'main' branch since the last fetch at {at}"
        ]
        assert local_storage_.mock_calls == [
            call.put("last_fetched", now),
            call.flush()
        ]
        assert git_repo_.mock_calls == []


@pytest.mark.usefixtures(git_changed_plugin.__name__)
async def test_plugin_cleanup(*, dispatcher: Dispatcher, git_repo_: Mock, local_storage_: Mock):
    with given:
        await fire_arg_parsed_event(dispatcher, changed_against_branch="main")

        git_repo_.get_changed_files.return_value = [
            make_scenarios_path() / "scenario1.py"
        ]
        with patch("time.time", return_value=(now := 12345)):
            await fire_startup_event(dispatcher)

        local_storage_.reset_mock()
        git_repo_.reset_mock()

    with when:
        report = await fire_cleanup_event(dispatcher)

    with then:
        assert report.summary == []
        assert local_storage_.mock_calls == [
            call.put("last_fetched", now),
            call.flush()
        ]
        assert git_repo_.mock_calls == []
