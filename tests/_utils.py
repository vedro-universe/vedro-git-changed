from argparse import ArgumentParser, Namespace
from pathlib import Path
from typing import Optional
from unittest.mock import AsyncMock, Mock

import pytest
from niltype import Nil
from vedro import Config as _Config
from vedro import Scenario
from vedro.core import Dispatcher
from vedro.core import MonotonicScenarioScheduler as ScenarioScheduler
from vedro.core import Report, VirtualScenario
from vedro.core.exp.local_storage import LocalStorage
from vedro.events import (
    ArgParsedEvent,
    ArgParseEvent,
    CleanupEvent,
    ConfigLoadedEvent,
    StartupEvent,
)

from vedro_git_changed import GitRepo, VedroGitChanged, VedroGitChangedPlugin

__all__ = ("local_storage_", "git_repo_", "git_changed_plugin", "dispatcher",
           "fire_arg_parsed_event", "fire_startup_event", "fire_cleanup_event",
           "fire_config_loaded_event", "make_scenarios_path", "make_vscenario", "project_dir")


@pytest.fixture()
def local_storage_() -> Mock:
    return Mock(spec=LocalStorage, get=AsyncMock(return_value=Nil))


@pytest.fixture()
def git_repo_() -> Mock:
    return Mock(spec=GitRepo, get_changed_files=Mock(return_value=[]))


@pytest.fixture()
def git_changed_plugin(dispatcher: Dispatcher, git_repo_: Mock,
                       local_storage_: LocalStorage) -> VedroGitChangedPlugin:
    git_repo_factory = Mock(return_value=git_repo_)
    local_storage_factory = Mock(return_value=local_storage_)

    plugin = VedroGitChangedPlugin(VedroGitChanged,
                                   git_repo_factory=git_repo_factory,
                                   local_storage_factory=local_storage_factory)
    plugin.subscribe(dispatcher)
    return plugin


@pytest.fixture()
def dispatcher() -> Dispatcher:
    return Dispatcher()


def make_scenarios_path() -> Path:
    return Path("scenarios/").absolute()


def make_vscenario(filename: str) -> VirtualScenario:
    class _Scenario(Scenario):
        __file__ = make_scenarios_path() / filename

    return VirtualScenario(_Scenario, steps=[])


@pytest.fixture()
def project_dir(tmp_path: Path) -> Path:
    return tmp_path


async def fire_config_loaded_event(dispatcher: Dispatcher, project_dir_: Path) -> None:
    class Config(_Config):
        project_dir = project_dir_

    await dispatcher.fire(ConfigLoadedEvent(Path(), Config))


async def fire_arg_parsed_event(dispatcher: Dispatcher, project_dir: Path, *,
                                changed_against_branch: str,
                                changed_fetch_cache: int = 60,
                                changed_no_fetch: bool = False):
    await fire_config_loaded_event(dispatcher, project_dir)

    arg_parser = ArgumentParser()
    await dispatcher.fire(ArgParseEvent(arg_parser))

    args = Namespace(changed_against_branch=changed_against_branch,
                     changed_fetch_cache=changed_fetch_cache,
                     changed_no_fetch=changed_no_fetch)
    await dispatcher.fire(ArgParsedEvent(args))


async def fire_startup_event(dispatcher: Dispatcher, *,
                             scheduler: Optional[ScenarioScheduler] = None):
    if scheduler is None:
        scheduler = ScenarioScheduler(scenarios=[])
    await dispatcher.fire(StartupEvent(scheduler))


async def fire_cleanup_event(dispatcher: Dispatcher) -> Report:
    report = Report()
    await dispatcher.fire(CleanupEvent(report))
    return report
