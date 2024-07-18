import time
from datetime import datetime
from pathlib import Path
from typing import Callable, Type, Union

from niltype import Nil, Nilable
from vedro.core import Dispatcher, Plugin, PluginConfig
from vedro.core.exp.local_storage import LocalStorageFactory, create_local_storage
from vedro.events import (
    ArgParsedEvent,
    ArgParseEvent,
    CleanupEvent,
    ConfigLoadedEvent,
    StartupEvent,
)

from ._git_repo import GitRepo

__all__ = ("VedroGitChanged", "VedroGitChangedPlugin",)


class VedroGitChangedPlugin(Plugin):
    def __init__(self, config: Type["VedroGitChanged"], *,
                 git_repo_factory: Callable[[], GitRepo] = GitRepo,
                 local_storage_factory: LocalStorageFactory = create_local_storage) -> None:
        super().__init__(config)
        self._local_storage_factory = local_storage_factory
        self._git_repo = git_repo_factory()
        self._branch: Union[str, None] = None
        self._default_cache_duration: int = 60
        self._cache_duration: int = self._default_cache_duration
        self._last_fetched: Nilable[int] = Nil
        self._no_fetch: bool = False
        self._no_changed: bool = False

    def subscribe(self, dispatcher: Dispatcher) -> None:
        dispatcher.listen(ConfigLoadedEvent, self.on_config_loaded) \
                  .listen(ArgParseEvent, self.on_arg_parse) \
                  .listen(ArgParsedEvent, self.on_arg_parsed) \
                  .listen(StartupEvent, self.on_startup) \
                  .listen(CleanupEvent, self.on_cleanup)

    def on_config_loaded(self, event: ConfigLoadedEvent) -> None:
        self._local_storage = self._local_storage_factory(self, event.config.project_dir)

    def on_arg_parse(self, event: ArgParseEvent) -> None:
        group = event.arg_parser.add_argument_group("Git Changed")
        group.add_argument("--changed-against-branch",
                           help=("Run only scenarios that have changed relative to the "
                                 "specified git branch"))
        group.add_argument("--changed-fetch-cache", type=int, default=self._default_cache_duration,
                           help=("Duration to cache the results of 'git fetch' "
                                 f"(default: {self._cache_duration} seconds)"))
        group.add_argument("--changed-no-fetch", action="store_true",
                           help="Do not fetch the latest changes from the remote repository")

    def on_arg_parsed(self, event: ArgParsedEvent) -> None:
        self._branch = event.args.changed_against_branch
        self._cache_duration = event.args.changed_fetch_cache
        self._no_fetch = event.args.changed_no_fetch

        if self._cache_duration < 0:
            raise ValueError("Cache duration must be non-negative. "
                             "Please provide a valid value for '--changed-fetch-cache'.")

        if self._no_fetch and self._cache_duration != self._default_cache_duration:
            raise ValueError("The options '--changed-no-fetch' and '--changed-fetch-cache' "
                             "cannot be used together. Please choose one.")

    async def on_startup(self, event: StartupEvent) -> None:
        if self._branch is None:
            return

        if not self._no_fetch:
            self._last_fetched = await self._local_storage.get("last_fetched")
            if self._should_fetch(self._last_fetched):
                self._git_repo.fetch()
                self._last_fetched = self._now()

        target_directory = Path.cwd() / "scenarios/"
        changed_files = self._git_repo.get_changed_files(self._branch, target_directory)
        self._no_changed = len(changed_files) == 0

        async for scenario in event.scheduler:
            if self._no_changed or (scenario.path not in changed_files):
                event.scheduler.ignore(scenario)

    async def on_cleanup(self, event: CleanupEvent) -> None:
        if self._branch is None:
            return

        if self._no_changed:
            event.report.add_summary(self._create_summary())

        if self._last_fetched is not Nil:
            await self._local_storage.put("last_fetched", self._last_fetched)
            await self._local_storage.flush()

    def _create_summary(self) -> str:
        summary = f"No scenarios have changed relative to the '{self._branch}' branch"
        if self._last_fetched is not Nil:
            at = datetime.fromtimestamp(self._last_fetched).strftime("%Y-%m-%d %H:%M:%S")
            summary += f" since the last fetch at {at}"
        return summary

    def _now(self) -> int:
        return int(time.time())

    def _should_fetch(self, last_fetched: Nilable[int]) -> bool:
        return (last_fetched is Nil) or (self._now() - last_fetched > self._cache_duration)


class VedroGitChanged(PluginConfig):
    plugin = VedroGitChangedPlugin
    description = ("Runs only scenarios that have changed based on git diff "
                   "against a specified branch")
