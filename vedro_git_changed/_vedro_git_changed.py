from datetime import datetime
from pathlib import Path
from time import time
from typing import Callable, Set, Type, Union

from git import PathLike, Repo
from git.exc import GitCommandError, InvalidGitRepositoryError
from niltype import Nil, Nilable
from vedro.core import Dispatcher, Plugin, PluginConfig
from vedro.core.exp.local_storage import LocalStorageFactory, create_local_storage
from vedro.events import ArgParsedEvent, ArgParseEvent, CleanupEvent, StartupEvent

__all__ = ("VedroGitChanged", "VedroGitChangedPlugin", "VedroGitChangedError",)


class VedroGitChangedError(Exception):
    pass


def _create_git_repo(path: PathLike) -> Repo:
    return Repo(path, search_parent_directories=True)


class VedroGitChangedPlugin(Plugin):
    def __init__(self, config: Type["VedroGitChanged"], *,
                 local_storage_factory: LocalStorageFactory = create_local_storage,
                 git_repo_factory: Callable[[PathLike], Repo] = _create_git_repo) -> None:
        super().__init__(config)
        self._local_storage = local_storage_factory(self)
        self._git_repo_factory = git_repo_factory
        self._repo: Union[Repo, None] = None
        self._branch: Union[str, None] = None
        self._cache_duration: int = 60
        self._last_fetched: Nilable[int] = Nil
        self._no_changed: bool = False

    def subscribe(self, dispatcher: Dispatcher) -> None:
        dispatcher.listen(ArgParseEvent, self.on_arg_parse) \
                  .listen(ArgParsedEvent, self.on_arg_parsed) \
                  .listen(StartupEvent, self.on_startup) \
                  .listen(CleanupEvent, self.on_cleanup)

    def on_arg_parse(self, event: ArgParseEvent) -> None:
        group = event.arg_parser.add_argument_group("Git Changed")
        group.add_argument("--changed-against-branch",
                           help=("Run only scenarios that have changed relative to the "
                                 "specified git branch"))
        group.add_argument("--changed-fetch-cache", type=int, default=self._cache_duration,
                           help=("Duration to cache the results of 'git fetch' "
                                 f"(default: {self._cache_duration} seconds)"))

    def on_arg_parsed(self, event: ArgParsedEvent) -> None:
        self._branch = event.args.changed_against_branch
        self._cache_duration = event.args.changed_fetch_cache
        if self._cache_duration < 0:
            raise ValueError("Cache duration must be non-negative. "
                             "Please provide a valid value for '--changed-fetch-cache'.")

    async def on_startup(self, event: StartupEvent) -> None:
        if self._branch is None:
            return

        try:
            self._repo = self._git_repo_factory(Path.cwd())
        except InvalidGitRepositoryError as e:
            message = (
                "Unable to find a Git repository in the current or any parent directories. "
                "Ensure you are in a directory that is part of a valid git repository."
            )
            raise VedroGitChangedError(message) from e

        self._last_fetched = await self._local_storage.get("last_fetched")
        if self._should_fetch(self._last_fetched):
            try:
                self._repo.git.fetch()
            except GitCommandError as e:
                message = (
                    "An error occurred during 'git fetch'. This may be due to network issues, "
                    "authentication problems, or inaccessible repository. Verify your remote "
                    "settings and network connectivity."
                )
                raise VedroGitChangedError(message) from e
            else:
                self._last_fetched = self._now()

        changed_files = self._get_changed_files()
        self._no_changed = len(changed_files) == 0

        async for scenario in event.scheduler:
            if self._no_changed or (scenario.path not in changed_files):
                event.scheduler.ignore(scenario)

    async def on_cleanup(self, event: CleanupEvent) -> None:
        if self._branch is None:
            return

        if self._no_changed:
            summary = f"No scenarios have changed relative to the '{self._branch}' branch"
            if self._last_fetched is not Nil:
                at = datetime.fromtimestamp(self._last_fetched).strftime("%Y-%m-%d %H:%M:%S")
                summary += f" since the last fetch at {at}"
            event.report.add_summary(summary)

        if self._last_fetched is not Nil:
            await self._local_storage.put("last_fetched", self._last_fetched)
            await self._local_storage.flush()

    def _now(self) -> int:
        return int(time())

    def _should_fetch(self, last_fetched: Nilable[int]) -> bool:
        return (last_fetched is Nil) or (self._now() - last_fetched > self._cache_duration)

    def _get_scenarios_path(self) -> Path:
        return Path.cwd() / "scenarios/"

    def _get_changed_files(self) -> Set[Path]:
        assert self._repo is not None  # for type checker

        try:
            diff = self._repo.git.diff("--name-only", "--diff-filter=ACMTR",
                                       f"origin/{self._branch}...HEAD", "--", ".")
        except GitCommandError as e:
            message = (
                "Failed to retrieve the file differences from the git repository for the branch "
                f"'{self._branch}'. Please ensure that the branch name is correct and exists."
            )
            raise VedroGitChangedError(message) from e

        git_path = Path(self._repo.working_dir)
        scenarios_path = self._get_scenarios_path()

        changed_files = set()
        for file in diff.splitlines():
            path = git_path / file
            if scenarios_path in path.parents:
                changed_files.add(path)
        return changed_files


class VedroGitChanged(PluginConfig):
    plugin = VedroGitChangedPlugin
    description = ("Runs only scenarios that have changed based on git diff "
                   "against a specified branch")
