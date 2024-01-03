from pathlib import Path
from typing import Callable, Set, Type, Union

from git import PathLike, Repo
from git.exc import GitCommandError, InvalidGitRepositoryError
from vedro.core import Dispatcher, Plugin, PluginConfig
from vedro.events import ArgParsedEvent, ArgParseEvent, StartupEvent

__all__ = ("VedroGitChanged", "VedroGitChangedPlugin", "VedroGitChangedError",)


class VedroGitChangedError(Exception):
    pass


def _create_git_repo(path: PathLike) -> Repo:
    return Repo(path, search_parent_directories=True)


class VedroGitChangedPlugin(Plugin):
    def __init__(self, config: Type["VedroGitChanged"], *,
                 git_repo_factory: Callable[[PathLike], Repo] = _create_git_repo) -> None:
        super().__init__(config)
        self._git_repo_factory = git_repo_factory
        self._branch: Union[str, None] = None
        self._repo: Union[Repo, None] = None

    def subscribe(self, dispatcher: Dispatcher) -> None:
        dispatcher.listen(ArgParseEvent, self.on_arg_parse) \
                  .listen(ArgParsedEvent, self.on_arg_parsed) \
                  .listen(StartupEvent, self.on_startup)

    def on_arg_parse(self, event: ArgParseEvent) -> None:
        event.arg_parser.add_argument("--git-changed-against",
                                      help="Git branch to compare against")

    def on_arg_parsed(self, event: ArgParsedEvent) -> None:
        self._branch = event.args.git_changed_against

    async def on_startup(self, event: StartupEvent) -> None:
        if self._branch is None:
            return

        try:
            self._repo = self._git_repo_factory(Path.cwd())
        except InvalidGitRepositoryError as e:
            message = (
                "Unable to find a Git repository in the current directory or any parent "
                "directories. Please ensure that you are in a directory that is part of a "
                "valid git repository."
            )
            raise VedroGitChangedError(message) from e

        try:
            self._repo.git.fetch()
        except GitCommandError as e:
            message = (
                "An error occurred while attempting to fetch updates from the remote git "
                "repository. This may be due to network issues, authentication problems, "
                "or the repository being inaccessible. Please verify your repository's "
                "remote settings and network connectivity."
            )
            raise VedroGitChangedError(message) from e

        changed_files = self._get_changed_files()

        async for scenario in event.scheduler:
            if len(changed_files) == 0 or (scenario.path not in changed_files):
                event.scheduler.ignore(scenario)

    def _get_scenarios_path(self) -> Path:
        return Path.cwd() / "scenarios/"

    def _get_changed_files(self) -> Set[Path]:
        assert self._repo is not None  # for type checker

        try:
            diff = self._repo.git.diff("--name-only", "--diff-filter=ACMTR",
                                       f"origin/{self._branch}...HEAD", "--", ".")
        except GitCommandError as e:
            message = (
                "Failed to retrieve the file differences from the git repository for branch "
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
    description = "Run only changed scenarios based on git diff"
