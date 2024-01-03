from functools import partial
from pathlib import Path
from typing import Callable, Set, Union

from git import PathLike, Repo
from git.exc import GitCommandError, InvalidGitRepositoryError

__all__ = ("GitRepo", "GitRepoError",)


_create_git_repo = partial(Repo, search_parent_directories=True)


class GitRepoError(Exception):
    pass


class GitRepo:
    def __init__(self, *, git_repo_factory: Callable[[PathLike], Repo] = _create_git_repo) -> None:
        self._git_repo_factory = git_repo_factory
        self._git_repo: Union[Repo, None] = None

    @property
    def repo(self) -> Repo:
        if self._git_repo is None:
            self._git_repo = self._find_repo()
        return self._git_repo

    def _find_repo(self) -> Repo:
        try:
            return self._git_repo_factory(Path.cwd())
        except InvalidGitRepositoryError as e:
            message = (
                "Unable to find a git repository in the current or any parent directories. "
                "Ensure you are in a directory that is part of a valid git repository."
            )
            raise GitRepoError(message) from e

    def fetch(self) -> None:
        try:
            self.repo.git.fetch()
        except GitCommandError as e:
            message = (
                "An error occurred during 'git fetch'. This may be due to network issues, "
                "authentication problems, or inaccessible repository. Verify your remote "
                "settings and network connectivity."
            )
            raise GitRepoError(message) from e

    def get_changed_files(self, against_branch: str, target_directory: Path) -> Set[Path]:
        try:
            diff = self.repo.git.diff("--name-only", "--diff-filter=ACMTR",
                                      f"origin/{against_branch}...HEAD", "--", ".")
        except GitCommandError as e:
            message = (
                "Failed to retrieve the file differences from the git repository for the branch "
                f"'{against_branch}'. Please ensure that the branch name is correct and exists."
            )
            raise GitRepoError(message) from e

        git_path = Path(self.repo.working_dir)

        changed_files = set()
        for file in diff.splitlines():
            path = git_path / file
            if target_directory in path.parents:
                changed_files.add(path)
        return changed_files
