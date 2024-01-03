import os
from pathlib import Path
from typing import List, Optional, Tuple
from unittest.mock import Mock, call

import git
from baby_steps import given, then, when
from pytest import raises

from vedro_git_changed import GitRepo, GitRepoError


def _create_repo_factory(files: Optional[List[str]] = None) -> Tuple[Mock, Mock]:
    mock_ = Mock(spec=git.Repo)
    mock_.working_dir = "."
    mock_.git.diff.return_value = os.linesep.join(files or [])

    return Mock(return_value=mock_), mock_


def test_fetch():
    with given:
        repo_factory_, repo_ = _create_repo_factory()
        git_repo = GitRepo(git_repo_factory=repo_factory_)

    with when:
        res = git_repo.fetch()

    with then:
        assert res is None
        assert repo_.mock_calls == [
            call.git.fetch()
        ]
        assert repo_factory_.mock_calls == [
            call(Path.cwd())
        ]


def test_fetch_cmd_error():
    with given:
        repo_factory_, repo_ = _create_repo_factory()
        git_repo = GitRepo(git_repo_factory=repo_factory_)

        repo_.git.fetch.side_effect = git.GitCommandError("fetch")

    with when, raises(Exception) as exception:
        git_repo.fetch()

    with then:
        assert exception.type is GitRepoError
        assert str(exception.value) == (
            "An error occurred during 'git fetch'. This may be due to network issues, "
            "authentication problems, or inaccessible repository. Verify your remote "
            "settings and network connectivity."
        )


def test_fetch_init_error():
    with given:
        repo_factory_, repo_ = _create_repo_factory()
        git_repo = GitRepo(git_repo_factory=repo_factory_)

        repo_factory_.side_effect = git.InvalidGitRepositoryError("error")

    with when, raises(Exception) as exception:
        git_repo.fetch()

    with then:
        assert exception.type is GitRepoError
        assert str(exception.value) == (
            "Unable to find a Git repository in the current or any parent directories. "
            "Ensure you are in a directory that is part of a valid git repository."
        )


def test_get_changed_files():
    with given:
        files = [
            ".gitignore",
            "contexts/registered_user.py",
            "scenarios/login_as_user.py",
            "scenarios/register/register_via_email.py",
        ]
        repo_factory_, repo_ = _create_repo_factory(files=files)
        git_repo = GitRepo(git_repo_factory=repo_factory_)

        branch_name = "main"
        target_directory = Path("scenarios/")

    with when:
        changed = git_repo.get_changed_files(branch_name, target_directory)

    with then:
        assert changed == {
            Path("scenarios/login_as_user.py"),
            Path("scenarios/register/register_via_email.py")
        }
        assert repo_.mock_calls == [
            call.git.diff("--name-only", "--diff-filter=ACMTR",
                          f"origin/{branch_name}...HEAD", "--", ".")
        ]
        assert repo_factory_.mock_calls == [
            call(Path.cwd())
        ]


def test_get_changed_files_no_files():
    with given:
        repo_factory_, repo_ = _create_repo_factory(files=[])
        git_repo = GitRepo(git_repo_factory=repo_factory_)

        branch_name = "main"
        target_directory = Path("scenarios/")

    with when:
        changed = git_repo.get_changed_files(branch_name, target_directory)

    with then:
        assert changed == set()
        assert repo_.mock_calls == [
            call.git.diff("--name-only", "--diff-filter=ACMTR",
                          f"origin/{branch_name}...HEAD", "--", ".")
        ]
        assert repo_factory_.mock_calls == [
            call(Path.cwd())
        ]


def test_get_changed_files_error():
    with given:
        repo_factory_, repo_ = _create_repo_factory(files=[])
        git_repo = GitRepo(git_repo_factory=repo_factory_)

        repo_.git.diff.side_effect = git.GitCommandError("diff")

        branch_name = "main"
        target_directory = Path("scenarios/")

    with when, raises(Exception) as exception:
        git_repo.get_changed_files(branch_name, target_directory)

    with then:
        assert exception.type is GitRepoError
        assert str(exception.value) == (
            "Failed to retrieve the file differences from the git repository "
            "for the branch 'main'. Please ensure that the branch name is correct and exists."
        )
