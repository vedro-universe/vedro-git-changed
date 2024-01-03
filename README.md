# Vedro Git Changed

[![Codecov](https://img.shields.io/codecov/c/github/vedro-universe/vedro-git-changed/master.svg?style=flat-square)](https://codecov.io/gh/vedro-universe/vedro-git-changed)
[![PyPI](https://img.shields.io/pypi/v/vedro-git-changed.svg?style=flat-square)](https://pypi.python.org/pypi/vedro-git-changed/)
[![PyPI - Downloads](https://img.shields.io/pypi/dm/vedro-git-changed?style=flat-square)](https://pypi.python.org/pypi/vedro-git-changed/)
[![Python Version](https://img.shields.io/pypi/pyversions/vedro-git-changed.svg?style=flat-square)](https://pypi.python.org/pypi/vedro-git-changed/)

[vedro-git-changed](https://pypi.org/project/vedro-git-changed/) is a Vedro plugin that runs test scenarios which have changed relative to the specified git branch.

## Installation

<details open>
<summary>Quick</summary>
<p>

For a quick installation, you can use a plugin manager as follows:

```shell
$ vedro plugin install vedro-git-changed
```

</p>
</details>

<details>
<summary>Manual</summary>
<p>

To install manually, follow these steps:

1. Install the package using pip:

```shell
$ pip3 install vedro-git-changed
```

2. Next, activate the plugin in your `vedro.cfg.py` configuration file:

```python
# ./vedro.cfg.py
import vedro
import vedro_git_changed

class Config(vedro.Config):

    class Plugins(vedro.Config.Plugins):

        class VedroGitChanged(vedro_git_changed.VedroGitChanged):
            enabled = True
```

</p>
</details>

## Usage

To run test scenarios that have been modified compared to the `main` branch, use the following command:

```shell
$ vedro run --changed-against-branch=main
```

By default, the plugin caches `git fetch` results for 60 seconds. To change this duration, specify a different cache duration in seconds with the `--changed-fetch-cache` argument.

For example, to disable caching, set the cache duration to 0:

```shell
$ vedro run --changed-against-branch=main --changed-fetch-cache=0
```
