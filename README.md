# bulum

## Installation

This package may be installed using pip from GitHub, directly from PyPI (public), or from a .tar.gz. Examples are shown below.

```bash
pip install git+https://github.com/odhydrology/bulum
```

```bash
pip install bulum
```

```bash
pip install .\dist\bulum-0.0.32.tar.gz
```

## Usage

```python
import bulum

# returns the package version
bulum.__version__

# prints 'Hello world!' to the console
bulum.hello_world()
```

API documentation is available at [odhydrology.github.io/bulum](https://odhydrology.github.io/bulum/).

## Build and upload to PyPI

Pushing any tag starting with `v` runs the Release workflow. The workflow has four jobs.

- **Run CI** runs the CI workflow.
- **Build distribution** checks that the tag matches `v` plus the version in `src/bulum/version.py`, then builds the sdist and wheel with `python -m build`. A tag that does not match fails this job, and the later jobs do not run.
- **Publish to PyPI** uploads the distributions to PyPI by trusted publishing, so it needs no API token. It is skipped for tags containing `dev`.
- **Build documentation on 'docs' branch** merges `main` into the `docs` branch, rebuilds the Sphinx documentation and pushes it. It runs for every tag that passes the build, including `dev` tags.

To build and publish by hand, run these from the root of the project:

```bash
uv build
uv publish --token <API token>
```

Publishing by hand needs a PyPI API token instead of a password. The token is a long string starting with "pypi-".

Where can I find the API token password? Chas has it in his emails. It is also here on the network at *.\ODH working files\Professional development, reading, etc\Software\ODHSoftware\bulum\PyPi_password_and_instructions.txt*.

How do I make a new API token? Go to your PyPI account settings, and click on "API tokens". Then click on "Add API token", and give it a name. The token will be displayed on the next screen.

## Unit Tests

For development testing, first sync with extras=dev via
```bash
uv sync --extra dev
```

To run tests with the current or a particular version of python, from the project root run pytest via (for example):
```bash
uv run pytest
uv run 
uv run tox -e py312 # to run a particular version
```

Before publishing, check the library is functional across all supported versions of python via (one of):
```bash
uv run tox
uv run tox -p auto # This is run in parallel
```

Note: if you are missing python versions, install via (e.g.) 
```bash
uv python install 3.12
```

### Legacy

WARNING: Run unit tests from an anaconda environment with compatible dependencies!

Install the nose2 test-runner framework. 

```bash
pip install nose2
```

Then from the root project folder run the nose2 module. You can do this as a python modules, or just directly from the anaconda prompt (both examples given below). This will automatically find and run tests in any modules named "test_*".

```bash
python -m nose2
```

```bash
nose2
```

You can run specific tests by specifying the module name. Example below.

```bash
nose2 src.bulum.stats.tests
```

## License

Refer to LICENCE.md
