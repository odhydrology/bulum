# bulum

## Installation

This package may be installed using pip from GitHub, directly from PyPi (public), or from a .tar.gz. Examples are shown below.

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

## Build and Upload to PyPi

First build a distribution from an anaconda prompt in the root of your project, and then upload the dist to PyPi using Twine.

```bash
python setup.py sdist
```

```bash
twine upload dist\bulum-0.0.32.tar.gz
```

As of Nov 2023, PyPi uses an API token instead of a conventional password. You can still use Twine, but the username is "\_\_token__", and password is the API token which is very long string starting with "pypi-". 

``` bash
username = __token__
password = pypi-#####################################################################################
```

Where can I find the API token password? Chas has it in his emails. It is also here on the network at *.\ODH working files\Professional development, reading, etc\Software\ODHSoftware\bulum\PyPi_password_and_instructions.txt*.

How do I make a new API token? Go to your PyPi account settings, and click on "API tokens". Then click on "Add API token", and give it a name. The token will be displayed on the next screen.

Alternatively, 
```bash
uv build
uv publish --token XXXXX
```

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
