# bulum

## Installation

This package may be installed using pip from Bitbucket (requires authentication), or directly from PyPi (public), or from a .tar.gz. Examples are shown below.

```bash
pip install git+https://bitbucket.org/odhydrology/bulum.git
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

## Build and Upload to PyPi

First build a distribution from an anaconda prompt in the root of your project, and then upload the dist to PyPi using Twine.

```bash
python setup.py sdist
```

```bash
twine upload dist\bulum-0.0.32.tar.gz
```

As of Nov 2023, PyPi uses an API token instead of a conventional password. You can still use Twine, but the username is "__token__", and password is the API token which is very long string starting with "pypi-". 

``` bash
username = __token__
password = pypi-#####################################################################################
```

Where can I find the API token password? Chas has it in his emails. It is also here on the network at *.\ODH working files\Professional development, reading, etc\Software\ODHSoftware\bulum\PyPi_password_and_instructions.txt*.

How do I make a new API token? Go to your PyPi account settings, and click on "API tokens". Then click on "Add API token", and give it a name. The token will be displayed on the next screen.

## Unit Tests

WARNING: Run unit tests from an anaconda environment with compatable dependencies!

Install the nose2 test-runner framework. 

```bash
pip install nose2
```

Then from the root project folder run the nose2 module. You can do this as a python modules, or just direcly from the anaconda prompt (both examples given below). This will automatically find and run tests in any modules named "test_*".

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

None.
