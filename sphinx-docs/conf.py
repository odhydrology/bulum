# pylint: skip-file

# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

import os
import datetime
from pathlib import Path
year = str(datetime.datetime.now().year)

project = 'Bulum'
copyright = f'{year}, OD Hydrology'
author = 'OD Hydrology'
try:
    _version_file = Path(__file__).parent / '..' / 'src' / 'bulum' / 'version.py'
    exec(open(_version_file).read())
    release = __version__  # noqa: F821
except FileNotFoundError as e:
    print(f"cwd={os.getcwd()}")
    raise

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx.ext.githubpages",
    # "sphinx.ext.apidoc",
    'sphinx.ext.napoleon',  # converts docstrings to sphinx compatible reST format
    "myst_parser",
    "sphinx.ext.intersphinx",  # allows linking to e.g. pandas, numpy
]

templates_path = ['_templates']

exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']

# io.read() vs io.general_csv.read()
add_module_names = False

# -- Extension configuration -------------------------------------------------

intersphinx_mapping = {
    'python': ('https://docs.python.org/3', None),
    'numpy': ('https://numpy.org/doc/stable/', None),
    'pandas': ('https://pandas.pydata.org/docs/', None),
}


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'sphinx_rtd_theme'
