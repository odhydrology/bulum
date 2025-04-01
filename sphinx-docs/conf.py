# pylint: skip-file

# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'Bulum'
copyright = '2025, OD Hydrology'
author = 'OD Hydrology'
release = '0.2.9'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx.ext.githubpages",
    # "sphinx.ext.apidoc",
    'sphinx.ext.napoleon',
    "myst_parser"
]

templates_path = ['_templates']

exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']

apidoc_modules = [
    {
        'path': '../src/bulum',
        'destination': 'source/',
        'exclude_patterns': ['**/test*',
                             '*.gitignore.*'],
        # 'max_depth': 4,
        'follow_links': False,
        'separate_modules': True,
        'include_private': False,
        'no_headings': False,
        'module_first': False,
        'implicit_namespaces': False,
        'automodule_options': {
            'members', 'show-inheritance', 'undoc-members'
        },
    },
]


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'sphinx_rtd_theme'
html_static_path = ['static']
