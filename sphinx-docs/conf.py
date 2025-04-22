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
release = '0.2.10'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx.ext.githubpages",
    # "sphinx.ext.apidoc",
    'sphinx.ext.napoleon',
    "myst_parser",
    "sphinx.ext.intersphinx",
]

templates_path = ['_templates']

exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']

# io.read() vs io.general_csv.read()
add_module_names = False

# -- Extension configuration -------------------------------------------------

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

intersphinx_mapping = {
    'python': ('https://docs.python.org/3', None),
    'numpy': ('https://numpy.org/doc/stable/', None),
    'pandas': ('https://pandas.pydata.org/docs/', None),
}


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'sphinx_rtd_theme'
html_static_path = ['_static']
