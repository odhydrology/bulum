# Guide to updating docs.

- Install sphinx and extensions: `pip install sphinx sphinx_rtd_theme myst-parser`
- If there are new parts to the api, run `sphinx-apidoc`. You will need to edit the `.rst` files to revert clobbered changes and apply new formatting. Or just get Ciaran to check if the rst files need to be updated.
- Run the bat file `make-docs.bat`.
- Push to GitHub --- the documentation page should automatically update after a few seconds.
