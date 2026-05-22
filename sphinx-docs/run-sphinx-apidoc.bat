rem to be run from the sphinx-docs folder

:: https://www.sphinx-doc.org/en/master/man/sphinx-apidoc.html
uv run sphinx-apidoc -o ./source -e -M --automodule-options members,show-inheritance,undoc-members ../src/bulum "../src/bulum/demo.py" "../src/bulum/version.py" "../src/bulum/*test*"

pause
