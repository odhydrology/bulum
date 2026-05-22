rem to be run from the sphinx-docs folder

cd ..

rm ../docs/**/*.html

uv run sphinx-build sphinx-docs docs

pause.
