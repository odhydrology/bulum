import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

exec(open('src/bulum/version.py').read())

setuptools.setup(
    name="bulum",
    version=__version__,
    python_requires=">=3.9",
    author="Chas Egan",
    author_email="chas@odhydrology.com",
    description="Open source python library for assessing hydrologic model results in Queensland",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://bitbucket.org/odhydrology/bulum.git",
    package_dir={'': 'src'},
    packages=setuptools.find_packages('src'),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    install_requires=[
        'altair[all]>=5.5.0',
        'folium>=0.14',
        'matplotlib>=3.8.3',
        'numpy>=1.26.4',
        'pandas>=2.2.0',
        'plotly>=5.18.0',
    ],    
)