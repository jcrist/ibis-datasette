import os
import versioneer

from setuptools import setup


install_requires = ["httpx", "ibis-framework", "sqlalchemy"]


setup(
    name="ibis-datasette",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description="An ibis backend for querying datasette",
    long_description=(
        open("README.rst", encoding="utf-8").read()
        if os.path.exists("README.rst")
        else ""
    ),
    maintainer="Jim Crist-Harif",
    maintainer_email="jcristharif@gmail.com",
    url="https://github.com/jcrist/ibis-datasette",
    project_urls={
        "Source": "https://github.com/jcrist/ibis-datasette/",
        "Issue Tracker": "https://github.com/jcrist/ibis-datasette/issues",
    },
    keywords="ibis datasette pandas sqlite",
    classifiers=[
        "License :: OSI Approved :: BSD License",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
    license="BSD",
    packages=["ibis_datasette"],
    python_requires=">=3.8",
    install_requires=install_requires,
    entry_points={"ibis.backends": ["datasette = ibis_datasette"]},
    zip_safe=False,
)
