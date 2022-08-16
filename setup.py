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
    license="BSD",
    packages=["ibis_datasette"],
    python_requires=">=3.8",
    install_requires=install_requires,
    entry_points={"ibis.backends": ["datasette = ibis_datasette"]},
    zip_safe=False,
)
