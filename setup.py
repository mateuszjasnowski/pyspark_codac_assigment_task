""" Setup file for codac_app """
from setuptools import setup, find_packages

setup(
    name="CodacApp",
    author="Mateusz Jasnowski",
    author_email="mateusz.jasnowski@capgemini.com",
    version="0.1.6",
    description="PySpark application as upskilling task",
    packages=find_packages(exclude=['codac_app.tests'])
)
