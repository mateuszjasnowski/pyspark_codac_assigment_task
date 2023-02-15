""" Setup file for codac_app """
from setuptools import setup

with open("requirements.txt", mode='r', encoding='utf-8') as requirements:
    requirements = requirements.read()

setup(
    name="CodacApp",
    author="Mateusz Jasnowski",
    author_email="mateusz.jasnowski@capgemini.com",
    version="0.2.0",
    description="PySpark application as upskilling task",
    packages=['codac_app','codac_app.app'],
    install_requires = requirements,
    entry_points = {
        'console_scripts': ['codac-app=codac_app.__main__:app_start'],
    }
)
