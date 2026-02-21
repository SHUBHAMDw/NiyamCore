from setuptools import setup, find_packages

setup(
    name='niyamcore',
    version='0.1.0',
    description='Configurable data validation framework on PySpark',
    author='Shubham Dwivedi',
    packages=find_packages(include=['niyamcore*']),
    install_requires=[
        'pyyaml'
    ],
)