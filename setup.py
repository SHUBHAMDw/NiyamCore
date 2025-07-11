from setuptools import setup, find_packages

setup(
    name='niyamcore',
    version='0.1.0',
    description='Data Quality Validation Framework on PySpark',
    author='Shubham Dwivedi',
    packages=find_packages(),
    install_requires=[
        'pyspark>=3.1.2',
        'pyyaml'
    ],
)