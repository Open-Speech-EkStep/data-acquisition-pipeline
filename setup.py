# Automatically created by: shub deploy

from setuptools import setup, find_packages

setup(
    name         = 'project',
    version      = '1.0',
    packages     = find_packages(),
    package_data = {'data_acquisition_framework': ['configs/*.json']},
    entry_points = {'scrapy': ['settings = data_acquisition_framework.settings']},
)
