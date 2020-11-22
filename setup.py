from setuptools import setup

setup(
   name='jsonbuilder',
   version='0.1',
   description='A tool for converting tabular data to json',
   author='Oskar Petersen',
   author_email='oskar.petersen@trioptima.com',
   packages=['jsonbuilder', 'tests', 'scripts'],
   install_requires=['xlrd', 'pandas', 'asteval', 'python-rapidjson'],
)