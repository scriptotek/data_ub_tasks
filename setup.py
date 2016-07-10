from distutils.core import setup

setup(name='data_ub_tasks',
      version='2.2.1',
      py_modules=['data_ub_tasks'],
      install_requires=['rdflib', 'SPARQLWrapper', 'requests']
      )
