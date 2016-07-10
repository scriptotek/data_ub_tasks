from distutils.core import setup

setup(name='data_ub_tasks',
      version='2.2.2',
      py_modules=['data_ub_tasks'],
      install_requires=['rdflib', 'SPARQLWrapper', 'requests']
      )
