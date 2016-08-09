from distutils.core import setup

setup(name='data_ub_tasks',
      version='2.3.1',
      packages=['data_ub_tasks'],
      install_requires=['rdflib', 'SPARQLWrapper', 'requests']
      )
