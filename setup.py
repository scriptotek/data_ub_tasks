from distutils.core import setup

setup(name='data_ub_tasks',
      version='2.5.4',
      packages=['data_ub_tasks'],
      install_requires=['rdflib', 'SPARQLWrapper', 'requests']
      )
