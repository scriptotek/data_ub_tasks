from distutils.core import setup

setup(name='data_ub_tasks',
      version='2.5.5',
      packages=['data_ub_tasks'],
      install_requires=[
      	'rdflib',
      	'SPARQLWrapper',
      	'requests',
      	'elasticsearch',
      	'skosify',
      	],
      	dependency_links = ['https://github.com/NatLibFi/Skosify/tarball/master#egg=skosify-2.0.0beta']
      )
