from distutils.core import setup

setup(name='data_ub_tasks',
      version='2.5.5',
      packages=['data_ub_tasks'],
      install_requires=[
            'doit',
            'rdflib',
            'SPARQLWrapper',
            'requests',
            'elasticsearch',
            'skosify>=2.0.1',
      ])
