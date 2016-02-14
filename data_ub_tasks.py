# encoding=utf8
import os
import requests
from skosify import Skosify
import textwrap
from SPARQLWrapper import SPARQLWrapper, JSON
from rdflib.graph import Graph, URIRef
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
import logging

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

def download(remote, local):
    with open(local, 'wb') as out_file:
        response = requests.get(remote, stream=True)
        if not response.ok:
            raise Exception('Download failed')
        for block in response.iter_content(1024):
            if not block:
                break
            out_file.write(block)
    logger.info('Fetched new version of %s', remote)

def fetch_remote(task, remote, etag_cache):
    logger.info('Checking %s', remote)
    head = requests.head(remote)
    if head.status_code != 200:
        logger.warn('Got status code: %s', head.status_code)
        raise Exception('Got status code: %s', head.status_code)

    if 'etag' in head.headers:
        remote_etag = head.headers['etag']
        if os.path.isfile(etag_cache):
            with open(etag_cache, 'rb') as f:
                local_etag = f.read().decode('utf-8').strip()
        else:
            local_etag = '0'

        logger.info('   Remote file etag: %s', remote_etag)
        logger.info('    Local file etag: %s', local_etag)

        if remote_etag == local_etag:
            logger.info(' -> Local data are up-to-date.')
            task.uptodate = True
            return

        download(remote, task.targets[0])

        with open(etag_cache, 'wb') as f:
            f.write(remote_etag.encode('utf-8'))

        task.uptodate = False

def git_push_task_gen(config):
    return {
        'doc': 'Commit and push updated files to GitHub',
        'basename': 'git-push',
        'file_dep': [
            '%s.json' % config['basename'],
            'dist/%s.marc21.xml' % config['basename'],
            'dist/%s.ttl' % config['basename']
        ],
        'actions': [
            'git config user.name "ubo-bot"',
            'git config user.email "danmichaelo+ubobot@gmail.com"',
            'git add -u',
            'git diff-index --quiet --cached HEAD || git commit -m "Data update"',
            'git push --mirror origin'  # locally updated refs will be force updated on the remote end !
        ]
    }


def publish_dumps_task_gen(dumps_dir, files):

    file_deps = ['dist/{}'.format(filename) for filename in files]
    actions = ['mkdir -p {0}'.format(dumps_dir)]
    targets = []
    for filename in files:
        actions.extend([
            'bzip2 -f -k dist/{0}'.format(filename),
            'zip dist/{0}.zip dist/{0}'.format(filename),
            'cp dist/{0} dist/{0}.bz2 dist/{0}.zip {1}/'.format(filename, dumps_dir)
        ])
        targets.extend([
            '{0}/{1}'.format(dumps_dir, filename),
            '{0}/{1}.zip'.format(dumps_dir, filename),
            '{0}/{1}.bz2'.format(dumps_dir, filename)
        ])

    return {
        'doc': 'Publish uncompressed and compressed dumps',
        'basename': 'publish-dumps',
        'file_dep': file_deps,
        'actions': actions,
        'targets': targets
    }


def fuseki_task_gen(config):
    return {
        'doc': 'Push updated RDF to Fuseki',
        'file_dep': [
            'dist/%s.ttl' % config['basename']
        ],
        'actions': [
            (update_fuseki, [], {'config': config})
        ]
    }


def get_graph_count(config):
    logger.info('Querying {}/sparql'.format(config['fuseki']))
    sparql = SPARQLWrapper('{}/sparql'.format(config['fuseki']))
    sparql.setQuery(textwrap.dedent("""
        PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

        SELECT (COUNT(?s) as ?conceptCount)
        WHERE {
           GRAPH <%s> {
             ?s a skos:Concept .
           }
        }
    """ % (config['graph'])))
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()
    count = results['results']['bindings'][0]['conceptCount']['value']
    return int(count)


def quads(iterable, context, chunk_size=20000):
    chunk = []
    while True:
        try:
            chunk.append(next(iterable) + (context,))
            # chunk.append(next(iterable))
        except StopIteration:
            if len(chunk) == 0:
                raise
            yield chunk
            chunk = []
        if len(chunk) == chunk_size:
            yield chunk
            chunk = []


def load_vocabulary(sourcefile):
    graph = Graph()
    graph.load(sourcefile, format='turtle')

    skosify = Skosify()

    # Enrichments: broader <-> narrower, related <-> related
    logger.info("PostProcess: Enriching relations")
    skosify.enrich_relations(graph, False, True, True)

    return graph


def update_fuseki(config):

    source = load_vocabulary('dist/%s.ttl' % config['basename'])

    c0 = get_graph_count(config)
    logger.info("Fuseki: Pushing dist/%s.ttl", config['basename'])

    store = SPARQLUpdateStore('{}/sparql'.format(config['fuseki']), '{}/update'.format(config['fuseki']))
    context = Graph(store, URIRef(config['graph']))

    for q in quads(source.triples((None, None, None)), context):
        logger.info('Chunk %d', len(q))
        store.addN(q)

    c1 = get_graph_count(config)
    logger.info('Fuseki: Graph <%s> updated, from %d to %d concepts.', config['graph'], c0, c1)
