# encoding=utf8
import os
import requests
from skosify import Skosify
import textwrap
import SPARQLWrapper
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
            'git config --unset user.name',
            'git config --unset user.email',
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


def fuseki_task_gen(config, files=None):
    if files is None:
        files = ['dist/%(basename)s.ttl']
    files = [f % config for f in files]
    return {
        'doc': 'Push updated RDF to Fuseki',
        'file_dep': files,
        'actions': [
            (update_fuseki, [], {'config': config, 'files': files})
        ]
    }


def get_graph_count(config):
    logger.info('Querying {}/sparql'.format(config['fuseki']))
    sparql = SPARQLWrapper.SPARQLWrapper('{}/sparql'.format(config['fuseki']))
    sparql.setMethod(SPARQLWrapper.POST)  # to avoid caching
    sparql.setReturnFormat(SPARQLWrapper.JSON)
    sparql.setQuery(textwrap.dedent("""
        PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

        SELECT (COUNT(?s) as ?conceptCount)
        WHERE {
           GRAPH <%s> {
             ?s a skos:Concept .
           }
        }
    """ % (config['graph'])))
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


def enrich_and_concat(files, out_file):
    graph = Graph()
    for sourcefile in files:
        graph.load(sourcefile, format='turtle')

    skosify = Skosify()

    # Enrichments: broader <-> narrower, related <-> related
    logger.info("PostProcess: Enriching relations")
    skosify.enrich_relations(graph, False, True, True)

    graph.serialize(out_file, format='turtle')

    return len(graph)


def update_fuseki(config, files):
    """
    The current procedure looks quite silly, but I couldn't find a way to push a large number of triples to Fuseki in a streaming manner.
     - Using INSERT DATA with lots of triples caused Fuseki to 500.
     - Using INSERT DATA with chunks of about 20000 triples worked well when there were no blank nodes... but broke RDF lists with bnodes.

            Variables in QuadDatas are disallowed in INSERT DATA requests (see Notes 8 in the grammar). That is, the INSERT DATA statement
            only allows to insert ground triples. Blank nodes in QuadDatas are assumed to be disjoint from the blank nodes in the Graph Store,
            i.e., will be inserted with "fresh" blank nodes.

     - We could use tdbloader, but then we have to shut down the server and put the data on a volume accessible to the docker container..
       Quite a mess, so we stick with LOAD for now.
    """

    if config['dumps_dir'] is None:
        raise Exception("The 'dumps_dir' option must be set")

    if config['dumps_dir_url'] is None:
        raise Exception("The 'dumps_dir_url' option must be set")

    tmpfile = '{}/import_{}.ttl'.format(config['dumps_dir'].rstrip('/'), config['basename'])
    tmpfile_url = '{}/import_{}.ttl'.format(config['dumps_dir_url'].rstrip('/'), config['basename'])

    tc = enrich_and_concat(files, tmpfile)

    c0 = get_graph_count(config)

    store = SPARQLUpdateStore('{}/sparql'.format(config['fuseki']), '{}/update'.format(config['fuseki']))
    graph_uri = URIRef(config['graph'])
    graph = Graph(store, graph_uri)

    logger.info("Fuseki: Loading %d triples into %s from %s", tc, graph_uri, tmpfile_url)

    store.remove_graph(graph)
    store.add_graph(graph)
    store.update('LOAD <{}> INTO GRAPH <{}>'.format(tmpfile_url, graph_uri))

    c1 = get_graph_count(config)
    if c0 == c1:
        logger.info('Fuseki: Graph <%s> updated, number of concepts unchanged')
    else:
        logger.info('Fuseki: Graph <%s> updated, number of concepts changed from %d to %d.', config['graph'], c0, c1)
