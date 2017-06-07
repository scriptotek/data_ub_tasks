# encoding=utf8
import os
import requests
from skosify import Skosify
import hashlib
from .ttl2solr import ttl2solr
import textwrap
import SPARQLWrapper
from rdflib.graph import Graph, URIRef
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
import logging
import json
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

logging.getLogger('requests').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)


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

def sha1(filename):
    BUF_SIZE = 65536  # lets read stuff in 64kb chunks
    sha1 = hashlib.sha1()

    try:
        with open(filename, 'rb') as f:
            while True:
                data = f.read(BUF_SIZE)
                if not data:
                    break
                sha1.update(data)
    except IOError:
        return ''

    return sha1.hexdigest()


def fetch_remote(task, remote, etag_cache):

    def etag_changed(head):
        remote_etag = head.headers['etag']
        if os.path.isfile(etag_cache):
            with open(etag_cache, 'rb') as f:
                local_etag = f.read().decode('utf-8').strip()
        else:
            local_etag = '0'

        logger.debug('   Remote file etag: %s', remote_etag)
        logger.debug('    Local file etag: %s', local_etag)

        if remote_etag == local_etag:
            logger.debug(' -> Local data are up-to-date.')
            task.uptodate = True
            return False

        with open(etag_cache, 'wb') as f:
            f.write(remote_etag.encode('utf-8'))

        return True

    def fetch_and_check_sha1():
        old_sha1 = sha1(task.targets[0])
        download(remote, task.targets[0])
        new_sha1 = sha1(task.targets[0])

        if old_sha1 != new_sha1:
            logger.info('%s changed from  %s to %s', task.targets[0], old_sha1[:7], new_sha1[:7])
            task.uptodate = False

    logger.debug('Checking %s', remote)
    head = requests.head(remote)

    if head.status_code != 200:
        logger.warn('Got status code: %s', head.status_code)
        raise Exception('Got status code: %s', head.status_code)

    if 'etag' not in head.headers:
        logger.debug('   No Etag header found, this task will always run.')
        fetch_and_check_sha1()

    elif etag_changed(head):
        fetch_and_check_sha1()


def git_pull_task_gen(config):
    return {
        'doc': 'Pull updates from git',
        'name': 'git-pull',
        'actions': [
            'git config user.name "%s"' % config['git_user'],
            'git config user.email "%s"' % config['git_email'],
            'git pull',
            'git config --unset user.name',
            'git config --unset user.email',
        ]
    }


def git_push_task_gen(config):
    return {
        'doc': 'Commit and push updated files to GitHub',
        'basename': 'git-push',
        'file_dep': [
            'dist/%s.ttl' % config['basename']
        ],
        'actions': [
            'git config user.name "%s"' % config['git_user'],
            'git config user.email "%s"' % config['git_email'],
            'git add -u',
            'git diff-index --quiet --cached HEAD || git commit -m "Data update"',
            'git pull origin',   # --mirror : locally updated refs will be force updated on the remote end !
            'git push origin || echo "git pull failed from $(pwd)" | mail -s "data.ub.uio.no error" d.m.heggo@ub.uio.no',   # --mirror : locally updated refs will be force updated on the remote end !
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
    # logger.info('Querying {}/sparql'.format(config['fuseki']))
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
    logger.debug("Skosify: Enriching relations")
    skosify.enrich_relations(graph, False, True, True)

    with open(out_file + '.tmp', 'w') as handle:
        graph.serialize(handle, format='turtle')

    os.rename(out_file + '.tmp', out_file)

    return len(graph)


def invalidate_varnish_cache(pattern):
    response = requests.request('BAN', 'http://localhost:3030', headers={'X-Ban-Url': pattern})
    response.raise_for_status()


def update_fuseki(config, files):
    """
    The current procedure first dumps the enriched graph to a temporary file in a dir accessible by
    the web server, then loads the file using the SPARQL LOAD operation.

    I first tried pushing the enriched graph directly to the update endpoint
    without writing a temporary file, but that approach failed for two reasons:
     - Using INSERT DATA with "lots" of triples (>> 20k) caused Fuseki to give a 500 response.
     - Using INSERT DATA with chunks of 20k triples worked well... when there were no blank nodes.
       If the same bnode were referenced in two different chunks, it would end up as *two* bnodes.
       Since we're using bnodes in RDF lists, many lists ended up broken. From the SPARQL ref.:

            Variables in QuadDatas are disallowed in INSERT DATA requests (see Notes 8 in the grammar).
            That is, the INSERT DATA statement only allows to insert ground triples. Blank nodes in
            QuadDatas are assumed to be disjoint from the blank nodes in the Graph Store,
            i.e., will be inserted with "fresh" blank nodes.

    Using tdbloader would be another option, but then we would still need a temp file, we would also need
    to put that file on a volume accessible to the docker container, and we would need to shutdown the
    server while loading the file. And it's a solution tied to Fuseki.

    I'm not aware if there is a limit on how large graphs Fuseki can load with the LOAD operation.
    I guess we'll find out.
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

    logger.info("Fuseki: Loading %d triples into <%s> from %s", tc, graph_uri, tmpfile_url)

    # CLEAR GRAPH first to make sure all blank nodes are erased
    # https://github.com/scriptotek/emnesok/issues/70
    store.update('CLEAR GRAPH <{}>'.format(graph_uri))

    store.update('LOAD <{}> INTO GRAPH <{}>'.format(tmpfile_url, graph_uri))

    c1 = get_graph_count(config)
    if c0 == c1:
        logger.info('Fuseki: Graph <%s> updated, number of concepts unchanged', config['graph'])
    else:
        logger.info('Fuseki: Graph <%s> updated, number of concepts changed from %d to %d.', config['graph'], c0, c1)

    invalidate_varnish_cache(config['basename'])
    logger.info('Invalidated Varnish cache for %s', config['basename'])

def gen_solr_json(config, vocab_name=None, infile=None, outfile=None):

    if infile is None:
        infile = 'dist/%(basename)s.ttl'

    if outfile is None:
        outfile = 'dist/%(basename)s.json'

    infile = infile % config
    outfile = outfile % config

    return {
        'basename': 'build-solr-json',
        'doc': 'Generate SOLR JSON from Turtle',
        'file_dep': [
            infile
        ],
        'targets': [
            outfile
        ],
        'actions': [
            (ttl2solr, [], {'infile': infile, 'outfile': outfile, 'vocab_name': vocab_name})
        ]
    }


def gen_elasticsearch(config, vocab_name=None, infile=None):

    if infile is None:
        infile = 'dist/%(basename)s.json'

    infile = infile % config

    return {
        'basename': 'elasticsearch',
        'doc': 'Push data to Elasticsearch',
        'file_dep': [
            infile
        ],
        'targets': [
        ],
        'verbosity': 2,
        'uptodate': [False],  # never up-to-date?
        'actions': [
            (push_to_elasticsearch, [], {'infile': infile, 'index': config['es_index'], 'vocab_name': vocab_name})
        ]
    }


def push_to_elasticsearch(infile, index, vocab_name):
    data = json.load(open(infile))
    conn = Elasticsearch(['localhost:9200'])

    for x in data:
        x['_id'] = x['id']
        x['_index'] = index
        x['_type'] = 'record'

    res = bulk(conn, data, stats_only=True)
    print('%d inserted, %d failed' % res)

