import errno
import gc
import logging
import os
import sys
import threading
import time

import concurrent.futures
import ovirtsdk4
from ovirtsdk4 import types
import psutil

from utils import Utils


class EngineSetup(object):
    def __init__(self, engine, username, domain, password, inscure, ssl,
                 rootpass, version=None, level='INFO', logname=None):
        '''
        @param engine: engine \ manager host name
        @param username: user name
        @param domain: domain name
        @param password: password
        [@param inscure: using client cer key
        [@param root password: using client cer key
        '''
        self.connection = None
        self.engine = engine
        self.username = username
        self.domain = domain
        self.password = password
        self.inscure = inscure
        self.rootpass = rootpass
        self.is_secured = ssl

        # cache objects
        self.vms_obj = None
        self.hosts_obj = None
        self.clusters_obj = None
        self.storage = None

        # properties
        self.MAX_THREADS = 50  # Maximum threads
        self.THREADS_TIMEOUT = 500  # time out in sec
        self.BULKS = 50  # bulks of threads
        self.DELAY = 120  # delay in sec
        self.DB_MAX_ROWS = 500  # rows number in query

        self.version = version
        self.log = None
        self.log_level = level

        # set the log file
        FORMAT = '%(asctime)s - %(levelname)s - %(message)s'

        _logfile = '/tmp/eng.log'
        logging.basicConfig(filename=_logfile, filemode='w', level=self.get_log_level(), format=FORMAT)
        self.log = logging.getLogger(logname)
        self.log.info('starting')

        # connect to api
        self.connection = self.connector()

    def get_log_level(self):

        if 'INFO' is self.log_level:
            return logging.INFO
        elif 'DEBUG' is self.log_level:
            return logging.DEBUG
        elif 'ERROR' is self.log_level:
            return logging.ERROR
        elif 'FATAL' is self.log_level:
            return logging.FATAL
        else:
            return logging.INFO

    def get_logdir(self, logname):
        logfile = None
        _basedir = '{0}/logs'.format(os.path.dirname(__file__).split('.')[0])

        def dir_validation():
            try:
                os.makedirs(_basedir)
            except OSError as exception:
                if exception.errno != errno.EEXIST:
                    raise
                else:
                    pass

        if logname is None:
            _root = os.path.abspath(sys.modules['__main__'].__file__)
            for t in _root.split('/'):
                if '.' in t:
                    logname = t.split('.')[0]
            dir_validation()
            return '{0}/{1}.log'.format(_basedir, logname)
        elif '/' not in logname:
            dir_validation()
            return '{0}/{1}.log'.format(_basedir, logname)
        else:
            _basedir = logname
            dir_validation()
            return '{0}/{1}.log'.format(_basedir, logname)

    def get_engine_url(self, host, is_ssl=True):
        """
        Retrive url for hosted api
        @param host:
        @param is_ssl:
        @return:
        """
        if is_ssl:
            ssl = 'https'
        else:
            ssl = 'http'
            host += ':8080'

        if self.version >= 3.6:
            return '{0}://{1}/ovirt-engine/api'.format(ssl, host)
        else:
            return '{0}://{1}/api'.format(ssl, host)

    def connector(self):
        """
        Connecting to api by ovirt sdk and retrive the api object
        @return: api object
        """
        self.log.info('connecting to engine ...')
        dbug = False
        if self.log_level == "DEBUG":
            dbug = True

        try:
            api = ovirtsdk4.Connection(url=self.get_engine_url(self.engine, self.is_secured),
                                       username=self.username + '@' + self.domain,
                                       password=self.password,
                                       insecure=self.inscure,
                                       compress=True,
                                       debug=dbug)
            self.log.info('connected to engine ' + self.engine)
            return api
        except Exception as ex:
            self.log.error('failed to connect ' + str(ex))
            exit()
            return None

    def update_entitys(self, data_center='', entity=None, filter=None, maxvalue=None, cache=False):
        """
        Update list of entity's like vms, hosts, clusters e.g, and cache it.
        @param entity: vms, hots, cluster e.g...
        @param filter: the query string to use for filters
        @param maxvalue: how much rows.
        @return: a list of entity's
        """
        # TODO:// switch case
        # TODO:// absract utils
        # TODO:// caching mechanisem by using map(query, results)
        if filter is None:
            filter = ''

        # sortby cannot combined with other filters.
        if 'sortby name asc' in filter:
            filter = 'sortby name asc'

        if 'sortby name desc' in filter:
            filter = 'sortby name desc'

        if maxvalue is None:
            # maxvalue = self.DB_MAX_ROWS
            maxvalue = None

        self.log.info('Searching for \'{0}\' objects in \'{1}\' '
            'datacenter by the following filter \'{2}\' - maxrows={3}'
                .format(entity, data_center, filter, maxvalue))

        # TODO:// replace with switch case
        if entity == 'vms':
            if cache is False or self.vms_obj is None:
                self.log.debug('update_entitys hit th db')
                self.vms_obj = self.connection.system_service().\
                    vms_service().list(search=filter, max=maxvalue)
            return self.vms_obj
        elif entity == 'clusters':
            if cache is False or self.clusters_obj is None:
                self.log.debug('update_entitys hit th db')
                self.clusters_obj = self.connection.system_service().\
                    clusters_service().list(search=filter, max=maxvalue)
            return self.clusters_obj
        elif entity == 'hosts':
            if cache is False or self.hosts_obj is None:
                self.log.debug('update_entitys hit th db')
                self.hosts_obj = self.connection.system_service().\
                    hosts_service().list(search=filter, max=maxvalue)
            return self.hosts_obj
        elif entity == 'storagedomains':
            if cache is False or self.storage is None:
                self.log.debug('update_entitys hit th db')
                return self.connection.system_service().\
                    storage_domains_service().list()
        else:
            return None

    def get_hosts_as_list(self, querystring=None):
        return self.connection.hosts.list(max=500)

    def disconnect(self):
        """
        close connection and disconnect from engine
        @return:
        """
        logging.info('Shutting down and disconnect from engine')
        try:
            self.connection.disconnect()
        except Exception as ex:
            pass
            logging.info('disconnect from engine failed\n' + ex.message)
        finally:
            logging.shutdown()

    def gc_collect_int(self):
        self.log.debug("Garbage collector: collected %d objects." % (gc_collect()))

    def debug_info(self):
        self.log.debug('debug info - max threads %s' % self.MAX_THREADS)
        self.log.debug('debug info - bulks %s' % self.BULKS)
        self.log.debug('debug info - delay %s' % self.DELAY)

    def sdk4_object(self, obj=None):
        if 'types.Vm' in str(type(obj)):
            self.log.debug('object type vm')
            return self.connection.system_service().\
                vms_service().vm_service(obj.id)
        elif 'types.Host' in str(type(obj)):
            self.log.debug('object type host')
            return self.connection.system_service().\
                hosts_service().host_service(obj.id)
        elif 'types.StorageDomain' in str(type(obj)):
            self.log.debug('object type host')
            return self.connection.system_service().\
                storage_domains_service().storage_domain_service(obj.id)
        elif 'types.Clusters' in str(type(obj)):
            self.log.debug('object type host')
            return self.connection.system_service().\
                clusters_service().cluster_service(obj.id)
        else:
            self.log.error("cannot convert object {0}".format(obj))

    def action_executor(self, data_center=None, action=None, entity_type=None,
                        querystring=None, capacity=None, bulks=None, delay=None,
                        ignore=[], iterations=0, rampup=0, strict=True):
        """
        abstract rhevm operations in parallel
        @param action: start, stop
        @param entity_type: vms, hosts, clusters, storage_domains
        @param querystring: filter objects
        @param capacity: how many objects were taken action
        @param bulks: bulks size
        @param delay: delay in seconds
        @return: OK
        """
        # set cpu affinity for better performance while using threads.
        # set_cpu_affinity()
        utils = Utils(action, entity_type, data_center, log=self.log)
        thread_method = utils.action_factory(action)

        if entity_type is None:
            self.log.error("some error message")
            return False

        logging.info('About to run {0} {1} by action_executor'.
                     format(action, entity_type))
        if delay:
            self.DELAY = delay
        if bulks:
            self.BULKS = bulks
            self.MAX_THREADS = bulks

        self.debug_info()

        # using custom capacity if set
        entity_list = self.update_entitys(data_center=data_center,
                                          entity=entity_type,
                                          filter=utils.filter_generator(querystring),
                                          maxvalue=capacity)
        self.log.info('{0} {1} were found'.format(len(entity_list), entity_type))

        executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.MAX_THREADS)
        try:
            counter = 0
            while len(entity_list) > 0:
                results_pool = []
                for _threads, entity in enumerate(entity_list):
                    # ignore list check
                    if entity.name in str(ignore):
                        ignore.pop(0)
                        continue

                    # start the actual operation
                    results_pool.append(executor.submit(
                        thread_method, self.sdk4_object(entity)))

                    counter += 1

                    # ramp up threads by sec.
                    time.sleep(rampup / self.BULKS)

                    if _threads % self.MAX_THREADS == 0 and _threads > 0:
                        self.log.info("working in bulks of {0}, every {1} sec".
                                      format(self.MAX_THREADS, self.DELAY))
                        time.sleep(self.DELAY)

                        # make sure all threads were finished
                        if strict:
                            self.watchdog(results_pool, 500)

                    # stop actions when iterations is set
                    self.log.debug("counter %s" % counter)
                    if counter > 0 and (counter + 1) == iterations:
                        self.log.info('iterate for {0} items'.format(counter))
                        return True

                # list sync
                entity_list = self.update_entitys(data_center=data_center, entity=entity_type,
                                                  filter=utils.filter_generator(querystring), maxvalue=capacity)
                self.log.info('{0} {1} were found'.format(len(entity_list), entity_type))

                # gc collect each while cycle.
                self.gc_collect_int()
        finally:
            self.log.info('{0} {1} completed'.format(action, entity_type))
        return True

    def watchdog(self, t_pool=None, timeout=120, delay=0.5):
        _start = time.time()
        for i in range(timeout):
            if 'running' in str(t_pool):
                time.sleep(delay)
            else:
                self.log.info("bulk size {0} execution time {1}".format(
                    self.BULKS, (time.time() - _start)))
                break
        self.log.warn("warn msg")


def set_cpu_affinity():
    p = psutil.Process(os.getpid())
    p.cpu_affinity([0])


def gc_collect():
    return gc.collect()
