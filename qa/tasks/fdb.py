import logging

from io import BytesIO
from teuthology import misc as teuthology
from teuthology.task import Task

log = logging.getLogger(__name__)

class FDB(Task):

    def __init__(self, ctx, config):
        super(FDB, self).__init__(ctx, config)
        self.log = log
        log.info('FDB Task: __INIT__ ')

        clients = ['client.{id}'.format(id=id_)
                   for id_ in teuthology.all_roles_of_type(self.ctx.cluster, 'client')]
        self.all_clients = []
        for client in clients:
            if client in self.config:
                self.all_clients.extend([client])

    def setup(self):
        super(FDB, self).setup()
        log.info('FDB Task: SETUP')

    def begin(self):
        super(FDB, self).begin()
        log.info('FDB Task: BEGIN')

        for (host, roles) in self.ctx.cluster.remotes.items():
            log.debug('FDB Task: Cluster config is: {cfg}'.format(cfg=roles))
            log.debug('FDB Task: Host is: {host}'.format(host=host))

            for role in roles:
                if 'client' in role:
                    self.all_clients.extend([role])
        if self.all_clients is None:
            self.all_clients = 'client.0'

        log.debug('D4N Tests: Client list:')
        log.debug(self.all_clients)

        for client in self.all_clients:
            self.fdb_install(client)
        self.fdb_startup()

    def end(self):
        super(FDB, self).end()
        log.info('FDB Task: END')

        self.fdb_shutdown()

    def fdb_install(self, client):
        (remote,) = self.ctx.cluster.only(client).remotes.keys()
        if remote.os.package_type == 'deb':
            self.ctx.cluster.only(client).run(
                args=[
                    'wget',
                    '-O',
                    'foundationdb-clients_7.3.63-1_amd64.deb',
                    'https://github.com/apple/foundationdb/releases/download/7.3.63/foundationdb-clients_7.3.63-1_amd64.deb'
                    ],
                )
            self.ctx.cluster.only(client).run(
                args=[
                    'wget',
                    '-O',
                    'foundationdb-server_7.3.63-1_amd64.deb',
                    'https://github.com/apple/foundationdb/releases/download/7.3.63/foundationdb-server_7.3.63-1_amd64.deb'
                    ],
                )
            self.ctx.cluster.only(client).run(
                args=[
                    'sudo',
                    'apt', 
                    'install',
                    './foundationdb-clients_7.3.63-1_amd64.deb',
                    './foundationdb-server_7.3.63-1_amd64.deb'
                    ],
                )
        else:
            self.ctx.cluster.only(client).run(
                args=[
                    'wget',
                    '-O',
                    'foundationdb-clients-7.3.63-1.el7.x86_64.rpm',
                    'https://github.com/apple/foundationdb/releases/download/7.3.63/foundationdb-clients-7.3.63-1.el7.x86_64.rpm'
                    ],
                )
            self.ctx.cluster.only(client).run(
                args=[
                    'wget',
                    '-O',
                    'foundationdb-server-7.3.63-1.el7.x86_64.rpm',
                    'https://github.com/apple/foundationdb/releases/download/7.3.63/foundationdb-server-7.3.63-1.el7.x86_64.rpm'
                    ],
                )
            self.ctx.cluster.only(client).run(
                args=[
                    'sudo',
                    'rpm',
                    '-Uvh',
                    '--replacepkgs',
                    './foundationdb-clients-7.3.63-1.el7.x86_64.rpm',
                    './foundationdb-server-7.3.63-1.el7.x86_64.rpm'
                    ],
                )

    def fdb_startup(self):
        try:
            for client in self.all_clients:
                self.ctx.cluster.only(client).run(
                    args=[
                        'sudo',
                        'service',
                        'foundationdb',
                        'start'
                        ],
                    )

        except Exception as err:
            log.debug('FDB Task: Error starting up a FDB server')
            log.debug(err)
            raise

    def fdb_shutdown(self):
        try:
            for client in self.all_clients:
                self.ctx.cluster.only(client).run(
                    args=[
                        'sudo',
                        'service',
                        'foundationdb',
                        'stop'
                        ],
                    )

        except Exception as err:
            log.debug('FDB Task: Error shutting down a FDB server')
            log.debug(err)

task = FDB
