import logging

from packaging.version import Version

from teuthology import misc as teuthology
from teuthology.task import Task

log = logging.getLogger(__name__)

class Redis(Task):

    def __init__(self, ctx, config):
        super(Redis, self).__init__(ctx, config)
        self.log = log
        log.info('Redis Task: __INIT__ ')

        clients = ['client.{id}'.format(id=id_)
                   for id_ in teuthology.all_roles_of_type(self.ctx.cluster, 'client')]
        self.all_clients = []
        for client in clients:
            if client in self.config:
                self.all_clients.extend([client])
        if self.all_clients is None:
            self.all_clients = 'client.0'

    def setup(self):
        super(Redis, self).setup()
        log.info('Redis Task: SETUP')

    def begin(self):
        super(Redis, self).begin()
        log.info('Redis Task: BEGIN')

        for (host, roles) in self.ctx.cluster.remotes.items():
            log.debug('Redis Task: Cluster config is: {cfg}'.format(cfg=roles))
            log.debug('Redis Task: Host is: {host}'.format(host=host))

        self.redis_startup()

    def end(self):
        super(Redis, self).end()
        log.info('Redis Task: END')

        self.redis_shutdown()

    def valkey_install_percona(self, client):
        # Older Debian-based releases (e.g. Ubuntu jammy/22.04) don't ship
        # valkey-server natively, so pull it from Percona's repo.
        self.ctx.cluster.only(client).run(
            args=[
                'curl',
                '-O',
                'https://repo.percona.com/apt/percona-release_latest.generic_all.deb'
            ],
        )
        self.ctx.cluster.only(client).run(
            args=[
                'sudo',
                'apt',
                'install',
                'gnupg2',
                'lsb-release',
                './percona-release_latest.generic_all.deb',
                '-y'
            ],
        )
        self.ctx.cluster.only(client).run(
            args=[
                'sudo',
                'percona-release',
                'enable',
                'valkey',
                'testing'
            ],
        )
        self.ctx.cluster.only(client).run(
            args=[
                'sudo',
                'apt',
                'update'
            ],
        )
        self.ctx.cluster.only(client).run(
            args=[
                'sudo',
                'apt',
                'install',
                'valkey',
                '-y'
            ],
        )

    def valkey_install_native(self, client):
        # Ubuntu noble (24.04+) ships a working valkey-server package.
        # Percona's build breaks on noble: its systemd units conflict
        # with the merged /usr/lib layout.
        self.ctx.cluster.only(client).run(
            args=[
                'sudo',
                'apt',
                'update'
            ],
        )
        self.ctx.cluster.only(client).run(
            args=[
                'sudo',
                'apt',
                'install',
                'valkey-server',
                '-y'
            ],
        )

    def valkey_installed(self, remote):
        # missing valkey-server shows as a failed remote command
        # ('command not found'), not a local FileNotFoundError,
        # so check for the binary
        return remote.run(args=['which', 'valkey-server'], check_status=False).exitstatus == 0

    def redis_startup(self):
        try:
            for client in self.all_clients:
                (remote,) = self.ctx.cluster.only(client).remotes.keys()

                if remote.os.package_type == 'deb' and not self.valkey_installed(remote):
                    if remote.os.name == 'ubuntu' and Version(remote.os.version) < Version('24.04'):
                        self.valkey_install_percona(client)
                    else:
                        self.valkey_install_native(client)

                # the pkg's post-installation may have started the service.
                # stop it and let this task own the service
                for service in ('valkey-server', 'valkey-sentinel', 'valkey'):
                    self.ctx.cluster.only(client).run(
                        args=[
                            'sudo',
                            'systemctl',
                            'stop',
                            service
                            ],
                        check_status=False,
                        )

                self.ctx.cluster.only(client).run(
                    args=[
                        'sudo',
                        'valkey-server',
                        '--daemonize',
                        'yes'
                        ],
                    )

        except Exception as err:
            log.debug('Redis Task: Error starting up a Redis server')
            log.debug(err)
            raise

    def redis_shutdown(self):
        try:
            for client in self.all_clients:
                self.ctx.cluster.only(client).run(
                    args=[
                        'sudo',
                        'valkey-cli',
                        'shutdown',
                        ],
                    )

        except Exception as err:
            log.debug('Redis Task: Error shutting down a Redis server')
            log.debug(err)

task = Redis
