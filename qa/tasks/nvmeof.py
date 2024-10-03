import logging
import random
import time
from collections import defaultdict
from datetime import datetime
from textwrap import dedent
from gevent.event import Event
from gevent.greenlet import Greenlet
from teuthology.task import Task
from teuthology import misc
from teuthology.exceptions import ConfigError
from teuthology.orchestra import run
from tasks.util import get_remote_for_role
from tasks.cephadm import _shell
from tasks.thrasher import Thrasher

log = logging.getLogger(__name__)

conf_file = '/etc/ceph/nvmeof.env'


class Nvmeof(Task):
    """
    Setup nvmeof gateway on client and then share gateway config to target host.

        - nvmeof:
            client: client.0
            version: default
            rbd:
                pool_name: mypool
                image_name: myimage
                rbd_size: 1024
            gateway_config:
                source: host.a 
                target: client.2
                vars:
                    cli_version: latest
                    
    """

    def setup(self):
        super(Nvmeof, self).setup()
        try:
            self.client = self.config['client']
        except KeyError:
            raise ConfigError('nvmeof requires a client to connect with')

        self.cluster_name, type_, self.client_id = misc.split_role(self.client)
        if type_ != 'client':
            msg = 'client role ({0}) must be a client'.format(self.client)
            raise ConfigError(msg)
        self.remote = get_remote_for_role(self.ctx, self.client)

    def begin(self):
        super(Nvmeof, self).begin()
        self._set_defaults()
        self.deploy_nvmeof()
        self.set_gateway_cfg()

    def _set_defaults(self):
        self.gateway_image = self.config.get('gw_image', 'default')

        rbd_config = self.config.get('rbd', {})
        self.poolname = rbd_config.get('pool_name', 'mypool')
        self.rbd_image_name = rbd_config.get('image_name', 'myimage')
        self.rbd_size = rbd_config.get('rbd_size', 1024*8)

        gateway_config = self.config.get('gateway_config', {})
        self.cli_image = gateway_config.get('cli_image', 'quay.io/ceph/nvmeof-cli:latest')
        self.nqn_prefix = gateway_config.get('subsystem_nqn_prefix', 'nqn.2016-06.io.spdk:cnode')
        self.subsystems_count = gateway_config.get('subsystems_count', 1) 
        self.namespaces_count = gateway_config.get('namespaces_count', 1) # namepsaces per subsystem
        self.bdev = gateway_config.get('bdev', 'mybdev')
        self.serial = gateway_config.get('serial', 'SPDK00000000000001')
        self.port = gateway_config.get('port', '4420')
        self.srport = gateway_config.get('srport', '5500')

    def deploy_nvmeof(self):
        """
        Deploy nvmeof gateway.
        """
        log.info('[nvmeof]: deploying nvmeof gateway...')
        if not hasattr(self.ctx, 'ceph'):
            self.ctx.ceph = {}
        fsid = self.ctx.ceph[self.cluster_name].fsid

        nodes = []
        daemons = {}

        for remote, roles in self.ctx.cluster.remotes.items():
            for role in [r for r in roles
                         if misc.is_type('nvmeof', self.cluster_name)(r)]:
                c_, _, id_ = misc.split_role(role)
                log.info('Adding %s on %s' % (role, remote.shortname))
                nodes.append(remote.shortname + '=' + id_)
                daemons[role] = (remote, id_)

        if nodes:
            gw_image = self.gateway_image
            if (gw_image != "default"):
                log.info(f'[nvmeof]: ceph config set mgr mgr/cephadm/container_image_nvmeof {gw_image}')
                _shell(self.ctx, self.cluster_name, self.remote, [
                    'ceph', 'config', 'set', 'mgr', 
                    'mgr/cephadm/container_image_nvmeof',
                    gw_image
                ])

            poolname = self.poolname
            imagename = self.rbd_image_name

            log.info(f'[nvmeof]: ceph osd pool create {poolname}')
            _shell(self.ctx, self.cluster_name, self.remote, [
                'ceph', 'osd', 'pool', 'create', poolname
            ])

            log.info(f'[nvmeof]: rbd pool init {poolname}')
            _shell(self.ctx, self.cluster_name, self.remote, [
                'rbd', 'pool', 'init', poolname
            ])

            log.info(f'[nvmeof]: ceph orch apply nvmeof {poolname}')
            _shell(self.ctx, self.cluster_name, self.remote, [
                'ceph', 'orch', 'apply', 'nvmeof', poolname, 
                '--placement', str(len(nodes)) + ';' + ';'.join(nodes)
            ])

            total_images = int(self.namespaces_count) * int(self.subsystems_count)
            log.info(f'[nvmeof]: creating {total_images} images')
            for i in range(1, total_images + 1):
                imagename = self.image_name_prefix + str(i)
                log.info(f'[nvmeof]: rbd create {poolname}/{imagename} --size {self.rbd_size}')
                _shell(self.ctx, self.cluster_name, self.remote, [
                    'rbd', 'create', f'{poolname}/{imagename}', '--size', f'{self.rbd_size}'
                ])

        for role, i in daemons.items():
            remote, id_ = i
            self.ctx.daemons.register_daemon(
                remote, 'nvmeof', id_,
                cluster=self.cluster_name,
                fsid=fsid,
                logger=log.getChild(role),
                wait=False,
                started=True,
            )
        log.info("[nvmeof]: executed deploy_nvmeof successfully!")
        
    def set_gateway_cfg(self):
        log.info('[nvmeof]: running set_gateway_cfg...')
        gateway_config = self.config.get('gateway_config', {})
        source_host = gateway_config.get('source')
        target_host = gateway_config.get('target')
        if not (source_host and target_host):
            raise ConfigError('gateway_config requires "source" and "target"')
        remote = list(self.ctx.cluster.only(source_host).remotes.keys())[0]
        ip_address = remote.ip_address
        gateway_name = ""
        nvmeof_daemons = self.ctx.daemons.iter_daemons_of_role('nvmeof', cluster=self.cluster_name)
        for daemon in nvmeof_daemons:
            gateway_names += [daemon.remote.shortname]
            gateway_ips += [daemon.remote.ip_address]
        conf_data = dedent(f"""
            NVMEOF_GATEWAY_IP_ADDRESSES={",".join(gateway_ips)}
            NVMEOF_GATEWAY_NAMES={",".join(gateway_names)}
            NVMEOF_DEFAULT_GATEWAY_IP_ADDRESS={ip_address}
            NVMEOF_CLI_IMAGE="{self.cli_image}"
            NVMEOF_SUBSYSTEMS_PREFIX={self.nqn_prefix}
            NVMEOF_SUBSYSTEMS_COUNT={self.subsystems_count}
            NVMEOF_NAMESPACES_COUNT={self.namespaces_count}
            NVMEOF_PORT={self.port}
            NVMEOF_SRPORT={self.srport}
            """)
        target_remote = list(self.ctx.cluster.only(target_host).remotes.keys())[0]
        target_remote.write_file(
            path=conf_file,
            data=conf_data,
            sudo=True
        )
        log.info("[nvmeof]: executed set_gateway_cfg successfully!")


class NvmeofThrasher(Thrasher, Greenlet):
    """
    How it works::

    - pick a nvmeof daemon
    - kill it
    - wait for other thrashers to finish thrashing (if switch_thrashers True) 
    - sleep for 'revive_delay' seconds
    - do some checks after thrashing ('do_checks' method) 
    - revive daemons
    - wait for other thrashers to finish reviving (if switch_thrashers True)
    - sleep for 'thrash_delay' seconds
    - do some checks after reviving ('do_checks' method) 

    
    Options::

    seed                Seed to use on the RNG to reproduce a previous
                        behavior (default: None; i.e., not set) 
    checker_host:       Initiator client on which verification tests would 
                        run during thrashing (mandatory option)
    switch_thrashers:   Toggle this to switch between thrashers so it waits until all
                        thrashers are done thrashing before proceeding. And then
                        wait until all thrashers are done reviving before proceeding.
                        (default: false)          
    randomize:          Enables randomization and use the max/min values. (default: true)
    max_thrash:         Maximum number of daemons that can be thrashed at a time. 
                        (default: num_of_daemons-1, minimum of 1 daemon should be up)
    min_thrash_delay:   Minimum number of seconds to delay before thrashing again. 
                        (default: 60)
    max_thrash_delay:   Maximum number of seconds to delay before thrashing again. 
                        (default: min_thrash_delay + 30)
    min_revive_delay:   Minimum number of seconds to delay before bringing back a 
                        thrashed daemon. (default: 100)
    max_revive_delay:   Maximum number of seconds to delay before bringing back a 
                        thrashed daemon. (default: min_revive_delay + 30)

    daemon_max_thrash_times: 
                        For now, NVMeoF daemons have limitation that each daemon can 
                        be thrashed only 3 times in span of 30 mins. This option 
                        allows to set the amount of times it could be thrashed in a period
                        of time. (default: 3)
    daemon_max_thrash_period: 
                        This option goes with the above option. It sets the period of time
                        over which each daemons can be thrashed for daemon_max_thrash_times
                        amount of times. Time period in seconds. (default: 1800, i.e. 30mins)
    

    For example::
    tasks:
    - nvmeof.thrash:
        checker_host: 'client.3'
        switch_thrashers: True

    - mon_thrash:
        switch_thrashers: True

    - workunit:
        clients:
            client.3:
            - rbd/nvmeof_fio_test.sh --rbd_iostat
        env:
            RBD_POOL: mypool
            IOSTAT_INTERVAL: '10'
    
    """
    def __init__(self, ctx, config, daemons) -> None:
        super(NvmeofThrasher, self).__init__()

        if config is None:
            self.config = dict()
        self.config = config
        self.ctx = ctx
        self.daemons = daemons
        self.logger = log.getChild('[nvmeof.thrasher]')
        self.stopping = Event()
        if self.config.get("switch_thrashers"): 
            self.switch_thrasher = Event()
        self.checker_host = get_remote_for_role(self.ctx, self.config.get('checker_host'))
        self.devices = self._get_devices(self.checker_host)

        """ Random seed """
        self.random_seed = self.config.get('seed', None)
        if self.random_seed is None:
            self.random_seed = int(time.time())

        self.rng = random.Random()
        self.rng.seed(int(self.random_seed))

        """ Thrashing params """
        self.randomize = bool(self.config.get('randomize', True))
        self.max_thrash_daemons = int(self.config.get('max_thrash', len(self.daemons) - 1))

        # Limits on thrashing each daemon
        self.daemon_max_thrash_times = int(self.config.get('daemon_max_thrash_times', 3))
        self.daemon_max_thrash_period = int(self.config.get('daemon_max_thrash_period', 30 * 60)) # seconds

        self.min_thrash_delay = int(self.config.get('min_thrash_delay', 60))
        self.max_thrash_delay = int(self.config.get('max_thrash_delay', self.min_thrash_delay + 30))
        self.min_revive_delay = int(self.config.get('min_revive_delay', 100))
        self.max_revive_delay = int(self.config.get('max_revive_delay', self.min_revive_delay + 30))

    def _get_devices(self, remote):
        GET_DEVICE_CMD = "sudo nvme list --output-format=json | " \
            "jq -r '.Devices | sort_by(.NameSpace) | .[] | select(.ModelNumber == \"Ceph bdev Controller\") | .DevicePath'"
        devices = remote.sh(GET_DEVICE_CMD).split()
        return devices
    
    def log(self, x):
        self.logger.info(x)

    def _run(self): # overriding 
        try:
            self.do_thrash()
        except Exception as e:
            self.set_thrasher_exception(e)
            self.logger.exception("exception:")
            # allow successful completion so gevent doesn't see an exception...
            # The DaemonWatchdog will observe the error and tear down the test.
    
    def stop(self):
        self.stopping.set()

    def do_checks(self):
        """
        Run some checks to see if everything is running well during thrashing.
        """
        self.log('display and verify stats:')
        for d in self.daemons:
            d.remote.sh(d.status_cmd, check_status=False)
        check_cmd = [
            'ceph', 'orch', 'ls',
            run.Raw('&&'), 'ceph', 'orch', 'ps', '--daemon-type', 'nvmeof',
            run.Raw('&&'), 'ceph', 'health', 'detail',
            run.Raw('&&'), 'ceph', '-s',
        ]
        for dev in self.devices:
            check_cmd += [
                run.Raw('&&'), 'sudo', 'nvme', 'list-subsys', dev,
                run.Raw('|'), 'grep', 'live optimized'
            ] 
        self.checker_host.run(args=check_cmd).wait()        

    def switch_task(self):
        """
        Pause nvmeof thrasher till other thrashers are done with their iteration.
        This method would help to sync between multiple thrashers, like:
        1. thrasher-1 and thrasher-2: thrash daemons in parallel
        2. thrasher-1 and thrasher-2: revive daemons in parallel 
        This allows us to run some checks after each thrashing and reviving iteration.
        """
        if not hasattr(self, 'switch_thrasher'):
            return
        self.switch_thrasher.set()
        thrashers = self.ctx.ceph[self.config.get('cluster')].thrashers
        for t in thrashers:
            if not isinstance(t, NvmeofThrasher) and hasattr(t, 'switch_thrasher') and ( 
                isinstance(t.stopping, Event) and not t.stopping.is_set()
            ):
                other_thrasher = t
                self.log('switch_task: waiting for other thrasher')
                other_thrasher.switch_thrasher.wait(300)
                self.log('switch_task: done waiting for the other thrasher')
                other_thrasher.switch_thrasher.clear()

    def do_thrash(self):
        self.log('start thrashing')
        self.log(f'seed: {self.random_seed}, , '\
                 f'max thrash delay: {self.max_thrash_delay}, min thrash delay: {self.min_thrash_delay} '\
                 f'max revive delay: {self.max_revive_delay}, min revive delay: {self.min_revive_delay} '\
                 f'daemons: {len(self.daemons)} '\
                )
        daemons_thrash_history = defaultdict(list)
        summary = []

        while not self.stopping.is_set():
            killed_daemons = []

            weight = 1.0 / len(self.daemons)
            count = 0
            for daemon in self.daemons:
                skip = self.rng.uniform(0.0, 1.0)
                if weight <= skip:
                    self.log('skipping daemon {label} with skip ({skip}) > weight ({weight})'.format(
                        label=daemon.id_, skip=skip, weight=weight))
                    continue

                # For now, nvmeof daemons can only be thrashed 3 times in last 30mins. 
                # Skip thrashing if daemon was thrashed <daemon_max_thrash_times> 
                # times in last <daemon_max_thrash_period> seconds. 
                thrashed_history = daemons_thrash_history.get(daemon.id_, [])
                history_ptr = len(thrashed_history) - self.daemon_max_thrash_times
                if history_ptr >= 0: 
                    ptr_timestamp = thrashed_history[history_ptr]
                    current_timestamp = datetime.now()
                    if (current_timestamp - ptr_timestamp).total_seconds() < self.daemon_max_thrash_period:
                        self.log(f'skipping daemon {daemon.id_}: thrashed total {len(thrashed_history)} times, '\
                                 f'can only thrash {self.daemon_max_thrash_times} times '\
                                 f'in {self.daemon_max_thrash_period} seconds.')
                        continue

                self.log('kill {label}'.format(label=daemon.id_))
                daemon.stop()

                killed_daemons.append(daemon)
                daemons_thrash_history[daemon.id_] += [datetime.now()]

                # only thrash max_thrash_daemons amount of daemons
                count += 1
                if count >= self.max_thrash_daemons:
                    break

            if killed_daemons:
                summary += ["killed: " + ", ".join([d.id_ for d in killed_daemons])]
                # delay before reviving
                revive_delay = self.min_revive_delay
                if self.randomize:
                    revive_delay = random.randrange(self.min_revive_delay, self.max_revive_delay)

                self.log(f'waiting for {revive_delay} secs before reviving')
                time.sleep(revive_delay) # blocking wait
                self.log('done waiting before reviving')

                self.do_checks()
                self.switch_task()

                # revive after thrashing
                for daemon in killed_daemons:
                    self.log('reviving {label}'.format(label=daemon.id_))
                    daemon.restart()
                
                # delay before thrashing
                thrash_delay = self.min_thrash_delay
                if self.randomize:
                    thrash_delay = random.randrange(self.min_thrash_delay, self.max_thrash_delay)
                if thrash_delay > 0.0:
                    self.log(f'waiting for {thrash_delay} secs before thrashing')
                    time.sleep(thrash_delay) # blocking
                    self.log('done waiting before thrashing')

                self.do_checks()
                self.switch_task()
        self.log("Thrasher summary: ")
        for daemon in daemons_thrash_history:
            self.log(f'{daemon} was thrashed {len(daemons_thrash_history[daemon])} times')
        for index, string in enumerate(summary):
            self.log(f"Iteration {index}: {string}")

class ThrashTest(Nvmeof):
    name = 'nvmeof.thrash'
    def setup(self):
        if self.config is None:
            self.config = {}
        assert isinstance(self.config, dict), \
            'nvmeof.thrash task only accepts a dict for configuration'

        self.cluster = self.config['cluster'] = self.config.get('cluster', 'ceph')
        daemons = list(self.ctx.daemons.iter_daemons_of_role('nvmeof', self.cluster))
        assert len(daemons) > 1, \
            'nvmeof.thrash task requires at least 2 nvmeof daemon'
        self.thrasher = NvmeofThrasher(self.ctx, self.config, daemons)

    def begin(self):
        self.thrasher.start()
        self.ctx.ceph[self.cluster].thrashers.append(self.thrasher) 

    def end(self):
        log.info('joining nvmeof.thrash')
        self.thrasher.stop()
        if self.thrasher.exception is not None:
            raise RuntimeError('error during thrashing')
        self.thrasher.join()
        log.info('done joining')


task = Nvmeof
thrash = ThrashTest
