import base64
import os
import logging
import re
from ceph_volume import process, conf, terminal
from ceph_volume.util import constants, system
from ceph_volume.util.device import Device
from .prepare import write_keyring
from .disk import lsblk, device_family, get_part_entry_type
from packaging import version

logger = logging.getLogger(__name__)
mlogger = terminal.MultiLogger(__name__)

def set_dmcrypt_no_workqueue(target_version: str = '2.3.4') -> None:
    """Set `conf.dmcrypt_no_workqueue` to `True` if the installed version
    of `cryptsetup` is greater than or equal to the specified `target_version`.

    Depending on the crypsetup version, `cryptsetup --version` output can be different.
    Eg:

    CentOS Stream9:
    $ cryptsetup --version
    cryptsetup 2.6.0 flags: UDEV BLKID KEYRING FIPS KERNEL_CAPI PWQUALITY

    CentOS Stream8:
    $ cryptsetup --version
    cryptsetup 2.3.7

    Args:
        target_version (str, optional): The minimum version required for setting
            `conf.dmcrypt_no_workqueue` to `True`. Defaults to '2.3.4'.

    Raises:
        RuntimeError: If failed to retrieve the cryptsetup version.
        RuntimeError: If failed to parse the cryptsetup version.
        RuntimeError: If failed to compare the cryptsetup version with the target version.
    """
    command = ["cryptsetup", "--version"]
    out, err, rc = process.call(command)

    # This regex extracts the version number from
    # the `cryptsetup --version` output
    pattern: str = r'(\d+\.?)+'

    if rc:
        raise RuntimeError(f"Can't retrieve cryptsetup version: {err}")

    try:
        cryptsetup_version = re.search(pattern, out[0])

        if cryptsetup_version is None:
            _output: str = "\n".join(out)
            raise RuntimeError('Error while checking cryptsetup version.\n',
                               '`cryptsetup --version` output:\n',
                               f'{_output}')

        if version.parse(cryptsetup_version.group(0)) >= version.parse(target_version):
            conf.dmcrypt_no_workqueue = True
    except IndexError:
        mlogger.debug(f'cryptsetup version check: rc={rc} out={out} err={err}')
        raise RuntimeError("Couldn't check the cryptsetup version.")

def bypass_workqueue(device: str) -> bool:
    return not Device(device).rotational and conf.dmcrypt_no_workqueue

def get_key_size_from_conf():
    """
    Return the osd dmcrypt key size from config file.
    Default is 512.
    """
    default_key_size = '512'
    key_size = conf.ceph.get_safe(
        'osd',
        'osd_dmcrypt_key_size',
        default='512', check_valid=False)

    if key_size not in ['256', '512']:
        logger.warning(("Invalid value set for osd_dmcrypt_key_size ({}). "
                        "Falling back to {}bits".format(key_size, default_key_size)))
        return default_key_size

    return key_size

def create_dmcrypt_key():
    """
    Create the secret dm-crypt key (KEK) used to encrypt/decrypt the Volume Key.
    """
    random_string = os.urandom(128)
    key = base64.b64encode(random_string).decode('utf-8')
    return key


def luks_format(key, device):
    """
    Decrypt (open) an encrypted device, previously prepared with cryptsetup

    :param key: dmcrypt secret key, will be used for decrypting
    :param device: Absolute path to device
    """
    command = [
        'cryptsetup',
        '--batch-mode', # do not prompt
        '--key-size',
        get_key_size_from_conf(),
        '--key-file', # misnomer, should be key
        '-',          # because we indicate stdin for the key here
        'luksFormat',
        device,
    ]
    process.call(command, stdin=key, terminal_verbose=True, show_command=True)


def plain_open(key, device, mapping):
    """
    Decrypt (open) an encrypted device, previously prepared with cryptsetup in plain mode

    .. note: ceph-disk will require an additional b64decode call for this to work

    :param key: dmcrypt secret key
    :param device: absolute path to device
    :param mapping: mapping name used to correlate device. Usually a UUID
    """
    command = [
        'cryptsetup',
        '--key-file',
        '-',
        '--allow-discards',  # allow discards (aka TRIM) requests for device
        'open',
        device,
        mapping,
        '--type', 'plain',
        '--key-size', '256',
    ]

    if bypass_workqueue(device):
        command.extend(['--perf-no_read_workqueue',
                        '--perf-no_write_workqueue'])

    process.call(command, stdin=key, terminal_verbose=True, show_command=True)


def luks_open(key, device, mapping):
    """
    Decrypt (open) an encrypted device, previously prepared with cryptsetup

    .. note: ceph-disk will require an additional b64decode call for this to work

    :param key: dmcrypt secret key
    :param device: absolute path to device
    :param mapping: mapping name used to correlate device. Usually a UUID
    """
    command = [
        'cryptsetup',
        '--key-size',
        get_key_size_from_conf(),
        '--key-file',
        '-',
        '--allow-discards',  # allow discards (aka TRIM) requests for device
        'luksOpen',
        device,
        mapping,
    ]

    if bypass_workqueue(device):
        command.extend(['--perf-no_read_workqueue',
                        '--perf-no_write_workqueue'])

    process.call(command, stdin=key, terminal_verbose=True, show_command=True)


def dmcrypt_close(mapping, skip_path_check=False):
    """
    Encrypt (close) a device, previously decrypted with cryptsetup

    :param mapping: mapping name or path used to correlate device.
    :param skip_path_check: whether we need path presence validation.
    """
    if not skip_path_check and not os.path.exists(mapping):
        logger.debug('device mapper path does not exist %s' % mapping)
        logger.debug('will skip cryptsetup removal')
        return
    # don't be strict about the remove call, but still warn on the terminal if it fails
    process.run(['cryptsetup', 'remove', mapping], stop_on_error=False)

def get_dmcrypt_key(osd_id, osd_fsid, lockbox_keyring=None):
    """
    Retrieve the dmcrypt (secret) key stored initially on the monitor. The key
    is sent initially with JSON, and the Monitor then mangles the name to
    ``dm-crypt/osd/<fsid>/luks``

    The ``lockbox.keyring`` file is required for this operation, and it is
    assumed it will exist on the path for the same OSD that is being activated.
    To support scanning, it is optionally configurable to a custom location
    (e.g. inside a lockbox partition mounted in a temporary location)
    """
    if lockbox_keyring is None:
        lockbox_keyring = '/var/lib/ceph/osd/%s-%s/lockbox.keyring' % (conf.cluster, osd_id)
    name = 'client.osd-lockbox.%s' % osd_fsid
    config_key = 'dm-crypt/osd/%s/luks' % osd_fsid

    mlogger.info(f'Running ceph config-key get {config_key}')
    stdout, stderr, returncode = process.call(
        [
            'ceph',
            '--cluster', conf.cluster,
            '--name', name,
            '--keyring', lockbox_keyring,
            'config-key',
            'get',
            config_key
        ],
        show_command=True,
        logfile_verbose=False
    )
    if returncode != 0:
        raise RuntimeError('Unable to retrieve dmcrypt secret')
    return ' '.join(stdout).strip()


def write_lockbox_keyring(osd_id, osd_fsid, secret):
    """
    Helper to write the lockbox keyring. This is needed because the bluestore OSD will
    not persist the keyring.

    For bluestore: A tmpfs filesystem is mounted, so the path can get written
    to, but the files are ephemeral, which requires this file to be created
    every time it is activated.
    """
    if os.path.exists('/var/lib/ceph/osd/%s-%s/lockbox.keyring' % (conf.cluster, osd_id)):
        return

    name = 'client.osd-lockbox.%s' % osd_fsid
    write_keyring(
        osd_id,
        secret,
        keyring_name='lockbox.keyring',
        name=name
    )


def status(device):
    """
    Capture the metadata information of a possibly encrypted device, returning
    a dictionary with all the values found (if any).

    An encrypted device will contain information about a device. Example
    successful output looks like::

        $ cryptsetup status /dev/mapper/ed6b5a26-eafe-4cd4-87e3-422ff61e26c4
        /dev/mapper/ed6b5a26-eafe-4cd4-87e3-422ff61e26c4 is active and is in use.
          type:    LUKS1
          cipher:  aes-xts-plain64
          keysize: 256 bits
          device:  /dev/sdc2
          offset:  4096 sectors
          size:    20740063 sectors
          mode:    read/write

    As long as the mapper device is in 'open' state, the ``status`` call will work.

    :param device: Absolute path or UUID of the device mapper
    """
    command = [
        'cryptsetup',
        'status',
        device,
    ]
    out, err, code = process.call(command, show_command=True, verbose_on_failure=False)

    metadata = {}
    if code != 0:
        logger.warning('failed to detect device mapper information')
        return metadata
    for line in out:
        # get rid of lines that might not be useful to construct the report:
        if not line.startswith(' '):
            continue
        try:
            column, value = line.split(': ')
        except ValueError:
            continue
        metadata[column.strip()] = value.strip().strip('"')
    return metadata


def legacy_encrypted(device):
    """
    Detect if a device was encrypted with ceph-disk or not. In the case of
    encrypted devices, include the type of encryption (LUKS, or PLAIN), and
    infer what the lockbox partition is.

    This function assumes that ``device`` will be a partition.
    """
    disk_meta = {}
    if os.path.isdir(device):
        mounts = system.Mounts(paths=True).get_mounts()
        # yes, rebind the device variable here because a directory isn't going
        # to help with parsing
        device = mounts.get(device, [None])[0]
        if not device:
            raise RuntimeError('unable to determine the device mounted at %s' % device)
    metadata = {'encrypted': False, 'type': None, 'lockbox': '', 'device': device}
    # check if the device is online/decrypted first
    active_mapper = status(device)
    if active_mapper:
        # normalize a bit to ensure same values regardless of source
        metadata['type'] = active_mapper['type'].lower().strip('12')  # turn LUKS1 or LUKS2 into luks
        metadata['encrypted'] = True if metadata['type'] in ['plain', 'luks'] else False
        # The true device is now available to this function, so it gets
        # re-assigned here for the lockbox checks to succeed (it is not
        # possible to guess partitions from a device mapper device otherwise
        device = active_mapper.get('device', device)
        metadata['device'] = device
    else:
        uuid = get_part_entry_type(device)
        guid_match = constants.ceph_disk_guids.get(uuid, {})
        encrypted_guid = guid_match.get('encrypted', False)
        if encrypted_guid:
            metadata['encrypted'] = True
            metadata['type'] = guid_match['encryption_type']

    # Lets find the lockbox location now, to do this, we need to find out the
    # parent device name for the device so that we can query all of its
    # associated devices and *then* look for one that has the 'lockbox' label
    # on it. Thanks for being awesome ceph-disk
    if not device == 'tmpfs':
        disk_meta = lsblk(device, abspath=True)
    if not disk_meta:
        return metadata
    parent_device = disk_meta['PKNAME']
    # With the parent device set, we can now look for the lockbox listing associated devices
    devices = [Device(i['NAME']) for i in device_family(parent_device)]
    for d in devices:
        if d.ceph_disk.type == 'lockbox':
            metadata['lockbox'] = d.path
            break
    return metadata

def prepare_dmcrypt(key, device, mapping):
    """
    Helper for devices that are encrypted. The operations needed for
    block, db, wal, or data/journal devices are all the same
    """
    if not device:
        return ''
    # format data device
    luks_format(
        key,
        device
    )
    luks_open(
        key,
        device,
        mapping
    )
    return '/dev/mapper/%s' % mapping
