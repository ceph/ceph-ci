# service_installer.py - generic service and package installation utilities

import logging
from typing import List, Optional

from .context import CephadmContext
from .packagers import Packager, create_packager, get_distro
from .systemd import enable_service, check_unit
from .exceptions import Error
from .call_wrappers import call, CallVerbosity

logger = logging.getLogger()


def _normalize_package_names(packages: List[str]) -> List[str]:
    """
    Normalize package names based on the distribution.
    Some packages have different names on different distributions.
    """
    distro, _, _ = get_distro()
    normalized = []

    for pkg in packages:
        # Map distribution-specific package names
        if pkg == 'nfs-utils':
            # On Debian/Ubuntu, nfs-utils is called nfs-common
            if distro in ['ubuntu', 'debian']:
                normalized.append('nfs-common')
            else:
                normalized.append('nfs-utils')
        else:
            normalized.append(pkg)
    return normalized


def _is_package_installed(ctx: CephadmContext, package: str) -> bool:
    """
    Check if a package is already installed on the system.
    """
    distro, _, _ = get_distro()
    try:
        if distro in ['ubuntu', 'debian']:
            # Use dpkg-query to check if package is installed
            out, err, code = call(
                ctx,
                ['dpkg-query', '-W', '-f=${Status}', package],
                verbosity=CallVerbosity.QUIET,
            )
            return code == 0 and 'install ok installed' in out
        else:
            # For RPM-based systems (yum/dnf/zypper), use rpm -q
            out, err, code = call(
                ctx,
                ['rpm', '-q', package],
                verbosity=CallVerbosity.QUIET,
            )
            return code == 0
    except Exception as e:
        logger.debug('Error checking if package %s is installed: %s' % (package, e))
        # If we can't check, assume it's not installed
        return False


def _filter_installed_packages(ctx: CephadmContext, packages: List[str]) -> List[str]:
    """
    Filter out packages that are already installed.
    """
    packages_to_install = []
    for pkg in packages:
        if _is_package_installed(ctx, pkg):
            logger.info('Package %s is already installed, skipping' % pkg)
        else:
            packages_to_install.append(pkg)
    return packages_to_install


def install_services_and_packages(
    ctx: CephadmContext,
    packages: Optional[List[str]] = None,
    services: Optional[List[str]] = None,
    pkg: Optional[Packager] = None,
) -> None:
    """
    Generic function to install packages and enable/start services.
    """
    if not packages:
        packages = []
    if not services:
        services = []

    packages = _normalize_package_names(packages)
    packages_to_install = _filter_installed_packages(ctx, packages)
    if packages_to_install:
        if not pkg:
            pkg = create_packager(ctx)
        logger.info('Installing packages: %s' % ', '.join(packages_to_install))
        try:
            pkg.install(packages_to_install)
        except Error as e:
            logger.warning(
                'Failed to install packages %s: %s' % (', '.join(packages_to_install), e)
            )
    elif packages:
        logger.info('All packages (%s) are already installed' % ', '.join(packages))

    # Enable and start services if provided
    for service in services:
        enabled, state, installed = check_unit(ctx, service)
        if not installed:
            logger.warning(
                'Service %s is not installed. It may need to be installed '
                'as part of a package.' % service
            )
            continue
        elif enabled and state == 'running':
            logger.info('Service %s is already enabled and running' % service)
            continue
        else:
            logger.info('Enabling and starting service %s...' % service)
            try:
                enable_service(ctx, service)
                call(ctx, ['systemctl', 'start', service], verbosity=CallVerbosity.DEBUG)
            except Error as e:
                logger.warning(
                    'Failed to enable/start service %s: %s' % (service, e)
                )
                continue


def install_nfs_services(
    ctx: CephadmContext,
    pkg: Optional[Packager] = None,
) -> None:
    """
    Install and enable NFS/RPC services required for NFS operations.
    """
    # NFS/RPC service package and service names
    NFS_PACKAGES = ['rpcbind', 'nfs-utils']
    NFS_SERVICES = ['rpcbind', 'rpc-statd']

    logger.info('Installing required NFS/RPC services...')
    install_services_and_packages(
        ctx,
        packages=NFS_PACKAGES,
        services=NFS_SERVICES,
        pkg=pkg,
    )


def install_required_services(
    ctx: CephadmContext,
    pkg: Optional[Packager] = None,
) -> None:
    """
    Installing services
    - NFS services (rpcbind, rpc-statd)
    """
    install_nfs_services(ctx, pkg=pkg)
