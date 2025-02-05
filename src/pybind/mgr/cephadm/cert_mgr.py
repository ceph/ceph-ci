from typing import TYPE_CHECKING, Tuple, Union, List, Dict, Optional, cast, Any
import logging
import copy

from cephadm.ssl_cert_utils import SSLCerts, SSLConfigException
from mgr_util import verify_tls, ServerConfigException
from cephadm.ssl_cert_utils import get_certificate_info, get_private_key_info
from cephadm.tlsobject_types import Cert, PrivKey
from cephadm.tlsobject_store import TLSObjectStore, TLSObjectScope

if TYPE_CHECKING:
    from cephadm.module import CephadmOrchestrator

logger = logging.getLogger(__name__)


class CertInfo:
    """
      - is_valid: True if the certificate is valid.
      - is_close_to_expiration: True if the certificate is close to expiration.
      - days_to_expiration: Number of days until expiration.
      - error_info: Details of any exception encountered during validation.
    """
    def __init__(self, cert_name: str,
                 target: Optional[str],
                 is_valid: bool = False,
                 is_close_to_expiration: bool = False,
                 days_to_expiration: int = 0,
                 error_info: str = ''):
        self.cert_name = cert_name
        self.target = target or ''
        self.is_valid = is_valid
        self.is_close_to_expiration = is_close_to_expiration
        self.days_to_expiration = days_to_expiration
        self.error_info = error_info

    def __str__(self) -> str:
        return f'{self.cert_name} ({self.target})' if self.target else f'{self.cert_name}'

    def get_status_description(self) -> str:
        if not self.is_valid:
            if 'expired' in self.error_info.lower():
                return 'certificate has expired'
            else:
                return 'certificate is not valid'
        elif self.is_close_to_expiration:
            return f'certificate is about to expire (remaining days: {self.days_to_expiration})'

        return 'certificate is valid'


class CertMgr:
    """
    Cephadm Certificate Manager plays a crucial role in maintaining a secure and automated certificate
    lifecycle within Cephadm deployments. CertMgr manages SSL/TLS certificates for all services
    handled by cephadm, acting as the root Certificate Authority (CA) for all certificates.
    This class provides mechanisms for storing, validating, renewing, and monitoring certificate status.

    It tracks known certificates and private keys, associates them with services, and ensures
    their validity. If certificates are close to expiration or invalid, depending on the configuration
    (governed by the mgr/cephadm/certificate_automated_rotation_enabled parameter), CertMgr generates
    warnings or attempts renewal for self-signed certificates.

    Additionally, CertMgr provides methods for certificate management, including retrieving, saving,
    and removing certificates and keys, as well as reporting certificate health status in case of issues.

    This class holds two important mappings: known_certs and known_keys. Both of them define all the known
    certificates managed by cephadm. Each certificate/key has a pre-defined scope: Global, Host, or Service.

       - Global: The same certificates is used for all the service daemons (e.g mgmt-gateway).
       - Host: Certificates specific to individual hosts within the cluster (e.g Grafana).
       - Service: Certificates tied to specific service (e.g RGW).

    The cert_to_service is an inverse mapping that associates each certificate with its service. This is
    needed trigger the corresponding service reconfiguration when updating some certificate and also when
    setting the cert/key from CLI.

    """

    CEPHADM_ROOT_CA_CERT = 'cephadm_root_ca_cert'
    CEPHADM_ROOT_CA_KEY = 'cephadm_root_ca_key'
    CEPHADM_CERTMGR_HEALTH_ERR = 'CEPHADM_CERT_ERROR'

    ####################################################
    #  cephadm certmgr known Certificates section
    known_certs = {
        TLSObjectScope.SERVICE: [
            'iscsi_ssl_cert',
            'rgw_frontend_ssl_cert',
            'ingress_ssl_cert',
            'nvmeof_server_cert',
            'nvmeof_client_cert',
            'nvmeof_root_ca_cert',
        ],
        TLSObjectScope.HOST: [
            'grafana_cert',
        ],
        TLSObjectScope.GLOBAL: [
            'mgmt_gw_cert',
            'oauth2_proxy_cert',
            CEPHADM_ROOT_CA_CERT,
        ],
    }

    ####################################################
    #  cephadm certmgr known Keys section
    known_keys = {
        TLSObjectScope.SERVICE: [
            'iscsi_ssl_key',
            'ingress_ssl_key',
            'nvmeof_server_key',
            'nvmeof_client_key',
            'nvmeof_encryption_key',
        ],
        TLSObjectScope.HOST: [
            'grafana_key',
        ],
        TLSObjectScope.GLOBAL: [
            'mgmt_gw_key',
            'oauth2_proxy_key',
            CEPHADM_ROOT_CA_KEY,
        ],
    }

    ####################################################
    #  inverse cert -> service mapping
    cert_to_service = {
        'rgw_frontend_ssl_cert': 'rgw',
        'iscsi_ssl_cert': 'iscsi',
        'ingress_ssl_cert': 'ingress',
        'nvmeof_server_cert': 'nvmeof',
        'nvmeof_client_cert': 'nvmeof',
        'nvmeof_root_ca_cert': 'nvmeof',
        'mgmt_gw_cert': 'mgmt-gateway',
        'oauth2_proxy_cert': 'oauth2-proxy',
        'grafana_cert': 'grafana',
    }

    def __init__(self,
                 mgr: "CephadmOrchestrator",
                 mgr_ip: str) -> None:
        self.mgr = mgr
        self.mgr_ip = mgr_ip
        self._init_tlsobject_store()
        self._initialize_root_ca(mgr_ip)
        self.bad_certificates: List[CertInfo] = []

    def _init_tlsobject_store(self) -> None:
        self.cert_store = TLSObjectStore(self.mgr, Cert, self.known_certs)
        self.cert_store.load()
        self.key_store = TLSObjectStore(self.mgr, PrivKey, self.known_keys)
        self.key_store.load()

    def load(self) -> None:
        self._init_tlsobject_store()

    def _initialize_root_ca(self, ip: str) -> None:
        self.ssl_certs: SSLCerts = SSLCerts(self.mgr._cluster_fsid, self.mgr.certificate_duration_days)
        old_cert = cast(Cert, self.cert_store.get_tlsobject(self.CEPHADM_ROOT_CA_CERT))
        old_key = cast(PrivKey, self.key_store.get_tlsobject(self.CEPHADM_ROOT_CA_KEY))
        if old_key and old_cert:
            try:
                self.ssl_certs.load_root_credentials(old_cert.cert, old_key.key)
            except SSLConfigException as e:
                raise SSLConfigException("Cannot load cephadm root CA certificates.") from e
        else:
            self.ssl_certs.generate_root_cert(addr=ip)
            self.cert_store.save_tlsobject(self.CEPHADM_ROOT_CA_CERT, self.ssl_certs.get_root_cert())
            self.key_store.save_tlsobject(self.CEPHADM_ROOT_CA_KEY, self.ssl_certs.get_root_key())

    def get_root_ca(self) -> str:
        return self.ssl_certs.get_root_cert()

    def generate_cert(
        self,
        host_fqdn: Union[str, List[str]],
        node_ip: Union[str, List[str]],
        custom_san_list: Optional[List[str]] = None,
    ) -> Tuple[str, str]:
        return self.ssl_certs.generate_cert(host_fqdn, node_ip, custom_san_list=custom_san_list)

    def get_cert(self, cert_name: str, service_name: Optional[str] = None, host: Optional[str] = None) -> Optional[str]:
        cert_obj = cast(Cert, self.cert_store.get_tlsobject(cert_name, service_name, host))
        return cert_obj.cert if cert_obj else None

    def get_key(self, key_name: str, service_name: Optional[str] = None, host: Optional[str] = None) -> Optional[str]:
        key_obj = cast(PrivKey, self.key_store.get_tlsobject(key_name, service_name, host))
        return key_obj.key if key_obj else None

    def save_cert(self, cert_name: str, cert: str, service_name: Optional[str] = None, host: Optional[str] = None, user_made: bool = False) -> None:
        self.cert_store.save_tlsobject(cert_name, cert, service_name, host, user_made)

    def save_key(self, key_name: str, key: str, service_name: Optional[str] = None, host: Optional[str] = None, user_made: bool = False) -> None:
        self.key_store.save_tlsobject(key_name, key, service_name, host, user_made)

    def rm_cert(self, cert_name: str, service_name: Optional[str] = None, host: Optional[str] = None) -> None:
        self.cert_store.rm_tlsobject(cert_name, service_name, host)

    def rm_key(self, key_name: str, service_name: Optional[str] = None, host: Optional[str] = None) -> None:
        self.key_store.rm_tlsobject(key_name, service_name, host)

    def cert_ls(self, include_datails: bool = False) -> Dict[str, Union[bool, Dict[str, Dict[str, bool]]]]:
        ls: Dict = copy.deepcopy(self.cert_store.get_tlsobjects())
        for k, v in ls.items():
            if isinstance(v, dict):
                tmp: Dict[str, Any] = {key: get_certificate_info(cast(Cert, v[key]).cert, include_datails) for key in v if isinstance(v[key], Cert)}
                ls[k] = tmp if tmp else {}
            elif isinstance(v, Cert):
                ls[k] = get_certificate_info(cast(Cert, v).cert, include_datails) if bool(v) else {}
        return ls

    def key_ls(self) -> Dict[str, Union[bool, Dict[str, bool]]]:
        ls: Dict = copy.deepcopy(self.key_store.get_tlsobjects())
        if self.CEPHADM_ROOT_CA_KEY in ls:
            del ls[self.CEPHADM_ROOT_CA_KEY]
        for k, v in ls.items():
            if isinstance(v, dict) and v:
                tmp: Dict[str, Any] = {key: get_private_key_info(cast(PrivKey, v[key]).key) for key in v if v[key]}
                ls[k] = tmp if tmp else {}
            elif isinstance(v, PrivKey):
                ls[k] = get_private_key_info(cast(PrivKey, v).key)
        return ls

    def list_entity_known_certificates(self, entity: str) -> List[str]:
        return [cert_name for cert_name, service in self.cert_to_service.items() if service == entity]

    def entity_ls(self, get_scope: bool = False) -> List[Union[str, Tuple[str, str]]]:
        if get_scope:
            return [(entity, self.determine_scope(entity)) for entity in set(self.cert_to_service.values())]
        else:
            return list(self.cert_to_service.values())

    def get_cert_scope(self, cert_name: str) -> TLSObjectScope:
        for scope, certificates in self.known_certs.items():
            if cert_name in certificates:
                return scope
        return TLSObjectScope.UNKNOWN

    def determine_scope(self, entity: str) -> str:
        for cert, service in self.cert_to_service.items():
            if service == entity:
                if cert in self.known_certs[TLSObjectScope.SERVICE]:
                    return TLSObjectScope.SERVICE.value
                elif cert in self.known_certs[TLSObjectScope.HOST]:
                    return TLSObjectScope.HOST.value
                elif cert in self.known_certs[TLSObjectScope.GLOBAL]:
                    return TLSObjectScope.GLOBAL.value
        return TLSObjectScope.UNKNOWN.value

    def _notify_certificates_health_status(self, problematic_certificates: List[CertInfo]) -> None:

        if not problematic_certificates:
            self.mgr.remove_health_warning(CertMgr.CEPHADM_CERTMGR_HEALTH_ERR)
            return

        detailed_error_msgs = []
        invalid_count = 0
        expired_count = 0
        expiring_count = 0
        for cert_info in problematic_certificates:
            target = f' ({cert_info.target})' if cert_info.target else ''
            cert_details = f"'{cert_info.cert_name}{target}'"
            if not cert_info.is_valid:
                if 'expired' in cert_info.error_info.lower():
                    expired_count += 1
                    detailed_error_msgs.append(f'Certificate {cert_details} has expired.')
                else:
                    invalid_count += 1
                    detailed_error_msgs.append(f'Invalid certificate {cert_details}: {cert_info.error_info}.')
            elif cert_info.is_close_to_expiration:
                expiring_count += 1
                detailed_error_msgs.append(f'Certificate {cert_details} is close to expiration. Remaining days: {cert_info.days_to_expiration}.')

        # Generate a short description with a summery of all the detected issues
        issues = [
            f'{invalid_count} invalid' if invalid_count > 0 else '',
            f'{expired_count} expired' if expired_count > 0 else '',
            f'{expiring_count} expiring' if expiring_count > 0 else ''
        ]
        issues_description = ', '.join(filter(None, issues))  # collect only non-empty issues
        total_issues = invalid_count + expired_count + expiring_count
        short_error_msg = (f'Detected {total_issues} cephadm certificate(s) issues: {issues_description}')

        if invalid_count > 0 or expired_count > 0:
            logger.error(short_error_msg)
            self.mgr.set_health_error(CertMgr.CEPHADM_CERTMGR_HEALTH_ERR, short_error_msg, total_issues, detailed_error_msgs)
        else:
            logger.warning(short_error_msg)
            self.mgr.set_health_warning(CertMgr.CEPHADM_CERTMGR_HEALTH_ERR, short_error_msg, total_issues, detailed_error_msgs)

    def check_certificate_state(self, cert_name: str, target: str, cert: str, key: str) -> CertInfo:
        """
        Checks if a certificate is valid and close to expiration.

        Returns:
            - is_valid: True if the certificate is valid.
            - is_close_to_expiration: True if the certificate is close to expiration.
            - days_to_expiration: Number of days until expiration.
            - exception_info: Details of any exception encountered during validation.
        """
        cert_obj = Cert(cert, True)
        key_obj = PrivKey(key, True)
        return self._check_certificate_state(cert_name, target, cert_obj, key_obj)

    def _check_certificate_state(self, cert_name: str, target: Optional[str], cert: Cert, key: PrivKey) -> CertInfo:
        """
        Checks if a certificate is valid and close to expiration.

        Returns: CertInfo
        """
        try:
            days_to_expiration = verify_tls(cert.cert, key.key)
            is_close_to_expiration = days_to_expiration < self.mgr.certificate_renewal_threshold_days
            return CertInfo(cert_name, target, True, is_close_to_expiration, days_to_expiration, "")
        except ServerConfigException as e:
            return CertInfo(cert_name, target, False, False, 0, str(e))

    def _renew_self_signed_certificate(self, cert_info: CertInfo, cert_obj: Cert, target: str) -> None:
        try:
            logger.info(f'Renewing self-signed certificate for {cert_info.cert_name}')
            new_cert, new_key = self.ssl_certs.renew_cert(cert_obj.cert, self.mgr.certificate_duration_days)
            service_name, host = self.cert_store.determine_tlsobject_target(cert_info.cert_name, target)
            self.cert_store.save_tlsobject(cert_info.cert_name, new_cert, service_name=service_name, host=host)
            key_name = cert_info.cert_name.replace('_cert', '_key')
            self.key_store.save_tlsobject(key_name, new_key, service_name=service_name, host=host)
        except SSLConfigException as e:
            logger.error(f'Error while trying to renew self-signed certificate for {cert_info.cert_name}: {e}')

    def _log_problematic_certificate(self, cert_info: CertInfo, cert_obj: Cert) -> None:
        cert_source = 'user-made' if cert_obj.user_made else 'self-signed'
        if cert_info.is_close_to_expiration:
            logger.warning(f'Detected a {cert_source} certificate close to its expiration: {cert_info}')
        elif not cert_info.is_valid:
            if 'expired' in cert_info.error_info.lower():
                logger.error(f'Detected a {cert_source} expired certificate: {cert_info}')
            else:
                logger.error(f'Detected a {cert_source} invalid certificate: {cert_info}')

    def prepare_certificates(self,
                             cert_name: str,
                             key_name: str,
                             host_fqdns: Union[str, List[str]],
                             host_ips: Union[str, List[str]],
                             target_host: str = '',
                             target_service: str = '',
                             ) -> Tuple[Optional[str], Optional[str]]:

        if not cert_name or not key_name:
            logger.error("Certificate name and key name must be provided when calling prepare_certificates.")
            return None, None

        cert_obj = cast(Cert, self.cert_store.get_tlsobject(cert_name, target_service, target_host))
        key_obj = cast(PrivKey, self.key_store.get_tlsobject(key_name, target_service, target_host))
        if cert_obj and key_obj:
            target = target_host or target_service
            cert_info = self._check_certificate_state(cert_name, target, cert_obj, key_obj)
            if cert_info.is_valid and not cert_info.is_close_to_expiration:
                return cert_obj.cert, key_obj.key
            elif cert_obj.user_made:
                self._notify_certificates_health_status([cert_info])  # TODO: handle previous value of warnings/erros
                return None, None
            else:
                logger.warning(f'Found invalid cephadm self-signed certificates {cert_name}/{key_name}, '
                               f'status: {cert_info.get_status_description()}, '
                               f'error: {cert_info.error_info}')

        # Reaching this point means either certificates are not present or they are
        # invalid self-signed certificates. Either way, we will just generate new ones.
        logger.info(f'Generating cephadm self-signed certificates for {cert_name}/{key_name}')
        cert, pkey = self.generate_cert(host_fqdns, host_ips)
        self.mgr.cert_mgr.save_cert(cert_name, cert, host=target_host, service_name=target_service)
        self.mgr.cert_mgr.save_key(key_name, pkey, host=target_host, service_name=target_service)
        return cert, pkey

    def get_problematic_certificates(self) -> List[Tuple[CertInfo, Cert]]:

        def get_key(cert_name: str, target: Optional[str]) -> Optional[PrivKey]:
            key_name = cert_name.replace('_cert', '_key')
            service_name, host = self.cert_store.determine_tlsobject_target(cert_name, target)
            key = cast(PrivKey, self.key_store.get_tlsobject(key_name, service_name=service_name, host=host))
            return key

        # Filter non-empty entries skipping cephadm root CA cetificate
        certs_tlsobjs = [c for c in self.cert_store.list_tlsobjects() if c[1] and c[0] != self.CEPHADM_ROOT_CA_CERT]
        problematics_certs: List[Tuple[CertInfo, Cert]] = []
        for cert_name, cert_tlsobj, target in certs_tlsobjs:
            cert_obj = cast(Cert, cert_tlsobj)
            key_obj = get_key(cert_name, target)
            if cert_obj and key_obj:
                cert_info = self._check_certificate_state(cert_name, target, cert_obj, key_obj)
                if not cert_info.is_valid or cert_info.is_close_to_expiration:
                    problematics_certs.append((cert_info, cert_obj))
                else:
                    target_info = f" ({target})" if target else ""
                    logger.info(f'Certificate for "{cert_name}{target_info}" is still valid for {cert_info.days_to_expiration} days.')
            elif cert_obj:
                # Cert is present but key is None, could only happen if somebody has put manually a bad key!
                logger.warning(f"Key is missing for certificate '{cert_name}'. Attempting renewal.")
                cert_info = CertInfo(cert_name, target, False, False, 0, "Missing key")
                problematics_certs.append((cert_info, cert_obj))
            else:
                logger.error(f'Cannot get cert/key {cert_name}')

        return problematics_certs

    def check_services_certificates(self) -> List[str]:

        certs_to_fix: List[CertInfo] = []  # Certificates requiring manual user intervention
        services_to_reconfig = set()

        for cert_info, cert_obj in self.get_problematic_certificates():

            self._log_problematic_certificate(cert_info, cert_obj)

            if cert_info.is_close_to_expiration:
                if self.mgr.certificate_automated_rotation_enabled and not cert_obj.user_made:
                    self._renew_self_signed_certificate(cert_info, cert_obj, cert_info.target)
                    services_to_reconfig.add(self.cert_to_service[cert_info.cert_name])
                else:
                    certs_to_fix.append(cert_info)
            elif not cert_info.is_valid:
                if cert_obj.user_made:
                    certs_to_fix.append(cert_info)
                elif self.mgr.certificate_automated_rotation_enabled:
                    # Found a self-signed invalid certificate.. shouldn't happen but let's try to fix it if possible
                    service_name, host = self.cert_store.determine_tlsobject_target(cert_info.cert_name, cert_info.target)
                    logger.info(f'Removing invalid certificate for {cert_info.cert_name} to trigger regeneration (service: {service_name}, host: {host}).')
                    self.cert_store.rm_tlsobject(cert_info.cert_name, service_name, host)
                    services_to_reconfig.add(self.cert_to_service.get(cert_info.cert_name, 'unknown'))

        self._notify_certificates_health_status(certs_to_fix)

        if services_to_reconfig:
            logger.info(f'certmgr: services to reconfigure {services_to_reconfig}')

        # return the list of services that need reconfiguration
        return list(services_to_reconfig)
