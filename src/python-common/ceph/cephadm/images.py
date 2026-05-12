# Default container images -----------------------------------------------------

from typing import NamedTuple
from enum import Enum


class ContainerImage(NamedTuple):
    image_ref: str  # reference to default container image
    key: str  # image key
    desc: str  # description of image

    def __repr__(self) -> str:
        return self.image_ref


def _create_image(image_ref: str, key: str) -> ContainerImage:
    _img_prefix = 'container_image_'
    description = key.replace('_', ' ').capitalize()
    return ContainerImage(
        image_ref,
        f'{_img_prefix}{key}',
        f'{description} container image'
    )


class DefaultImages(Enum):
    PROMETHEUS = _create_image('registry.redhat.io/openshift4/ose-prometheus:v4.15', 'prometheus')
    LOKI = _create_image('registry.redhat.io/openshift-logging/logging-loki-rhel8:v3.1.0', 'loki')
    PROMTAIL = _create_image('registry.redhat.io/rhceph/rhceph-promtail-rhel9:v3.0.0', 'promtail')
    ALLOY = _create_image('registry.redhat.io/rhceph/alloy-rhel10:v9.1', 'alloy')
    NODE_EXPORTER = _create_image('registry.redhat.io/openshift4/ose-prometheus-node-exporter:v4.15', 'node_exporter')
    ALERTMANAGER = _create_image('registry.redhat.io/openshift4/ose-prometheus-alertmanager:v4.15', 'alertmanager')
    GRAFANA = _create_image('registry.redhat.io/rhceph/grafana-rhel10:v9.1', 'grafana')
    HAPROXY = _create_image('registry.redhat.io/rhceph/rhceph-haproxy-rhel10:v9.1', 'haproxy')
    KEEPALIVED = _create_image('registry.redhat.io/rhceph/keepalived-rhel10:v9.1', 'keepalived')
    NVMEOF = _create_image('cp.icr.io/cp/ibm-ceph/nvmeof-rhel9:1.6', 'nvmeof')
    SNMP_GATEWAY = _create_image('registry.redhat.io/rhceph/snmp-notifier-rhel10:v9.1', 'snmp_gateway')
    SAMBA = _create_image('cp.icr.io/cp/ibm-ceph/samba-server-rhel10:v9.1', 'samba')
    SAMBA_METRICS = _create_image('cp.icr.io/cp/ibm-ceph/samba-metrics-rhel10:v9.1', 'samba_metrics')
    NGINX = _create_image('registry.redhat.io/rhel9/nginx-124:latest', 'nginx')
    OAUTH2_PROXY = _create_image('registry.redhat.io/rhceph/oauth2-proxy-rhel10:v7.14.0', 'oauth2_proxy')

    @property
    def image_ref(self) -> str:
        return self.value.image_ref

    @property
    def key(self) -> str:
        return self.value.key

    @property
    def desc(self) -> str:
        return self.value.desc


class NonCephImageServiceTypes(Enum):
    prometheus = 'prometheus'
    loki = 'loki'
    alloy = 'alloy'
    node_exporter = 'node-exporter'
    alertmanager = 'alertmanager'
    grafana = 'grafana'
    promtail = 'promtail'
    nvmeof = 'nvmeof'
    snmp_gateway = 'snmp-gateway'
    elasticsearch = 'elasticsearch'
    jaeger_collector = 'jaeger-collector'
    jaeger_query = 'jaeger-query'
    jaeger_agent = 'jaeger-agent'
    samba = 'smb'
    oauth2_proxy = 'oauth2-proxy'
