"""
IBM Ceph Call Home Agent
Authors:
    Yaarit Hatuka <yhatuka@ibm.com>
    Juan Miguel Olmo Martinez <jolmomar@ibm.com>
"""

from typing import List, Any, Tuple, Dict, Optional, Callable
import time
import json
import requests
import asyncio
import os
from datetime import datetime
import uuid
import glob
import re
import base64
import zstandard as zstd

from mgr_module import (Option, CLIReadCommand, CLIWriteCommand, MgrModule,
                        HandleCommandResult)
# from .dataClasses import ReportHeader, ReportEvent
from .dataDicts import ReportHeader, ReportEvent, ceph_command

# Dict to store operations requested from Call Home Mesh
operations = {}

# Constants for operations types:
from .dataDicts import UPLOAD_SNAP, UPLOAD_FILE, DISABLE_SI_MESSAGES, CONFIRM_RESPONSE, NOT_SUPPORTED

# Constants for operation status
from .dataDicts import OPERATION_STATUS_NEW, OPERATION_STATUS_IN_PROGRESS, \
                       OPERATION_STATUS_COMPLETE, OPERATION_STATUS_ERROR, \
                       OPERATION_STATUS_REQUEST_REJECTED

# Constants for operation status delivery
from .dataDicts import ST_NOT_SENT, ST_SENT

from .config import get_settings

# Constant for store default ceph logs folder
# Diagnostic files are collected in this folder
DIAGS_FOLDER = '/var/log/ceph'

class SendError(Exception):
    pass

# Prometheus API returns all alerts. We want to send only deltas in the alerts
# report - i.e. send a *new* alert that has been fired since the last report
# was sent, and send a “resolved” notification when an alert is removed from
# the prometheus API.
# To do so we keep a list of alerts (“sent_alerts”) we have already sent, and
# use that to create a delta report in generate_alerts_report(). The alert
# report is not sent if there are no deltas.
# `ceph callhome reset alerts` zeros out sent_alerts list and therefore the
# next report will contain the relevant alerts that are fetched from the
# Prometheus API.
sent_alerts = {}

def get_prometheus_url(mgr: Any) -> str:
    """
    Provides the prometheus server URL
    """
    daemon_list = mgr.remote('cephadm', 'list_daemons', service_name='prometheus')
    if daemon_list.exception_str:
        raise Exception(f"Alert report: Error finding the Prometheus instance: {daemon_list.exception_str}")
    if len(daemon_list.result) < 1:
        raise Exception(f"Alert report: Can't find the Prometheus instance")

    d = daemon_list.result[0]
    host = d.ip if d.ip else d.hostname  # ip is of type str
    port = str(d.ports[0]) if d.ports else ""  # ports is a list of ints
    if not (host and port):
        raise Exception(f"Can't get Prometheus IP and/or port from manager")

    return f"http://{host}:{port}/api/v1"

def get_status(mgr: Any) -> dict:
    r, outb, outs = mgr.mon_command({
        'prefix': 'status',
        'format': 'json'
    })
    if r:
        error = f"status command failed: {outs}"
        mgr.log.error(error)
        return {'error': error}
    try:
        status_dict = json.loads(outb)
        status_dict["ceph_version"] = mgr.version
        status_dict["health_detail"] = json.loads(mgr.get('health')['json'])
        status_dict["support"] = get_support_metrics(mgr)
        status_dict["support"]["health_status"] = status_dict["health_detail"]["status"]
        status_dict["support"]["health_summary"] = get_health_summary(status_dict["health_detail"])
        return status_dict
    except Exception as ex:
        mgr.log.exception(str(ex))
        return {'exception': str(ex)}

def get_health_summary(ceph_health: dict) -> str:
    health_summary = ""
    for error_key, error_details in ceph_health["checks"].items():
        msg = "\n".join([item["message"] for item in error_details.get("detail",[])])
        health_summary += f'{error_key}({error_details["severity"]}): {error_details["summary"]["message"]}\n{msg}\n'
    return health_summary

def get_support_metrics(mgr) -> dict:
    """
    Collect cluster metrics needed for Ceph support team tools
    """
    support_metrics = {}
    status_interval_minutes = os.environ.get('CHA_INTERVAL_STATUS_REPORT_SECONDS',
                              mgr.get_module_option('interval_status_report_seconds'))
    try:
        prom_url = get_prometheus_url(mgr)
        query_url = f"{prom_url}/query"
        queries = {
            'total_capacity_bytes': 'sum(ceph_osd_stat_bytes)',
            'total_raw_usage_bytes': 'sum(ceph_osd_stat_bytes_used)',
            'usage_percentage': '(sum(ceph_osd_stat_bytes_used)/sum(ceph_osd_stat_bytes)) * 100',
            'slow_ops_total': 'sum(ceph_daemon_health_metrics{type="SLOW_OPS", ceph_daemon=~"osd.*"})',
            'osds_total_with_slow_ops': 'count(ceph_daemon_health_metrics{type="SLOW_OPS", ceph_daemon=~"osd.*"}>0) or on() vector(0)',
            'pg_total': 'sum(ceph_pg_total)',
            'pg_active': 'sum(ceph_pg_active)',
            'pg_clean': 'sum(ceph_pg_clean)',
            'pg_degraded': 'sum(ceph_pg_degraded)',
            'pg_unknown': 'sum(ceph_pg_unknown)',
            'pg_down': 'sum(ceph_pg_down)',
            'pg_scrubbing': 'sum(ceph_pg_scrubbing)',
            'pg_deep_scrubbing': 'sum(ceph_pg_deep)',
            'network_receive_errors': f'avg(increase(node_network_receive_errs_total{{device!="lo"}}[{status_interval_minutes}m]))',
            'network_send_errors': f'avg(increase(node_network_transmit_errs_total{{device!="lo"}}[{status_interval_minutes}m]))',
            'network_receive_packet_drops': f'avg(increase(node_network_receive_drop_total{{device!="lo"}}[{status_interval_minutes}m]))',
            'network_transmit_packet_drops': f'avg(increase(node_network_transmit_drop_total{{device!="lo"}}[{status_interval_minutes}m]))',
            'inconsistent_mtu': 'sum(node_network_mtu_bytes * (node_network_up{device!="lo"} > 0) ==  scalar(max by (device) (node_network_mtu_bytes * (node_network_up{device!="lo"} > 0)) !=  quantile by (device) (.5, node_network_mtu_bytes * (node_network_up{device!="lo"} > 0))  )or node_network_mtu_bytes * (node_network_up{device!="lo"} > 0) ==  scalar(min by (device) (node_network_mtu_bytes * (node_network_up{device!="lo"} > 0)) !=  quantile by (device) (.5, node_network_mtu_bytes * (node_network_up{device!="lo"} > 0))) or vector(0))',
            'pool_number': 'count(ceph_pool_bytes_used)',
            'raw_capacity_bytes': 'sum(ceph_osd_stat_bytes)',
            'raw_capacity_consumed_bytes': 'sum(ceph_pool_bytes_used)',
            'logical_stored_bytes': 'sum(ceph_pool_stored)',
            'pool_growth_bytes': f'sum(delta(ceph_pool_stored[{status_interval_minutes}m]))',
            'pool_bandwidth_bytes': f'sum(rate(ceph_pool_rd_bytes[{status_interval_minutes}m]) + rate(ceph_pool_wr_bytes[{status_interval_minutes}m]))',
            'pg_per_osd_ratio':'(avg(ceph_osd_numpg)/sum(ceph_pg_total))*100',
            'monitors_number': 'count(ceph_mon_metadata)',
            'monitors_not_in_quorum_number': 'count(ceph_mon_quorum_status!=1) or on() vector(0)',
            'clock_skews_number': 'ceph_health_detail{name="MON_CLOCK_SKEW"} or on() vector(0)',
        }

        t1 = time.time()
        for k,q in queries.items():
            data = exec_prometheus_query(query_url, q)
            try:
                support_metrics[k] = float(data['data']['result'][0]['value'][1])
            except Exception as ex:
                 mgr.log.error(f"Error reading status metric for support <{k}>: {ex} - {data}")
        total_time = round((time.time() - t1) * 1000, 2)
        support_metrics['time_to_get_support_data_ms'] = total_time
        mgr.log.info(f"Time to get support data for status report: {total_time} ms")
    except Exception as ex:
        mgr.log.error(f"Error collecting support data for status report: {ex}")

    return support_metrics

def exec_prometheus_query(query_url: str, prom_query: str) -> dict:
    """
    Execute a Prometheus query and returns the result as dict
    """
    result = {}
    r = None
    try:
        r = requests.get(query_url, params={'query': prom_query})
        result = json.loads(r.text)
        r.raise_for_status()
    except Exception as ex:
        raise Exception(f"Error executing Prometheus query: {ex}-{result}")
    return result

def get_prometheus_status(prometheus_url:str) -> dict:
    """Get information about prometheus server status"""

    result = {}
    r = None
    try:
        r = requests.get(f"{prometheus_url}/targets")
        r.raise_for_status()
        result = json.loads(r.text)
    except Exception as ex:
        raise Exception(f"Error trying to get Prometheus status: {ex}")
    return result

def inventory_get_hardware_status(mgr: Any) -> dict:
    try:
        hw_status = mgr.remote('orchestrator', 'node_proxy_summary')
        if hw_status.exception_str:
            raise Exception(hw_status.exception_str)
        return hw_status.result
    except Exception as e:
        mgr.log.exception(str(e))
        return {'error': str(e)}

def inventory(mgr: Any) -> dict:
    """
    Produce the content for the inventory report

    Returns a dict with a json structure with the ceph cluster inventory information
    """
    inventory = {}
    inventory["crush_map"] = mgr.get("osd_map_crush")
    inventory["devices"] = mgr.get("devices")
    inventory["df"] = mgr.get("df")
    inventory["fs_map"] = mgr.get("fs_map")
    inventory["hosts"] = mgr.list_servers()
    inventory["manager_map"] = mgr.get("mgr_map")
    inventory["mon_map"] = mgr.get("mon_map")
    inventory["osd_map"] = mgr.get("osd_map")
    inventory["osd_metadata"] = mgr.get("osd_metadata")
    inventory["osd_tree"] = mgr.get("osd_map_tree")
    inventory["pg_summary"] = mgr.get("pg_summary")
    inventory["service_map"] = mgr.get("service_map")
    inventory["status"] = get_status(mgr)
    inventory["hardware_status"] = inventory_get_hardware_status(mgr)

    return {'inventory': inventory}

def performance(mgr: Any) -> dict:
    """
    Produce the content for the performance report

    Returns a dict with a json structure with the ceph cluster performance information
    """

    performance_metrics = {}
    perf_interval_minutes = int(os.environ.get('CHA_INTERVAL_PERFORMANCE_REPORT_SECONDS',
                              mgr.get_module_option('interval_performance_report_seconds'))/60)

    try:
        queries = {
            "ceph_osd_op_r_avg"        : {"query": f"sum(avg_over_time(ceph_osd_op_r[{perf_interval_minutes}m]))/count(ceph_osd_metadata)",
                                          "help" : f"Average of read operations per second and per OSD in the cluster in the last {perf_interval_minutes} minutes"},
            "ceph_osd_op_r_min"        : {"query": f"min(min_over_time(ceph_osd_op_r[{perf_interval_minutes}m]))",
                                          "help" : f"Minimum read operations per second in the cluster in the last {perf_interval_minutes} minutes"},
            "ceph_osd_op_r_max"        : {"query": f"max(max_over_time(ceph_osd_op_r[{perf_interval_minutes}m]))",
                                           "help": f"Maximum of write operations per second in the cluster in the last {perf_interval_minutes} minutes"},
            "ceph_osd_r_out_bytes_avg" : {"query": f"sum(avg_over_time(ceph_osd_op_r_out_bytes[{perf_interval_minutes}m]))/count(ceph_osd_metadata)",
                                          "help" : f"Average of cluster output bytes(reads) and per OSD in the last {perf_interval_minutes} minutes"},
            "ceph_osd_r_out_bytes_min" : {"query": f"min(min_over_time(ceph_osd_op_r_out_bytes[{perf_interval_minutes}m]))",
                                          "help" : f"Minimum of cluster output bytes(reads) in the last {perf_interval_minutes} minutes"},
            "ceph_osd_r_out_bytes_max" : {"query": f"max(max_over_time(ceph_osd_op_r_out_bytes[{perf_interval_minutes}m]))",
                                          "help" : f"Maximum of cluster output bytes(reads) in the last {perf_interval_minutes} minutes"},
            "ceph_osd_op_w_avg"        : {"query": f"sum(avg_over_time(ceph_osd_op_w[{perf_interval_minutes}m]))/count(ceph_osd_metadata)",
                                          "help" : f"Average of cluster input operations per second(writes) in the last {perf_interval_minutes} minutes"},
            "ceph_osd_op_w_min"        : {"query": f"min(min_over_time(ceph_osd_op_w[{perf_interval_minutes}m]))",
                                          "help" : f"Mimimum of cluster input operations per second(writes) in the last {perf_interval_minutes} minutes"},
            "ceph_osd_op_w_max"        : {"query": f"max(max_over_time(ceph_osd_op_w[{perf_interval_minutes}m]))",
                                          "help" : f"Maximum of cluster input operations per second(writes) in the last {perf_interval_minutes} minutes"},
            "ceph_osd_op_w_in_bytes_avg"       : {"query": f"sum(avg_over_time(ceph_osd_op_w_in_bytes[{perf_interval_minutes}m]))/count(ceph_osd_metadata)",
                                                  "help" : f"Average of cluster input bytes(writes) in the last {perf_interval_minutes} minutes"},
            "ceph_osd_op_w_in_bytes_min"       : {"query": f"min(min_over_time(ceph_osd_op_w_in_bytes[{perf_interval_minutes}m]))",
                                                  "help" : f"Minimum of cluster input bytes(writes) in the last {perf_interval_minutes} minutes"},
            "ceph_osd_op_w_in_bytes_max"       : {"query": f"max(max_over_time(ceph_osd_op_w_in_bytes[{perf_interval_minutes}m]))",
                                                  "help" : f"Maximum of cluster input bytes(writes) in the last {perf_interval_minutes} minutes"},
            "ceph_osd_op_read_latency_avg_ms"  : {"query": f"avg(rate(ceph_osd_op_r_latency_sum[{perf_interval_minutes}m]) or vector(0) / on (ceph_daemon) rate(ceph_osd_op_r_latency_count[{perf_interval_minutes}m]) * 1000)",
                                                  "help" : f"Average of cluster output latency(reads) in milliseconds in the last {perf_interval_minutes} minutes"},
            "ceph_osd_op_read_latency_max_ms"  : {"query": f"max(rate(ceph_osd_op_r_latency_sum[{perf_interval_minutes}m]) or vector(0) / on (ceph_daemon) rate(ceph_osd_op_r_latency_count[{perf_interval_minutes}m]) * 1000)",
                                                  "help" : f"Maximum of cluster output latency(reads) in milliseconds in the last {perf_interval_minutes} minutes"},
            "ceph_osd_op_read_latency_min_ms"  : {"query": f"min(rate(ceph_osd_op_r_latency_sum[{perf_interval_minutes}m]) or vector(0) / on (ceph_daemon) rate(ceph_osd_op_r_latency_count[{perf_interval_minutes}m]) * 1000)",
                                                  "help" : f"Minimum of cluster output latency(reads) in milliseconds  in the last {perf_interval_minutes} minutes"},
            "ceph_osd_op_write_latency_avg_ms" : {"query": f"avg(rate(ceph_osd_op_w_latency_sum[{perf_interval_minutes}m]) or vector(0) / on (ceph_daemon) rate(ceph_osd_op_w_latency_count[{perf_interval_minutes}m]) * 1000)",
                                                  "help" : f"Average of cluster input latency(writes) in milliseconds in the last {perf_interval_minutes} minutes"},
            "ceph_osd_op_write_latency_max_ms" : {"query": f"max(rate(ceph_osd_op_w_latency_sum[{perf_interval_minutes}m]) or vector(0) / on (ceph_daemon) rate(ceph_osd_op_w_latency_count[{perf_interval_minutes}m]) * 1000)",
                                                  "help" : f"Maximum of cluster input latency(writes) in milliseconds  in the last {perf_interval_minutes} minutes"},
            "ceph_osd_op_write_latency_min_ms" : {"query": f"min(rate(ceph_osd_op_w_latency_sum[{perf_interval_minutes}m]) or vector(0) / on (ceph_daemon) rate(ceph_osd_op_w_latency_count[{perf_interval_minutes}m]) * 1000)",
                                                  "help" : f"Maximum of cluster input latency(writes) in milliseconds in the last {perf_interval_minutes} minutes"},
            "ceph_physical_device_latency_reads_ms"    : {"query": 'node_disk_read_time_seconds_total / node_disk_reads_completed_total * on (instance, device) group_left(ceph_daemon) label_replace(ceph_disk_occupation_human, "device", "$1", "device", "/dev/(.*)") * 1000',
                                                        "help" : "Read latency in milliseconds per physical device used by ceph OSD daemons"},
            "ceph_physical_device_latency_writes_ms"   : {"query": 'node_disk_write_time_seconds_total / node_disk_writes_completed_total * on (instance, device) group_left(ceph_daemon) label_replace(ceph_disk_occupation_human, "device", "$1", "device", "/dev/(.*)") * 1000',
                                                        "help" : "Write latency in milliseconds per physical device used by ceph OSD daemons"},
            "ceph_physical_device_read_iops"           : {"query": 'node_disk_reads_completed_total * on (instance, device) group_left(ceph_daemon)  label_replace(ceph_disk_occupation_human, "device", "$1", "device", "/dev/(.*)")',
                                                        "help" : "Read operations per second per physical device used by ceph OSD daemons"},
            "ceph_physical_device_write_iops"          : {"query": 'node_disk_writes_completed_total * on (instance, device) group_left(ceph_daemon)  label_replace(ceph_disk_occupation_human, "device", "$1", "device", "/dev/(.*)")',
                                                        "help" : "Write operations per second per physical device used by ceph OSD daemons"},
            "ceph_physical_device_read_bytes"          : {"query": 'node_disk_read_bytes_total * on (instance, device) group_left(ceph_daemon)  label_replace(ceph_disk_occupation_human, "device", "$1", "device", "/dev/(.*)")',
                                                        "help" : "Read bytes per physical device used by ceph OSD daemons in the last"},
            "ceph_physical_device_written_bytes"       : {"query": 'node_disk_written_bytes_total * on (instance, device) group_left(ceph_daemon)  label_replace(ceph_disk_occupation_human, "device", "$1", "device", "/dev/(.*)")',
                                                        "help" : "Write bytes per physical device used by ceph OSD daemons in the last"},
            "ceph_physical_device_utilization_seconds" : {"query": '(node_disk_io_time_seconds_total * on (instance, device) group_left(ceph_daemon)  label_replace(ceph_disk_occupation_human, "device", "$1", "device", "/dev/(.*)")) * on (ceph_daemon) group_left(device_class) ceph_osd_metadata',
                                                          "help":"Seconds total of Input/Output operations per physical device used by ceph OSD daemons"},
            "ceph_pool_objects"     : {"query": "ceph_pool_objects * on(pool_id) group_left(instance, name) ceph_pool_metadata",
                                       "help": "Number of Ceph pool objects per Ceph pool"},
            "ceph_pool_write_iops"  : {"query": f"rate(ceph_pool_wr[{perf_interval_minutes}m]) * on(pool_id) group_left(instance, name) ceph_pool_metadata",
                                       "help" : "Per-second average rate of increase of write operations per Ceph pool during the last {perf_interval_minutes} minutes"},
            "ceph_pool_read_iops"   : {"query": f"rate(ceph_pool_rd[{perf_interval_minutes}m]) * on(pool_id) group_left(instance, name) ceph_pool_metadata",
                                       "help" : f"Per-second average rate of increase of read operations per Ceph pool during the last {perf_interval_minutes} minutes"},
            "ceph_pool_write_bytes" : {"query": f"rate(ceph_pool_wr_bytes[{perf_interval_minutes}m]) * on(pool_id) group_left(instance, name) ceph_pool_metadata",
                                       "help" : f"Per-second average rate of increase of written bytes per Ceph pool during the last {perf_interval_minutes} minutes"},
            "ceph_pool_read_bytes"  : {"query": f"rate(ceph_pool_rd_bytes[{perf_interval_minutes}m]) * on(pool_id) group_left(instance, name) ceph_pool_metadata",
                                       "help" : f"Per-second average rate of increase of read bytes per Ceph pool during the last {perf_interval_minutes} minutes"},
            "ceph_pg_activating"    : {"query": f"rate(ceph_pg_activating[{perf_interval_minutes}m]) * on(pool_id) group_left(instance, name) ceph_pool_metadata",
                                       "help" : f"Per-second average rate of Placement Groups activated per Ceph pool during the last {perf_interval_minutes} minutes"},
            "ceph_pg_backfilling"   : {"query": f"rate(ceph_pg_backfilling[{perf_interval_minutes}m]) * on(pool_id) group_left(instance, name) ceph_pool_metadata",
                                       "help" : f"Per-second average rate of Placement Groups backfilled per Ceph pool during the last {perf_interval_minutes} minutes"},
            "ceph_pg_creating"      : {"query": f"rate(ceph_pg_creating[{perf_interval_minutes}m]) * on(pool_id) group_left(instance, name) ceph_pool_metadata",
                                       "help" : f"Per-second average rate of Placement Groups created per Ceph pool during the last {perf_interval_minutes} minutes"},
            "ceph_pg_recovering"    : {"query": f"rate(ceph_pg_recovering[{perf_interval_minutes}m]) * on(pool_id) group_left(instance, name) ceph_pool_metadata",
                                       "help" : f"Per-second average rate of Placement Groups recovered per Ceph pool during the last {perf_interval_minutes} minutes"},
            "ceph_pg_deep"          : {"query": f"rate(ceph_pg_deep[{perf_interval_minutes}m]) * on(pool_id) group_left(instance, name) ceph_pool_metadata",
                                       "help":  f"Per-second average rate of Placement Groups deep scrubbed per Ceph pool during the last {perf_interval_minutes} minutes"},
            "ceph_rgw_avg_get_latency_ms" : {"query": f'(rate(ceph_rgw_get_initial_lat_sum[{perf_interval_minutes}m]) or vector(0)) * 1000 / rate(ceph_rgw_get_initial_lat_count[{perf_interval_minutes}m]) * on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata',
                                             "help" : f"Average latency in milliseconds for GET operations per Ceph RGW daemon during the last {perf_interval_minutes} minutes"},
            "ceph_rgw_avg_put_latency_ms" : {"query": f"(rate(ceph_rgw_put_initial_lat_sum[{perf_interval_minutes}m]) or vector(0)) * 1000 / rate(ceph_rgw_put_initial_lat_count[{perf_interval_minutes}m]) * on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata",
                                             "help" : f"Average latency in milliseconds for PUT operations per Ceph RGW daemon during the last {perf_interval_minutes} minutes"},
            "ceph_rgw_requests_per_second": {"query": f'sum by (rgw_host) (label_replace(rate(ceph_rgw_req[{perf_interval_minutes}m]) * on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata, "rgw_host", "$1", "ceph_daemon", "rgw.(.*)"))',
                                             "help" : f"Request operations per second per Ceph RGW daemon during the last {perf_interval_minutes} minutes"},
            "ceph_rgw_get_size_bytes" :     {"query": f'label_replace(sum by (instance_id) (rate(ceph_rgw_get_b[{perf_interval_minutes}m])) * on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata, "rgw_host", "$1", "ceph_daemon", "rgw.(.*)")',
                                             "help" : f"Per-second average rate of GET operations size per Ceph RGW daemon during the last {perf_interval_minutes} minutes"},
            "ceph_rgw_put_size_bytes" :     {"query": f'label_replace(sum by (instance_id) (rate(ceph_rgw_put_b[{perf_interval_minutes}m])) * on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata, "rgw_host", "$1", "ceph_daemon", "rgw.(.*)")',
                                             "help" : f"Per-second average rate of PUT operations size per Ceph RGW daemon during the last {perf_interval_minutes} minutes"},
            "ceph_mds_read_requests_per_second"   : {"query": f'rate(ceph_objecter_op_r{{ceph_daemon=~"mds.*"}}[{perf_interval_minutes}m])',
                                                     "help" : f"Per-second average rate of read requests per Ceph MDS daemon during the last {perf_interval_minutes} minutes"},
            "ceph_mds_write_requests_per_second"  : {"query": f'rate(ceph_objecter_op_w{{ceph_daemon=~"mds.*"}}[{perf_interval_minutes}m])',
                                                     "help" : f"Per-second average rate of write requests per Ceph MDS daemon during the last {perf_interval_minutes} minutes"},
            "ceph_mds_client_requests_per_second" : {"query": f'rate(ceph_mds_server_handle_client_request[{perf_interval_minutes}m])',
                                                     "help" : f"Per-second average rate of client requests per Ceph MDS daemon during the last {perf_interval_minutes} minutes"},
            "ceph_mds_reply_latency_avg_ms" : {"query": f'avg(rate(ceph_mds_reply_latency_sum[{perf_interval_minutes}m]) or vector(0) / on (ceph_daemon) rate(ceph_mds_reply_latency_count[{perf_interval_minutes}m]) * 1000)',
                                               "help" : f"Average of the per-second average rate of reply latency(seconds) per Ceph MDS daemon during the last {perf_interval_minutes} minutes"},
            "ceph_mds_reply_latency_max_ms" : {"query": f'max(rate(ceph_mds_reply_latency_sum[{perf_interval_minutes}m]) or vector(0) / on (ceph_daemon) rate(ceph_mds_reply_latency_count[{perf_interval_minutes}m]) * 1000)',
                                               "help" : f"Maximum of the per-second average rate of reply latency(seconds) per Ceph MDS daemon during the last {perf_interval_minutes} minutes"},
            "ceph_mds_reply_latency_min_ms" : {"query": f'min(rate(ceph_mds_reply_latency_sum[{perf_interval_minutes}m]) or vector(0) / on (ceph_daemon) rate(ceph_mds_reply_latency_count[{perf_interval_minutes}m]) * 1000)',
                                               "help" : f"Minimum of the per-second average rate of reply latency(seconds) per Ceph MDS daemon during the last {perf_interval_minutes} minutes"},
            "hw_cpu_busy"                          : {"query": f"1- rate(node_cpu_seconds_total{{mode='idle'}}[{perf_interval_minutes}m])",
                                                      "help" : f"Percentaje of CPU utilization per core during the last {perf_interval_minutes} minutes"},
            "hw_ram_utilization"                   : {"query": f'(node_memory_MemTotal_bytes -(node_memory_MemFree_bytes + node_memory_Cached_bytes + node_memory_Buffers_bytes + node_memory_Slab_bytes))/node_memory_MemTotal_bytes',
                                                      "help" : "RAM utilization"},
            "hw_node_physical_disk_read_ops_rate"  : {"query": f"rate(node_disk_reads_completed_total[{perf_interval_minutes}m])",
                                                      "help" : f"Per-second average rate of read operations per physical storage device in the host during the last {perf_interval_minutes} minutes"},
            "hw_node_physical_disk_write_ops_rate" : {"query": f"rate(node_disk_writes_completed_total[{perf_interval_minutes}m])",
                                                      "help" : f"Per-second average rate of write operations per physical storage device in the host during the last {perf_interval_minutes} minutes"},
            "hw_disk_utilization_rate"             : {"query": f"rate(node_disk_io_time_seconds_total[{perf_interval_minutes}m])",
                                                      "help" : f"Per-second average rate of input/output operations time(seconds) per physical storage device in the host during the last {perf_interval_minutes} minutes"},
            "hw_network_bandwidth_receive_load_bytes" : {"query": f"rate(node_network_receive_bytes_total[{perf_interval_minutes}m])",
                                                         "help" : f"Per-second average rate of received bytes per network card in the host during the last {perf_interval_minutes} minutes"},
            "hw_network_bandwidth_transmit_load_bytes": {"query": f"rate(node_network_transmit_bytes_total[{perf_interval_minutes}m])",
                                                         "help" : f"Per-second average rate of transmitted bytes per network card in the host during the last {perf_interval_minutes} minutes"},
            "ceph_nvmeof_gateway_total"                        : {"query": "count by(group) (ceph_nvmeof_gateway_info) or vector(0)",
                                                                  "help" : "Number of Ceph NVMe-oF daemons or gatways running"},
            "ceph_nvmeof_subsystem_total"                      : {"query": "count by(group) (count by(nqn,group) (ceph_nvmeof_subsystem_metadata))",
                                                                  "help" : "Number of Ceph NVMe-oF subsystems running"},
            "ceph_nvmeof_reactor_total"                        : {"query": 'max by(group) (max by(instance) (count by(instance) (ceph_nvmeof_reactor_seconds_total{mode="busy"})) * on(instance) group_right ceph_nvmeof_gateway_info)',
                                                                  "help" : "Number of reactors per gateway"},
            "ceph_nvmeof_gateway_reactor_cpu_seconds_total"    : {"query": f'max by(group) (avg by(instance) (rate(ceph_nvmeof_reactor_seconds_total{{mode="busy"}}[{perf_interval_minutes}m])) * on(instance) group_right ceph_nvmeof_gateway_info)',
                                                                   "help" : "Highest gateway CPU load"},
            "ceph_nvmeof_namespaces_total"                     : {"query": "max by(group) (count by(instance) (count by(bdev_name,instance) (ceph_nvmeof_bdev_metadata )) * on(instance) group_right ceph_nvmeof_gateway_info)",
                                                                  "help" : "Total number of namespaces"},
            "ceph_nvmeof_capacity_exported_bytes_total"        : {"query": "topk(1,sum by(instance) (ceph_nvmeof_bdev_capacity_bytes)) * on(instance) group_left(group) ceph_nvmeof_gateway_info",
                                                                  "help" : "Ceph NVMe-oF total capacity exposed"},
            "ceph_nvmeof_clients_connected_total "             : {"query": "count by(instance) (sum by(instance,host_nqn) (ceph_nvmeof_host_connection_state == 1)) * on(instance) group_left(group) ceph_nvmeof_gateway_info",
                                                                  "help" : "Number of clients connected to Ceph NVMe-oF"},
            "ceph_nvmeof_gateway_iops_total "                  : {"query": f"sum by(instance) (rate(ceph_nvmeof_bdev_reads_completed_total[{perf_interval_minutes}m]) + rate(ceph_nvmeof_bdev_writes_completed_total[{perf_interval_minutes}m])) * on(instance) group_left(group) ceph_nvmeof_gateway_info",
                                                                  "help" : "IOPS per Ceph NVMe-oF gateway"},
            "ceph_nvmeof_subsystem_iops_total"                 : {"query": f"sum by(group,nqn) (((rate(ceph_nvmeof_bdev_reads_completed_total[{perf_interval_minutes}m]) + rate(ceph_nvmeof_bdev_writes_completed_total[{perf_interval_minutes}m])) * on(instance,bdev_name) group_right ceph_nvmeof_subsystem_namespace_metadata) * on(instance) group_left(group) ceph_nvmeof_gateway_info)",
                                                                  "help" : "IOPS per Ceph NVMe-oF subsystem"},
            "ceph_nvmeof_gateway_throughput_bytes_total"       : {"query": f"sum by(instance) (rate(ceph_nvmeof_bdev_read_bytes_total[{perf_interval_minutes}m]) + rate(ceph_nvmeof_bdev_written_bytes_total[{perf_interval_minutes}m])) * on(instance) group_left(group) ceph_nvmeof_gateway_info",
                                                                  "help" : "Throughput per Ceph NVMe-oF gateway"},
            "ceph_nvmeof_subsystem_throughput_bytes_total"     : {"query": f"sum by(group,nqn) (((rate(ceph_nvmeof_bdev_read_bytes_total[{perf_interval_minutes}m]) + rate(ceph_nvmeof_bdev_written_bytes_total[{perf_interval_minutes}m])) * on(instance,bdev_name) group_right ceph_nvmeof_subsystem_namespace_metadata) * on(instance) group_left(group) ceph_nvmeof_gateway_info)",
                                                                  "help" : "Throughput per Ceph NVMe-oF subsystem"},
            "ceph_nvmeof_gateway_read_avg_latency_seconds"     : {"query": f"avg by(group,instance) (((rate(ceph_nvmeof_bdev_read_seconds_total[{perf_interval_minutes}m]) / rate(ceph_nvmeof_bdev_reads_completed_total[{perf_interval_minutes}m])) > 0) * on(instance) group_left(group) ceph_nvmeof_gateway_info)",
                                                                  "help" : "Read latency average in seconds per Ceph NVMe-oF gateway"},
            "ceph_nvmeof_gateway_write_avg_latency_seconds "   : {"query": f"avg by(group,instance) (((rate(ceph_nvmeof_bdev_write_seconds_total[{perf_interval_minutes}m]) / rate(ceph_nvmeof_bdev_writes_completed_total[{perf_interval_minutes}m])) > 0) * on(instance) group_left(group) ceph_nvmeof_gateway_info)",
                                                                  "help":  "Write average in seconds per Ceph NVMe-oF gateway"},
            "ceph_nvmeof_gateway_read_p95_latency_seconds"     : {"query": f"quantile by(group,instance) (.95,((rate(ceph_nvmeof_bdev_read_seconds_total[{perf_interval_minutes}m]) / (rate(ceph_nvmeof_bdev_reads_completed_total[{perf_interval_minutes}m]) >0)) * on(instance) group_left(group) ceph_nvmeof_gateway_info))",
                                                                  "help":  "Read latency for 95{%} of the Ceph NVMe-oF gateways"},
            "ceph_nvmeof_gateway_write_p95_latency_seconds"    : {"query": f"quantile by(group,instance) (.95,((rate(ceph_nvmeof_bdev_write_seconds_total[{perf_interval_minutes}m]) / (rate(ceph_nvmeof_bdev_writes_completed_total[{perf_interval_minutes}m]) >0)) * on(instance) group_left(group) ceph_nvmeof_gateway_info))",
                                                                  "help":  "Write latency for 95{%} of the Ceph NVMe-oF gateways"}
        }

        t1 = time.time()

        status = ""
        prometheus_url = get_prometheus_url(mgr)
        query_url = f"{prometheus_url}/query"

        # Metrics retrieval
        metrics_errors = False
        for k,q in queries.items():
            try:
                data = exec_prometheus_query(query_url, q["query"])
                # remove single metric timestamps
                try:
                    for metric in data['data']['result']:
                        metric["value"] = metric["value"][1:]
                except Exception:
                    pass
                performance_metrics[k] = {"help": q["help"],
                                          "result": data['data']['result']}
            except Exception as ex:
                msg = f"Error reading performance metric <{k}>: {ex}"
                mgr.log.error(msg)
                metrics_errors = True
                continue

        if metrics_errors:
            status = "Error getting metrics from Prometheus. Active Ceph Manager log contains details\n"

        # Prometheus server health
        prometheus_status = get_prometheus_status(prometheus_url)
        targets_down = list(filter(lambda x: x['health'] != 'up', prometheus_status['data']['activeTargets']))
        if targets_down:
            status += f"Error(scrape targets not up): Not able to retrieve metrics from {targets_down} target/s. Review Prometheus server status\n"

        # Ceph status
        performance_metrics["ceph_version"] = mgr.version
        performance_metrics["ceph_health_detail"] = json.loads(mgr.get('health')['json'])

        total_time = round((time.time() - t1) * 1000, 2)
        performance_metrics['time_to_get_performance_metrics_ms'] = total_time
        mgr.log.info(f"Time to get performance metrics: {total_time} ms")
        performance_metrics['timestamp'] = t1
        performance_metrics['human_timestamp'] = datetime.fromtimestamp(t1).strftime('%Y-%m-%d %H:%M:%S')
    except Exception as ex:
        msg = f"Error collecting performance metrics: {ex}"
        mgr.log.error(msg)
        status += msg + '\n'

    # Performance report status
    if status == "":
        performance_metrics["status"] = "OK"
    else:
        performance_metrics["status"] = status

    # performance data compressed and serialized to a JSON string
    performance_json = json.dumps(performance_metrics)
    cctx = zstd.ZstdCompressor()
    compressed_perfo = cctx.compress(performance_json.encode('utf-8'))

    compressed_base64_perfo = base64.b64encode(compressed_perfo).decode('utf-8')


    return {"perfstats": {
                        "file_stamp": performance_metrics['human_timestamp'],
                        "file_stamp_ms": int(t1 * 1000),
                        "local_file_stamp": performance_metrics['human_timestamp'],
                        "nd_stats": compressed_base64_perfo,
                        "ng_stats": "",
                        "nm_stats": "",
                        "nn_stats": "",
                        "nv_stats": "",
                        "node_number": 1,     # because IBM Call Home reqs.
                        "nodes_in_cluster": 1 # because IBM Call Home reqs.
                        }
            }

def status(mgr: Any) -> dict:
    """
    Produce the content for the status report

    Returns a dict with a json structure with the ceph cluster health information
    """
    return {'status': get_status(mgr)}

def last_contact(mgr: Any) -> dict:
    """
    Produce the content for the last_contact report

    Returns a dict with just the timestamp of the last contact with the cluster
    """
    return {'last_contact': format(int(time.time()))}

def get_operation(key) -> dict:
    """
    Retuns the operation data.
    Used for keep compatibility with the Report class API
    """
    return operations[key]

def collect_diagnostic_commands(mgr: Any, operation_key: str) -> str:
    """
    Collect information from the cluster

        ceph status
        ceph health detail
        ceph osd tree
        ceph report
        ceph osd dump
        ceph df

    """
    mgr.log.info(f"Operations ({operation_key}): Collecting diagnostics commands")
    output = ""
    output += "\nceph status\n" + ceph_command(mgr=mgr, srv_type='mon', prefix='status')
    output += "\n'ceph health detail\n" + ceph_command(mgr=mgr, srv_type='mon', prefix='health', detail='detail')
    output += "\nceph osd tree\n" + ceph_command(mgr=mgr, srv_type='mon', prefix='osd tree')
    output += "\nceph report\n" + ceph_command(mgr=mgr, srv_type='mon', prefix='report')
    output += "\nceph osd dump\n" + ceph_command(mgr=mgr, srv_type='mon', prefix='osd dump')
    output += "\nceph df detail\n" + ceph_command(mgr=mgr, srv_type='mon', prefix='df', detail='detail')

    mgr.log.info(f"Operations ({operation_key}): diagnostics commands collected")

    try:
        cmds_file_prefix = 'ceph_commands_case'
        # Remove previous commands files
        for file in glob.glob(f'{DIAGS_FOLDER}/{cmds_file_prefix}*'):
            os.remove(file)
        timestamp_sos_file = int(time.time() * 1000)
        try:
            case_id = operations[operation_key]['pmr']
        except KeyError:
            case_id = "unknown"
        file_name = f'{cmds_file_prefix}_{case_id}_{timestamp_sos_file}.txt'
        with open(f'{DIAGS_FOLDER}/{file_name}', 'w') as commands_file:
            commands_file.write(output)
        mgr.log.info(f"Operations ({operation_key}): diagnostics commands stored in file {file_name}")
    except Exception as ex:
        raise Exception(f"Operations ({operation_key}): Error trying to save the commands file for diagnostics: {ex}")

    return file_name
def get_best_collect_node(mgr: Any) -> Tuple[str, str]:
    """
    Select the best monitor node where to run a sos report command
    retuns the best monitor node and the active manager
    """
    nodes = {}
    active_manager = ""
    best_monitor = ""

    # We add all the monitors
    monitors = mgr.remote('cephadm', 'list_daemons', service_name='mon')
    if monitors.exception_str:
        raise Exception(monitors.exception_str)

    for daemon in monitors.result:
        nodes[daemon.hostname] = 1

    # lets add one point to a monitor if it is a cephadm admin node
    cluster_nodes = mgr.remote('cephadm', 'get_hosts')
    if cluster_nodes.exception_str:
        raise Exception(cluster_nodes.exception_str)

    for host in cluster_nodes.result:
        if '_admin' in host.labels:
            try:
                nodes[host.hostname] += 1
                break
            except KeyError:
                pass

    # get the active mgr.
    managers = mgr.remote('cephadm', 'list_daemons', service_name='mgr')
    if managers.exception_str:
        raise Exception(monitors.exception_str)

    for daemon in managers.result:
        if daemon.is_active:
            active_manager = daemon.hostname
            try:
                nodes[daemon.hostname] += 1
            except KeyError:
                pass

    # get the winner monitor
    best_monitor = max(nodes, key=nodes.get)

    return best_monitor, active_manager

def collect_sos_report(mgr: Any, operation_key: str) -> str:
    """
    SOS report gathered from a Ceph Monitor node
    Best node to execute the sos command is
    1. Monitor + admin node + active mgr
    2. Monitor + admin node
    3. monitor
    """

    # Remove previous sos report files:
    for file in glob.glob(f'{DIAGS_FOLDER}/sosreport_case_*'):
        os.remove(file)

    # Get the best monitor node to execute the sos report
    best_mon, active_mgr = get_best_collect_node(mgr)
    mgr_target = ""
    if best_mon != active_mgr and active_mgr:
        mgr_target = f"--mgr-target {active_mgr}"
    support_case = operations[operation_key]["pmr"]
    mgr.log.info(f"Operations ({operation_key}): selected host for sos command is {best_mon}, active manager is {active_mgr}")

    # Execute the sos report command
    sos_cmd_execution = mgr.remote('cephadm', 'sos',
                                      hostname = best_mon,
                                      sos_params = f'{mgr_target} report --batch --quiet --case-id {support_case}')
    mgr.log.info(f"Operations ({operation_key}): sos command executed succesfully")
    if sos_cmd_execution.exception_str:
        raise Exception(f"Error trying to get the sos report files for diagnostics(error_code): {sos_cmd_execution.exception_str}")

    # output is like:
    # ['New sos report files can be found in /var/log/ceph/<fsid>/sosreport_case_124_1706548742636_*']
    pattern = r'sosreport_case_\S+'
    matches = re.findall(pattern, sos_cmd_execution.result[0])
    if matches:
        mgr.log.info(f"Operations ({operation_key}): sos command files pattern is: {matches[0]}")
        result = matches[0]
    else:
        mgr.log.error(f"Operations ({operation_key}): sos report files pattern not found in: {sos_cmd_execution.result[0]}")
        result = ""

    # If there is any issue executing the command, the output will be like:
    # ['Issue executing <['sos', 'report', '--batch', '--quiet', '--case-id', 'TS015034298', '-p', 'container']>: 0:[plugin:ceph_mon] Failed to find ceph version, command collection will be limited
    #
    # New sos report files can be found in /var/log/ceph/<fsid>/sosreport_case_TS015034298_1709809018376_*']
    # in this case, we leave a warning in the log about the issue
    pattern = r'^Issue executing.*'
    matches = re.findall(pattern, sos_cmd_execution.result[0])
    if matches:
         mgr.log.warn(f"Operations ({operation_key}): review sos command execution in {best_mon}: {matches[0]}")

    return result


def notProcessed(item: dict) -> bool:
    """
    Determines if a received <inbound request> containing a si_requestid
    has been already processed
    """
    not_processed = True

    si_requestid = item.get('options', {}).get('si_requestid', '')
    if si_requestid:
        not_processed = operations.get(si_requestid, "") == ""

    return not_processed

def add_operation(item: dict, l1_cooling_window_seconds: int,
                  l2_cooling_window_seconds: int, event_id: str = '') -> str:
    """
    Add an operation coming from an inbound request to the operation dicts.
    return the the key to locate the new operation in operations dict

    item = {  'operation': 'upload_snap',
                'options': {'pmr': 'TS1234567',
                            'level': '3',
                            'si_requestid':'2345',
                            'enable_status':
                            'true',
                            'version': 1}
            }

    Items in the operations dict are like:

    {'2345': {'pmr': 'TS1234567',
              'level': '3',
              'si_requestid':'2345',
              'enable_status': 'true',
              'version': 1,
              'type': 'upload_snap',
              'status': 'new',
              'description: '',
              'status_sent': '',
              'progress': 0,
              'event_id': 'IBM-RedHatMarine-ceph-368ffc04.....'},
              'created': 1707818993.8846028
     '1234' : {....}
    }
    """

    key = str(uuid.uuid4())

    # reject requests with no valid structure:
    if 'operation' not in item or 'options' not in item:
        operations[key]['type']= NOT_SUPPORTED
        operations[key]['status'] = OPERATION_STATUS_REQUEST_REJECTED
        operations[key]['progress'] = 0
        operations[key]['description'] = f'Operations ({key}): Received unknown operation: {item}'
        operations[key]['status_sent'] = ST_NOT_SENT
        operations[key]['event_id'] = event_id
        return key

    # reject operations not supported
    if item['operation'] != UPLOAD_SNAP: # Only "upload snap" ops are allowed
        operations[key] = item['options']
        operations[key]['type']= item['operation']
        operations[key]['status'] = OPERATION_STATUS_REQUEST_REJECTED
        operations[key]['progress'] = 0
        operations[key]['description'] = f'Operations ({key}): Rejected <{item["operation"]}> operation <{key}>: Operation not supported'
        operations[key]['status_sent'] = ST_NOT_SENT
        operations[key]['event_id'] = event_id
        return key

    # reject UPLOAD SNAP operations without required fields
    if not ('pmr' in item['options'] and
            'level' in  item['options'] and
            'si_requestid' in item['options']):
        operations[key]['type']= NOT_SUPPORTED
        operations[key]['status'] = OPERATION_STATUS_REQUEST_REJECTED
        operations[key]['progress'] = 0
        operations[key]['description'] = f"Operations ({key}): required fields (pmr, level, si_requestid)\
              not present in <{item['operation']}> operation: {item}"
        operations[key]['status_sent'] = ST_NOT_SENT
        operations[key]['event_id'] = event_id
        return key

    # reject UPLOAD SNAP operations with same level than other being processed
    # if they are inside the cooling window time interval for the level
    for op_key, op in operations.items():
        if op['type'] == UPLOAD_SNAP and op['level'] == item['options']['level']:
            # we have another log upload operation for same level
            # verify if it is inside the "cooling window for the level"
            if op['level'] == 1:
                cooling_window_seconds = l1_cooling_window_seconds
            else:
                cooling_window_seconds = l2_cooling_window_seconds
            if int(time.time() - op['created']) <= cooling_window_seconds:
                operations[key] = item['options']
                operations[key]['type']= item['operation']
                operations[key]['status'] = OPERATION_STATUS_REQUEST_REJECTED
                operations[key]['progress'] = 0
                operations[key]['description'] = f"Operations ({key}): <{item['operation']}> operation\
                    <{item['options']['pmr']}>:There is another operation with identifier {op_key}\
                        which has the same level and is already being processed"
                operations[key]['status_sent'] = ST_NOT_SENT
                operations[key]['event_id'] = event_id
                return key

    #reject UPLOAD SNAP operations with same si_requestid than other being processed
    if item['options']['si_requestid'] in operations.keys():
        operations[key] = item['options']
        operations[key]['type']= item['operation']
        operations[key]['status'] = OPERATION_STATUS_REQUEST_REJECTED
        operations[key]['progress'] = 0
        operations[key]['description'] = f"Operations ({key}): <{item['operation']}> \
            operation <{item['options']['si_requestid']}>: operation is being processed now"
        operations[key]['status_sent'] = ST_NOT_SENT
        operations[key]['event_id'] = event_id
        return key

    # Accept valid UPLOAD SNAP operation
    key = item['options']['si_requestid']
    operations[key] = item['options']
    operations[key]['type']= item['operation']
    operations[key]['status'] = OPERATION_STATUS_NEW
    operations[key]['progress'] = 0
    operations[key]['description'] = f'Operations ({key}): Accepted new <{item["operation"]}> \
        operation <{key}>'
    operations[key]['status_sent'] = ST_NOT_SENT
    operations[key]['event_id'] = event_id
    operations[key]['created'] = time.time()
    return key

class Report:
    def __init__(self, report_type: str, component: str, description: str, icn: str, owner_tenant_id: str, fn: Callable[[], str], url: str, proxy: str, seconds_interval: int,
                 mgr_module: Any, key: str = "", event_id: str = ''):
        self.report_type = report_type                # name of the report
        self.component = component                    # component
        self.icn = icn                                # ICN = IBM Customer Number
        self.owner_tenant_id = owner_tenant_id        # IBM tenant ID
        self.fn = fn                                  # function used to retrieve the data
        self.url = url                                # url to send the report
        self.interval = seconds_interval              # interval to send the report (seconds)
        self.mgr = mgr_module
        self.description = description
        self.last_id = ''
        self.proxies = {'http': proxy, 'https': proxy} if proxy else {}
        self.key = key                                # used in operations reports
        self.event_id = event_id                      # used in operations reports

        # Last upload settings
        self.last_upload_option_name = 'report_%s_last_upload' % self.report_type
        last_upload = self.mgr.get_store(self.last_upload_option_name, None)
        if last_upload is None:
            self.last_upload = str(int(time.time()) - self.interval + 1)
        else:
            self.last_upload = str(int(last_upload))

    def generate_report(self) -> dict:
        try:
            if self.key:
                content = self.fn(self.key)
            else:
                content = self.fn(self.mgr)
            if content is None:
                return None

            report = {}
            report_dt = datetime.timestamp(datetime.now())

            report = ReportHeader.collect(self.report_type,
                                  self.mgr.get('mon_map')['fsid'],
                                  self.mgr.version,
                                  report_dt,
                                  self.mgr,
                                  self.mgr.target_space,
                                  self.event_id)

            event_section = ReportEvent.collect(self.report_type,
                                        self.component,
                                        report_dt,
                                        self.mgr.get('mon_map')['fsid'],
                                        self.icn,
                                        self.owner_tenant_id,
                                        self.description,
                                        content,
                                        self.mgr,
                                        self.key)

            report['events'].append(event_section)
            self.last_id = report["event_time_ms"]

            return report
        except Exception as ex:
            raise Exception('<%s> report not available: %s\n%s' % (self.report_type, ex, report))

    def filter_report(self, fields_to_remove: list) -> str:
        filtered_report = self.generate_report()
        if filtered_report is None:
            return None

        for field in fields_to_remove:
            if field in filtered_report:
                del filtered_report[field]

        return json.dumps(filtered_report)

    def send(self, force: bool = False) -> str:
        # Do not send report if the required interval is not reached
        if not force:
            if (int(time.time()) - int(self.last_upload)) < self.interval:
                self.mgr.log.info('%s report not sent because interval not reached', self.report_type)
                return ""

        # Do not sent report if interval is set to 0
        if self.interval == 0 and not force:
            self.mgr.log.info('%s report not sent because interval set to 0', self.report_type)
            return ""

        resp = None
        try:
            report = self.generate_report()
            if report is None:
                # the report can tell that it doesnt want to be sent by returning None
                return None
            if self.proxies:
                self.mgr.log.info('Sending <%s> report to <%s> (via proxies <%s>)', self.report_type, self.url,
                                  self.proxies)
            else:
                self.mgr.log.info('Sending <%s> report to <%s>', self.report_type, self.url)
            resp = requests.post(url=self.url,
                                 headers={'accept': 'application/json', 'content-type': 'application/json'},
                                 data=json.dumps(report),
                                 proxies=self.proxies)
            self.mgr.log.debug(f"Report response: {resp.text}")
            resp.raise_for_status()
            self.process_response(self.report_type, resp)
            self.last_upload = str(int(time.time()))
            self.mgr.set_store(self.last_upload_option_name, self.last_upload)
            self.mgr.health_checks.pop('CHA_ERROR_SENDING_REPORT', None)
            self.mgr.log.info('Successfully sent <%s> report(%s) to <%s>', self.report_type, self.last_id, self.url)
            return resp.text
        except Exception as e:
            explanation = resp.text if resp else ""
            raise SendError('Failed to send <%s> to <%s>: %s %s' % (self.report_type, self.url, str(e), explanation))

    def process_response(self, report_type: str, resp: requests.Response) -> None:
        """
        Process operations after sending a "report" and receiving a succesful response
        """
        try:
            if report_type == 'last_contact':
                # retrieve operations from response
                inbound_requests = resp.json().get('response_state', {}).get('transactions',{}).get('Unsolicited_Storage_Insights_RedHatMarine_ceph_Request', {}).get('response_object', {}).get('product_request', {}).get('asset_event_detail', {}).get('body', {}).get('inbound_requests', {})

                if inbound_requests:
                    event_id = resp.json().get('transaction', {}).get('event_id', '')
                    self.mgr.log.info(f"Operations: New inbound_requests = {inbound_requests} for event_id {event_id}")
                    # Add the operation to the operations queue
                    for item in inbound_requests:
                        if notProcessed(item):
                            # Add Confirm response operation to operations dict
                            key = str(uuid.uuid4())
                            operations[key] = {}
                            operations[key]['type']= CONFIRM_RESPONSE
                            operations[key]['status'] = OPERATION_STATUS_COMPLETE
                            operations[key]['progress'] = 0
                            operations[key]['description'] = CONFIRM_RESPONSE
                            operations[key]['status_sent'] = ST_NOT_SENT
                            operations[key]['event_id'] = event_id

                            self.mgr.log.info(f"Operations: Added confirm response operation for {item}")

                            # Add the operation to operations dict
                            key = add_operation(item,
                                                self.mgr.level_one_upload_cooling_window_seconds,
                                                self.mgr.level_two_upload_cooling_window_seconds,
                                                event_id)
                            self.mgr.log.info(f"Operations: Added operation {item}")
                        else:
                            self.mgr.log.info(f"Operations: Rejected already processed operation with SI request id = {item.get('options', {}).get('si_requestid', '')}")
        except Exception as ex:
            self.mgr.log.error(f"Operations: error: {ex} adding {item}")

def alert_uid(alert: dict) -> str:
    """
    Retuns a unique string identifying this alert
    """
    return json.dumps(alert['labels'], sort_keys=True) + alert['activeAt'] + alert['value']

def is_alert_relevant(alert: dict) -> bool:
    """
    Returns True if this alert should be sent, False if it should be filtered out of the report
    """
    state = alert.get('state', '')
    severity = alert.get('labels', {}).get('severity', '')

    return state == 'firing' and severity == 'critical'

def get_prometheus_alerts(mgr):
    """
    Returns a list of all the alerts currently active in Prometheus
    """
    try:
        alerts_url = f"{get_prometheus_url(mgr)}/alerts"
        # Get the alerts
        resp = {}
        try:
            resp = requests.get(alerts_url).json()
        except Exception as e:
            raise Exception(f"Error getting alerts from Prometheus at {alerts_url} : {e}")

        if 'data' not in resp or 'alerts' not in resp['data']:
            raise Exception(f"Prometheus returned a bad reply: {resp}")

        alerts = resp['data']['alerts']
        return alerts
    except Exception as e:
        mgr.log.error(f"Can't fetch alerts from Prometheus: {e}")
        return [{
                'labels': {
                    'alertname': 'callhomeErrorFetchPrometheus',
                    'severity': 'critical'
                },
                'annotations': {
                    'description': str(e)
                },
                'state': 'firing',
                # 'activeAt' and 'value' are here for alert_uid() to work. they should be '0' so that we won't send this alert again and again
                'activeAt': '0',
                'value': '0'
            }]

def generate_alerts_report(mgr : Any):
    global sent_alerts
    # Filter the alert list
    current_alerts_list = list(filter(is_alert_relevant, get_prometheus_alerts(mgr)))

    current_alerts = {alert_uid(a):a for a in current_alerts_list}
    # Find all new alerts - alerts that are currently active but were not sent until now (not in sent_alerts)
    new_alerts = [a for uid, a in current_alerts.items() if uid not in sent_alerts]
    resolved_alerts = [a for uid, a in sent_alerts.items() if uid not in current_alerts]

    sent_alerts = current_alerts
    if len(new_alerts) == 0 and len(resolved_alerts) == 0:
        return None  # This will prevent the report from being sent
    alerts_to_send = {'new_alerts': new_alerts, 'resolved_alerts': resolved_alerts}
    return alerts_to_send

class CallHomeAgent(MgrModule):
    MODULE_OPTIONS: List[Option] = [
        Option(
            name='target',
            type='str',
            default='https://esupport.ibm.com/connect/api/v1',
            desc='Call Home endpoint'
        ),
        Option(
            name='interval_inventory_report_seconds',
            type='int',
            min=0,
            default=60 * 60 * 24,  # one day
            desc='Time frequency for the inventory report'
        ),
        Option(
            name='interval_performance_report_seconds',
            type='int',
            min=0,
            default=60 * 5,  # 5 minutes
            desc='Time frequency for the performance report'
        ),
        Option(
            name='interval_status_report_seconds',
            type='int',
            min=0,
            default=60 * 30,  # 30 minutes
            desc='Time frequency for the status report'
        ),
        Option(
            name='interval_last_contact_report_seconds',
            type='int',
            min=0,
            default=60 * 30,  # 30 minutes
            desc='Time frequency for the last contact report'
        ),
        Option(
            name='interval_alerts_report_seconds',
            type='int',
            min=0,
            default=60 * 5,  # 5 minutes
            desc='Time frequency for the alerts report'
        ),
        Option(
            name='customer_email',
            type='str',
            default='',
            desc='Customer contact email'
        ),
        Option(
            name='icn',
            type='str',
            default='',
            desc='IBM Customer Number'
        ),
        Option(
            name='customer_first_name',
            type='str',
            default='',
            desc='Customer first name'
        ),
        Option(
            name='customer_last_name',
            type='str',
            default='',
            desc='Customer last name'
        ),
        Option(
            name='customer_phone',
            type='str',
            default='',
            desc='Customer phone'
        ),
        Option(
            name='customer_company_name',
            type='str',
            default='',
            desc='Customer phone'
        ),
        Option(
            name='customer_address',
            type='str',
            default='',
            desc='Customer address'
        ),
        Option(
            name='customer_country_code',
            type='str',
            default='',
            desc='Customer country code'
        ),
        Option(
            name='owner_tenant_id',
            type='str',
            default="",
            desc='IBM tenant Id for IBM Storage Insigths'
        ),
        Option(
            name='owner_ibm_id',
            type='str',
            default="",
            desc='IBM w3id identifier for IBM Storage Insights'
        ),
        Option(
            name='owner_company_name',
            type='str',
            default="",
            desc='User Company name for IBM storage Insights'
        ),
        Option(
            name='owner_first_name',
            type='str',
            default="",
            desc='User first name for IBM storage Insights'
        ),
        Option(
            name='owner_last_name',
            type='str',
            default="",
            desc='User last name for IBM storage Insights'
        ),
        Option(
            name='owner_email',
            type='str',
            default="",
            desc='User email for IBM storage Insights'
        ),
        Option(
            name='proxy',
            type='str',
            default='',
            desc='Proxy to reach Call Home endpoint'
        ),
        Option(
            name='target_space',
            type='str',
            default='prod',
            desc='Target space for reports (dev, staging or production)'
        ),
        Option(
            name='si_web_service_url',
            type='str',
            default='https://join.insights.ibm.com/api/v1/em-integration',
            desc='URL used to register Ceph cluster in SI (staging or production)'
        ),
        Option(
            name='valid_container_registry',
            type='str',
            default=r'^.+\.icr\.io',
            desc='Container registry pattern for urls where cephadm credentials(JWT token) are valid'
        ),
        Option(
            name='ecurep_url',
            type='str',
            default='https://www.secure.ecurep.ibm.com',
            desc='ECuRep file exchange systems'
        ),
        Option(
            name='ecurep_userid',
            type='str',
            default="",
            desc='Userid obtained from the IBM Transfer ID service'
        ),
        Option(
            name='ecurep_password',
            type='str',
            default="",
            desc='Password obtained from the IBM Transfer ID service'
        ),
        Option(
            name='upload_ops_persistence_seconds',
            type='int',
            default=864000,
            desc='Time interval during which requests with same SI request ID will not be processed'
        ),
        Option(
            name='level_one_upload_cooling_window_seconds',
            type='int',
            default=300,
            desc='Time interval needed to pass before a new diagnostics upload operation level one will be accepted'
        ),
        Option(
            name='level_two_upload_cooling_window_seconds',
            type='int',
            default=3600,
            desc='Time interval needed to pass before a new diagnostics upload operation level two(or upper) will be accepted'
        ),
    ]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super(CallHomeAgent, self).__init__(*args, **kwargs)

        # set up some members to enable the serve() method and shutdown()
        self.run = True

        # Load operations from db, this makes them persistent across mgr restarts
        self.init_operations()

        # Module options
        self.refresh_options()

        # Health checks
        self.health_checks: Dict[str, Dict[str, Any]] = dict()

        # Coroutines management
        self.loop = asyncio.new_event_loop()  # type: ignore
        # Array to hold coroutines launched
        self.tasks = []

        # Prepare reports
        self.prepare_reports()

    def init_operations(self) -> None:
        # We fetch from db the operations we already processed,
        # and assign it to the global operations dictionary
        db_operations = self.get_store('db_operations')

        global operations
        if db_operations is not None:
            # We already set_store('db_operations') in the past
            operations = json.loads(db_operations)
            self.log.debug(f"operations loaded from db after restart: {operations}")

    def refresh_options(self):
        # Env vars (if they exist) have preference over module options
        self.cha_target_url = str(os.environ.get('CHA_TARGET', self.get_module_option('target')))

        self.interval_inventory_seconds = int(
            os.environ.get('CHA_INTERVAL_INVENTORY_REPORT_SECONDS',
                           self.get_module_option('interval_inventory_report_seconds')))  # type: ignore
        self.interval_performance_seconds = int(
            os.environ.get('CHA_INTERVAL_PERFORMANCE_REPORT_SECONDS',
                           self.get_module_option('interval_performance_report_seconds')))  # type: ignore
        self.interval_status_seconds = int(
            os.environ.get('CHA_INTERVAL_STATUS_REPORT_SECONDS',
                           self.get_module_option('interval_status_report_seconds')))  # type: ignore
        self.interval_last_contact_seconds = int(
            os.environ.get('CHA_INTERVAL_LAST_CONTACT_REPORT_SECONDS',
                           self.get_module_option('interval_last_contact_report_seconds')))  # type: ignore
        self.interval_alerts_seconds = int(
            os.environ.get('CHA_INTERVAL_ALERTS_REPORT_SECONDS',
                           self.get_module_option('interval_alerts_report_seconds')))  # type: ignore
        self.proxy = str(os.environ.get('CHA_PROXY', self.get_module_option('proxy')))
        self.target_space = os.environ.get('CHA_TARGET_SPACE', self.get_module_option('target_space'))
        self.si_web_service_url = os.environ.get('CHA_SI_WEB_SERVICE_URL', self.get_module_option('si_web_service_url'))

        # Customer identifiers do not use environment vars to be set
        self.icn = self.get_module_option('icn')
        self.customer_email = self.get_module_option('customer_email')
        self.customer_first_name = self.get_module_option('customer_first_name')
        self.customer_last_name = self.get_module_option('customer_last_name')
        self.customer_phone = self.get_module_option('customer_phone')
        self.customer_company_name = self.get_module_option('customer_company_name')
        self.customer_address = self.get_module_option('customer_address')
        self.customer_country_code = self.get_module_option('customer_country_code')

        # Owner identifiers used in IBM storage insights do not use environment vars to be set
        self.owner_tenant_id = self.get_module_option('owner_tenant_id')
        self.owner_ibm_id = self.get_module_option('owner_ibm_id')
        self.owner_company_name = self.get_module_option('owner_company_name')
        self.owner_first_name = self.get_module_option('owner_first_name')
        self.owner_last_name = self.get_module_option('owner_last_name')
        self.owner_email = self.get_module_option('owner_email')

        # Other options not using env vars
        self.valid_container_registry = self.get_module_option('valid_container_registry')
        self.upload_ops_persistence_seconds = self.get_module_option('upload_ops_persistence_seconds')
        self.level_one_upload_cooling_window_seconds = self.get_module_option('level_one_upload_cooling_window_seconds')
        self.level_two_upload_cooling_window_seconds = self.get_module_option('level_two_upload_cooling_window_seconds')

        # ecurep options:
        self.ecurep_url = self.get_module_option('ecurep_url')
        self.ecurep_userid = self.get_module_option('ecurep_userid')
        self.ecurep_password = self.get_module_option('ecurep_password')

    def upload_file(self, op_key: str, file_name: str, chunk_pattern: str = '') -> None:
        """
        Upload a file to ecurep.
        If a chunk_pattern is provided the file is divided in chunks
        """

        # We first consider the module options to allow for flexible
        # workarounds should we need them, otherwise we load the default keys
        if self.ecurep_userid and self.ecurep_password:
            ecurep_userid = self.ecurep_userid
            ecurep_password = self.ecurep_password
        else:
            try:
                id_data = get_settings()
                # bail out early when the keys are missing
                ecurep_userid = id_data['ecurep_transfer_id']
                ecurep_password = id_data['ecurep_password']
            except Exception as e:
                self.log.error(f"Error loading ECuRep keys: {e}")
                raise

        auth = (ecurep_userid, ecurep_password)
        if self.owner_company_name == "":
            owner = "MyCompanyUploadClient"
        else:
            owner = self.owner_company_name
        case_id = operations[op_key]['pmr']
        si_requestid = operations[op_key]['si_requestid']
        resp = None

        # Get the unique Upload ID for the file
        try:
            # 1. Obtain the file id to upload the file
            ecurep_file_id_url = f'{self.ecurep_url}/app/upload_tid?name={file_name}&client={owner}'
            self.log.info(f"Operations ({si_requestid}): getting unique upload id from <{ecurep_file_id_url}>")
            resp = requests.post(url=ecurep_file_id_url, auth=auth)
            resp.raise_for_status()
            file_id_for_upload = resp.json().get('id')
            self.log.info(f"Operations ({si_requestid}): unique id for upload is <{file_id_for_upload}>")
        except Exception as ex:
            explanation = resp.text if resp else ""
            raise SendError(f'Operations ({si_requestid}): Failed to send <{file_name}> to <{ecurep_file_id_url}>: {ex}: {explanation}')

        try:
            # 2. Upload the file
            ecurep_file_upload_url = f'{self.ecurep_url}/app/upload_sf/files/{file_id_for_upload}?case_id={case_id}&client={owner}'
            file_size = 0
            if chunk_pattern:
                files_to_upload = (glob.glob(f'{DIAGS_FOLDER}/{chunk_pattern}'))
                for part in files_to_upload:
                    file_size += os.path.getsize(part)
            else:
                files_to_upload = [f'{DIAGS_FOLDER}/{file_name}']
                file_size = os.path.getsize(f'{DIAGS_FOLDER}/{file_name}')

            start_byte = 0
            part_sent = 0
            self.log.info(f"Operations ({si_requestid}): uploading file {file_name} to <{ecurep_file_upload_url}>")
            for file_path in sorted(files_to_upload):
                chunk_size = os.path.getsize(file_path)
                with open(file_path, 'rb') as file:
                    if chunk_pattern:
                        self.log.info(f"Operations ({si_requestid}): uploading part {file_path} to <{ecurep_file_upload_url}>")
                    resp = requests.post(url = ecurep_file_upload_url,
                                        data = file.read(),
                                        headers = {'Content-Type': 'application/octet-stream',
                                                   'X-File-Name': file_name,
                                                   'X-File-Size': f'{file_size}',
                                                   'Content-Range': f'bytes {start_byte}-{chunk_size + start_byte}/{file_size}'
                                        },
                    )
                    self.log.info(f'Operations ({si_requestid}): uploaded {file_name} -> bytes {start_byte}-{chunk_size + start_byte}/{file_size}')
                    resp.raise_for_status()
                start_byte += chunk_size
                part_sent += 1
                if chunk_pattern:
                    operations[op_key]["progress"] = int(part_sent/len(files_to_upload) * 100)
                operations[op_key]["description"] = f"file <{file_name}> is being sent"
                self.send_operation_report(op_key)
        except Exception as ex:
            explanation = resp.text if resp else ""
            raise SendError(f'Operations ({si_requestid}): Failed to send <{file_path}> to <{ecurep_file_upload_url}>: {ex}: {explanation}')

    def prepare_reports(self):
        self.reports = {'inventory': Report('inventory',
                                            'ceph_inventory',
                                            'Ceph cluster composition',
                                            self.icn,
                                            self.owner_tenant_id,
                                            inventory,
                                            self.cha_target_url,
                                            self.proxy,
                                            self.interval_inventory_seconds,
                                            self),
                        'status': Report('status',
                                         'ceph_health',
                                         'Ceph cluster status and health',
                                         self.icn,
                                         self.owner_tenant_id,
                                         status,
                                         self.cha_target_url,
                                         self.proxy,
                                         self.interval_status_seconds,
                                         self),
                        'last_contact': Report('last_contact',
                                               'ceph_last_contact',
                                               'Last contact timestamps with Ceph cluster',
                                               self.icn,
                                               self.owner_tenant_id,
                                               last_contact,
                                               self.cha_target_url,
                                               self.proxy,
                                               self.interval_last_contact_seconds,
                                               self),
                        'alerts': Report('status',
                                         'ceph_alerts',
                                         'Ceph cluster alerts',
                                         self.icn,
                                         self.owner_tenant_id,
                                         generate_alerts_report,
                                         self.cha_target_url,
                                         self.proxy,
                                         self.interval_alerts_seconds,
                                         self),
                        'performance': Report('performance',
                                              'ceph_performance',
                                              'Cluster performance metrics',
                                              self.icn,
                                              self.owner_tenant_id,
                                              performance,
                                              self.cha_target_url,
                                              self.proxy,
                                              self.interval_performance_seconds,
                                              self)
        }

    def config_notify(self) -> None:
        """
        This only affects changes in ceph config options.
        To change configuration using env. vars a restart of the module
        will be neeed or the change in one config option will refresh
        configuration coming from env vars
        """
        self.refresh_options()
        self.prepare_reports()
        self.clean_coroutines()
        self.launch_coroutines()

    async def control_task(self, seconds: int) -> None:
        """
            Coroutine to allow cancel and reconfigure coroutines in only 10s
        """
        try:
            while self.run:
                await asyncio.sleep(seconds)
        except asyncio.CancelledError:
            return

    async def process_operations(self, seconds: int) -> None:
        """
            Coroutine to process operations:

            Remove "completed" operations
            Takes "new" operations moving them to "in progress"
            Process the operation moving it to "complete" or "error"

            {{'1234': {'pmr': 'TS1234567',
                       'level': '3',
                       'enable_status': 'true',
                       'version': 1,
                       'status': 0,
                       'type': 'upload_snap',
                       'si_requestid': '1234',
                       'created': 1707818993.8846028}}

        """
        try:
            while self.run:
                try:
                    self.log.info("Operations: started")
                    # Clean any operation in final state
                    for operation_key in list(operations):
                        self.log.info("Operations: cleaning finished operations")
                        if operations[operation_key]['status'] in [OPERATION_STATUS_COMPLETE,
                                                                  OPERATION_STATUS_ERROR,
                                                                  OPERATION_STATUS_REQUEST_REJECTED] and operations[operation_key]['status_sent'] == ST_SENT:

                            # Do not delete operations inside the upload snap cooling window
                            if operations[operation_key]['type'] == UPLOAD_SNAP and 'created' in operations[operation_key].keys():
                               if int(time.time() - operations[operation_key]['created']) <= self.upload_ops_persistence_seconds:
                                   continue

                            self.log.info(f'Operations ({operation_key}): Removed finished  <{operations[operation_key]["type"]}> operation with status <{operations[operation_key]["status"]}>')
                            del operations[operation_key]

                    # Process rest of operations
                    self.log.info("Operations: Processing ....")
                    for operation_key, operation in operations.items():

                        # Pending finished operations
                        if  operation['status'] in [OPERATION_STATUS_COMPLETE,
                                                    OPERATION_STATUS_ERROR,
                                                    OPERATION_STATUS_REQUEST_REJECTED] and operation['status_sent'] == ST_NOT_SENT:
                            self.log.info("Operations: Processing finished operations ....")
                            self.send_operation_report(operation_key)


                        # Process new operations
                        if operation["status"] == OPERATION_STATUS_NEW:
                            self.log.info("Operations: Processing new operations ....")
                            try:
                                operation["status"] = OPERATION_STATUS_IN_PROGRESS
                                self.log.info(f'Operations ({operation_key}):  <{operation["type"]}> operation status is <{operation["status"]}> now>')
                                commands_file = collect_diagnostic_commands(self, operation_key)
                                sos_files_pattern = ""
                                if int(operation["level"]) > 1:
                                    sos_files_pattern = collect_sos_report(self, operation_key)
                                self.send_diagnostics(operation_key, commands_file, sos_files_pattern)
                                self.log.info(f'Operations ({operation_key}): Completed <{operation["type"]}> operation')
                                operation["status"] = OPERATION_STATUS_COMPLETE
                                operation["progress"] = 100
                                operation["description"] = OPERATION_STATUS_COMPLETE
                            except Exception as ex:
                                self.log.error(f'Operations ({operation_key}): Error processing <{operation["type"]}> operation: {ex}')
                                operation["status"] = OPERATION_STATUS_ERROR

                            # if it was ok or not, we always report the state
                            self.send_operation_report(operation_key)
                    self.log.info('Operations: Processing operations finished')
                except Exception as ex:
                    self.log.error(f"Operations ({operation_key}): error: {ex}")

                # persist operations
                self.set_store('db_operations', json.dumps(operations))
                self.log.debug(f"updating operations db: {json.dumps(operations)}")

                await asyncio.sleep(seconds)
        except asyncio.CancelledError:
            return

    async def report_task(self, report: Report) -> None:
        """
            Coroutine for sending the report passed as parameter
        """
        self.log.info('Launched task for <%s> report each %s seconds)', report.report_type, report.interval)

        try:
            while self.run:
                try:
                    report.send()
                except Exception as ex:
                    send_error = str(ex)
                    self.log.error(send_error)
                    self.health_checks.update({
                        'CHA_ERROR_SENDING_REPORT': {
                            'severity': 'error',
                            'summary': 'IBM Ceph Call Home Agent manager module: error sending <{}> report to '
                                    'endpoint {}'.format(report.report_type, self.cha_target_url),
                            'detail': [send_error]
                        }
                    })

                self.set_health_checks(self.health_checks)
                await asyncio.sleep(report.interval)
        except asyncio.CancelledError:
            return

    def launch_coroutines(self) -> None:
        """
         Launch module coroutines (reports or any other async task)
        """
        try:
            # tasks for periodic reports
            for report_name, report in self.reports.items():
                t = self.loop.create_task(self.report_task(report))
                self.tasks.append(t)
            # task for process requested operations
            t = self.loop.create_task(self.process_operations(30))
            self.tasks.append(t)
            # create control task to allow to reconfigure reports in 10 seconds
            t = self.loop.create_task(self.control_task(10))
            self.tasks.append(t)
            # run the async loop
            self.loop.run_forever()
        except Exception as ex:
            if str(ex) != 'This event loop is already running':
                self.log.exception(str(ex))

    def serve(self) -> None:
        """
            - Launch coroutines for report tasks
        """
        self.log.info('Starting IBM Ceph Call Home Agent')

        # Launch coroutines for the reports
        self.launch_coroutines()

        self.log.info('Call home agent finished')

    def clean_coroutines(self) -> None:
        """
        This method is called by the mgr when the module needs to shut
        down (i.e., when the serve() function needs to exit).
        """
        self.log.info('Cleaning coroutines')
        for t in self.tasks:
            t.cancel()
        self.tasks = []

    def shutdown(self) -> None:
        self.log.info('Stopping IBM call home module')
        self.run = False
        self.clean_coroutines
        self.loop.stop()
        return super().shutdown()

    def send_diagnostics(self, op_key: str, cmd_file_name: str, sos_files_pattern: str) -> None:
        """
        """
        # Send commands file:
        self.upload_file(op_key, cmd_file_name)

        # Send sos file splitted when we have files
        if sos_files_pattern:
            sos_file_name = f'{sos_files_pattern[:-2]}.xz'
            self.upload_file(op_key, sos_file_name, sos_files_pattern)

    def send_operation_report(self, key:str) -> None:
        try:
            op_report = Report(report_type= f'status',
                               component = 'ceph_operations',
                               description=f'operation {operations[key]["type"]}',
                               icn= self.icn,
                               owner_tenant_id= self.owner_tenant_id,
                               fn= get_operation,
                               url= self.cha_target_url,
                               proxy= self.proxy,
                               seconds_interval= 0,
                               mgr_module = self,
                               key= key,
                               event_id= operations[key]["event_id"])
            op_report.send(force=True)
            operations[key]['status_sent'] = ST_SENT
            self.log.info(f'Operations ({key}): call home report sent. description: {operations[key]["description"]}, status: {operations[key]["status"]}, progress: {operations[key]["progress"]}')
            return
        except Exception as ex:
            self.log.error(f'Operations ({key}): Error sending <{operations[key]["type"]}> \
                             operation report <{key}>: {ex}')
            raise(ex)

    @CLIReadCommand('callhome stop')
    def stop_cmd(self) -> Tuple[int, str, str]:
        self.shutdown()
        return HandleCommandResult(stdout=f'Remember to disable the '
                                   'call home module')

    @CLIReadCommand('callhome reset alerts')
    def reset_alerts(self, mock: Optional[bool] = False) -> Tuple[int, str, str]:
        """
        Resets the local list of alerts that were sent to Call Home to allow
        for existing alerts to be resent.

        :param mock: generates a dummy alert
        """
        global sent_alerts
        if mock:
            # If there are no relevant alerts in the cluster, an "alerts" report will not be sent.
            # "--mock" is useful in this case, to allow the user to send a dummy "alerts" report to Call Home.
            mocked_alert = {'labels': {'label': 'test'}, 'activeAt': '42', 'value': '17'}
            sent_alerts = {alert_uid(mocked_alert): mocked_alert}
        else:
            sent_alerts = {}
        return HandleCommandResult(stdout=f"Sent alerts list has been reset. Next alerts report will send all current alerts.")

    @CLIReadCommand('callhome show')
    def print_report_cmd(self, report_type: str) -> Tuple[int, str, str]:
        """
            Prints the report requested.
            Available reports: inventory, status, last_contact, alerts, performance
            Example:
                ceph callhome show inventory
        """
        global sent_alerts
        if report_type in self.reports.keys():
            if report_type == 'alerts':
                # The "alerts" report only sends alerts that are not in 'sent_alerts', and then updates 'sent_alerts'
                # with the alerts sent. For 'callhome show' not to affect the regular workflow, we need to restore
                # 'sent_alerts' to what it was before 'callhome show' generated the alerts report.
                tmp_sent_alerts = sent_alerts
            filtered_report = self.reports[report_type].filter_report(['api_key', 'private_key'])
            if report_type == 'alerts':
                sent_alerts = tmp_sent_alerts
            if filtered_report is None:
                return HandleCommandResult(stdout=f"Report is empty")
            return HandleCommandResult(stdout=f"{filtered_report}")
        else:
            return HandleCommandResult(stderr='Unknown report type')

    @CLIReadCommand('callhome send')
    def send_report_cmd(self, report_type: str) -> Tuple[int, str, str]:
        """
            Command for sending the report requested.
            Available reports: inventory, status, last_contact, alerts, performance
            Example:
                ceph callhome send inventory
        """
        try:
            if report_type in self.reports.keys():
                resp = self.reports[report_type].send(force=True)
            else:
                raise Exception('Unknown report type')
        except Exception as ex:
            return HandleCommandResult(stderr=str(ex))
        else:
            if resp == None:
                return HandleCommandResult(stdout=f'{report_type} report: Nothing to send\n')
            else:
                return HandleCommandResult(stdout=f'{report_type} report sent successfully:\n{resp}')


    @CLIReadCommand('callhome list-tenants')
    def list_tenants(self, owner_ibm_id: str, owner_company_name: str,
                       owner_first_name: str, owner_last_name: str,
                       owner_email: str) -> Tuple[int, str, str]:
        """
        Retrieves the list of tenant ids linked with an specific IBM id owner
        """
        mon_map = self.get('mon_map')
        mon_ips = ','.join([mon['addr'] for mon in mon_map['mons']])
        owner_data = {'owner-ibm-id': owner_ibm_id,
                'company-name': owner_company_name,
                'owner-first-name': owner_first_name,
                'owner-last-name': owner_last_name,
                'owner-email': owner_email,
                'check-only': True,
                'device-serial': mon_map['fsid'],
                'device-IP': mon_ips
                }

        resp = None
        try:
            resp = requests.post(url=self.si_web_service_url,
                                headers={'accept': 'application/json',
                                        'content-type': 'application/json',
                                        'IBM-SRM-SenderApp': 'CEPH-EM',
                                        'IBM-SRM-Request': 'SI-SignUp-Check'},
                                data=json.dumps(owner_data),
                                proxies=self.proxy)

            resp.raise_for_status()
        except Exception as ex:
            explanation = resp.text if resp else str(ex)
            self.log.error(explanation)
            return HandleCommandResult(stderr=explanation)
        else:
            return HandleCommandResult(stdout=f'{json.dumps(resp.json())}')

    @CLIWriteCommand('callhome set tenant')
    def set_tenant_id(self, owner_tenant_id: str, owner_ibm_id: str,
                      owner_company_name: str, owner_first_name: str,
                      owner_last_name: str, owner_email: str) -> Tuple[int, str, str]:
        """
        Set the IBM tenant id included in reports sent to IBM Storage Insights
        """
        try:
            self.set_module_option('owner_tenant_id', owner_tenant_id)
            self.set_module_option('owner_ibm_id', owner_ibm_id)
            self.set_module_option('owner_company_name', owner_company_name)
            self.set_module_option('owner_first_name', owner_first_name)
            self.set_module_option('owner_last_name', owner_last_name)
            self.set_module_option('owner_email', owner_email)
            self.prepare_reports()
        except Exception as ex:
            return HandleCommandResult(stderr=str(ex))
        else:
            return HandleCommandResult(stdout=f'IBM tenant id set to {owner_tenant_id}')

    @CLIReadCommand('callhome get user info')
    def customer(self) ->  Tuple[int, str, str]:
        """
        Show the information about the customer used to identify the customer
        in IBM call home and IBM storage insights systems
        """
        return HandleCommandResult(stdout=json.dumps(
            {'IBM_call_home': {
                    'icn': self.icn,
                    'customer_first_name': self.customer_first_name,
                    'customer_last_name': self.customer_last_name,
                    'customer_phone': self.customer_phone,
                    'customer_address': self.customer_address,
                    'customer_email': self.customer_email,
                    'customer_company_name': self.customer_company_name,
                    'customer_country_code': self.customer_country_code
                },
             'IBM_storage_insights': {
                    'owner_ibm_id': self.owner_ibm_id,
                    'owner_company_name': self.owner_company_name,
                    'owner_first_name': self.owner_first_name,
                    'owner_last_name': self.owner_last_name,
                    'owner_email': self.owner_email,
                    'owner_tenant_id': self.owner_tenant_id
                },
            }))

    @CLIReadCommand('callhome upload diagnostics')
    def upload_diags(self, support_ticket: str, level: int) ->  Tuple[int, str, str]:
        """
        Upload Ceph cluster diagnostics to Ecurep for an specific customer support ticket
        """

        try:
            request = {'operation': 'upload_snap',
                    'options': {'pmr': f'{support_ticket}',
                                'level': f'{level}',
                                'si_requestid':f'si_request_{uuid.uuid4()}',
                                'enable_status': 'true',
                                'version': 1}
            }
            key = add_operation(request,
                                self.level_one_upload_cooling_window_seconds,
                                self.level_two_upload_cooling_window_seconds)

        except Exception as ex:
            return HandleCommandResult(stderr=str(ex))
        else:
            return HandleCommandResult(stdout=f'{operations[key]}')

    @CLIReadCommand('callhome operations')
    def list_operations(self) ->  Tuple[int, str, str]:
        """
        Show the operations list
        """
        try:
            output = '\n'.join(f'{key}:{value}' for key, value in operations.items())
        except Exception as ex:
            return HandleCommandResult(stderr=str(ex))
        else:
            return HandleCommandResult(stdout=output)

    @CLIWriteCommand('callhome operations clean')
    def clean_operations(self, operation_id: str = "") ->  Tuple[int, str, str]:
        """
        Remove an operation (if provided operation_id) from the operations list
        If no operation_id provided clean completelly the operations list
        """
        try:
            if operation_id:
                if operation_id in operations.keys():
                    del operations[operation_id]
            else:
                operations.clear()

            output = json.dumps(operations)

            # persist operations
            self.set_store('db_operations', json.dumps(operations))
            self.log.debug(f"updating operations db after cleaning: {json.dumps(operations)}")

        except Exception as ex:
            return HandleCommandResult(stderr=str(ex))
        else:
            return HandleCommandResult(stdout=output)

