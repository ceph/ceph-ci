from .report import Report, ReportTimes, Event
from .prometheus import Prometheus
import time
import json
import requests
import zstandard
import math
import base64
from datetime import datetime
from typing import Optional

class ReportPerformance(Report):

    def __init__(self, agent) -> None:
        super().__init__(agent, 'performance', [EventPerformance])


class EventPerformance(Event):
    def generate(self, report_times: ReportTimes) -> None:
        super().generate('performance', 'ceph_performance', report_times)


        self.data['body'].update( {
            "context": {
                "origin": 2,
                "timestamp": report_times.time_ms,
                "transid": report_times.time_ms
            },
            "description": 'Cluster performance metrics',
            "payload": {
                "perfstats": self.gather()
            }
        } )

        return self

    def gather(self) -> dict:

        p_i_m = math.ceil(self.agent.interval_performance_report_seconds / 60)  # Performance Interval in Minutes

        queries = {
            "ceph_osd_op_r_avg"        : {"query": f"sum(avg_over_time(ceph_osd_op_r[{p_i_m}m]))/count(ceph_osd_metadata)",
                                          "help" : f"Average of read operations per second and per OSD in the cluster in the last {p_i_m} minutes"},
            "ceph_osd_op_r_min"        : {"query": f"min(min_over_time(ceph_osd_op_r[{p_i_m}m]))",
                                          "help" : f"Minimum read operations per second in the cluster in the last {p_i_m} minutes"},
            "ceph_osd_op_r_max"        : {"query": f"max(max_over_time(ceph_osd_op_r[{p_i_m}m]))",
                                           "help": f"Maximum of write operations per second in the cluster in the last {p_i_m} minutes"},
            "ceph_osd_r_out_bytes_avg" : {"query": f"sum(avg_over_time(ceph_osd_op_r_out_bytes[{p_i_m}m]))/count(ceph_osd_metadata)",
                                          "help" : f"Average of cluster output bytes(reads) and per OSD in the last {p_i_m} minutes"},
            "ceph_osd_r_out_bytes_min" : {"query": f"min(min_over_time(ceph_osd_op_r_out_bytes[{p_i_m}m]))",
                                          "help" : f"Minimum of cluster output bytes(reads) in the last {p_i_m} minutes"},
            "ceph_osd_r_out_bytes_max" : {"query": f"max(max_over_time(ceph_osd_op_r_out_bytes[{p_i_m}m]))",
                                          "help" : f"Maximum of cluster output bytes(reads) in the last {p_i_m} minutes"},
            "ceph_osd_op_w_avg"        : {"query": f"sum(avg_over_time(ceph_osd_op_w[{p_i_m}m]))/count(ceph_osd_metadata)",
                                          "help" : f"Average of cluster input operations per second(writes) in the last {p_i_m} minutes"},
            "ceph_osd_op_w_min"        : {"query": f"min(min_over_time(ceph_osd_op_w[{p_i_m}m]))",
                                          "help" : f"Mimimum of cluster input operations per second(writes) in the last {p_i_m} minutes"},
            "ceph_osd_op_w_max"        : {"query": f"max(max_over_time(ceph_osd_op_w[{p_i_m}m]))",
                                          "help" : f"Maximum of cluster input operations per second(writes) in the last {p_i_m} minutes"},
            "ceph_osd_op_w_in_bytes_avg"       : {"query": f"sum(avg_over_time(ceph_osd_op_w_in_bytes[{p_i_m}m]))/count(ceph_osd_metadata)",
                                                  "help" : f"Average of cluster input bytes(writes) in the last {p_i_m} minutes"},
            "ceph_osd_op_w_in_bytes_min"       : {"query": f"min(min_over_time(ceph_osd_op_w_in_bytes[{p_i_m}m]))",
                                                  "help" : f"Minimum of cluster input bytes(writes) in the last {p_i_m} minutes"},
            "ceph_osd_op_w_in_bytes_max"       : {"query": f"max(max_over_time(ceph_osd_op_w_in_bytes[{p_i_m}m]))",
                                                  "help" : f"Maximum of cluster input bytes(writes) in the last {p_i_m} minutes"},
            "ceph_osd_op_read_latency_avg_ms"  : {"query": f"avg(rate(ceph_osd_op_r_latency_sum[{p_i_m}m]) or vector(0) / on (ceph_daemon) rate(ceph_osd_op_r_latency_count[{p_i_m}m]) * 1000)",
                                                  "help" : f"Average of cluster output latency(reads) in milliseconds in the last {p_i_m} minutes"},
            "ceph_osd_op_read_latency_max_ms"  : {"query": f"max(rate(ceph_osd_op_r_latency_sum[{p_i_m}m]) or vector(0) / on (ceph_daemon) rate(ceph_osd_op_r_latency_count[{p_i_m}m]) * 1000)",
                                                  "help" : f"Maximum of cluster output latency(reads) in milliseconds in the last {p_i_m} minutes"},
            "ceph_osd_op_read_latency_min_ms"  : {"query": f"min(rate(ceph_osd_op_r_latency_sum[{p_i_m}m]) or vector(0) / on (ceph_daemon) rate(ceph_osd_op_r_latency_count[{p_i_m}m]) * 1000)",
                                                  "help" : f"Minimum of cluster output latency(reads) in milliseconds  in the last {p_i_m} minutes"},
            "ceph_osd_op_write_latency_avg_ms" : {"query": f"avg(rate(ceph_osd_op_w_latency_sum[{p_i_m}m]) or vector(0) / on (ceph_daemon) rate(ceph_osd_op_w_latency_count[{p_i_m}m]) * 1000)",
                                                  "help" : f"Average of cluster input latency(writes) in milliseconds in the last {p_i_m} minutes"},
            "ceph_osd_op_write_latency_max_ms" : {"query": f"max(rate(ceph_osd_op_w_latency_sum[{p_i_m}m]) or vector(0) / on (ceph_daemon) rate(ceph_osd_op_w_latency_count[{p_i_m}m]) * 1000)",
                                                  "help" : f"Maximum of cluster input latency(writes) in milliseconds  in the last {p_i_m} minutes"},
            "ceph_osd_op_write_latency_min_ms" : {"query": f"min(rate(ceph_osd_op_w_latency_sum[{p_i_m}m]) or vector(0) / on (ceph_daemon) rate(ceph_osd_op_w_latency_count[{p_i_m}m]) * 1000)",
                                                  "help" : f"Maximum of cluster input latency(writes) in milliseconds in the last {p_i_m} minutes"},
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
            "ceph_pool_write_iops"  : {"query": f"rate(ceph_pool_wr[{p_i_m}m]) * on(pool_id) group_left(instance, name) ceph_pool_metadata",
                                       "help" : "Per-second average rate of increase of write operations per Ceph pool during the last {p_i_m} minutes"},
            "ceph_pool_read_iops"   : {"query": f"rate(ceph_pool_rd[{p_i_m}m]) * on(pool_id) group_left(instance, name) ceph_pool_metadata",
                                       "help" : f"Per-second average rate of increase of read operations per Ceph pool during the last {p_i_m} minutes"},
            "ceph_pool_write_bytes" : {"query": f"rate(ceph_pool_wr_bytes[{p_i_m}m]) * on(pool_id) group_left(instance, name) ceph_pool_metadata",
                                       "help" : f"Per-second average rate of increase of written bytes per Ceph pool during the last {p_i_m} minutes"},
            "ceph_pool_read_bytes"  : {"query": f"rate(ceph_pool_rd_bytes[{p_i_m}m]) * on(pool_id) group_left(instance, name) ceph_pool_metadata",
                                       "help" : f"Per-second average rate of increase of read bytes per Ceph pool during the last {p_i_m} minutes"},
            "ceph_pg_activating"    : {"query": f"rate(ceph_pg_activating[{p_i_m}m]) * on(pool_id) group_left(instance, name) ceph_pool_metadata",
                                       "help" : f"Per-second average rate of Placement Groups activated per Ceph pool during the last {p_i_m} minutes"},
            "ceph_pg_backfilling"   : {"query": f"rate(ceph_pg_backfilling[{p_i_m}m]) * on(pool_id) group_left(instance, name) ceph_pool_metadata",
                                       "help" : f"Per-second average rate of Placement Groups backfilled per Ceph pool during the last {p_i_m} minutes"},
            "ceph_pg_creating"      : {"query": f"rate(ceph_pg_creating[{p_i_m}m]) * on(pool_id) group_left(instance, name) ceph_pool_metadata",
                                       "help" : f"Per-second average rate of Placement Groups created per Ceph pool during the last {p_i_m} minutes"},
            "ceph_pg_recovering"    : {"query": f"rate(ceph_pg_recovering[{p_i_m}m]) * on(pool_id) group_left(instance, name) ceph_pool_metadata",
                                       "help" : f"Per-second average rate of Placement Groups recovered per Ceph pool during the last {p_i_m} minutes"},
            "ceph_pg_deep"          : {"query": f"rate(ceph_pg_deep[{p_i_m}m]) * on(pool_id) group_left(instance, name) ceph_pool_metadata",
                                       "help":  f"Per-second average rate of Placement Groups deep scrubbed per Ceph pool during the last {p_i_m} minutes"},
            "ceph_rgw_avg_get_latency_ms" : {"query": f'(rate(ceph_rgw_get_initial_lat_sum[{p_i_m}m]) or vector(0)) * 1000 / rate(ceph_rgw_get_initial_lat_count[{p_i_m}m]) * on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata',
                                             "help" : f"Average latency in milliseconds for GET operations per Ceph RGW daemon during the last {p_i_m} minutes"},
            "ceph_rgw_avg_put_latency_ms" : {"query": f"(rate(ceph_rgw_put_initial_lat_sum[{p_i_m}m]) or vector(0)) * 1000 / rate(ceph_rgw_put_initial_lat_count[{p_i_m}m]) * on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata",
                                             "help" : f"Average latency in milliseconds for PUT operations per Ceph RGW daemon during the last {p_i_m} minutes"},
            "ceph_rgw_requests_per_second": {"query": f'sum by (rgw_host) (label_replace(rate(ceph_rgw_req[{p_i_m}m]) * on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata, "rgw_host", "$1", "ceph_daemon", "rgw.(.*)"))',
                                             "help" : f"Request operations per second per Ceph RGW daemon during the last {p_i_m} minutes"},
            "ceph_rgw_get_size_bytes" :     {"query": f'label_replace(sum by (instance_id) (rate(ceph_rgw_get_b[{p_i_m}m])) * on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata, "rgw_host", "$1", "ceph_daemon", "rgw.(.*)")',
                                             "help" : f"Per-second average rate of GET operations size per Ceph RGW daemon during the last {p_i_m} minutes"},
            "ceph_rgw_put_size_bytes" :     {"query": f'label_replace(sum by (instance_id) (rate(ceph_rgw_put_b[{p_i_m}m])) * on (instance_id) group_left (ceph_daemon) ceph_rgw_metadata, "rgw_host", "$1", "ceph_daemon", "rgw.(.*)")',
                                             "help" : f"Per-second average rate of PUT operations size per Ceph RGW daemon during the last {p_i_m} minutes"},
            "ceph_mds_read_requests_per_second"   : {"query": f'rate(ceph_objecter_op_r{{ceph_daemon=~"mds.*"}}[{p_i_m}m])',
                                                     "help" : f"Per-second average rate of read requests per Ceph MDS daemon during the last {p_i_m} minutes"},
            "ceph_mds_write_requests_per_second"  : {"query": f'rate(ceph_objecter_op_w{{ceph_daemon=~"mds.*"}}[{p_i_m}m])',
                                                     "help" : f"Per-second average rate of write requests per Ceph MDS daemon during the last {p_i_m} minutes"},
            "ceph_mds_client_requests_per_second" : {"query": f'rate(ceph_mds_server_handle_client_request[{p_i_m}m])',
                                                     "help" : f"Per-second average rate of client requests per Ceph MDS daemon during the last {p_i_m} minutes"},
            "ceph_mds_reply_latency_avg_ms" : {"query": f'avg(rate(ceph_mds_reply_latency_sum[{p_i_m}m]) or vector(0) / on (ceph_daemon) rate(ceph_mds_reply_latency_count[{p_i_m}m]) * 1000)',
                                               "help" : f"Average of the per-second average rate of reply latency(seconds) per Ceph MDS daemon during the last {p_i_m} minutes"},
            "ceph_mds_reply_latency_max_ms" : {"query": f'max(rate(ceph_mds_reply_latency_sum[{p_i_m}m]) or vector(0) / on (ceph_daemon) rate(ceph_mds_reply_latency_count[{p_i_m}m]) * 1000)',
                                               "help" : f"Maximum of the per-second average rate of reply latency(seconds) per Ceph MDS daemon during the last {p_i_m} minutes"},
            "ceph_mds_reply_latency_min_ms" : {"query": f'min(rate(ceph_mds_reply_latency_sum[{p_i_m}m]) or vector(0) / on (ceph_daemon) rate(ceph_mds_reply_latency_count[{p_i_m}m]) * 1000)',
                                               "help" : f"Minimum of the per-second average rate of reply latency(seconds) per Ceph MDS daemon during the last {p_i_m} minutes"},
            "hw_cpu_busy"                          : {"query": f"1- rate(node_cpu_seconds_total{{mode='idle'}}[{p_i_m}m])",
                                                      "help" : f"Percentaje of CPU utilization per core during the last {p_i_m} minutes"},
            "hw_ram_utilization"                   : {"query": f'(node_memory_MemTotal_bytes -(node_memory_MemFree_bytes + node_memory_Cached_bytes + node_memory_Buffers_bytes + node_memory_Slab_bytes))/node_memory_MemTotal_bytes',
                                                      "help" : "RAM utilization"},
            "hw_node_physical_disk_read_ops_rate"  : {"query": f"rate(node_disk_reads_completed_total[{p_i_m}m])",
                                                      "help" : f"Per-second average rate of read operations per physical storage device in the host during the last {p_i_m} minutes"},
            "hw_node_physical_disk_write_ops_rate" : {"query": f"rate(node_disk_writes_completed_total[{p_i_m}m])",
                                                      "help" : f"Per-second average rate of write operations per physical storage device in the host during the last {p_i_m} minutes"},
            "hw_disk_utilization_rate"             : {"query": f"rate(node_disk_io_time_seconds_total[{p_i_m}m])",
                                                      "help" : f"Per-second average rate of input/output operations time(seconds) per physical storage device in the host during the last {p_i_m} minutes"},
            "hw_network_bandwidth_receive_load_bytes" : {"query": f"rate(node_network_receive_bytes_total[{p_i_m}m])",
                                                         "help" : f"Per-second average rate of received bytes per network card in the host during the last {p_i_m} minutes"},
            "hw_network_bandwidth_transmit_load_bytes": {"query": f"rate(node_network_transmit_bytes_total[{p_i_m}m])",
                                                         "help" : f"Per-second average rate of transmitted bytes per network card in the host during the last {p_i_m} minutes"},
            "ceph_nvmeof_gateway_total"                        : {"query": "count by(group) (ceph_nvmeof_gateway_info) or vector(0)",
                                                                  "help" : "Number of Ceph NVMe-oF daemons or gatways running"},
            "ceph_nvmeof_subsystem_total"                      : {"query": "count by(group) (count by(nqn,group) (ceph_nvmeof_subsystem_metadata))",
                                                                  "help" : "Number of Ceph NVMe-oF subsystems running"},
            "ceph_nvmeof_reactor_total"                        : {"query": 'max by(group) (max by(instance) (count by(instance) (ceph_nvmeof_reactor_seconds_total{mode="busy"})) * on(instance) group_right ceph_nvmeof_gateway_info)',
                                                                  "help" : "Number of reactors per gateway"},
            "ceph_nvmeof_gateway_reactor_cpu_seconds_total"    : {"query": f'max by(group) (avg by(instance) (rate(ceph_nvmeof_reactor_seconds_total{{mode="busy"}}[{p_i_m}m])) * on(instance) group_right ceph_nvmeof_gateway_info)',
                                                                   "help" : "Highest gateway CPU load"},
            "ceph_nvmeof_namespaces_total"                     : {"query": "max by(group) (count by(instance) (count by(bdev_name,instance) (ceph_nvmeof_bdev_metadata )) * on(instance) group_right ceph_nvmeof_gateway_info)",
                                                                  "help" : "Total number of namespaces"},
            "ceph_nvmeof_capacity_exported_bytes_total"        : {"query": "topk(1,sum by(instance) (ceph_nvmeof_bdev_capacity_bytes)) * on(instance) group_left(group) ceph_nvmeof_gateway_info",
                                                                  "help" : "Ceph NVMe-oF total capacity exposed"},
            "ceph_nvmeof_clients_connected_total "             : {"query": "count by(instance) (sum by(instance,host_nqn) (ceph_nvmeof_host_connection_state == 1)) * on(instance) group_left(group) ceph_nvmeof_gateway_info",
                                                                  "help" : "Number of clients connected to Ceph NVMe-oF"},
            "ceph_nvmeof_gateway_iops_total "                  : {"query": f"sum by(instance) (rate(ceph_nvmeof_bdev_reads_completed_total[{p_i_m}m]) + rate(ceph_nvmeof_bdev_writes_completed_total[{p_i_m}m])) * on(instance) group_left(group) ceph_nvmeof_gateway_info",
                                                                  "help" : "IOPS per Ceph NVMe-oF gateway"},
            "ceph_nvmeof_subsystem_iops_total"                 : {"query": f"sum by(group,nqn) (((rate(ceph_nvmeof_bdev_reads_completed_total[{p_i_m}m]) + rate(ceph_nvmeof_bdev_writes_completed_total[{p_i_m}m])) * on(instance,bdev_name) group_right ceph_nvmeof_subsystem_namespace_metadata) * on(instance) group_left(group) ceph_nvmeof_gateway_info)",
                                                                  "help" : "IOPS per Ceph NVMe-oF subsystem"},
            "ceph_nvmeof_gateway_throughput_bytes_total"       : {"query": f"sum by(instance) (rate(ceph_nvmeof_bdev_read_bytes_total[{p_i_m}m]) + rate(ceph_nvmeof_bdev_written_bytes_total[{p_i_m}m])) * on(instance) group_left(group) ceph_nvmeof_gateway_info",
                                                                  "help" : "Throughput per Ceph NVMe-oF gateway"},
            "ceph_nvmeof_subsystem_throughput_bytes_total"     : {"query": f"sum by(group,nqn) (((rate(ceph_nvmeof_bdev_read_bytes_total[{p_i_m}m]) + rate(ceph_nvmeof_bdev_written_bytes_total[{p_i_m}m])) * on(instance,bdev_name) group_right ceph_nvmeof_subsystem_namespace_metadata) * on(instance) group_left(group) ceph_nvmeof_gateway_info)",
                                                                  "help" : "Throughput per Ceph NVMe-oF subsystem"},
            "ceph_nvmeof_gateway_read_avg_latency_seconds"     : {"query": f"avg by(group,instance) (((rate(ceph_nvmeof_bdev_read_seconds_total[{p_i_m}m]) / rate(ceph_nvmeof_bdev_reads_completed_total[{p_i_m}m])) > 0) * on(instance) group_left(group) ceph_nvmeof_gateway_info)",
                                                                  "help" : "Read latency average in seconds per Ceph NVMe-oF gateway"},
            "ceph_nvmeof_gateway_write_avg_latency_seconds "   : {"query": f"avg by(group,instance) (((rate(ceph_nvmeof_bdev_write_seconds_total[{p_i_m}m]) / rate(ceph_nvmeof_bdev_writes_completed_total[{p_i_m}m])) > 0) * on(instance) group_left(group) ceph_nvmeof_gateway_info)",
                                                                  "help":  "Write average in seconds per Ceph NVMe-oF gateway"},
            "ceph_nvmeof_gateway_read_p95_latency_seconds"     : {"query": f"quantile by(group,instance) (.95,((rate(ceph_nvmeof_bdev_read_seconds_total[{p_i_m}m]) / (rate(ceph_nvmeof_bdev_reads_completed_total[{p_i_m}m]) >0)) * on(instance) group_left(group) ceph_nvmeof_gateway_info))",
                                                                  "help":  "Read latency for 95{%} of the Ceph NVMe-oF gateways"},
            "ceph_nvmeof_gateway_write_p95_latency_seconds"    : {"query": f"quantile by(group,instance) (.95,((rate(ceph_nvmeof_bdev_write_seconds_total[{p_i_m}m]) / (rate(ceph_nvmeof_bdev_writes_completed_total[{p_i_m}m]) >0)) * on(instance) group_left(group) ceph_nvmeof_gateway_info))",
                                                                  "help":  "Write latency for 95{%} of the Ceph NVMe-oF gateways"}
        }

        errors = []
        performance_metrics = {}
        t1 = time.time()
        try:
            prometheus = Prometheus(self.agent)

            # Metrics retrieval
            query_errors = 0
            for k, v in queries.items():
                try:
                    data = prometheus.query(v["query"])
                    # remove single metric timestamps
                    try:
                        for metric in data['data']['result']:
                            metric["value"] = metric["value"][1:]
                    except Exception:
                        pass
                    performance_metrics[k] = {"result": data['data']['result']}
                except Exception as e:
                    self.agent.log.error(f"Error reading performance metric \"{k}\": {e}")
                    query_errors += 1
                    continue

            if query_errors:
                errors.append(f"Error getting metrics from Prometheus. Got {query_errors} errors. Active Ceph Manager log contains details")

            # Prometheus server health
            prometheus_status = prometheus.status()
            targets_down = list(filter(lambda x: x['health'] != 'up', prometheus_status['data']['activeTargets']))
            if targets_down:
                errors.append(f"Error(scrape targets not up): Not able to retrieve metrics from {targets_down} targets. Review Prometheus server status")

            # Ceph status
            performance_metrics["ceph_health_detail"] = json.loads(self.agent.get('health')['json'])
        except Exception as e:
            msg = f"Error collecting performance metrics: {e}"
            self.agent.log.error(msg)
            errors.append(msg)

        performance_metrics["ceph_version"] = self.agent.version
        total_time = round((time.time() - t1) * 1000, 2)
        performance_metrics['time_to_get_performance_metrics_ms'] = total_time
        self.agent.log.debug(f"Time to get performance metrics: {total_time} ms")
        performance_metrics['timestamp'] = t1
        performance_metrics['human_timestamp'] = datetime.fromtimestamp(t1).strftime('%Y-%m-%d %H:%M:%S')

        # Performance report status
        if errors:
            performance_metrics["status"] = "\n".join(errors)
        else:
            performance_metrics["status"] = "OK"

        # performance data compressed and serialized to a JSON string
        performance_json = json.dumps(performance_metrics)
        cctx = zstandard.ZstdCompressor()
        compressed = cctx.compress(performance_json.encode('utf-8'))
        compressed_base64 = base64.b64encode(compressed).decode('utf-8')


        return {"perfstats": {
                            "file_stamp": performance_metrics['human_timestamp'],
                            "file_stamp_ms": int(t1 * 1000),
                            "local_file_stamp": performance_metrics['human_timestamp'],
                            "nd_stats": compressed_base64,
                            "ng_stats": "",
                            "nm_stats": "",
                            "nn_stats": "",
                            "nv_stats": "",
                            "node_number": 1,     # because IBM Call Home reqs.
                            "nodes_in_cluster": 1 # because IBM Call Home reqs.
                            }
                }


