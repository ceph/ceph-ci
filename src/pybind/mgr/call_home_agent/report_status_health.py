from .report import Report, ReportTimes, EventGeneric
import time
import json
import math
from .prometheus import Prometheus

class ReportStatusHealth(Report):
    def __init__(self, agent) -> None:
        super().__init__(agent, 'status', [EventStatusHealth])

class EventStatusHealth(EventGeneric):
    def generate(self, report_times: ReportTimes):
        super().generate('status', 'ceph_health','Ceph cluster status and health', report_times)

        self.data["body"]["event_transaction_id"] = f"IBM_event_RedHatMarine_ceph_{self.agent.ceph_cluster_id}_{report_times.time_ms}_status_event"
        self.data["body"]["complete"] = True
        content = self.gather()
        self.set_content(content)
        try:
            status = content['status']['health']['status']
        except:
            status = "Unknown status"
        self.data["body"]["state"] = status
        return self

    def gather(self) -> dict:
        r, outb, outs = self.agent.mon_command({
            'prefix': 'status',
            'format': 'json'
        })
        if r:
            error = f"status command failed: {outs}"
            self.agent.log.error(error)
            return {'status': {'error': error}}
        try:
            status_dict = json.loads(outb)
            status_dict["ceph_version"] = self.agent.version
            status_dict["health_detail"] = json.loads(self.agent.get('health')['json'])
            status_dict["support"] = self.get_support_metrics()
            status_dict["support"]["health_status"] = status_dict["health_detail"].get("status", "")
            status_dict["support"]["health_summary"] = self.get_health_summary(status_dict["health_detail"])
            return {'status' : status_dict}
        except Exception as e:
            self.agent.log.exception(str(e))
            return {'status' : {'exception': str(e)}}

    def get_health_summary(self, ceph_health: dict) -> str:
        """
        Stringify Ceph's health status
        """
        try:
            health_items = []
            for error_key, error_details in ceph_health["checks"].items():
                details = "\n".join([item["message"] for item in error_details.get("detail",[])])
                health_items.append(f'{error_key}({error_details["severity"]}): {error_details["summary"]["message"]}\n{details}')
            return "\n\n".join(health_items)
        except Exception as e:
            return f"Error getting health status: {e}"

    def get_support_metrics(self) -> dict:
        """
        Collect cluster metrics needed for Ceph support team tools
        """
        support_metrics = {}
        s_i_m = math.ceil(self.agent.interval_status_report_seconds / 60)  # Status Interval in Minutes
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
            'network_receive_errors': f'avg(increase(node_network_receive_errs_total{{device!="lo"}}[{s_i_m}m]))',
            'network_send_errors': f'avg(increase(node_network_transmit_errs_total{{device!="lo"}}[{s_i_m}m]))',
            'network_receive_packet_drops': f'avg(increase(node_network_receive_drop_total{{device!="lo"}}[{s_i_m}m]))',
            'network_transmit_packet_drops': f'avg(increase(node_network_transmit_drop_total{{device!="lo"}}[{s_i_m}m]))',
            'inconsistent_mtu': 'sum(node_network_mtu_bytes * (node_network_up{device!="lo"} > 0) ==  scalar(max by (device) (node_network_mtu_bytes * (node_network_up{device!="lo"} > 0)) !=  quantile by (device) (.5, node_network_mtu_bytes * (node_network_up{device!="lo"} > 0))  )or node_network_mtu_bytes * (node_network_up{device!="lo"} > 0) ==  scalar(min by (device) (node_network_mtu_bytes * (node_network_up{device!="lo"} > 0)) !=  quantile by (device) (.5, node_network_mtu_bytes * (node_network_up{device!="lo"} > 0))) or vector(0))',
            'pool_number': 'count(ceph_pool_bytes_used)',
            'raw_capacity_bytes': 'sum(ceph_osd_stat_bytes)',
            'raw_capacity_consumed_bytes': 'sum(ceph_pool_bytes_used)',
            'logical_stored_bytes': 'sum(ceph_pool_stored)',
            'pool_growth_bytes': f'sum(delta(ceph_pool_stored[{s_i_m}m]))',
            'pool_bandwidth_bytes': f'sum(rate(ceph_pool_rd_bytes[{s_i_m}m]) + rate(ceph_pool_wr_bytes[{s_i_m}m]))',
            'pg_per_osd_ratio':'(avg(ceph_osd_numpg)/sum(ceph_pg_total))*100',
            'monitors_number': 'count(ceph_mon_metadata)',
            'monitors_not_in_quorum_number': 'count(ceph_mon_quorum_status!=1) or on() vector(0)',
            'clock_skews_number': 'ceph_health_detail{name="MON_CLOCK_SKEW"} or on() vector(0)',
        }

        try:
            prometheus = Prometheus(self.agent)
            t1 = time.time()
            for k, v in queries.items():
                data = prometheus.query(v)
                try:
                    support_metrics[k] = float(data['data']['result'][0]['value'][1])
                except Exception as e:
                     self.agent.log.error(f"Error reading status metric for support \"{k}\": {e} - {data}")
            total_time = round((time.time() - t1) * 1000, 2)
            support_metrics['time_to_get_support_data_ms'] = total_time
            self.agent.log.debug(f"Time to get support data for status report: {total_time} ms")
        except Exception as e:
            self.agent.log.error(f"Error collecting support data for status report: {e}")

        return support_metrics

