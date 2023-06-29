from .report import Report, ReportTimes, EventGeneric
from .report_status_health import EventStatusHealth
import time

class ReportInventory(Report):
    def __init__(self, agent) -> None:
        super().__init__(agent, 'inventory', [EventInventory])

class EventInventory(EventGeneric):
    def gather(self):
        inventory = {}
        inventory["crush_map"] = self.agent.get("osd_map_crush")
        inventory["devices"] = self.agent.get("devices")
        inventory["df"] = self.agent.get("df")
        inventory["fs_map"] = self.agent.get("fs_map")
        inventory["hosts"] = self.agent.list_servers()
        inventory["manager_map"] = self.agent.get("mgr_map")
        inventory["mon_map"] = self.agent.get("mon_map")
        inventory["osd_map"] = self.agent.get("osd_map")
        inventory["osd_metadata"] = self.agent.get("osd_metadata")
        inventory["osd_tree"] = self.agent.get("osd_map_tree")
        inventory["pg_summary"] = self.agent.get("pg_summary")
        inventory["service_map"] = self.agent.get("service_map")
        inventory["hardware_status"] = self._get_hardware_status()

        # Gather status report
        inventory.update(EventStatusHealth(self.agent).gather())
        return {'inventory': inventory}

    def generate(self, report_times: ReportTimes):
        super().generate('inventory', 'ceph_inventory', 'Ceph cluster composition', report_times)
        self.set_content(self.gather())
        return self

    def _get_hardware_status(self) -> dict:
        try:
            hw_status = self.agent.remote('orchestrator', 'node_proxy_summary')
            if hw_status.exception_str:
                raise Exception(hw_status.exception_str)
            return hw_status.result
        except Exception as e:
            self.agent.log.exception(str(e))
            return {'error': str(e)}

