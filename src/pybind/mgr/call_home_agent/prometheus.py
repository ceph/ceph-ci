import requests
from typing import Optional

class Prometheus():

    def __init__(self, mgr) -> None:
        self.mgr = mgr
        self.url = self.prometheus_url()

    def prometheus_url(self) -> str:
        daemon_list = self.mgr.remote('cephadm', 'list_daemons', service_name='prometheus')
        if daemon_list.exception_str:
            raise Exception(f"Error finding the Prometheus instance: {daemon_list.exception_str}")
        if len(daemon_list.result) < 1:
            raise Exception(f"Can't find the Prometheus instance")

        d = daemon_list.result[0]
        host = d.ip if d.ip else d.hostname  # ip is of type str
        port = str(d.ports[0]) if d.ports else ""  # ports is a list of ints
        if not (host and port):
            raise Exception(f"Can't get Prometheus IP and/or port from manager")

        return f"http://{host}:{port}/api/v1"

    def get(self, endpoint: str, params: Optional[dict] = None) -> dict:
        """
        Execute a Prometheus query and return the result as dict
        """
        result = {}
        try:
            r = requests.get(f"{self.url}/{endpoint}", params=params)
            r.raise_for_status()
            result = r.json()
        except Exception as e:
            raise Exception(f"Error executing Prometheus query: {e} - {result}")
        return result

    def query(self, query: str) -> dict:
        return self.get("query", {'query': query})

    def status(self) -> dict:
        """Get information about prometheus server status"""
        return self.get("targets")

