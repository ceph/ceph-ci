import json
import subprocess


class CephResult:
    def __init__(self, returncode, stdout, stderr):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr
        self.obj = None
        if stdout:
            self.obj = json.loads(stdout)


def ceph_cmd(args, json_output=False, check=True):
    """Run a ceph CLI command and optionally parse JSON stdout."""
    cmd = ['ceph']
    if json_output:
        cmd.extend(['-f', 'json'])
    cmd.extend(args)
    proc = subprocess.run(cmd, capture_output=True, text=True)
    result = CephResult(proc.returncode, proc.stdout.strip(), proc.stderr.strip())
    if check and result.returncode != 0:
        raise AssertionError(
            f"Command failed with return code {result.returncode}: "
            f"{' '.join(cmd)}\nstdout: {result.stdout}\nstderr: {result.stderr}"
        )
    return result
