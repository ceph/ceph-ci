# NVMe-oF Teuthology Test Suite Overview

Under `qa/suites/nvmeof` there are **3 suite families** and, in practical teuthology terms, **9 concrete workload/test scenarios** plus **3 upgrade starting-point variants**.

## High-level count

### Concrete scenarios you can run
- **Basic suite:** 4 workloads (1 extended with a copy test)
- **Thrash suite:** 4 combinations
- **Upgrade suite:** 3 start-version variants, all feeding into 1 common upgrade validation flow

If you count each final suite matrix combination as a separate test definition:

- **basic:** 4
- **thrash:** 2 setup variants × 2 thrasher/workload variants = **4**
- **upgrade:** 3 starting distro/version variants = **3**

**Total runnable scenarios: 11**

If instead you count unique logical test types:
- 4 basic logical tests
- 4 thrash logical combinations
- 1 upgrade flow exercised from 3 different starting versions

**Total unique logical tests: 9**

---

## How teuthology composes these

These directories are not each a single monolithic test file. Teuthology builds a final run from fragments:
- cluster topology
- install/start fragments
- workload fragments
- thrasher fragments
- upgrade steps

So the "number of tests" depends on whether you mean:
1. **distinct workload YAML fragments**, or
2. **full composed runnable scenarios**.

For `qa/suites/nvmeof/basic`, the main workload-bearing files are in `qa/suites/nvmeof/basic/workloads`.  
For `qa/suites/nvmeof/thrash`, teuthology combines one file from `gateway-initiator-setup`, one from `thrashers`, and one from `workloads`.  
For `qa/suites/nvmeof/upgrade`, teuthology combines one cluster file, one start-version file, then shared upgrade steps and workload validation.

---

# 1. Basic suite

Cluster topology:
- `qa/suites/nvmeof/basic/clusters/4-gateways-2-initiator.yaml`  
  This defines:
  - **4 NVMe-oF gateway hosts**
  - **2 initiator-only clients**: `client.0`, `client.1`
  - Ceph mons/mgr/osds colocated on gateway hosts
  - monitor tuning and log ignore rules for transient gateway conditions

Base install:
- `qa/suites/nvmeof/basic/base/install.yaml`

### Basic tests count: 4

## 1.1 Initiator distribution test
File: `qa/suites/nvmeof/basic/workloads/nvmeof_initiator.yaml`

### What it sets up
- Deploys the `nvmeof` task
- Uses:
  - `subsystems_count: 3`
  - `namespaces_count: 20` per subsystem
- Waits for gateway service (`cephadm.wait_for_service`)
- Runs subsystem setup script (`nvmeof/setup_subsystem.sh`)

### What it validates
Then it runs, on two initiators:
- `nvmeof/basic_tests.sh`
- `nvmeof/fio_test.sh --start_ns 1 --end_ns 30 --rbd_iostat` on `client.0`
- `nvmeof/basic_tests.sh`
- `nvmeof/fio_test.sh --start_ns 31 --end_ns 60` on `client.1`

### Interpretation
This is the main **multi-initiator functional + IO** test:
- verifies subsystem exposure
- verifies initiator connectivity from two clients
- verifies namespace slicing across clients
- verifies sustained IO with `fio`
- collects RBD IO stats on one side via `--rbd_iostat`

---

## 1.2 mTLS test
File: `qa/suites/nvmeof/basic/workloads/nvmeof_mtls.yaml`

### What it sets up
- Deploys NVMe-oF with:
  - `pool_name: mypool`
  - `subsystems_count: 3`
  - `namespaces_count: 20`
  - `create_mtls_secrets: true`

### What it validates
First workunit runs:
- `nvmeof/setup_subsystem.sh`
- `nvmeof/basic_tests.sh`
- `nvmeof/fio_test.sh --rbd_iostat`

Then a second workunit runs:
- `nvmeof/mtls_test.sh`

### Interpretation
This is the **secure transport/authentication test**:
- validates gateway deployment with mTLS secret creation
- checks that normal NVMe-oF functionality still works under mTLS
- runs a dedicated mTLS validation script afterward

So this test is about **certificate-based secure connectivity plus regression coverage for normal IO**.

---

## 1.3 Namespace lifecycle test
File: `qa/suites/nvmeof/basic/workloads/nvmeof_namespaces.yaml`

### What it sets up
- Deploys NVMe-oF with:
  - `subsystems_count: 3`
  - `namespaces_count: 20`

### What it validates
First workunit:
- `nvmeof/setup_subsystem.sh`
- `nvmeof/basic_tests.sh`

Second workunit:
- on `client.0`: `nvmeof/fio_test.sh --rbd_iostat`
- on `client.1`:
  - `nvmeof/basic_tests.sh`
  - `nvmeof/namespace_test.sh`
  - `nvmeof/cross_namespace_copy_test.sh`

Environment includes:
- `NEW_NAMESPACES_COUNT: '5'`

### Interpretation
This is the **namespace management / dynamic namespace test**:
- validates baseline subsystem behavior
- runs IO while namespace manipulation likely occurs
- exercises adding or managing **5 new namespaces**
- confirms namespace lifecycle operations do not break connectivity or IO
- validates the NVMe Simple Copy command (`nvme copy`) against a live namespace

This is aimed at **control-plane correctness while data-plane IO is active**.

---

## 1.4 Scalability / gateway topology change test
File: `qa/suites/nvmeof/basic/workloads/nvmeof_scalability.yaml`

### What it sets up
- Deploys NVMe-oF with:
  - `subsystems_count: 3`
  - `namespaces_count: 20`

It also adds a Ceph log ignorelist for transient gateway-down conditions:
- `NVMEOF_GATEWAY_DOWN`

### What it validates
First workunit:
- `nvmeof/setup_subsystem.sh`
- `nvmeof/basic_tests.sh`
- `nvmeof/fio_test.sh --rbd_iostat`

Second workunit:
- `client.0`: `nvmeof/fio_test.sh`
- `client.3`: three invocations of `nvmeof/scalability_test.sh`
  - `nvmeof.a,nvmeof.b`
  - `nvmeof.b,nvmeof.c,nvmeof.d`
  - `nvmeof.b,nvmeof.c`

Environment:
- `SCALING_DELAYS: '400'`
- `RUNTIME: '1200'`

### Interpretation
This is the **gateway scale/reconfiguration resilience test**:
- starts with healthy IO
- keeps IO running
- changes gateway participation sets using `scalability_test.sh`
- checks system behavior through gateway topology changes / reduced sets / expanded sets

This is less about "more namespaces" and more about **service continuity when gateway membership or serving topology changes**.

---

# 2. Thrash suite

Base files:
- Cluster: `qa/suites/nvmeof/thrash/clusters/4-gateways-1-initiator.yaml`
- Install: `qa/suites/nvmeof/thrash/base/install.yaml`

This suite is compositional:
- **2 setup variants**
- **2 thrasher variants**
- **1 workload variant**

So total runnable composed tests:
**2 × 2 × 1 = 4**

## Setup variants

### 2.1 Standard higher-subsystem setup
File: `qa/suites/nvmeof/thrash/gateway-initiator-setup/16-subsys-4-namespace.yaml`

This configures:
- `subsystems_count: 16`
- `namespaces_count: 4` per subsystem

Then runs:
- `nvmeof/setup_subsystem.sh`
- `nvmeof/basic_tests.sh`

#### Meaning
This prepares a **moderately wide topology** with many subsystems and validates baseline connectivity before chaos starts.

---

### 2.2 Large namespace count / no huge pages setup
File: `qa/suites/nvmeof/thrash/gateway-initiator-setup/10-subsys-90-namespace-no_huge_pages.yaml`

This configures:
- `subsystems_count: 10`
- `namespaces_count: 90` per subsystem

Then it exports and edits the orchestrator spec:
- `ceph orch ls nvmeof --export`
- injects `spdk_mem_size: 8192` via `sed`
- reapplies and redeploys the service via `ceph orch apply`

Then runs:
- `nvmeof/setup_subsystem.sh`
- `nvmeof/basic_tests.sh`

#### Meaning
This is a **stressier pre-chaos configuration**:
- many namespaces
- altered SPDK memory behavior
- explicit redeploy before testing

This looks designed to validate that gateway deployment remains functional in a heavier namespace configuration before thrashing starts.

---

## Thrasher variants

### 2.3 NVMe-oF daemon thrasher
File: `qa/suites/nvmeof/thrash/thrashers/nvmeof_thrash.yaml`

Runs:
- `nvmeof.thrash`
  - `checker_host: 'client.0'`
  - `randomize: False`

It also broadens the Ceph ignorelist for expected health noise:
- OSD down/degraded
- single gateway / gateway unavailable
- cephadm daemon failures/placement noise

#### Meaning
This is the core **gateway daemon chaos test**:
- repeatedly disrupts NVMe-oF gateway daemons
- checks from `client.0`
- accepts expected transient health warnings during chaos

---

### 2.4 Combined NVMe-oF + MON thrasher
File: `qa/suites/nvmeof/thrash/thrashers/nvmeof_mon_thrash.yaml`

Runs:
- `nvmeof.thrash`
- `mon_thrash`

`mon_thrash` parameters:
- `revive_delay: 60`
- `thrash_delay: 60`
- `thrash_many: true`

#### Meaning
This is the **control-plane + data-plane chaos** case:
- thrashes gateways
- also destabilizes monitor quorum
- validates service resilience when both Ceph control-plane and NVMe-oF service placement are under pressure

This is likely the harshest chaos scenario in the suite.

---

## Workload variant

### 2.5 IO under thrash
File: `qa/suites/nvmeof/thrash/workloads/fio.yaml`

Runs:
- `nvmeof/fio_test.sh --random_devices 32`

Environment:
- `RBD_POOL: mypool`
- `NVMEOF_GROUP: mygroup0`
- `RUNTIME: '1200'`

#### Meaning
This is the workload paired with each thrash scenario:
- long-running `fio`
- random device selection across 32 devices
- designed to catch disconnects, stalls, pathing failures, or IO corruption during daemon/mon disruptions

---

## Final thrash scenario matrix

The 4 runnable scenarios are:

1. `16-subsys-4-namespace.yaml` + `nvmeof_thrash.yaml` + `fio.yaml`
2. `16-subsys-4-namespace.yaml` + `nvmeof_mon_thrash.yaml` + `fio.yaml`
3. `10-subsys-90-namespace-no_huge_pages.yaml` + `nvmeof_thrash.yaml` + `fio.yaml`
4. `10-subsys-90-namespace-no_huge_pages.yaml` + `nvmeof_mon_thrash.yaml` + `fio.yaml`

---

# 3. Upgrade suite

This suite validates upgrade of an already deployed NVMe-oF environment.

Cluster topology:
- `qa/suites/nvmeof/upgrade/0-clusters/4-gateways-1-initiator.yaml`

There are **3 start-version variants**, each followed by the same upgrade/validation path.

## Start-version variants count: 3

### 3.1 Start from squid-nvmeof branch
File: `qa/suites/nvmeof/upgrade/1-start-distro/1-start-centos_9.stream-squid.yaml`

Installs:
- Ceph branch `squid-nvmeof` via `install`
- cephadm image `quay.ceph.io/ceph-ci/ceph:squid-nvmeof`

Then deploys NVMe-oF with:
- `subsystems_count: 3`
- `namespaces_count: 20`

---

### 3.2 Start from tentacle branch
File: `qa/suites/nvmeof/upgrade/1-start-distro/1-start-centos_9.stream-tentacle.yaml`

Installs:
- Ceph branch `tentacle`
- cephadm image `quay.ceph.io/ceph-ci/ceph:tentacle`

Then deploys the same baseline NVMe-oF configuration.

---

### 3.3 Start from released v20.2.0
File: `qa/suites/nvmeof/upgrade/1-start-distro/1-start-centos_9.stream-v20.2.0.yaml`

Installs:
- Ceph `tag: v20.2.0`
- container image `quay.io/ceph/ceph:v20.2.0`

Again deploys the same NVMe-oF topology.

---

## Common upgrade flow

After any of the 3 starts, teuthology applies the same following steps.

### 3.4 Baseline subsystem setup
File: `qa/suites/nvmeof/upgrade/2-setup_subsystem.yaml`

Runs:
- `nvmeof/setup_subsystem.sh`
- `nvmeof/basic_tests.sh`

#### Meaning
Ensures the starting deployment is healthy before initiating the upgrade.

---

### 3.5 Upgrade execution
File: `qa/suites/nvmeof/upgrade/3-upgrade/simple.yaml`

Runs on `mon.a` through `cephadm.shell`:
- pre-upgrade status capture: `ceph health detail`, `ceph orch ps`, `ceph -s`
- disables insecure global-id reclaim warnings
- starts orchestrated upgrade:
  - `ceph orch upgrade start --image quay.ceph.io/ceph-ci/ceph:$sha1`

#### Meaning
This is the actual **Ceph/NVMe-oF upgrade trigger** to the candidate image identified by `sha1`.

---

### 3.6 Wait and verify upgrade completion
File: `qa/suites/nvmeof/upgrade/4-wait.yaml`

Loops while upgrade is in progress:
- `ceph orch upgrade status`, `ceph orch ps`, `ceph versions`, `ceph health detail`

Then validates:
- NVMe-oF image setting: `ceph config get mgr mgr/cephadm/container_image_nvmeof`
- NVMe-oF daemons exist: `ceph orch ps | grep "nvmeof"`
- NVMe-oF daemons are running: `grep "running"`
- all daemons converge to one overall version: `ceph versions | jq -e '.overall | length == 1'`

#### Meaning
This checks **upgrade convergence and daemon health**, specifically ensuring NVMe-oF daemons survive and end up running on the target image/version.

---

### 3.7 Post-upgrade workload under NVMe-oF thrash
File: `qa/suites/nvmeof/upgrade/5-workloads/nvmeof-thrasher.yaml`

Runs:
1. baseline functional check: `nvmeof/basic_tests.sh`
2. gateway thrasher: `nvmeof.thrash`
3. IO validation: `nvmeof/fio_test.sh --rbd_iostat`

#### Meaning
This is stronger than a simple post-upgrade smoke test:
- first proves upgraded deployment still functions
- then deliberately thrashes gateways
- then runs IO to validate resilience after upgrade

So the upgrade suite verifies both **upgrade success** and **post-upgrade operational robustness**.

---

# What each suite is really covering

## Coverage summary by intent

| Suite   | Scenario                           | Main purpose                                              |
|---------|------------------------------------|-----------------------------------------------------------|
| basic   | initiator                          | multi-initiator connectivity and IO distribution          |
| basic   | mtls                               | mTLS-secured deployment and secure connectivity           |
| basic   | namespaces                         | namespace lifecycle, copy command validation while IO runs |
| basic   | scalability                        | gateway membership/topology scaling under IO              |
| thrash  | 16x4 + nvmeof_thrash               | gateway daemon chaos with moderate topology               |
| thrash  | 16x4 + nvmeof_mon_thrash           | gateway + monitor chaos with moderate topology            |
| thrash  | 10x90 no_huge_pages + nvmeof_thrash | gateway chaos with heavy namespace config                |
| thrash  | 10x90 no_huge_pages + nvmeof_mon_thrash | combined chaos with heavy namespace/SPDK config      |
| upgrade | squid start                        | upgrade path from squid-nvmeof baseline                   |
| upgrade | tentacle start                     | upgrade path from tentacle baseline                       |
| upgrade | v20.2.0 start                      | upgrade path from released v20.2.0 baseline               |

---

# Bottom line

## If you want the strict runnable-scenario count
There are **11 tests/scenarios** under `qa/suites/nvmeof`:
- **4** basic
- **4** thrash
- **3** upgrade

## If you want unique logical test definitions
There are **9 logical tests**:
- **4** basic workload definitions
- **4** thrash combinations
- **1** upgrade flow exercised from **3 different starting versions**
