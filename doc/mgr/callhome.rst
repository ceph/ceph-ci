.. _mgr-callhome:

IBM Call Home Agent module
==========================

Overview
--------

The IBM Ceph Call Home Agent module sends data about the Ceph cluster to IBM systems like IBM Call Home or IBM storage Insights.

This information is used to get improved user support experience. It prevents the user from collecting information about the Ceph cluster in any support incidence, and at the same time, makes it easy for support engineers to have the right information needed to diagnose and solve incidences.

Equally important is the live surveillance capacity. This allows support teams to anticipate or detect immediately any anomaly situation.

The owner of the cluster must explicitly opt-in to IBM Call Home and/or IBM Storage Insights services. Visual warnings on the Ceph dashboard will be shown to remember the possibility to opt-in to these services.

Enabling the Ceph Call Home Agent when installing the Ceph cluster
-------------------------------------------------------------------

By default, the Ceph Call Home Agent is disabled. This means that no Ceph cluster data is sent to IBM support systems.

The agent can be set to enable when the Ceph Cluster is going to be installed. The cephadm bootstrap command provides parameters to allow that:
.. prompt:: bash $
    cephadm bootstrap … [--enable-call-home] [–ibm-tenant-id TENANT-ID]

* **--enable-call-home**

Enables the Ceph Call Home Agent and data communication with IBM Call Home. It will start to send data when the Ceph Cluster is available for the first time.

* **–ibm-tenant-id <IBM-TENANT-ID>**

Introduces in the reports the user IBM tenant-id which will allow IBM Storage Insight systems to process properly the information provided by the Ceph Call Home Agent.

This parameter cannot be used alone, it must be provided together with the “enable-call-home" parameter.
.. note::
  The user must be sure that the ``IBM-TENANT-ID`` provided is the right one. An error in this parameter will make impossible the identification of the cluster in IBM Storage Insights systems, and therefore to provide the technical support for the cluster.

Summary of results depending on the parameters used in cephadm bootstrap command:

+----------------------------+-------------------------+-----------------------------------------+
| enable-call-home provided? | ibm-tenant-id provided? |                 result                  |
+============================+=========================+=========================================+
|             yes            |           no            |  Agent sends periodically Ceph cluster  |
|                            |                         |  data to IBM Call Home systems          |
+----------------------------+-------------------------+-----------------------------------------+
|             yes            |          yes            |  Agent sends periodically Ceph cluster  |
|                            |                         |  data to IBM Call Home and this         |
|                            |                         |  information includes the IBM tenant ID.|
|                            |                         |  This fact allows IBM Storage Insights  |
|                            |                         |  systems to process properly the        |
|                            |                         |  information and to provide support     |
|                            |                         |  service over the cluster               |
+----------------------------+-------------------------+-----------------------------------------+
|             no             |           no            | The agent remains disabled after Ceph   |
|                            |                         | cluster installation. No information    |
|                            |                         | sent to IBM systems                     |
+----------------------------+-------------------------+-----------------------------------------+
|             no             |          yes            | Not supported.                          |
|                            |                         | The agent remains disabled after Ceph   |
|                            |                         | cluster installation. No information    |
|                            |                         | sent to IBM systems                     |
+----------------------------+-------------------------+-----------------------------------------+

Enabling the Ceph Call Home Agent when the cluster is installed.
----------------------------------------------------------------

It can be done using the command line interface or the Ceph dashboard.

**Command line interface**

Enable the call home agent manager module:
.. prompt:: bash $
    ceph mgr module enable call_home_agent

Add the IBM tenant ID to allow IBM Storage Insights to process the cluster information for the customer.
.. prompt:: bash $
    ceph callhome set tenant <IBM-TENANT-ID>

.. note::
  The user must be sure that the IBM-TENANT-ID provided is the right one. An error in this parameter will make impossible the identification of the cluster in IBM Storage Insights systems, and therefore to provide the technical support for the cluster.

**Ceph Dashboard**

<TODO, including screen captures>

Disabling the Ceph Call Home Agent
----------------------------------

It can be done using the command line interface or the Ceph dashboard. Once the Agent is disabled, the communication of cluster data to IBM support systems will be stopped completely.

The agent can be enabled again in any moment, reestablishing the communication of data with IBM support systems

**Command line interface**

Disable the call home agent manager module:
.. prompt:: bash $
    ceph mgr module disable call_home_agent

**Ceph Dashboard**

<TODO, including screen captures>

Ceph Call home Agent available reports
--------------------------------------

The agent sends periodically to IBM support systems the following reports. All the reports can be obtained and examined by the user (see Ceph Call Home Agent commands section)

**Reports (default frequency):**
* Inventory (24 hours): A complete description of the Ceph cluster composition. It includes information about hosts and storage devices composing the cluster. Monitor, manager and OSDs daemons. Storage utilization. Storage pools. Placement groups and services offered.
* Status (5 min): Current cluster status and health, alerts, storage capacity used/free.
* Performance (5 min): Performance information (iops (r/w), latency, bandwidth used, etc...) about hosts, storage devices and OSDs
* Last-contact (5 min): Ceph cluster health.


Ceph Call home commands
-----------------------

The agent provides the following commands:

**show:**
Prints the selected report. (inventory, status, performance, last_contact)
Example:
.. prompt:: bash $
    ceph callhome show inventory

**send:**
Sends immediately the selected report (inventory, status, performance, last_contact)
.. prompt:: bash $
    ceph callhome send inventory

**list-tenants:**
Provides information about IBM tenant-ids owned by a certain user
.. prompt:: bash $
    ceph callhome list-tenants owner_ibm_id, owner_company_name, owner_first_name, owner_last_name, owner_email

    note: owner_ibm_id is the IBM w3id (usually an email)

**set tenant:**
Set the IBM tenant ID to be included in reports sent to IBM Storage Insights.
.. prompt:: bash $
    ceph callhome set tenant owner_tenant_id owner_ibm_id owner_company_name owner_first_name owner_last_name owner_email

**callhome get user info:**
Show the customer information included in reports sent to IBM Call Home and Storae Insights systems.
.. prompt:: bash $
    ceph callhome get user info


Ceph Call Home Agent configuration
----------------------------------

The configuration provided by default is established as the idoneous one. Usually, it will not require any change, but if needed, consider that these configuration parameters define the working behavior of the agent, a misconfiguration can drive to stop data transmission.

Configuration parameters can be provided as environment variables (with the CHA prefix) or as Ceph configuration options. Environment variables have precedence over Ceph config options, but a restart of the manager daemons will be needed to apply the changes. This kind of configuration must be used in kubernetes environments, where the start of the manager pod can use a config map (or any other persistence strategy for the env. vars) with the required configuration settings expressed as environment variables.

When using Ceph config options, the changes in the configuration are applied immediately and it does not require any further action (restart/failover) over the manager.

**Configuration options:**

**target/CHA_TARGET:**
IBM Call Home endpoint. Defines the endpoint where the agent sends all the support information about the Ceph cluster

Example:
.. prompt:: bash $
    ceph config set mgr mgr/call_home_agent/target https://ibmesupport.com

    EXPORT CHA_TARGET=https://ibmesupport.com

**interval_inventory_report_seconds/CHA_INTERVAL_INVENTORY_REPORT_SECONDS:**
Inventory report shipping frequency

Example:
.. prompt:: bash $
    ceph config set mgr mgr/call_home_agent/ interval_inventory_report_seconds 300

    EXPORT CHA_INTERVAL_INVENTORY_REPORT_SECONDS=300

Default value: One week

**interval_performance_report_seconds/CHA_INTERVAL_PERFORMANCE_REPORT_SECONDS:**
Performance report shipping frequency

Example:
.. prompt:: bash $
    ceph config set mgr mgr/call_home_agent/ interval_performance_report_seconds 300

    EXPORT CHA_INTERVAL_PERFORMANCE_REPORT_SECONDS=300

Default value: 5 minutes

**interval_status_report_seconds/CHA_INTERVAL_STATUS_REPORT_SECONDS:**
Status report shipping frequency

Example:
.. prompt:: bash $
    ceph config set mgr mgr/call_home_agent/ interval_status_report_seconds 300

    EXPORT CHA_INTERVAL_STATUS_REPORT_SECONDS=300

Default value: 5 minutes

**interval_last_contact_report_seconds/CHA_INTERVAL_LAST_CONTACT_REPORT_SECONDS:**
Last contact report shipping frequency

Example:
.. prompt:: bash $
    ceph config set mgr mgr/call_home_agent/ interval_last_contact_report_seconds 300

    EXPORT CHA_INTERVAL_LAST_CONTACT_REPORT_SECONDS=300

Default value: 5 minutes

**proxy/CHA_PROXY:**
Internal network Proxy used to reach external IBM Call Home endpoint.

Example:
.. prompt:: bash $
    ceph config set mgr mgr/call_home_agent/proxy http://128.64.64.12:8090

    EXPORT CHA_PROXY=http://128.64.64.12:8090

Default value: Not set

**target_space/CHA_TARGET_SPACE:**
Target space for IBM Call Home events

Example:
.. prompt:: bash $
    ceph config set mgr mgr/call_home_agent/target_space dev

    EXPORT CHA_TARGET_SPACE=dev

Default value: Prod
.. note::
    This is a setting typically only used for module development purposes. Change it can cause to lose communication of reports with IBM support systems.

**si_web_service_url/CHA_SI_WEB_SERVICE_URL:**
Integration with IBM SI FAB3 systems. URL used to get the list of IBM tenant ids for a specific IBM user id.

Example:
.. prompt:: bash $
    ceph config set mgr mgr/call_home_agent/ si_web_service_url https://fab3.ibm.com

    EXPORT CHA_SI_WEB_SERVICE_URL= https://fab3.ibm.com

**Configuration options to store customer data:**
These configuration options cannot be set using env vars, only Ceph commands.

The configurations options used to store **IBM Call Home customer data**, can be set using Ceph config commands. The options available are:
.. prompt:: bash $
    mgr/call_home_agent/customer_address
    mgr/call_home_agent/customer_company_name
    mgr/call_home_agent/customer_country_code
    mgr/call_home_agent/customer_email
    mgr/call_home_agent/customer_first_name
    mgr/call_home_agent/customer_last_name
    mgr/call_home_agent/customer_phone


The configurations options used to store **IBM Storage Insights customer data**, can be set using the ``callhome set tenant`` command or using ceph configuration options commands. The options available are:
.. prompt:: bash $
    mgr/call_home_agent/owner_company_name
    mgr/call_home_agent/owner_email
    mgr/call_home_agent/owner_first_name
    mgr/call_home_agent/owner_ibm_id
    mgr/call_home_agent/owner_last_name
    mgr/call_home_agent/owner_tenant_id
