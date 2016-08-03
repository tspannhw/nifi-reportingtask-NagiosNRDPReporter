# nifi-reportingtask-NagiosNRDPReporter

NiFi Reporting Task that reports NiFi metrics to Nagios via NRDP.

Each metric can be either disabled, alerted on, or set to report metrics only.

This enables you to set alerts on specific metrics or just ingest metrics into your monitoring system.

The metrics that can be monitored include:

    * Number of active threads for the process group
    * Number of flowfiles queued in the process group
    * Bytes queued in the process group
    * The number of FlowFiles received via Site-to-Site in the last 5 minutes
    * The number of bytes pulled from Output Ports via Site-to-Site in the last 5 minutes
    * The number of bytes read from disk by NiFi in the last 5 minutes
    * The number of bytes written to disk by NiFi in the last 5 minutes
    
These metrics/alerts can be enabled for either the root process group or all process groups in the workflow.
    
