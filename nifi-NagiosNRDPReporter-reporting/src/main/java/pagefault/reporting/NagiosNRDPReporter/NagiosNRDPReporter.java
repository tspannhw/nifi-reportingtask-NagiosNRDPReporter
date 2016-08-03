/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package pagefault.reporting.NagiosNRDPReporter;

import java.util.ArrayList;
import java.util.List;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.AbstractReportingTask;
import org.apache.nifi.reporting.ReportingContext;
import java.net.*;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.components.AllowableValue;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
 
import java.io.StringWriter;
import java.io.*;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactoryConfigurationError;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.DOMException;
import org.xml.sax.SAXException;

@Tags({"nagios", "nrdp", "alerting", "stats"})
@CapabilityDescription("Reports metrics to Nagios via NRDP.")
public class NagiosNRDPReporter extends AbstractReportingTask {

    protected static final String DISABLED_VALUE  = "Disabled";
    protected static final String ALERTING_VALUE  = "Alerting";
    protected static final String REPORTING_VALUE = "Reporting";
    protected static final String ENABLED_VALUE   = "Enabled";
    
    protected static final AllowableValue COMPLEX_FIELD_DISABLED_VALUE  = new AllowableValue(DISABLED_VALUE, DISABLED_VALUE,   "Do not run this functionality.");
    protected static final AllowableValue COMPLEX_FIELD_ALERTING_VALUE  = new AllowableValue(ALERTING_VALUE, ALERTING_VALUE,   "Enable alerting for this functionality. The warning and critical thresholds must be set. When these thresholds are exceeded, either a Warning or Critical message will be sent to Nagios depending on which threshold is exceeded.");
    protected static final AllowableValue COMPLEX_FIELD_REPORTING_VALUE = new AllowableValue(REPORTING_VALUE, REPORTING_VALUE, "Report the current counter for this functionality only. No warning or critical messages will be sent to Nagios, all messages will be set to normal. This is for reporting current statistics.");
    protected static final AllowableValue COMPLEX_FIELD_ENABLED_VALUE   = new AllowableValue(ENABLED_VALUE, ENABLED_VALUE,     "Enable this functionality.");
    
    public static final PropertyDescriptor URL = new PropertyDescriptor.Builder()
            .name("NRDP URL")
            .description("The URL where Nagios NRDP is running")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    
    public static final PropertyDescriptor TOKEN = new PropertyDescriptor.Builder()
            .name("NRDP Token")
            .description("The NRDP Token for Nagios NRDP")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    
    public static final PropertyDescriptor HOSTNAME = new PropertyDescriptor.Builder()
            .name("Hostname")
            .description("Hostname to use for posting to Nagios, default is value returned from DNS resolution")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    
    public static final PropertyDescriptor INCLUDE_PERFORMANCE_DATA = new PropertyDescriptor.Builder()
            .name("Enable Performance Data")
            .description("Include performance data with Nagios status data")
            .required(true)
            .allowableValues(COMPLEX_FIELD_ENABLED_VALUE, COMPLEX_FIELD_DISABLED_VALUE)
            .defaultValue(ENABLED_VALUE)
            .build();
    
    public static final PropertyDescriptor PROCESS_ALL_GROUPS = new PropertyDescriptor.Builder()
            .name("Report All Process Groups")
            .description("If False, report only the root node which includes all stats, if True, in addition report for all process groups")
            .required(true)
            .allowableValues(COMPLEX_FIELD_ENABLED_VALUE, COMPLEX_FIELD_DISABLED_VALUE)
            .defaultValue(ENABLED_VALUE)
            .build();
    
    public static final PropertyDescriptor ACTIVE_THREAD_COUNT = new PropertyDescriptor.Builder()
            .name("Active Thread Count")
            .description("Number of active threads for this process group")
            .required(true)
            .allowableValues(COMPLEX_FIELD_DISABLED_VALUE, COMPLEX_FIELD_ALERTING_VALUE, COMPLEX_FIELD_REPORTING_VALUE)
            .defaultValue(DISABLED_VALUE)
            .build();
    public static PropertyDescriptor ACTIVE_THREAD_COUNT_WARN = new PropertyDescriptor.Builder()
            .name("Active Thread Count Warning Threshold")
            .description("Warning threshold for the number of active threads for this process group")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();
    public static PropertyDescriptor ACTIVE_THREAD_COUNT_CRIT = new PropertyDescriptor.Builder()
            .name("Active Thread Count Critical Threshold")
            .description("Critical threshold for the number of active threads for this process group")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor FLOWFILES_QUEUED = new PropertyDescriptor.Builder()
            .name("Flowfiles Queued")
            .description("Number of flowfiles queued in this process group")
            .required(true)
            .allowableValues(COMPLEX_FIELD_DISABLED_VALUE, COMPLEX_FIELD_ALERTING_VALUE, COMPLEX_FIELD_REPORTING_VALUE)
            .defaultValue(DISABLED_VALUE)
            .build();
    public static PropertyDescriptor FLOWFILES_QUEUED_WARN = new PropertyDescriptor.Builder()
            .name("Flowfiles Queued Warning Threshold")
            .description("Warning threshold for the number of flowfiles queued in this process group")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();
    public static PropertyDescriptor FLOWFILES_QUEUED_CRIT = new PropertyDescriptor.Builder()
            .name("Flowfiles Queued Critical Threshold")
            .description("Critical threshold for the number of flowfiles queued in this process group")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();
    
    public static final PropertyDescriptor BYTES_QUEUED = new PropertyDescriptor.Builder()
            .name("Bytes Queued")
            .description("Bytes queued in this process group")
            .required(true)
            .allowableValues(COMPLEX_FIELD_DISABLED_VALUE, COMPLEX_FIELD_ALERTING_VALUE, COMPLEX_FIELD_REPORTING_VALUE)
            .defaultValue(DISABLED_VALUE)
            .build();
    public static PropertyDescriptor BYTES_QUEUED_WARN = new PropertyDescriptor.Builder()
            .name("Bytes Queued Warning Threshold")
            .description("Warning threshold for the number of bytes queued in this process group")
            .required(false)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .build();
    public static PropertyDescriptor BYTES_QUEUED_CRIT = new PropertyDescriptor.Builder()
            .name("Bytes Queued Critical Threshold")
            .description("Critical threshold for the number of bytes queued in this process group")
            .required(false)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .build();
    
    public static final PropertyDescriptor BYTES_IN = new PropertyDescriptor.Builder()
            .name("Bytes In")
            .description("The number of bytes received via Site-to-Site in the last 5 minutes")
            .required(true)
            .allowableValues(COMPLEX_FIELD_DISABLED_VALUE, COMPLEX_FIELD_ALERTING_VALUE, COMPLEX_FIELD_REPORTING_VALUE)
            .defaultValue(DISABLED_VALUE)
            .build();
    public static PropertyDescriptor BYTES_IN_WARN = new PropertyDescriptor.Builder()
            .name("Bytes In Warning Threshold")
            .description("Warning threshold for the number of bytes received via Site-to-Site in the last 5 minutes")
            .required(false)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .build();
    public static PropertyDescriptor BYTES_IN_CRIT = new PropertyDescriptor.Builder()
            .name("Bytes In Critical Threshold")
            .description("Critical threshold for the number of bytes received via Site-to-Site in the last 5 minutes")
            .required(false)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .build();
    
    public static final PropertyDescriptor BYTES_OUT = new PropertyDescriptor.Builder()
            .name("Bytes Out")
            .description("The number of bytes pulled from Output Ports via Site-to-Site in the last 5 minutes")
            .required(true)
            .allowableValues(COMPLEX_FIELD_DISABLED_VALUE, COMPLEX_FIELD_ALERTING_VALUE, COMPLEX_FIELD_REPORTING_VALUE)
            .defaultValue(DISABLED_VALUE)
            .build();
    public static PropertyDescriptor BYTES_OUT_WARN = new PropertyDescriptor.Builder()
            .name("Bytes Out Warning Threshold")
            .description("Warning threshold for the number of bytes pulled from Output Ports via Site-to-Site in the last 5 minutes")
            .required(false)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .build();
    public static PropertyDescriptor BYTES_OUT_CRIT = new PropertyDescriptor.Builder()
            .name("Bytes Out Critical Threshold")
            .description("Critical threshold for the number of bytes pulled from Output Ports via Site-to-Site in the last 5 minutes")
            .required(false)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .build();
    
    public static final PropertyDescriptor FLOWFILES_IN = new PropertyDescriptor.Builder()
            .name("Flowfiles In")
            .description("The number of FlowFiles received via Site-to-Site in the last 5 minutes")
            .required(true)
            .allowableValues(COMPLEX_FIELD_DISABLED_VALUE, COMPLEX_FIELD_ALERTING_VALUE, COMPLEX_FIELD_REPORTING_VALUE)
            .defaultValue(DISABLED_VALUE)
            .build();
    public static PropertyDescriptor FLOWFILES_IN_WARN = new PropertyDescriptor.Builder()
            .name("Flowfiles In Warning Threshold")
            .description("Warning threshold for the number of FlowFiles received via Site-to-Site in the last 5 minutes")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();
    public static PropertyDescriptor FLOWFILES_IN_CRIT = new PropertyDescriptor.Builder()
            .name("Flowfiles In Critical Threshold")
            .description("Critical threshold for the number of FlowFiles received via Site-to-Site in the last 5 minutes")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();
    
    public static final PropertyDescriptor FLOWFILES_OUT = new PropertyDescriptor.Builder()
            .name("Flowfiles Out")
            .description("The number of FlowFiles pulled from Output Ports via Site-to-Site in the last 5 minutes")
            .required(true)
            .allowableValues(COMPLEX_FIELD_DISABLED_VALUE, COMPLEX_FIELD_ALERTING_VALUE, COMPLEX_FIELD_REPORTING_VALUE)
            .defaultValue(DISABLED_VALUE)
            .build();
    public static PropertyDescriptor FLOWFILES_OUT_WARN = new PropertyDescriptor.Builder()
            .name("Flowfiles Out Warning Threshold")
            .description("Warning threshold for the number of FlowFiles pulled from Output Ports via Site-to-Site in the last 5 minutes")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();
    public static PropertyDescriptor FLOWFILES_OUT_CRIT = new PropertyDescriptor.Builder()
            .name("Flowfiles Out Critical Threshold")
            .description("Critical threshold for the number of FlowFiles pulled from Output Ports via Site-to-Site in the last 5 minutes")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();
    
    public static final PropertyDescriptor BYTES_READ = new PropertyDescriptor.Builder()
            .name("Bytes Read")
            .description("The number of bytes read from disk by NiFi in the last 5 minutes")
            .required(true)
            .allowableValues(COMPLEX_FIELD_DISABLED_VALUE, COMPLEX_FIELD_ALERTING_VALUE, COMPLEX_FIELD_REPORTING_VALUE)
            .defaultValue(DISABLED_VALUE)
            .build();
    public static PropertyDescriptor BYTES_READ_WARN = new PropertyDescriptor.Builder()
            .name("Bytes Read Warning Threshold")
            .description("Warning threshold for the number of bytes read from disk by NiFi in the last 5 minutes")
            .required(false)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .build();
    public static PropertyDescriptor BYTES_READ_CRIT = new PropertyDescriptor.Builder()
            .name("Bytes Read Critical Threshold")
            .description("Critical threshold for the number of bytes read from disk by NiFi in the last 5 minutes")
            .required(false)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .build();
    
    public static final PropertyDescriptor BYTES_WRITTEN = new PropertyDescriptor.Builder()
            .name("Bytes Written")
            .description("The number of bytes written to disk by NiFi in the last 5 minutes")
            .required(true)
            .allowableValues(COMPLEX_FIELD_DISABLED_VALUE, COMPLEX_FIELD_ALERTING_VALUE, COMPLEX_FIELD_REPORTING_VALUE)
            .defaultValue(DISABLED_VALUE)
            .build();
    public static PropertyDescriptor BYTES_WRITTEN_WARN = new PropertyDescriptor.Builder()
            .name("Bytes Written Warning Threshold")
            .description("Warning threshold for the number of bytes written to disk by NiFi in the last 5 minutes")
            .required(false)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .build();
    public static PropertyDescriptor BYTES_WRITTEN_CRIT = new PropertyDescriptor.Builder()
            .name("Bytes Written Critical Threshold")
            .description("Critical threshold for the number of bytes written to disk by NiFi in the last 5 minutes")
            .required(false)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .build();

    private static final Logger nifiLogger = LoggerFactory.getLogger(NagiosNRDPReporter.class);
    
    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {

        final List<PropertyDescriptor> _descriptors = new ArrayList<>();
            _descriptors.add(URL);
            _descriptors.add(TOKEN);
            _descriptors.add(HOSTNAME);
            _descriptors.add(INCLUDE_PERFORMANCE_DATA);
            _descriptors.add(PROCESS_ALL_GROUPS);
            
            _descriptors.add(ACTIVE_THREAD_COUNT);
            _descriptors.add(ACTIVE_THREAD_COUNT_WARN);
            _descriptors.add(ACTIVE_THREAD_COUNT_CRIT);
            
            _descriptors.add(FLOWFILES_QUEUED);
            _descriptors.add(FLOWFILES_QUEUED_WARN);
            _descriptors.add(FLOWFILES_QUEUED_CRIT);
            
            _descriptors.add(BYTES_QUEUED);
            _descriptors.add(BYTES_QUEUED_WARN);
            _descriptors.add(BYTES_QUEUED_CRIT);
            
            _descriptors.add(BYTES_IN);
            _descriptors.add(BYTES_IN_WARN);
            _descriptors.add(BYTES_IN_CRIT);
            
            _descriptors.add(BYTES_OUT);
            _descriptors.add(BYTES_OUT_WARN);
            _descriptors.add(BYTES_OUT_CRIT);
            
            _descriptors.add(FLOWFILES_IN);
            _descriptors.add(FLOWFILES_IN_WARN);
            _descriptors.add(FLOWFILES_IN_CRIT);
            
            _descriptors.add(FLOWFILES_OUT);
            _descriptors.add(FLOWFILES_OUT_WARN);
            _descriptors.add(FLOWFILES_OUT_CRIT);
            
            _descriptors.add(BYTES_READ);
            _descriptors.add(BYTES_READ_WARN);
            _descriptors.add(BYTES_READ_CRIT);
            
            _descriptors.add(BYTES_WRITTEN);
            _descriptors.add(BYTES_WRITTEN_WARN);
            _descriptors.add(BYTES_WRITTEN_CRIT);
            
            return _descriptors;
    }
    
    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {

        if (descriptor.equals(ACTIVE_THREAD_COUNT)) {
            NagiosNRDPReporter.ACTIVE_THREAD_COUNT_WARN = new PropertyDescriptor.Builder()
                .name("Active Thread Count Warning Level")
                .description("Warning threshold for the number of active threads for this process group")
                .required(newValue.equals(ALERTING_VALUE))
                .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
                .build();
            NagiosNRDPReporter.ACTIVE_THREAD_COUNT_CRIT = new PropertyDescriptor.Builder()
                .name("Active Thread Count Critical Level")
                .description("Critical threshold for the number of active threads for this process group")
                .required(newValue.equals(ALERTING_VALUE))
                .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
                .build();
        
        } else if (descriptor.equals(FLOWFILES_QUEUED)) {
            NagiosNRDPReporter.FLOWFILES_QUEUED_WARN = new PropertyDescriptor.Builder()
                .name("Flowfiles Queued Warning Threshold")
                .description("Warning threshold for the number of flowfiles queued in this process group")
                .required(newValue.equals(ALERTING_VALUE))
                .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
                .build();
            NagiosNRDPReporter.FLOWFILES_QUEUED_CRIT = new PropertyDescriptor.Builder()
                .name("Flowfiles Queued Critical Threshold")
                .description("Critical threshold for the number of flowfiles queued in this process group")
                .required(newValue.equals(ALERTING_VALUE))
                .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
                .build();
        
        } else if (descriptor.equals(BYTES_QUEUED)) {
            NagiosNRDPReporter.BYTES_QUEUED_WARN = new PropertyDescriptor.Builder()
                .name("Bytes Queued Warning Threshold")
                .description("Warning threshold for the number of bytes queued in this process group")
                .required(newValue.equals(ALERTING_VALUE))
                .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
                .build();
            NagiosNRDPReporter.BYTES_QUEUED_CRIT = new PropertyDescriptor.Builder()
                .name("Bytes Queued Critical Threshold")
                .description("Critical threshold for the number of bytes queued in this process group")
                .required(newValue.equals(ALERTING_VALUE))
                .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
                .build();
        
        } else if (descriptor.equals(BYTES_IN)) {
            NagiosNRDPReporter.BYTES_IN_WARN = new PropertyDescriptor.Builder()
                .name("Bytes In Warning Threshold")
                .description("Warning threshold for the number of bytes received via Site-to-Site in the last 5 minutes")
                .required(newValue.equals(ALERTING_VALUE))
                .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
                .build();
            NagiosNRDPReporter.BYTES_IN_CRIT = new PropertyDescriptor.Builder()
                .name("Bytes In Critical Threshold")
                .description("Critical threshold for the number of bytes received via Site-to-Site in the last 5 minutes")
                .required(newValue.equals(ALERTING_VALUE))
                .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
                .build();
            
        }  else if (descriptor.equals(BYTES_OUT)) {
            NagiosNRDPReporter.BYTES_OUT_WARN = new PropertyDescriptor.Builder()
                .name("Bytes Out Warning Threshold")
                .description("Warning threshold for the number of bytes pulled from Output Ports via Site-to-Site in the last 5 minutes")
                .required(newValue.equals(ALERTING_VALUE))
                .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
                .build();
            NagiosNRDPReporter.BYTES_OUT_CRIT = new PropertyDescriptor.Builder()
                .name("Bytes Out Critical Threshold")
                .description("Critical threshold for the number of bytes pulled from Output Ports via Site-to-Site in the last 5 minutes")
                .required(newValue.equals(ALERTING_VALUE))
                .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
                .build();

        } else if (descriptor.equals(FLOWFILES_IN)) {
            NagiosNRDPReporter.FLOWFILES_IN_WARN = new PropertyDescriptor.Builder()
                .name("Flowfiles In Warning Threshold")
                .description("Warning threshold for the number of FlowFiles received via Site-to-Site in the last 5 minutes")
                .required(newValue.equals(ALERTING_VALUE))
                .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
                .build();
            NagiosNRDPReporter.FLOWFILES_IN_CRIT = new PropertyDescriptor.Builder()
                .name("Flowfiles In Critical Threshold")
                .description("Critical threshold for the number of FlowFiles received via Site-to-Site in the last 5 minutes")
                .required(newValue.equals(ALERTING_VALUE))
                .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
                .build();
    
        } else if (descriptor.equals(FLOWFILES_OUT)) {
            NagiosNRDPReporter.FLOWFILES_OUT_WARN = new PropertyDescriptor.Builder()
                .name("Flowfiles Out Warning Threshold")
                .description("Warning threshold for the number of FlowFiles pulled from Output Ports via Site-to-Site in the last 5 minutes")
                .required(newValue.equals(ALERTING_VALUE))
                .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
                .build();
            NagiosNRDPReporter.FLOWFILES_OUT_CRIT = new PropertyDescriptor.Builder()
                .name("Flowfiles Out Critical Threshold")
                .description("Critical threshold for the number of FlowFiles pulled from Output Ports via Site-to-Site in the last 5 minutes")
                .required(newValue.equals(ALERTING_VALUE))
                .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
                .build();
        } else if (descriptor.equals(BYTES_READ)) {
            NagiosNRDPReporter.BYTES_READ_WARN = new PropertyDescriptor.Builder()
                .name("Bytes Read Warning Threshold")
                .description("Warning threshold for the number of bytes read from disk by NiFi in the last 5 minutes")
                .required(newValue.equals(ALERTING_VALUE))
                .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
                .build();
            NagiosNRDPReporter.BYTES_READ_CRIT = new PropertyDescriptor.Builder()
                .name("Bytes Read Critical Threshold")
                .description("Critical threshold for the number of bytes read from disk by NiFi in the last 5 minutes")
                .required(newValue.equals(ALERTING_VALUE))
                .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
                .build();
        }  else if (descriptor.equals(BYTES_WRITTEN)) {
            NagiosNRDPReporter.BYTES_WRITTEN_WARN = new PropertyDescriptor.Builder()
                .name("Bytes Written Warning Threshold")
                .description("Warning threshold for the number of bytes written to disk by NiFi in the last 5 minutes")
                .required(newValue.equals(ALERTING_VALUE))
                .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
                .build();
            NagiosNRDPReporter.BYTES_WRITTEN_CRIT = new PropertyDescriptor.Builder()
                .name("Bytes Written Critical Threshold")
                .description("Critical threshold for the number of bytes written to disk by NiFi in the last 5 minutes")
                .required(newValue.equals(ALERTING_VALUE))
                .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
                .build();
        }
    }
    
    private Document buildXML() { 
    
        DocumentBuilderFactory icFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder icBuilder;
        
        try {
            icBuilder = icFactory.newDocumentBuilder();
            Document doc = icBuilder.newDocument();
            Element mainRootElement = doc.createElement("checkresults");
            doc.appendChild(mainRootElement);
            return doc;
        } catch (ParserConfigurationException | DOMException T) {
            nifiLogger.error("{} Error creating XML document: {}", this, T);
        }
        return null;
    }
    
    private static Node addTextXMLNode(Document doc, String name, String value) {
        
        Element node = doc.createElement(name);
        node.appendChild(doc.createTextNode(value));
        return node;
    }
    
    private static Document addCheckResultServiceToXML(Document doc, String servicename, String hostname, nagiosResult nagios, boolean performanceData)
    {
        Element root = doc.getDocumentElement();
        Element node = doc.createElement("checkresult");
        node.setAttribute("checktype", "1");
        node.setAttribute("type", "service");
        
        node.appendChild(addTextXMLNode(doc, "servicename", servicename));
        node.appendChild(addTextXMLNode(doc, "hostname", hostname));
        node.appendChild(addTextXMLNode(doc, "state", String.format ("%d", nagios.getState())));
        
        if (performanceData)
            node.appendChild(addTextXMLNode(doc, "output", nagios.getOutput() + " | " + nagios.getPerformance()));
        else
            node.appendChild(addTextXMLNode(doc, "output", nagios.getOutput()));
        root.appendChild(node);
        
        return doc;
    }
    
    final static class nagiosResult {
        
        private int state = 0;
        private String output = "OK";
        private String performance = "";
        
        public nagiosResult(String label, String comment, long currentValue, long warningValue, long criticalValue, String performance_label, boolean alerting) {
            
            if (alerting && (currentValue >= criticalValue)) {
                this.state = 2;
                this.output = label + " CRIT - " + currentValue + " " + comment;
            } else if (alerting && (currentValue >= warningValue)) {
                this.state = 1;
                this.output = label + " WARN - " + currentValue + " " + comment;
            } else {
                this.state = 0;
                this.output = label + " OK - " + currentValue + " " + comment;
            }
            
            this.performance = performance_label + "=" + currentValue;
        }
        
        public nagiosResult(String label, String comment, double currentValue, double warningValue, double criticalValue, String performance_label, boolean alerting) {
            
            if (alerting && (currentValue >= criticalValue)) {
                this.state = 2;
                this.output = label + " CRIT - " + currentValue + " " + comment;
            } else if (alerting && (currentValue >= warningValue)) {
                this.state = 1;
                this.output = label + " WARN - " + currentValue + " " + comment;
            } else {
                this.state = 0;
                this.output = label + " OK - " + currentValue + " " + comment;
            }
            
            this.performance = performance_label + "=" + currentValue;
        }
        
        public nagiosResult(String label, String comment, double currentValue, double warningValue, double criticalValue, String performance_label, String formattedCurrentValue, boolean alerting) {
            
            if (alerting && (currentValue >= criticalValue)) {
                this.state = 2;
                this.output = label + " CRIT - " + formattedCurrentValue + " " + comment;
            } else if (alerting && (currentValue >= warningValue)) {
                this.state = 1;
                this.output = label + " WARN - " + formattedCurrentValue + " " + comment;
            } else {
                this.state = 0;
                this.output = label + " OK - " + formattedCurrentValue + " " + comment;
            }
            
            this.performance = performance_label + "=" + currentValue;
        }
        
        public int getState() {
            return this.state;
        }
        
        public String getOutput() {
            return this.output;
        }
        
        public String getPerformance() {
            return this.performance;
        }
    }
    
    private void printProcessGroupStatus(ProcessGroupStatus procGroup, final ReportingContext context) {
        
        try {
            String procGroupName = procGroup.getName();
            String hostname = InetAddress.getLocalHost().getHostName();
            boolean include_performance_data = false;
            
            if (context.getProperty(INCLUDE_PERFORMANCE_DATA).getValue().equals(ENABLED_VALUE))
                include_performance_data = true;
            
            if (context.getProperty(HOSTNAME).isSet())
                hostname = context.getProperty(HOSTNAME).getValue();

            Document doc = buildXML();
            
            if (!context.getProperty(ACTIVE_THREAD_COUNT).getValue().equals(DISABLED_VALUE)) {
                
                nagiosResult myResult;
                
                if (context.getProperty(ACTIVE_THREAD_COUNT).getValue().equals(ALERTING_VALUE))
                    myResult = new nagiosResult("ACTIVE THREAD COUNT", "active threads", procGroup.getActiveThreadCount(), 
                        context.getProperty(ACTIVE_THREAD_COUNT_WARN).asInteger(), 
                        context.getProperty(ACTIVE_THREAD_COUNT_CRIT).asInteger(), 
                        "active_thread_count", true);
                else
                    myResult = new nagiosResult("ACTIVE THREAD COUNT", "active threads", procGroup.getActiveThreadCount(), 0, 0, "active_thread_count", false);
                
                doc = addCheckResultServiceToXML(doc, procGroupName + " - NiFi Active Thread Count", hostname, myResult, include_performance_data);
            }
            
            if (!context.getProperty(FLOWFILES_QUEUED).getValue().equals(DISABLED_VALUE)) {
                
                nagiosResult myResult;
                
                if (context.getProperty(FLOWFILES_QUEUED).getValue().equals(ALERTING_VALUE))
                    myResult = new nagiosResult("FLOWFILES QUEUED", "flowfiles queued", procGroup.getQueuedCount(), 
                        context.getProperty(FLOWFILES_QUEUED_WARN).asLong(), 
                        context.getProperty(FLOWFILES_QUEUED_CRIT).asLong(), 
                        "flowfiles_queued", true);
                else
                    myResult = new nagiosResult("FLOWFILES QUEUED", "flowfiles queued", procGroup.getQueuedCount(), 0L, 0L, "flowfiles_queued", false);
                
                doc = addCheckResultServiceToXML(doc, procGroupName + " - NiFi Flowfiles Queued", hostname, myResult, include_performance_data);
            }
            
            if (!context.getProperty(BYTES_QUEUED).getValue().equals(DISABLED_VALUE)) {
                
                nagiosResult myResult;
                
                if (context.getProperty(BYTES_QUEUED).getValue().equals(ALERTING_VALUE))
                    myResult = new nagiosResult("BYTES QUEUED", "queued", procGroup.getQueuedContentSize(), 
                        context.getProperty(BYTES_QUEUED_WARN).asDataSize(DataUnit.B), 
                        context.getProperty(BYTES_QUEUED_CRIT).asDataSize(DataUnit.B), 
                        "bytes_queued", 
                        FormatUtils.formatDataSize(procGroup.getQueuedContentSize()), true);
                else
                    myResult = new nagiosResult("BYTES QUEUED", "queued", procGroup.getQueuedContentSize(), 
                        0.0, 0.0, "bytes_queued", FormatUtils.formatDataSize(procGroup.getQueuedContentSize()), false);
                
                doc = addCheckResultServiceToXML(doc, procGroupName + " - NiFi Data Queued", hostname, myResult, include_performance_data);
            }
            
            if (!context.getProperty(BYTES_IN).getValue().equals(DISABLED_VALUE)) {
                
                nagiosResult myResult;
                
                if (context.getProperty(BYTES_IN).getValue().equals(ALERTING_VALUE))
                    myResult = new nagiosResult("BYTES IN", "in", procGroup.getBytesReceived(), 
                        context.getProperty(BYTES_IN_WARN).asDataSize(DataUnit.B), 
                        context.getProperty(BYTES_IN_CRIT).asDataSize(DataUnit.B), 
                        "bytes_in", 
                        FormatUtils.formatDataSize(procGroup.getBytesReceived()), true);
                else
                    myResult = new nagiosResult("BYTES IN", "in", procGroup.getBytesReceived(), 0.0, 0.0, "bytes_in", 
                        FormatUtils.formatDataSize(procGroup.getBytesReceived()), false);
                
                doc = addCheckResultServiceToXML(doc, procGroupName + " - NiFi Data In", hostname, myResult, include_performance_data);
            }

            if (!context.getProperty(BYTES_OUT).getValue().equals(DISABLED_VALUE)) {
                
                nagiosResult myResult;
                
                if (context.getProperty(BYTES_OUT).getValue().equals(ALERTING_VALUE))
                    myResult = new nagiosResult("BYTES OUT", "out", procGroup.getBytesSent(), 
                        context.getProperty(BYTES_OUT_WARN).asDataSize(DataUnit.B), 
                        context.getProperty(BYTES_OUT_CRIT).asDataSize(DataUnit.B), 
                        "bytes_out", 
                        FormatUtils.formatDataSize(procGroup.getBytesSent()), true);
                else
                    myResult = new nagiosResult("BYTES OUT", "out", procGroup.getBytesSent(), 0.0, 0.0, "bytes_out", 
                        FormatUtils.formatDataSize(procGroup.getBytesSent()), false);
                
                doc = addCheckResultServiceToXML(doc, procGroupName + " - NiFi Data Out", hostname, myResult, include_performance_data);
            }

            if (!context.getProperty(FLOWFILES_IN).getValue().equals(DISABLED_VALUE)) {
                
                nagiosResult myResult;
                
                if (context.getProperty(FLOWFILES_IN).getValue().equals(ALERTING_VALUE))
                    myResult = new nagiosResult("FLOWFILES IN", "flowfiles in", procGroup.getFlowFilesReceived(), 
                        context.getProperty(FLOWFILES_IN_WARN).asLong(), 
                        context.getProperty(FLOWFILES_IN_CRIT).asLong(), 
                        "flowfiles_in", true);
                else
                    myResult = new nagiosResult("FLOWFILES IN", "flowfiles in", procGroup.getFlowFilesReceived(), 0L, 0L, "flowfiles_in", false);
                
                doc = addCheckResultServiceToXML(doc, procGroupName + " - NiFi Flowfiles In", hostname, myResult, include_performance_data);
            }
            
            if (!context.getProperty(FLOWFILES_OUT).getValue().equals(DISABLED_VALUE)) {
                
                nagiosResult myResult;
                
                if (context.getProperty(FLOWFILES_OUT).getValue().equals(ALERTING_VALUE))
                    myResult = new nagiosResult("FLOWFILES OUT", "flowfiles out", procGroup.getFlowFilesSent(), 
                        context.getProperty(FLOWFILES_OUT_WARN).asLong(), 
                        context.getProperty(FLOWFILES_OUT_CRIT).asLong(), 
                        "flowfiles_out", true);
                else
                    myResult = new nagiosResult("FLOWFILES OUT", "flowfiles out", procGroup.getFlowFilesSent(), 0L, 0L, "flowfiles_out", false);
                
                doc = addCheckResultServiceToXML(doc, procGroupName + " - NiFi Flowfiles Out", hostname, myResult, include_performance_data);
            }
            
            if (!context.getProperty(BYTES_READ).getValue().equals(DISABLED_VALUE)) {
                
                nagiosResult myResult;
                
                if (context.getProperty(BYTES_READ).getValue().equals(ALERTING_VALUE))
                    myResult = new nagiosResult("BYTES READ", "data read", procGroup.getBytesRead(), 
                        context.getProperty(BYTES_READ_WARN).asDataSize(DataUnit.B), 
                        context.getProperty(BYTES_READ_CRIT).asDataSize(DataUnit.B), 
                        "data_read", 
                        FormatUtils.formatDataSize(context.getProperty(BYTES_READ_WARN).asDataSize(DataUnit.B)), true);
                else
                    myResult = new nagiosResult("BYTES READ", "data read", procGroup.getBytesRead(), 0.0, 0.0, "data_read", false);
                
                doc = addCheckResultServiceToXML(doc, procGroupName + " - NiFi Data Read", hostname, myResult, include_performance_data);
            }
            
            if (!context.getProperty(BYTES_WRITTEN).getValue().equals(DISABLED_VALUE)) {
                
                nagiosResult myResult;
                
                if (context.getProperty(BYTES_WRITTEN).getValue().equals(ALERTING_VALUE))
                    myResult = new nagiosResult("BYTES WRITTEN", "data written", procGroup.getBytesRead(), 
                        context.getProperty(BYTES_WRITTEN_WARN).asDataSize(DataUnit.B), 
                        context.getProperty(BYTES_WRITTEN_CRIT).asDataSize(DataUnit.B), 
                        "data_written", 
                        FormatUtils.formatDataSize(procGroup.getBytesRead()), true);
                else
                    myResult = new nagiosResult("BYTES WRITTEN", "data written", procGroup.getBytesRead(), 0.0, 0.0, "data_written", false);
                
                doc = addCheckResultServiceToXML(doc, procGroupName + " - NiFi Data Written", hostname, myResult, include_performance_data);
            }
            
            Transformer transformer = TransformerFactory.newInstance().newTransformer();
            DOMSource source = new DOMSource(doc);
            StringWriter writer = new StringWriter();
            StreamResult result = new StreamResult(writer);
            
            transformer.transform(source, result);
            
            String url = context.getProperty(URL).getValue();
            String token = context.getProperty(TOKEN).getValue();
            String charset = "UTF-8";
            
            String query = String.format("token=%s&cmd=submitcheck&XMLDATA=%s\n", 
                URLEncoder.encode(token, charset), 
                URLEncoder.encode(writer.toString(), charset));
            
            try {
                URLConnection connection = new URL(url).openConnection();
                connection.setDoOutput(true);
                connection.setRequestProperty("Accept-Charset", charset);
                connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded;charset=" + charset);

                try (OutputStream output = connection.getOutputStream()) {
                    output.write(query.getBytes(charset));
                }

                int respCode = ((HttpURLConnection)connection).getResponseCode();
                
                if (respCode == 200) {
            
                    String message = null;
                    int status = 0;
                    
                    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
                    DocumentBuilder db = dbf.newDocumentBuilder();
                    Document outputDoc = db.parse(connection.getInputStream());

                    XPathFactory xpathFactory = XPathFactory.newInstance();
                    XPath xpath = xpathFactory.newXPath();

                    XPathExpression expr = xpath.compile("/result/status/text()");
                    NodeList nodes = (NodeList) expr.evaluate(outputDoc, XPathConstants.NODESET);
                    
                    if (nodes.getLength() == 1)
                        status = Integer.parseInt(nodes.item(0).getNodeValue());
                    
                    if (status == 0) {

                        nifiLogger.info("{} Posted metrics to NRDP host {}", this, url);
                    
                    } else {
                    
                        expr = xpath.compile("/result/message/text()");
                        nodes = (NodeList) expr.evaluate(outputDoc, XPathConstants.NODESET);
                    
                        if (nodes.getLength() == 1)
                            message = nodes.item(0).getNodeValue();
                    
                        nifiLogger.error("{} Error posting metrics to NRDP host {}", this, message);
                    }
                } else {
                    nifiLogger.error("{} Error accessing {} : {}", this, url, respCode);
                }
            } catch (IOException | ParserConfigurationException | SAXException | XPathExpressionException | DOMException | NumberFormatException T) {
                nifiLogger.error("{} Error connecting to NRDP server:  {}", this, T);
            }
        } catch (UnknownHostException | TransformerFactoryConfigurationError | TransformerException | UnsupportedEncodingException T) {
            nifiLogger.error("{} Error posting metrics to NRDP:  {}", this, T);
        }
    }
    
    private void itterateThroughProcessGroupStatus(ProcessGroupStatus procGroup, final ReportingContext context) {
        
        printProcessGroupStatus(procGroup, context);
        
        for (final ProcessGroupStatus childGroupStatus : procGroup.getProcessGroupStatus()) {
            itterateThroughProcessGroupStatus(childGroupStatus, context);
        }
        
    }
    
    @Override
    public void onTrigger(final ReportingContext context) {
        
        final ProcessGroupStatus rootGroupStatus = context.getEventAccess().getControllerStatus();
        
        final boolean processSubProcessgroups = context.getProperty(PROCESS_ALL_GROUPS).getValue().equals(ENABLED_VALUE);
        
        if(!processSubProcessgroups) {
            printProcessGroupStatus(rootGroupStatus, context);
        } else {
            itterateThroughProcessGroupStatus(rootGroupStatus, context);
        }
    }
}