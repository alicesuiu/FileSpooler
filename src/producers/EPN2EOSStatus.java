package producers;

import alien.config.ConfigUtils;
import lazyj.Utils;
import lazyj.mail.Mail;
import lazyj.mail.Sendmail;
import lia.Monitor.Store.Cache;
import lia.Monitor.monitor.AppConfig;
import lia.Monitor.monitor.Result;
import lia.Monitor.monitor.TimestampedResult;
import lia.Monitor.monitor.monPredicate;
import lia.Monitor.JiniClient.Store.DataProducer;
import lia.web.utils.Formatare;
import policymaker.MonitorNode;
import policymaker.MonitorNodeUtils;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * @author costing, asuiu
 * @since 2023-06-19
 */
public class EPN2EOSStatus implements DataProducer {
    private static final long REARM_INTERVAL = 1000L * 60 * 60;
    private static final long EXECUTION_INTERVAL = 1000L * 60;
    private static final int CONSECUTIVE_ERRORS = 5;
    private static final long START_SENDING_ALERTS = 5 * 60 * 1000L;

    private static enum ErrorCodes {
        METADATA_DIR_NOT_WRITABLE(1, "Metadata directory (/data/epn2eos_tool/epn2eos) is not writable"),
        REGISTRATION_DIR_NOT_WRITABLE(2, "Registration directory (/data/epn2eos_tool/daqSpool) is not writable"),
        TRANSFER_ERRORS_DIR_NOT_WRITABLE(4, "Transfer errors directory (/data/epn2eos_tool/error) is not writable"),
        REGISTRATION_ERRORS_DIR_NOT_WRITABLE(8, "Registration errors directory (/data/epn2eos_tool/errorReg) is not writable"),
        DISK_FULL(16, "EPN2EOS exited since the disk was full and it could not manage its files");
        private final int value;
        private final String value_string;
        ErrorCodes(final int value, final String value_string) {
            this.value = value;
            this.value_string = value_string;
        }
    }
    private static final Logger logger = ConfigUtils.getLogger(EPN2EOSStatus.class.getCanonicalName());
    private long lastRun = 0;
    private long lastDiskEmailSent = 0;
    private long lastRunningEmailSent = 0;
    private int errorsDiskSeenSoFar = 0;
    private int errorsRunningSeenSoFar = 0;
    private String emailBody = null;
    private String subject = null;
    private Set<String> prevRunningNodesStatus = new TreeSet<>();
    RuntimeMXBean rb = ManagementFactory.getRuntimeMXBean();
    @Override
    public Vector<Object> getResults() {
        if (System.currentTimeMillis() - lastRun < EXECUTION_INTERVAL)
            return null;

        try {
            final boolean allNodesOk = getNodesDiskStatus();
            if (allNodesOk) {
                errorsDiskSeenSoFar = 0;
                lastDiskEmailSent = 0;
            }
            else {
                if (++errorsDiskSeenSoFar >= CONSECUTIVE_ERRORS &&
                        (System.currentTimeMillis() - lastDiskEmailSent) >= REARM_INTERVAL) {
                    sendMail();
                    lastDiskEmailSent = System.currentTimeMillis();
                }
            }

            Set<String> currentRunningNodesStatus =  getNodesRunningStatus();
            final boolean allProdNodesOk = currentRunningNodesStatus.isEmpty();
            if (allProdNodesOk) {
                errorsRunningSeenSoFar = 0;
                lastRunningEmailSent = 0;
                prevRunningNodesStatus.clear();
            }
            else {
                if (rb.getUptime() < START_SENDING_ALERTS)
                    return null;
                errorsRunningSeenSoFar += 1;
                if (((errorsRunningSeenSoFar >= CONSECUTIVE_ERRORS) &&
                        (!prevRunningNodesStatus.equals(currentRunningNodesStatus))) ||
                        (System.currentTimeMillis() - lastRunningEmailSent) >= REARM_INTERVAL) {
                    sendMail();
                    lastRunningEmailSent = System.currentTimeMillis();
                    prevRunningNodesStatus = currentRunningNodesStatus;
                }
            }
        }
        finally {
            lastRun = EXECUTION_INTERVAL;
            emailBody = null;
            subject = null;
        }

        return null;
    }

    private void sendMail() {
        try {
            final Mail m = new Mail();
            m.sFrom = "epn2eos-support@cern.ch";
            m.sTo = "epn2eos-support@cern.ch";
            m.sSubject = subject;
            m.sHTMLBody = emailBody;
            m.sBody = Utils.htmlToText(m.sHTMLBody);

            final Sendmail s = new Sendmail(m.sFrom, AppConfig.getProperty("lia.util.mail.MailServer", "cernmx.cern.ch"));
            s.send(m);
        } catch (final Throwable t) {
            logger.log(Level.WARNING, "Cannot send mail", t);
        }
    }

    /**
     * @return <code>true</code> if all nodes are in a good state, <code>false</code> otherwise and {@link #emailBody} filled with the message to (maybe) send out
     */
    private boolean getNodesDiskStatus() {
        final monPredicate pred = Formatare.toPred("alicdb3.cern.ch/ALIEN_spooler.Main_Nodes/*/-180000/-1/disk_full_error");

        final Vector<?> v = Cache.getLastValues(pred);
        final List<TimestampedResult> l = Cache.filterByTime(v, pred);

        final Map<ErrorCodes, Set<String>> nodesInState = new TreeMap<>();

        emailBody = "";

        for (final TimestampedResult tr : l) {
            final Result r = (Result) tr;

            final int param = (int) r.param[0];
            final String nodeName = r.NodeName;

            if (param <= 0)
                continue;

            for (final ErrorCodes ec : ErrorCodes.values())
                if ((param & ec.value) != 0)
                    nodesInState.computeIfAbsent(ec, (k) -> new TreeSet<>()).add(nodeName);
        }

        for (final Map.Entry<ErrorCodes, Set<String>> entry : nodesInState.entrySet())
            emailBody += "<B>" + entry.getKey().value_string + "</B><br>Nodes: " + String.join(", ", entry.getValue()) + "<br><br>";

        if (!nodesInState.isEmpty()) {
            subject = "EPN2EOS disk status - errors detected";
            emailBody = "Dear colleagues,<br><br>Please find below the list of nodes in various error states detected by EPN2EOS.<br><br>" + emailBody
                    + "Thank you for checking their disk issues,<br><br>The watchdog at work<br>";
        }
        return nodesInState.isEmpty();
    }

    private Set<String> getNodesRunningStatus() {
        Set<MonitorNode> allNodes = new TreeSet<>(MonitorNodeUtils.getAllNodes().values());
        MonitorNodeUtils.updateNodeStatus(allNodes);
        Set<MonitorNode> prodNodes = MonitorNodeUtils.getProdNodes(allNodes);
        Set<MonitorNode> downNodes = MonitorNodeUtils.getDownNodes(prodNodes);

        if (!downNodes.isEmpty()) {
            subject = "EPN2EOS running status - errors detected";
            emailBody = "Dear colleagues,<br><br>The watchdog has detected that " + downNodes.size() + " out of "
                    + prodNodes.size() + " EPN nodes are not running the EPN2EOS service. Please find below the "
                    + "list of nodes for which the EPN2EOS service seems to be down as it has not reported anything for "
                    + "more than 15 minutes:<br><br>"
                    + downNodes.stream().map(MonitorNode::getEpn).collect(Collectors.joining(", ")) + "<br><br>"
                    + "More details here: <a href=\"http://alimonitor.cern.ch/stats?page=epn2eos/overview\">EPN2EOS file "
                    + "transfer service</a><br><br>A summary of the current status of the service on the production nodes "
                    + "can be seen below:<br><table><tr><th>EPN</th><th>Status</th></tr>";

            for (MonitorNode node : prodNodes) {
                String color = "MediumSeaGreen";
                String uptime = "up";
                if (!node.isUptime()) {
                    color = "Tomato";
                    uptime = "down";
                }
                String row = "<tr><td>" + node.getEpn() + "</td><td style=\"background-color:" + color + ";color:white;\">"
                        + uptime + "</td></tr>";
                emailBody += row;
            }

            emailBody += "</table><br><br>Best regards,<br>The watchdog<br>";
        }
        return downNodes.stream().map(MonitorNode::getEpn).collect(Collectors.toSet());
    }
}
