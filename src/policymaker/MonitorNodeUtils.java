package policymaker;

import alien.config.ConfigUtils;
import lia.Monitor.Store.Cache;
import lia.Monitor.Store.Fast.DB;
import lia.Monitor.monitor.Result;
import lia.Monitor.monitor.TimestampedResult;
import lia.Monitor.monitor.monPredicate;
import lia.web.utils.Formatare;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class MonitorNodeUtils {
    private static final Logger logger = ConfigUtils.getLogger(MonitorRunUtils.class.getCanonicalName());
    private static final String regexActiveNodes = "(epn\\w+).*";
    private static final Pattern patternActiveNodes = Pattern.compile(regexActiveNodes);
    private static final String GET_ACTIVE_NODES = "http://alice-epn.cern.ch:8080/api/v0-monitoring/active-nodes";
    private static final long RUNNING_EXPIRATION_INTERVAL = 15 * 60 * 1000L;

    public static enum ErrorCodes {
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

        public int getValue() {
            return value;
        }

        public String getValue_string() {
            return value_string;
        }
    }

    public static Set<String> getProdNodes() {
        Set<String> nodes = new TreeSet<>();

        try {
            HttpClient httpClient = HttpClient.newBuilder()
                    .version(HttpClient.Version.HTTP_1_1)
                    .connectTimeout(Duration.ofSeconds(60))
                    .build();
            HttpRequest request = HttpRequest.newBuilder()
                    .GET()
                    .uri(URI.create(GET_ACTIVE_NODES))
                    .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != HttpServletResponse.SC_OK) {
                logger.log(Level.WARNING, "Response code for GET req is: " + response.statusCode());
                return nodes;
            }

            Matcher matcher = patternActiveNodes.matcher(response.body());
            while(matcher.find())
                nodes.add(matcher.group(1));
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
        return nodes;
    }

    public static Map<ErrorCodes, Set<String>> getDiskErrorNodes() {
        final monPredicate pred = Formatare.toPred("alicdb3.cern.ch/ALIEN_spooler.Main_Nodes/*/-180000/-1/disk_full_error");
        final Vector<?> v = Cache.getLastValues(pred);
        final List<TimestampedResult> l = Cache.filterByTime(v, pred);
        final Set<String> prodNodes = getProdNodes();
        final Map<ErrorCodes, Set<String>> nodes = new TreeMap<>();

        for (final TimestampedResult tr : l) {
            final Result r = (Result) tr;

            final int param = (int) r.param[0];
            final String nodeName = r.NodeName;
            Matcher matcher = patternActiveNodes.matcher(nodeName);
            String node = null;

            if (matcher.find())
                node = matcher.group(1);

            if (param <= 0)
                continue;

            if (node == null || !prodNodes.contains(node))
                continue;

            for (final ErrorCodes ec : ErrorCodes.values())
                if ((param & ec.value) != 0)
                    nodes.computeIfAbsent(ec, (k) -> new TreeSet<>()).add(node);
        }
        return nodes;
    }

    public static Map<String, Long> getJvmUptimeNodes() {
        final monPredicate pred = Formatare.toPred("alicdb3.cern.ch/ALIEN_Self_epn2eos_Nodes/*/-1/-1/jvm_uptime");
        final Vector<TimestampedResult> v = Cache.getLastValues(pred);
        Map<String, Long> nodes = new TreeMap<>();

        for (final TimestampedResult tr : v) {
            final Result r = (Result) tr;
            final long time = r.time;
            final String nodeName = r.NodeName;
            Matcher matcher = patternActiveNodes.matcher(nodeName);
            if (matcher.find())
                nodes.put(matcher.group(1), time);
        }
        return nodes;
    }

    public static Set<String> getDBNodes() {
        Set<String> nodes = new TreeSet<>();
        String select = "select epn from epn2eos_status;";
        DB db = new DB(select);
        while (db.moveNext())
            nodes.add(db.gets("epn"));
        return nodes;
    }

    public static Map<String, MonitorNode> getAllNodes() {
        Set<String> prodNodes = getProdNodes();
        Set<String> dbNodes = getDBNodes();
        Map<String, Long> monitoredNodes = getJvmUptimeNodes();
        Set<String> notProdNodes = new TreeSet<>();
        Map<String, MonitorNode> allNodes = new TreeMap<>();

        notProdNodes.addAll(dbNodes);
        notProdNodes.addAll(new TreeSet<>(monitoredNodes.keySet()));
        notProdNodes.removeAll(prodNodes);

        for (String node : prodNodes)
            allNodes.computeIfAbsent(node, (k) -> new MonitorNode(node, true, true));
        for (String node : notProdNodes)
            allNodes.computeIfAbsent(node, (k) -> new MonitorNode(node, false, true));

        for (MonitorNode node : allNodes.values()) {
            Long time = monitoredNodes.get(node.getEpn());
            if (time == null || time <= (System.currentTimeMillis() - RUNNING_EXPIRATION_INTERVAL)) {
                node.setUptime(false);
            }
        }
        return allNodes;
    }

    public static void updateNodeStatus(Set<MonitorNode> nodes) {
        nodes.forEach(MonitorNode::updateNodeDB);
    }

    public static Set<MonitorNode> getProdNodes(Set<MonitorNode> nodes) {
        return nodes.stream().filter(MonitorNode::isProduction).collect(Collectors.toCollection(TreeSet::new));
    }

    public static Set<MonitorNode> getDownNodes(Set<MonitorNode> nodes) {
        return nodes.stream().filter(n -> !n.isUptime()).collect(Collectors.toCollection(TreeSet::new));
    }
}
