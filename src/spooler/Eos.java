package spooler;

import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.monitoring.Timing;
import alien.se.SEUtils;
import lia.util.process.ExternalProcess.ExitStatus;
import utils.ProcessWithTimeout;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author asuiu
 * @since March 30, 2021
 */
public class Eos {
    private static final Monitor monitor = MonitorFactory.getMonitor(Eos.class.getCanonicalName());

    private static long getWaitTimeSend(FileElement element) {
        long bandwith;

        bandwith = (100000 << 3) / Main.nrFilesOnSend.get() / (element.getNrTries() + 1);
        return element.getFile().length() / bandwith;
    }

    static ExitStatus transfer(FileElement element) throws IOException, InterruptedException {
        long timeWaitSend;
        ProcessBuilder shellProcess;
        Process process;
        ProcessWithTimeout processTimeout;
        List<String> cmd = new ArrayList<>();

        cmd.add("eos");
        cmd.add(SEUtils.getSE(element.getSeName()).seioDaemons);
        cmd.add("cp");
        cmd.add("-n");
        cmd.add("-s");
        //cmd.add("-b");
        //cmd.add("33554432");
        cmd.add("--checksum");
        cmd.add("file:" + element.getFile().getAbsolutePath());
        cmd.add(element.getSurl());

        // TODO for rate limiting :
        // -t <integer value, in MB/s>
        // @see https://gitlab.cern.ch/jalien/jalien/blob/master/src/main/java/alien/io/protocols/Xrootd.java#L904

        try (Timing t = new Timing(monitor, "transfer_execution_time")) {
            shellProcess = new ProcessBuilder();
            shellProcess.command(cmd);
            process = shellProcess.start();
            processTimeout = new ProcessWithTimeout(process, shellProcess);
            timeWaitSend = getWaitTimeSend(element);
            processTimeout.waitFor(timeWaitSend, TimeUnit.SECONDS);

            return processTimeout.getExitStatus();
        }
    }
}
