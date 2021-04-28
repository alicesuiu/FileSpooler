import utils.ProcessWithTimeout;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Eos {

    public Eos() {}

    private static long getWaitTimeSend(FileElement element) {
        long bandwith;

        bandwith = (100000 << 3) / Main.nrFilesOnSend.get() / (element.getNrTries() + 1);
        return element.getFile().length() / bandwith;
    }

    public static EosCommand transfer(FileElement element) throws IOException, InterruptedException {
        boolean transfer;
        long timeWaitSend;
        ProcessBuilder shellProcess;
        Process process;
        ProcessWithTimeout processTimeout;
        List<String> cmd = new ArrayList<>();
        StringBuilder output;

        cmd.add("eos");
        cmd.add(Main.spoolerProperties.gets("eosServer", Main.defaultEosServer));
        cmd.add("cp");
        cmd.add("-n");
        cmd.add("-s");
        cmd.add("--checksum");
        cmd.add("file:" + element.getFile().getAbsolutePath());
        cmd.add(Main.spoolerProperties.gets("destinationDir", Main.defaultDestDir)
            + "/" + element.getFile().getName());

        element.setDurl(Main.spoolerProperties.gets("destinationDir", Main.defaultDestDir)
            + "/" + element.getFile().getName());
        shellProcess = new ProcessBuilder();
        shellProcess.command(cmd);
        process = shellProcess.start();
        processTimeout = new ProcessWithTimeout(process, shellProcess);
        timeWaitSend = getWaitTimeSend(element);
        transfer = processTimeout.waitFor(timeWaitSend, TimeUnit.SECONDS);
        output = processTimeout.getStdout();
        return new EosCommand(transfer, output);
    }
}
