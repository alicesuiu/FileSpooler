package spooler;

import utils.CachedThreadPool;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class FileExecutor {
    private BlockingQueue<FileElement> queue;
    private final CachedThreadPool threadPool;
    private final int nrThreads;

    FileExecutor(BlockingQueue<FileElement> queue, int nrThreads) {
        this.queue = queue;
        this.nrThreads = nrThreads;
        threadPool = new CachedThreadPool(nrThreads, 1, TimeUnit.SECONDS);
    }

    public BlockingQueue<FileElement> getQueue() {
        return queue;
    }

    public CachedThreadPool getThreadPool() {
        return threadPool;
    }

    public int getNrThreads() {
        return nrThreads;
    }
}
