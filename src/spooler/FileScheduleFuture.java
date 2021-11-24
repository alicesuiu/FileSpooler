package spooler;

import java.util.concurrent.*;

public class FileScheduleFuture<V> implements RunnableScheduledFuture<V> {
    private final FileOperator operator;
    private final RunnableScheduledFuture<V> futureTask;

    public FileScheduleFuture(FileOperator operator, RunnableScheduledFuture<V> futureTask) {
        this.operator = operator;
        this.futureTask = futureTask;
    }

    @Override
    public boolean isPeriodic() {
        return futureTask.isPeriodic();
    }

    @Override
    public long getDelay(TimeUnit unit) {
        return futureTask.getDelay(unit);
    }

    @Override
    public int compareTo(Delayed o) {
        return futureTask.compareTo(o);
    }

    @Override
    public void run() {
        futureTask.run();
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return futureTask.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled() {
        return futureTask.isCancelled();
    }

    @Override
    public boolean isDone() {
        return futureTask.isDone();
    }

    @Override
    public V get() throws InterruptedException, ExecutionException {
        return futureTask.get();
    }

    @Override
    public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return futureTask.get(timeout, unit);
    }

    public FileOperator getOperator() {
        return operator;
    }
}
