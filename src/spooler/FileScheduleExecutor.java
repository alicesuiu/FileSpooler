package spooler;

import java.util.concurrent.*;

public class FileScheduleExecutor extends ScheduledThreadPoolExecutor {

    public FileScheduleExecutor(int corePoolSize, ThreadFactory threadFactory) {
        super(corePoolSize, threadFactory);
    }

    @Override
    protected <V> RunnableScheduledFuture<V> decorateTask(Runnable runnable, RunnableScheduledFuture<V> task) {
        if (runnable instanceof Spooler) {
            return new FileScheduleFuture<>((Spooler) runnable, task);
        }
        return super.decorateTask(runnable, task);
    }
}
