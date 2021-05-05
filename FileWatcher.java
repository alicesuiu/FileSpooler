import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;

public class FileWatcher implements Runnable  {
    private List<FileListener> listeners;
    private final File directory;

    public FileWatcher(File directory) {
        this.directory = directory;
        listeners = new ArrayList<>();
    }

    @Override
    public void run() {
        try {
            Path path = Paths.get(directory.getName());
            WatchService watchService =  path.getFileSystem().newWatchService();
            path.register(watchService, StandardWatchEventKinds.ENTRY_CREATE);
            boolean poll = true;
            while (poll) {
                poll = pollEvents(watchService);
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
            Thread.currentThread().interrupt();
        }
    }

    private boolean pollEvents(WatchService watchService) throws InterruptedException {
        WatchKey key = watchService.take();
        Path path = (Path) key.watchable();
        for (WatchEvent<?> event : key.pollEvents()) {
            notifyListeners(event.kind(), path.resolve((Path) event.context()).toFile());
        }
        return key.reset();
    }

    private void notifyListeners(WatchEvent.Kind<?> kind, File file) {
        FileEvent event = new FileEvent(file);
        if (kind == StandardWatchEventKinds.ENTRY_CREATE) {
            for (FileListener listener : listeners) {
                listener.onCreated(event);
            }
        }
    }

    public FileWatcher addListener(FileListener listener) {
        listeners.add(listener);

        return this;
    }

    public void watch() {

        if (directory.exists()) {
            Thread thread = new Thread(this);
            thread.setDaemon(true);
            thread.start();
            thread.setName("Watcher Thread");
        }
    }
}
