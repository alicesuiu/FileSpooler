package spooler;

import java.util.EventListener;

interface FileListener extends EventListener {
    void onCreated(FileEvent event);
}
