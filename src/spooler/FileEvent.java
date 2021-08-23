package spooler;

import java.io.File;
import java.util.EventObject;

class FileEvent extends EventObject {
    FileEvent(File file) {
        super(file);
    }

    File getFile() {
        return (File) getSource();
    }
}
