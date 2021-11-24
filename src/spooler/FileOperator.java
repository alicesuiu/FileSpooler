package spooler;

public abstract class FileOperator implements Runnable {
    private final FileElement element;

    public FileOperator(FileElement element) {
        this.element = element;
    }

    public FileElement getElement() {
        return element;
    }

    abstract public void run();
}
