import java.io.File;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

public class FileElement implements Delayed {
    private final File file;
    private int nrTries;
    private long time;
    private String md5;
    private String xxhash;

    public FileElement(File file) {
        this.file = file;
        nrTries = 0;
        time = System.currentTimeMillis();
    }

    public File getFile() {
        return file;
    }

    public int getNrTries() {
        return nrTries;
    }

    public long getTime() {
        return time;
    }

    public String getMd5() {
        return md5;
    }

    public String getXxhash() {
        return xxhash;
    }

    public void setXxhash(String xxhash) {
        this.xxhash = xxhash;
    }

    public void setMd5(String md5) {
        this.md5 = md5;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public void setNrTries(int nrTries) {
        this.nrTries = nrTries;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FileElement{");
        sb.append("fileName='").append(file.getName()).append('\'');
        sb.append(", fileSize=").append(file.length());
        sb.append(", absolutePath='").append(file.getAbsolutePath()).append('\'');
        sb.append(", nrTries=").append(nrTries);
        sb.append(", time=").append(time);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public long getDelay(TimeUnit timeUnit) {
        long difference = time - System.currentTimeMillis();
        difference /= 1000;
        return timeUnit.convert(difference, TimeUnit.SECONDS);
    }

    @Override
    public int compareTo(Delayed delayed) {
        return Long.compare(getDelay(TimeUnit.SECONDS), delayed.getDelay(TimeUnit.SECONDS));
    }
}
