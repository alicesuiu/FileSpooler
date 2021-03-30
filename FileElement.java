import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

public class FileElement implements Delayed {
    private final String fileName;
    private final long fileSize;
    private final String absolutePath;
    private int nrTries;
    private long time;
    private String md5;

    public FileElement(String fileName, long fileSize, String absolutePath) {
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.absolutePath = absolutePath;
        nrTries = 0;
        time = System.currentTimeMillis();
    }

    public String getFileName() {
        return fileName;
    }

    public long getFileSize() {
        return fileSize;
    }

    public int getNrTries() {
        return nrTries;
    }

    public String getAbsolutePath() {
        return absolutePath;
    }

    public long getTime() {
        return time;
    }

    public String getMd5() {
        return md5;
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
        sb.append("fileName='").append(fileName).append('\'');
        sb.append(", fileSize=").append(fileSize);
        sb.append(", absolutePath='").append(absolutePath).append('\'');
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
