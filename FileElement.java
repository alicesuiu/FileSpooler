import java.io.File;
import java.util.UUID;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

public class FileElement implements Delayed {
    private final File file;
    private int nrTries;
    private long time;
    private String md5;
    private long xxhash;
    private final String surl;
    private final long size;
    private final String run;
    private final UUID guid;
    private final long ctime;
    private final String metaaccPeriod;
    private final String metaFilePath;

    public FileElement(String md5, String surl, long size, String run, UUID guid, long ctime, String metaaccPeriod, String metaFilePath) {
        this.md5 = md5;
        this.surl = surl;
        this.size = size;
        this.run = run;
        this.guid = guid;
        this.ctime = ctime;
        this.metaaccPeriod = metaaccPeriod;
        this.metaFilePath = metaFilePath;
        nrTries = 0;
        time = System.currentTimeMillis();
        file = new File(surl);
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

    public long getXXHash() {
        return xxhash;
    }

    public String getMetaFilePath() {
        return metaFilePath;
    }

    public String getSurl() {
        return surl;
    }

    public long getSize() {
        return size;
    }

    public String getRun() {
        return run;
    }

    public UUID getGuid() {
        return guid;
    }

    public long getCtime() {
        return ctime;
    }

    public String getMetaaccPeriod() {
        return metaaccPeriod;
    }

    public void setXXHash(long xxhash) {
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
        sb.append("fileName='").append(surl).append('\'');
        sb.append(", fileSize=").append(size);
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
