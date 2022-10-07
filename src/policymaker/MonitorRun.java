package policymaker;

public class MonitorRun {
    private long run;
    private long cnt;
    private long size;
    private double eta;
    private long lastSeen;

    public MonitorRun(long run, long cnt, long size, double eta, long lastSeen) {
        this.run = run;
        this.cnt = cnt;
        this.size = size;
        this.eta = eta;
        this.lastSeen = lastSeen;
    }

    public long getRun() {
        return run;
    }

    public void setRun(long run) {
        this.run = run;
    }

    public long getCnt() {
        return cnt;
    }

    public void setCnt(long cnt) {
        this.cnt = cnt;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public double getEta() {
        return eta;
    }

    public void setEta(double eta) {
        this.eta = eta;
    }

    public long getLastSeen() {
        return lastSeen;
    }

    public void setLastSeen(long lastSeen) {
        this.lastSeen = lastSeen;
    }
}
