public class EosCommand {
    private final boolean status;
    private StringBuilder output;

    public EosCommand(boolean status, StringBuilder output) {
        this.status = status;
        this.output = output;
    }

    public boolean isStatus() {
        return status;
    }

    public StringBuilder getOutput() {
        return output;
    }
}
