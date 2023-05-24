package policymaker;

public class RunAction {
    private Long run;
    private String action;
    private String filter;
    private String sourcese;
    private String targetse;

    public void setRun(Long run) {
        this.run = run;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public void setFilter(String filter) {
        this.filter = filter;
    }

    public void setSourcese(String sourcese) {
        this.sourcese = sourcese;
    }

    public void setTargetse(String targetse) {
        this.targetse = targetse;
    }

    public Long getRun() {
        return run;
    }

    public String getAction() {
        return action;
    }

    public String getFilter() {
        return filter;
    }

    public String getSourcese() {
        return sourcese;
    }

    public String getTargetse() {
        return targetse;
    }

    @Override
    public String toString() {
        return "RunAction: {" +
                "run=" + run +
                ", action='" + action + '\'' +
                ", filter='" + filter + '\'' +
                ", sourcese='" + sourcese + '\'' +
                ", targetse='" + targetse + '\'' +
                '}';
    }
}
