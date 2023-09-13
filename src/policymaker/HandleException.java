package policymaker;

import java.util.List;

public class HandleException extends Exception {
    private List<Long> list;
    private Integer errorCode;
    public HandleException(String message) {
        super(message);
    }

    public HandleException(String message, List<Long> list) {
        super(message);
        this.list = list;
    }

    public HandleException(String message, Integer errorCode) {
        super(message);
        this.errorCode = errorCode;
    }

    public List<Long> getList() {
        return list;
    }

    public Integer getErrorCode() {
        return errorCode;
    }

    public void sendMail() {
        String sTo, sFrom, sBody, sSubject;

        sTo = "Alice Suiu <asuiu@cern.ch>";
        sFrom = "monalisa@cern.ch";

        sBody = "Dear colleagues,\n\n";
        sBody += "The PATCH or GET request to the bookkeeping did not work. We got the following error message:\n";
        sBody += getMessage() + "\n";

        if (errorCode != null && errorCode > 0)
            sBody += "Also, we received the following error code: " + errorCode + "\n";

        if (list != null)
            sBody += "Also, we received the following list of runs: " + list + "\n";

        sBody += "\nBest regards,\nRunInfoThread.\n";
        sSubject = "Warning: The PATCH/GET request to bookkeeping failed";

        RunInfoUtils.sendMail(sTo, sFrom, sSubject, sBody);
    }
}
