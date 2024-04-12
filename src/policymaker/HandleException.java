package policymaker;

public class HandleException extends Exception {
    private Integer errorCode;
    private static long lastMailSent = 0;
    private static Integer prevErrorCode = 0;
    private static String prevErrorMessage = "";

    private static final long EXECUTION_INTERVAL = 1000L * 60 * 60; // 1 hour

    public HandleException(String message) {
        super(message);
    }

    public HandleException(String message, Integer errorCode) {
        super(message);
        this.errorCode = errorCode;
    }

    public Integer getErrorCode() {
        return errorCode;
    }

    public void isTimeToSentMail() {
        if (!getMessage().equalsIgnoreCase(prevErrorMessage) && errorCode != prevErrorCode ||
                (System.currentTimeMillis() - lastMailSent) >= EXECUTION_INTERVAL) {
            sendMail();
            lastMailSent = System.currentTimeMillis();
            prevErrorCode = errorCode;
            prevErrorMessage = getMessage();
        }

    }

    public void sendMail() {
        String sTo, sFrom, sBody, sSubject;

        sTo = "Alice Suiu <asuiu@cern.ch>";
        sFrom = "monalisa@cern.ch";

        sBody = "Dear colleagues,\n\n";
        sBody += "The PATCH or GET request to the bookkeeping did not work. We got the following error message:\n";
        sBody += getMessage() + "\n";

        if (errorCode != null)
            sBody += "Also, we received the following error code: " + errorCode + "\n";


        sBody += "\nBest regards,\nRunInfoThread.\n";
        sSubject = "Warning: The PATCH/GET request to bookkeeping failed";

        RunInfoUtils.sendMail(sTo, sFrom, sSubject, sBody);
    }
}
