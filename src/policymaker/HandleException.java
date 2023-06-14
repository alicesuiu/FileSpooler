package policymaker;

import alien.config.ConfigUtils;
import lazyj.mail.Mail;
import lazyj.mail.Sendmail;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class HandleException extends Exception {
    private List<Long> list;
    private Integer errorCode;
    private static Logger logger = ConfigUtils.getLogger(HandleException.class.getCanonicalName());
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
        try {
            final Mail m = new Mail();

            m.sTo = "Alice Suiu <asuiu@cern.ch>";
            m.sFrom = "monalisa@cern.ch";

            m.sBody = "Dear colleagues,\n\n";
            m.sBody += "The PATCH or GET request to the bookkeeping did not work. We got the following error message:\n";
            m.sBody += getMessage() + "\n";

            if (errorCode != null && errorCode > 0)
                m.sBody += "Also, we received the following error code: " + errorCode + "\n";

            if (list != null)
                m.sBody += "Also, we received the following list of runs: " + list + "\n";

            m.sBody += "\nBest regards,\nRunInfoThread.\n";

            m.sSubject = "Warning: The PATCH/GET request to bookkeeping failed";

            final Sendmail s = new Sendmail(m.sFrom);
            if (!s.send(m))
                logger.log(Level.WARNING, "Could not send mail : " + s.sError);
        }
        catch (final Throwable t) {
            logger.log(Level.WARNING, "Cannot send mail", t);
        }
    }
}
