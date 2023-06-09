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
}
