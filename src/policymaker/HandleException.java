package policymaker;

import java.util.List;

public class HandleException extends Exception {
    private List<Long> list;
    public HandleException(String message) {
        super(message);
    }

    public HandleException(String message, List<Long> list) {
        super(message);
        this.list = list;
    }

    public List<Long> getList() {
        return list;
    }
}
