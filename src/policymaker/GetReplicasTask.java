package policymaker;

import alien.catalogue.LFN;
import alien.catalogue.PFN;
import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;

import java.util.Set;
import java.util.HashSet;

public class GetReplicasTask extends Thread {
    private final LFN lfn;
    private Set<String> storages;

    public GetReplicasTask(Set<String> storages, LFN lfn) {
        this.storages = storages;
        this.lfn = lfn;
    }

    @Override
    public void run() {
        GUID g = GUIDUtils.getGUID(lfn);
        Set<PFN> pfns = g.getPFNs();
        Set<String> se = new HashSet<>();
        for (PFN pfn : pfns) {
            String seName = pfn.getSE().seName;
            se.add(seName);
        }
        storages.addAll(se);
    }
}