package influencemax.TimeCriticalIM.InfMaxAlgorithm;

import java.util.HashSet;

/**
 * Created by jack on 2018/2/28.
 */
public interface InfMaxInterface {
    public String getName();
    public HashSet<Long> getKSeeds(int k);
    public double getMaxDelay();
    public double getStartTimeSecond();
    public double getMaxInfVal();
}
