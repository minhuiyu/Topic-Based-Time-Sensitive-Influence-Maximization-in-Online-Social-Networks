package influencemax.TimeCriticalIM;

/**
 * Created by jack on 18/2/3.
 */
public class UserInfluence implements Comparable {
    public long uid;
    public double infval;
    public UserInfluence(){

    }
    public UserInfluence(long v_uid, double v_infval){
        uid = v_uid;
        infval = v_infval;
    }

    @Override
    public int compareTo(Object o) {
        //fixed修改成了从小到大排序
        return Double.compare(((UserInfluence)o).infval, infval);
    }

    @Override
    protected UserInfluence clone(){
        return new UserInfluence(uid, infval);
    }
}
