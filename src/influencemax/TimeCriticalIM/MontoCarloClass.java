package influencemax.TimeCriticalIM;


import influencemax.util.DBHelper;

import static influencemax.TimeCriticalIM.InfluenceMaxUtilMethod.fetchSingleDelayMap;
import static influencemax.TimeCriticalIM.InfluenceMaxUtilMethod.filterOnlineUserSet;

import java.sql.Connection;
import java.sql.Statement;
import java.util.*;

/**
 * Created by jack on 2018/2/8.
 * 通过蒙特卡洛模拟，计算种子集合的影响力
 */
public class MontoCarloClass {
    private HashSet<Long> m_seedSet;
    private HashMap<Long, HashMap<Long, Double>> m_infMap = new HashMap<>();//fixme 好像如果将一个Map对象传给函数的时候，如果不先初始化，不会将原值返回的
    private double m_maxDelay;
    private HashMap<Long, Double> m_singleDelayMap = new HashMap<>();
    private int m_simulationTime = 10000;
    private double m_startTimeSecond = 0.0;
    private double m_largeRate = 1.0;//模拟是否激活isPassed的时候有用。暂时不用
    private HashSet<Long> m_onlineUserSet;
    private String m_tableNameSuffix="";
    public static void main(String[] args){

    }
    //tododone 还需要过滤用户的在线时间,不需要了，因为传过来的hashmap已经是过滤过在线时间的。
    //tododone 这里需要把过滤在线用户的提成公共方法，因为此方法需要通用
    public MontoCarloClass(HashSet<Long> seedSet, double startTimeSecond, double maxDelay, double largeRate, String tableNameSuffix){
        this.m_seedSet = seedSet;
        //this.m_infMap = infMap;
        //this.m_singleDelayMap = delayMap;
        this.m_maxDelay = maxDelay;
        this.m_startTimeSecond = startTimeSecond;
        this.m_largeRate = largeRate;
        this.m_tableNameSuffix = tableNameSuffix;
        Connection conn = DBHelper.getConnection();
        try {
            Statement st = conn.createStatement();
            m_onlineUserSet = filterOnlineUserSet(st, m_startTimeSecond, m_maxDelay);
            //将所有单步影响力和时延取出存入对应的map
            HashMap<String, Double> tmp_delayMap = new HashMap<>();
            HashMap<String, Double> tmp_singleInfMap = new HashMap<>();
            fetchSingleDelayMap(m_onlineUserSet, st, m_singleDelayMap, tmp_singleInfMap, tmp_delayMap, m_infMap, m_tableNameSuffix);
        }
        catch (Exception ex){
            ex.printStackTrace();
        }
    }

    /**
     * 添加了模拟次数
     * @param seedSet
     * @param startTimeSecond
     * @param maxDelay
     * @param largeRate
     * @param simulationTimes
     */
    public MontoCarloClass(HashSet<Long> seedSet, double startTimeSecond, double maxDelay, double largeRate, String tableNameSuffix, int simulationTimes){
        this(seedSet, startTimeSecond, maxDelay, largeRate, tableNameSuffix);
        this.m_simulationTime = simulationTimes;
    }
    public double getMontoCarloInf(){
        double infVal = 0;
         for(int i = 0; i < m_simulationTime; i++){
             double curInfVal = monteCarloOneTime();
             //System.out.println("第" + i + "次模拟影响力：" + curInfVal);
             infVal+= curInfVal;
         }
         infVal = infVal / m_simulationTime;
         return infVal;
    }

    /**
     * 该方法是为也多次蒙特卡洛模拟使用(originGreedy算法使用)，能够加速计算过程
     * @return
     */
    public double getMontoCarloInfSimutaneously(HashSet<Long> seedSet){
        this.m_seedSet = seedSet;
        return getMontoCarloInf();
    }
    private double monteCarloOneTime(){
        //这里的比例怎么设置？因为很多影响力都是0.0234这么小的，所以可能被激活的概率非常小。之前过滤的阈值是1/320
        //fixme 这里先这么写着，以后如果有需要，再改影响概率。
        //要考虑时间，激活时间有先后。
        //时间先后不用管，因为早晚都会走一遍的，如果这一轮超时，那么下一轮再激活的时候再计算时间就行
        //但是有些节点被激活的先后会影响其子节点的激活时间，因此，每个节点要记录其父节点。
        //如果某个节点被激活，发现 其已经被激活，那么需要查看与已激活的时间的先后。如果现在较晚，不管。
        //但如果现在的较早，需要修改其所有子节点的激活时间
        Iterator<Long> iterSeed = m_seedSet.iterator();
        //记录是被哪个节点激活的//fixmefixed 或者直接改成HashMap<Long, HashSet<Long>>用来记录激活了哪些？既用parentMap，又用HashMap<Long, HashSet<Long>>
        HashMap<Long, Long> parentMap = new HashMap<>();
        //记录节点的激活时间
        HashMap<Long, Double> activeTimeMap = new HashMap<>();
        //存储已经激活的节点
        HashSet<Long> activatedSet = new HashSet<>();

        while(null != iterSeed && iterSeed.hasNext()){
            long curSeed = iterSeed.next();
            activatedSet.add(curSeed);
            activeTimeMap.put(curSeed, 0.0);
        }
        //如果新的激活时间小于已有的激活时间，那么通过ifMap来找下一级节点，不用parentMap了
        //fixmefixed 这里仍然需要parentMap，因为几乎所有边都是双向的，不知道某个节点是被谁激活的
        //fixmefixed 不存parentMap，但是存HashMap<Long,HashSet<Long>>
        HashMap<Long, HashSet<Long>> activeSetMap = new HashMap<>();

        HashSet<Long> alreadyCalcedSet = new HashSet<>();
        Iterator<Long> it = activatedSet.iterator();
        boolean isActiveSetChanged = false;
        while(null != it && it.hasNext()){
            if(isActiveSetChanged){
                //如果集合有改变，it需要重新设置
                it = activatedSet.iterator();
                isActiveSetChanged = false;
                continue;
            }
            long curActive = it.next();
            if(alreadyCalcedSet.contains(curActive)){
                //已计算过，直接pass
                continue;
            }
            HashMap<Long, Double> curInfMap = m_infMap.get(curActive);
            if(null == curInfMap){//边缘节点
                continue;
            }
            double curActiveTime = activeTimeMap.get(curActive);
            for(Map.Entry<Long, Double> entry : curInfMap.entrySet()){//fixmefixed 这个curInfMap有时候会为空 2018/2/26 22:34，此时说明是边缘节点，因此这里要判一次空
                //对于每一个
                long target = entry.getKey();
                double infVal = entry.getValue();
                if(isPassed(infVal)){//激活成功
                    double targetDelay = m_singleDelayMap.get(target);
                    double newTimeVal = curActiveTime + targetDelay;
                    //如果早于已有，全部更新，并且需要将其子节点移出已计算？不需要，因为没有时间剪枝。否则，不管
                    if(activatedSet.contains(target)){
                        double oldTargetTime = activeTimeMap.get(target);
                        if(newTimeVal < oldTargetTime){
                            //todo 更新所有下级，用HashMap<Long, HashSet<Long>>，还要删除激活集合
                            long oldParent = parentMap.get(target);
                            updateCurAndChildrenNode(target, newTimeVal, oldParent, curActive, activeTimeMap, activeSetMap, parentMap);
                        }
                    }
                    else{
                        //如果该节点是初次被激活，那么更新其父节点，激活时间等
                        parentMap.put(target, curActive);
                        activatedSet.add(target);
                        isActiveSetChanged = true;
                        activeTimeMap.put(target, newTimeVal);
                        if(activeSetMap.containsKey(curActive)){//加入当前结点的激活集合
                            HashSet<Long> set = activeSetMap.get(curActive);
                            set.add(target);
                            activeSetMap.put(curActive, set);
                        }
                        else{
                            HashSet<Long> set = new HashSet<>();
                            set.add(target);
                            activeSetMap.put(curActive, set);
                        }
                    }

                    //计算完更新时间后，统一移除时间大于阈值的
                }
            }//tododone 2018/2/26 15:42这个map应该最开始的时候把所有种子节点加上？不要加，因为初始时所有种子节点都没有计算完
            alreadyCalcedSet.add(curActive);
            //fixmefixed 此处在遍历过程中添加会出问题。三个set,一个总的保存已激活，一个用来当前的遍历，另外一个用来添加（不这样做了，如果集合有改变，直接重新产生iterator）
        }
        //fixmedone 应该修改为返回时限内的个数
        int result = 0;
        for(Map.Entry<Long, Double> entry : activeTimeMap.entrySet()){
            double time = entry.getValue();
            result+= time <= m_maxDelay ? 1 : 0;
        }
        return result;
    }

    /**
     * 当某一个节点被第二次激活时，发现其更新时间比以前的更新时间更早
     * 这时候需要更新该节点被谁激活、该节点以及该节点激活的所有节点的更新时间
     * @param target
     * @param newTimeVal
     * @param oldParent
     * @param newParent
     * @param activatedTime
     * @param activeSetMap
     */
    private void updateCurAndChildrenNode(long target, double newTimeVal, long oldParent, long newParent, HashMap<Long, Double> activatedTime, HashMap<Long, HashSet<Long>> activeSetMap, HashMap<Long, Long> parentMap){
        //设置target的来源
        boolean modified = false;//标识被target激活的集合有没有变化
        if(oldParent != newParent) {
            HashSet<Long> oldWhoActiveTarget = activeSetMap.get(oldParent);
            oldWhoActiveTarget.remove(target);
            HashSet<Long> newWhoActiveTargetSet = new HashSet<>();
            if (activeSetMap.containsKey(newWhoActiveTargetSet)) {
                newWhoActiveTargetSet = activeSetMap.get(newParent);
            }
            newWhoActiveTargetSet.add(target);
            activeSetMap.put(newParent, newWhoActiveTargetSet);
            parentMap.put(target, newParent);
        }
        activatedTime.put(target, newTimeVal);

        if(!activeSetMap.containsKey(target)){//说明该节点还没有激活其它节点
            //此时，只更新当前节点的更新时间，以及parentMap，还有target的来源
            //tododone 删旧加新，在上面的部分，无论有没有激活其它节点，都需要进行上面的操作
            return;
        }//tododone 仍然需要一个parentMap来存储是哪个节点激活了当前节点

        //TODOdone 设置target已经激活的所有节点，递归？
        if(activeSetMap.containsKey(target)){
            HashSet<Long> targetChildrenSet = activeSetMap.get(target);
            Iterator<Long> childrenIt = targetChildrenSet.iterator();
            while(null != childrenIt && childrenIt.hasNext()){
                long child = childrenIt.next();//fixme 这里报ConcurrentModificationException，原因是下面的递归会修改targetChildrenSet? 2018/2/26 22:44
                //改的不是targetChildrenSet，而是activeSetMap，
                double childDelay = m_singleDelayMap.get(child);
                double newChildTime = newTimeVal + childDelay;
                //由于子节点是当前节点所激活的，因此子节点的所有时间都是由当时节点计算来的。
                //所以，如果当前节点的时间有所改变的话，所有子节点的时候都必须改变。
                updateCurAndChildrenNode(child, newChildTime, target, target, activatedTime, activeSetMap, parentMap);
            }
        }
       // return modified;
    }
    private boolean isPassed(double val){
        //tododone 此函数实现该功能：根据概率判断是否被激活
        double rand = Math.random();
        //fixmefixed 需不需要统一乘以一个定值
        //统一把最大概率的激活概率设置为1/3
        //return (rand * 3 * m_largeRate <= val);
        return rand <= val * m_largeRate;//放大概率，原来的太小了
    }
}
