package influencemax.TimeCriticalIM.InfMaxAlgorithm;

import influencemax.TimeCriticalIM.MontoCarloClass;
import influencemax.TimeCriticalIM.UserInfluence;
import influencemax.util.DBHelper;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

/**
 * Created by jack on 2018/2/27.
 * 不过滤当前在线的用户，把所有时间的用户统一加入计算
 */
public class InfMax_WithoutOnlineFilter implements InfMaxInterface {
    private double m_maxDelay = 0;
    private  double m_theta = 1/320;//time-critital influence maximization in social networks with time-delayed diffusion process
    //存储所有的两两之间的影响力？比如("3-5",0.04)
    private HashMap<String, Double> m_singleInfMap = new HashMap<>();
    //比如3-<(2,0.4),(77,0.02))
    private HashMap<Long, HashMap<Long, Double>> m_infMap = new HashMap<>();

    //能影响到该节点的所有节点集合
    private HashMap<Long, HashSet<Long>> m_whoInfMeMap = new HashMap<>();

    //记录每个节点的时延
    private HashMap<Long, Double> m_singleDelayMap = new HashMap<>();

    //记录两两之间的时延信息，如<3-12,120>
    private HashMap<String, Double> m_delayMap = new HashMap<>();

    //传播的开始时间
    private double m_startTimeSecond = 0.0;

    //一跳的两两间的最大影响概率。该值的作用是防止monto carlo模拟的时候概率太小一直影响不成功
    private double m_maxInfVal = 1.0;

    public static void main(String[] args){
        //假设1小时
        InfMax_WithoutOnlineFilter test = new InfMax_WithoutOnlineFilter(36400, 3600, 1.0/ 320.0, "");
        HashSet<Long> seedSet = test.getKSeeds(100);
        Iterator<Long> it = seedSet.iterator();
        while(null != it && it.hasNext()){
            long val = it.next();
            System.out.println(val);
        }
        MontoCarloClass gi = new MontoCarloClass(seedSet, test.getStartTimeSecond(), test.getMaxDelay(), test.getMaxInfVal(), "", 10000);
        double curTotalInf = gi.getMontoCarloInf();
        System.out.println("影响力模拟值为：" + curTotalInf);
    }

    private String m_tableNameSuffix = "";
    /**
     *
     * @param startTime 开始的时间，以秒记
     * @param maxDelay 按秒计算的最大时延
     * @param theta 影响传播阈值
     */
    public InfMax_WithoutOnlineFilter(double startTime, double maxDelay, double theta, String tableNameSuffix){
        this.m_maxDelay = maxDelay;
        this.m_theta = theta;
        this.m_startTimeSecond = startTime;
        this.m_tableNameSuffix = tableNameSuffix;
    }
    public InfMax_WithoutOnlineFilter(double maxDelay, double[] qDist){
        this.m_maxDelay = maxDelay;
    }

    public HashMap<Long, HashMap<Long, Double>> getInfMap(){
        return this.m_infMap;
    }
    public HashMap<Long, Double> getDelayMap(){
        return this.m_singleDelayMap;
    }
    public double getMaxDelay(){
        return this.m_maxDelay;
    }
    public double getStartTimeSecond(){
        return this.m_startTimeSecond;
    }
    public double getMaxInfVal(){
        return this.m_maxInfVal;
    }

    @Override
    public String getName() {
        return "未考虑在线时间分布的算法";
    }

    @Override
    public HashSet<Long> getKSeeds(int k){
        //startCalcInfBetweenUsers();
        List<UserInfluence> infList = calcInitialInfluence();
        //记录已经选好的种子节点集合及其对应的影响力值
        HashSet<Long> seedSet = new HashSet<>();
        long curMaxMarginUid = -1;
        double curMaxMarginInf = 0;
        //用来存储每个节点受到的种子集合的影响力的值
        HashMap<Long, Double> seedInfMap = new HashMap<>();
        int curMaxIndex = -1;
        for(int i = 0; i < k; i++){
            //排序应该怎么排？用List和Map?，还是数组和Map?
            //在中途修改的过程中，需要重新排序，这时只调节部分节点的排序位置，因此用linkedlist比较快一点 2018/2/3 17：24
            Collections.sort(infList);
            //FIXMEfixed 上面这样是从小到大排序，需要修改comparable规则 2018/2/8 0:40
            //按顺序计算边际影响力
            for(int j = 0; j < infList.size(); j++){
                //特定条件下跳出（如果当前的最大边际影响力已经大于接下来的初始影响力）TODO 或者乘以一定阈值？
                if(seedSet.contains(infList.get(j).uid)){
                    continue;
                }
                if(j < infList.size() - 1 && curMaxMarginInf >= infList.get(j + 1).infval){
                    break;
                }
                double curMargin = getMarginInf(m_infMap, infList, seedSet, j, seedInfMap);
                //donetodo 这里需要将当前节点的影响力的值换成该边际影响力的值，因为随着种子节点集的变大，该节点的边际影响力是递减的
                //更新之后不需要重新排序，因为保存了最大边际影响力及其对应的uid。但是在下一轮之前需要重排序
                infList.get(j).infval = curMargin;
                if(curMargin > curMaxMarginInf){
                    curMaxMarginInf = curMargin;
                    curMaxMarginUid = infList.get(j).uid;
                    curMaxIndex = j;
                }
            }
            //将最大边际影响力的节点加入种子集合，更新影响力值(curMaxMarginInf, curMaxMarginUid)
            addUser2SeedSet(curMaxMarginUid, seedSet, seedInfMap, m_infMap);
            //fixed 这里需要将该种子节点移出infList，因为在判定是否能加入种子集合时，是与下一节点的影响力比较的
            infList.remove(curMaxIndex);
            //fixed，排序在开始时已经有了，这里不需要再排
            //每轮结束后，重新排序单个节点的影响力值
            //Collections.sort(infList);
            //fixed 重新选下一轮的时候，重置最大影响力
            curMaxMarginInf = 0;
            curMaxIndex = 0;
            curMaxMarginUid = -1;
        }
        return seedSet;
    }

    /**
     * 将用户加入种子节点集合
     * @param uid
     * @param seedSet
     * @param seedInfMap
     * @param infMap
     */
    private void addUser2SeedSet(long uid, HashSet<Long> seedSet, HashMap<Long, Double> seedInfMap, HashMap<Long, HashMap<Long, Double>> infMap){
        seedSet.add(uid);
        HashMap<Long, Double> curCanInfMap = infMap.get(uid);
        try {
            for (Map.Entry<Long, Double> entry : curCanInfMap.entrySet()) {
                //当前被影响到的节点
                long curBeInfUser = entry.getKey();
                //待选节点对该节点的影响力值
                double curInfTarget = entry.getValue();
                double beforeVal = 0;
                //防止不存在该key时报错
                if (seedInfMap.containsKey(curBeInfUser)) {
                    beforeVal = seedInfMap.get(curBeInfUser);
                }
                double afterVal = 1 - (1 - beforeVal) * (1 - curInfTarget);
                seedInfMap.put(curBeInfUser, afterVal);
            }
        }
        catch (Exception ex){
            ex.printStackTrace();
        }
    }

    /**
     * 计算第index个节点的边际影响力
     * @param infMap
     * @param infList
     * @param seedSet
     * @param index
     * @return
     */
    private double getMarginInf(HashMap<Long, HashMap<Long, Double>> infMap, List<UserInfluence>infList, HashSet<Long> seedSet, int index, HashMap<Long, Double> seedInfMap){
        //计算当前预备节点能影响到的各个节点   所增加的被影响力值
        //最后不要忘记加入当前节点的被影响力（从0.几变成1）
        Long curUser = infList.get(index).uid;
        //当前用户所能影响到的节点
        HashMap<Long, Double> curCanInfMap = infMap.get(curUser);
        //计算每个节点受到的影响力的增加值
        double delta = 0;
        for (Map.Entry<Long,Double> entry : curCanInfMap.entrySet()){
            //当前被影响到的节点
            long curBeInfUser = entry.getKey();
            //待选节点对该节点的影响力值
            double curInfTarget = entry.getValue();
            double beforeVal = 0;
            //防止不存在该key时报错
            if(seedInfMap.containsKey(curBeInfUser)){
                beforeVal = seedInfMap.get(curBeInfUser);
            }
            double afterVal = 1 - (1-beforeVal)*(1-curInfTarget);
            double curDelta = afterVal - beforeVal;
            delta+= curDelta;
        }
        return delta;
    }

    /**
     * 本方法从数据库中取出单步的影响力、时延等因素，m_singleDelayMap和m_infMap等字典中
     * @param st
     */
    private void fetchSingleDelayMap(Statement st){
        try {
            ResultSet rs = st.executeQuery("select * from user_delay_1115_1228");
            while(null != rs && rs.next()){
                Long uid = rs.getLong("uid");
                m_singleDelayMap.put(uid, rs.getDouble("delay"));
            }
            //读取两两之间的影响力，并存储两两之间的时延
            rs = st.executeQuery("select * from edges_1115_1228_infval_" + m_tableNameSuffix);
//            rs = st.executeQuery("select * from edges_1115_1228_infval");
            m_singleInfMap = new HashMap<>();
            m_delayMap = new HashMap<>();
            while(null != rs && rs.next()){
                Long uid = rs.getLong("uid");
                Long originUid = rs.getLong("originUid");
                //fixed 加入下面一段过滤，如果源用户和目标用户相同，直接路过
                if(uid == originUid){
                    continue;
                }
                double infval = rs.getDouble("infval");
                //fixed 原来写成了uid + "-" + originUid
                m_singleInfMap.put(originUid + "-" + uid, infval);
                //存储两两之间的时延
                m_delayMap.put(originUid + "-" + uid, m_singleDelayMap.get(uid));

                if(!m_infMap.containsKey(originUid)){
                    HashMap<Long, Double> curInfValMap = new HashMap<>();
                    m_infMap.put(originUid, curInfValMap);
                }
                HashMap<Long, Double> curInfValMap = m_infMap.get(originUid);
                curInfValMap.put(uid, infval);
                m_infMap.put(originUid, curInfValMap);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * 计算两两之间的初始影响力值（通过MIP的影响力值）
     * @return
     */
    public List<UserInfluence> calcInitialInfluence(){
        Connection conn = DBHelper.getConnection();
        List<UserInfluence> infList = new LinkedList<>();
        try{
            Statement st = conn.createStatement();
            //先全部存到map里，再计算两两之间的
            //filterOnlineUserSet(st, m_onlineUserSet, m_algrothimStartTime, m_maxDelay);

            //将所有单步影响力和时延取出存入对应的map
            fetchSingleDelayMap(st);

            //遍历每个map,采用dijkstra算法更新影响力。逐步计算，先算一步的，再算两步的,四步的，八步的……
            int steps = 1;
            //存储两两之间的时延信息
            //m_delayMap = new HashMap<>();
            while(true){
                //fixed 改成了newDelayMap，与m_delayMap交替存储当前轮的时延,每一轮得了要新建newDelayMap和newMap
                HashMap<String, Double> newDelayMap = new HashMap<>();
                HashMap<Long, HashMap<Long, Double>> newMap = new HashMap<>();
                //新增加的值或者修改的值放在新的Map里，等当前遍历结束时放回原Map，继续下一次修改
                for (Map.Entry<Long, HashMap<Long, Double>> entry : m_infMap.entrySet()){
                    //增加时延之后，m_infMap中对应的每个项都需要新增时延选项，该值与originUid,uid相关（表示从originUid到uid，包含originUid和uid的时延）
                    Long originUid = entry.getKey();
                    HashMap<Long, Double> subMap = entry.getValue();

                    for(Map.Entry<Long, Double> inEntry : subMap.entrySet()){
                        Long uid = inEntry.getKey();
                        double infVal = inEntry.getValue();
                        //originUid--uid--destUid
                        //fixmefixed 会exception 2018/2/7 10:55
                        double firstStepDelay = m_delayMap.get(originUid + "-" + uid);

                        if(m_infMap.containsKey(uid)){
                            //如果主map中存在该键值，那么遍历并计算所有的二跳影响力
                            HashMap<Long, Double> secondMap = m_infMap.get(uid);
                            for(Map.Entry<Long, Double> thirdEntry : secondMap.entrySet()){
                                Long destUid = thirdEntry.getKey();
                                //fixed 加入下面一段过滤，如果源用户和目标用户相同，直接路过
                                if(destUid == originUid){
                                    continue;
                                }
                                double destVal = thirdEntry.getValue();//第二跳的影响力
                                //fixme 第二次计算会出错
                                double secondStepDeley = m_delayMap.get(uid + "-" + destUid);

                                double finalDestInfVal = infVal * destVal;
                                double totalDelay = firstStepDelay + secondStepDeley;
                                //从原HashMap中比较找出影响较大的，插入新HashMap中
                                //TODOdone 这里考虑时间
                                InfDelayResult finalDestInfDelay = getMaxFromOldMap(m_infMap, originUid, destUid, finalDestInfVal, m_delayMap, totalDelay);
                                if(null != finalDestInfDelay) {
                                    addToNewMap(newMap, originUid, destUid, newDelayMap, finalDestInfDelay);
                                }
                            }
                        }
                        else{
                            //如果主map中不存在该键值，说明没有二跳的影响力，此时直接加入newMap中
                            InfDelayResult finalDestInfDelay = new InfDelayResult(firstStepDelay, infVal);
                            addToNewMap(newMap, originUid, uid, newDelayMap, finalDestInfDelay);
                        }
                    }
                }
                m_infMap = newMap;
                newMap = new HashMap<>();
                //fixed 将newDelayMap存储到m_delayMap
                m_delayMap = newDelayMap;
                steps++;
                //FIXME 这里要根据情况跳出
                if(steps > 3) {
                    break;
                }
            }
            //TODOdone 最终的结果入库？写入文本文档，包括影响力的值。不入库，因为不同的算法的影响力值是不一样的（有的在线用户没有过滤2018/2/7）
            //计算每个节点的初始影响力？放到LinkedList中，可以进行下一步的排序
            long tmptimestart = System.currentTimeMillis();
            infList = calcInitialInfByInfmap(m_infMap);
            //System.out.println("计算节点的初始影响力执行时间:" + (((double)(System.currentTimeMillis() - tmptimestart))/60000) + "min");
        }
        catch (Exception ex){
            ex.printStackTrace();
        }
        finally {
            try{
                conn.close();
            }
            catch (Exception ex){
                ex.printStackTrace();
            }
        }
        return infList;
    }

    /**
     * 根据两两之间的影响力，计算出每个节点的影响力
     * @param infMap
     * @return
     */
    private List<UserInfluence> calcInitialInfByInfmap(HashMap<Long, HashMap<Long, Double>> infMap){
        List<UserInfluence> infList = new LinkedList<>();
        for(Map.Entry<Long, HashMap<Long, Double>> entry : infMap.entrySet()){
            UserInfluence ui = new UserInfluence();
            ui.uid = entry.getKey();
            HashMap<Long, Double> curInfUsers = entry.getValue();
            for(Map.Entry<Long, Double> subEntry : curInfUsers.entrySet()){
                ui.infval += subEntry.getValue();
            }
            infList.add(ui);
        }
        return infList;
    }

    /**
     * 与旧的HashMap中的影响力值进行比较并取出较大的影响力值
     * @param oldMap
     * @param originUid
     * @param uid
     * @param infVal
     * @return
     */
    private InfDelayResult getMaxFromOldMap(HashMap<Long, HashMap<Long, Double>> oldMap, long originUid, Long uid, double infVal, HashMap<String, Double> delayMap, double totalDelay){
        //TODOdone 这里要改，返回时也要返回对应的时延
        //fixmefixed 这里返回的值应该是小于时延的最大影响力值 2018/2/7 22:58
        InfDelayResult result = null;
        InfDelayResult newResult = new InfDelayResult(totalDelay, infVal);
        if(!oldMap.containsKey(originUid)){
            if(isValid(newResult)){
                result = newResult;
            }
        }
        else {
            HashMap<Long, Double> subMap = oldMap.get(originUid);
            if (!subMap.containsKey(uid)) {
                if(isValid(newResult)){
                    result = newResult;
                }
            }
            else{
                //比较新旧影响力的值，并取其中较大影响力作为影响力值
                //TODO 以后可以考虑如果两者影响力比例相差不大，那么取其中传播时间较短的路径
                double oldVal = subMap.get(uid);
                double oldDelay = m_delayMap.get(originUid + "-" + uid);
                InfDelayResult oldResult = new InfDelayResult(oldDelay, oldVal);
                if(isValid(oldResult) && isValid(newResult)){
                    result = oldResult.infVal < newResult.infVal ? newResult : oldResult;
                }
                else if(isValid(oldResult)){
                    result = oldResult;
                }
                else if(isValid(newResult)){
                    result = newResult;
                }
                else{// all not valid, left empty
                }
            }
        }
        return result;
    }

    private boolean isValid(double infVal, double delayVal){
        return infVal > m_theta && delayVal < m_maxDelay;
    }
    private boolean isValid(InfDelayResult res){
        return res.infVal > m_theta && res.delayVal < m_maxDelay;
    }

    /**
     * 将新的影响力值 加入新HashMap，函数中会考虑大小等问题
     * @param newMap
     * @param originUid
     * @param uid
     * @param delayMap
     * @param insertVal
     */
    private void addToNewMap(HashMap<Long, HashMap<Long, Double>> newMap, long originUid, long uid, HashMap<String, Double> delayMap, InfDelayResult insertVal){
        //FIXME 这里没有过滤时间，要修改
        double infVal = insertVal.infVal;
        double delayVal = insertVal.delayVal;
        //影响力小于阈值直接pass
        if(infVal < m_theta){
            return;
        }
        if(!newMap.containsKey(originUid)){
            HashMap<Long, Double> mp = new HashMap<>();
            newMap.put(originUid, mp);
            delayMap.put(originUid + "-" + uid, delayVal);
        }
        HashMap<Long, Double> mp = newMap.get(originUid);
        double existVal = 0;
        if(mp.containsKey(uid)){
            existVal = mp.get(uid);
        }
        if(infVal > existVal){
            mp.put(uid, infVal);
            delayMap.put(originUid + "-" + uid, delayVal);
        }
        //否则使用已有的影响值作为影响力，说明影响时间已经计算好了，所以不需要重新更新
        //map的更新不需要放回
    }

    /**
     * 该类用于返回影响力和时延结果
     */
    private class InfDelayResult{
        public double delayVal;
        public double infVal;
        public InfDelayResult(){

        }
        public InfDelayResult(double delay, double inf){
            delayVal = delay;
            infVal = inf;
        }
    }
}

