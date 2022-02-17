package influencemax.TimeCriticalIM.InfMaxAlgorithm;

import influencemax.TimeCriticalIM.MontoCarloClass;
import influencemax.util.DBHelper;
import influencemax.TimeCriticalIM.InfluenceMaxUtilMethod;

import java.sql.Connection;
import java.sql.Statement;
import java.util.*;

import static influencemax.util.UtilMethod.sortLongDoubleMapByValueDesc;


/**
 * Created by jack on 2018/2/28.
 * 启发式种子节点选取算法//todo 这一方法接下来还需要继续优化 2018/3/1 0:46
 */
public class InfMax_TTH implements InfMaxInterface {
    private final double m_startTimeSecond;
    private final double m_maxDelay;
    private double m_theta = 1.0/320.0;
    private HashSet<Long> m_onlineUserSet = new HashSet<>();
    private HashMap<Long, HashMap<Long, Double>> m_infMap = new HashMap<>();
    private HashMap<String, Double> m_delayMap = new HashMap<>();
    private HashMap<Long, Double> m_singleDelayMap = new HashMap<>();
    private HashMap<String, Double> m_singleInfMap = new HashMap<>();
    private HashMap<Long, Double> m_heuristicValMap = new HashMap<>();
    private double m_maxInfVal = 1.0;

    public static void main(String[] args){
        InfMax_TTH test = new InfMax_TTH(36400, 3600, 1.0/ 320.0, "");
        //test.startCalcInfBetweenUsers();
        HashSet<Long> seedSet = test.getKSeeds(50);
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

    public InfMax_TTH(double startTimeSecond, double maxDelay, double theta, String tableNameSuffix){
        this.m_startTimeSecond = startTimeSecond;
        this.m_maxDelay = maxDelay;
        this.m_theta = theta;
        this.m_tableNameSuffix = tableNameSuffix;
    }

    public HashMap<Long, HashMap<Long, Double>> getInfMap(){
        return this.m_infMap;
    }
    public HashMap<Long, Double> getDelayMap(){
        return this.m_singleDelayMap;
    }
    @Override
    public double getMaxDelay(){
        return this.m_maxDelay;
    }
    @Override
    public double getStartTimeSecond(){
        return this.m_startTimeSecond;
    }
    @Override
    public double getMaxInfVal(){
        return this.m_maxInfVal;
    }

    @Override
    public String getName() {
        return "TTH";
    }

    @Override
    public HashSet<Long> getKSeeds(int k) {
        Connection conn = DBHelper.getConnection();
        HashSet<Long> resultSet = new HashSet<>();
        try {
            Statement st = conn.createStatement();
            //先全部存到map里，再计算两两之间的
            m_onlineUserSet = InfluenceMaxUtilMethod.filterOnlineUserSet(st, m_startTimeSecond, m_maxDelay);
            //将所有单步影响力和时延取出存入对应的map
            InfluenceMaxUtilMethod.fetchSingleDelayMap(m_onlineUserSet, st, m_singleDelayMap, m_singleInfMap, m_delayMap, m_infMap, m_tableNameSuffix);
            calcHeuristicVal();
            //tododone 2018/2/28 22:56这里需要进行排序并返回最大的k个
            List<Map.Entry<Long,Double>> resultList = sortLongDoubleMapByValueDesc(m_heuristicValMap);
            for(int i = 0; i < k; i++){
                resultSet.add(resultList.get(i).getKey());
            }
        }
        catch (Exception ex){
            ex.printStackTrace();
        }
        return resultSet;
    }

    /**
     * 计算二跳，考虑时延和影响力
     */
    private void calcHeuristicVal(){
        Iterator<Long> it = m_onlineUserSet.iterator();
        while(null != it && it.hasNext()){
            long curNode = it.next();
            double heuriVal = getOneHeuriVal(curNode);
            m_heuristicValMap.put(curNode, heuriVal);
        }
    }

    /**
     * 计算单个节点的启发值。
     * 时延越大，贡献越小；影响越大，贡献越大
     * infVal * e^[-delay / 平均时延]，并且一级的权重为1;24017091643/2518922=9534.67.select sum(timedelta) from timedelta_1115_1228_tmp;select sum(notNullCount) from timedelta_1115_1228_tmp;
     * 二级的为所有子节点和/平时度数=203012/7151=28.389{select count(DISTINCT(uid)) from edges_1115_1228;select count(*) from edges_1115_1228;}
     * @param uid
     * @return
     */
    private double getOneHeuriVal(long uid){
        double result = 0;
        if(m_infMap.containsKey(uid)){
            HashMap<Long, Double> firstStepChildrenMap = m_infMap.get(uid);
            for(Map.Entry<Long, Double> entry : firstStepChildrenMap.entrySet()){
                long curFirstChild = entry.getKey();
                if(!m_onlineUserSet.contains(curFirstChild)){
                    continue;
                }
                double curFirstInf = entry.getValue();
                //必然存在当前用户的时延
                double curFirstChildDelay = m_singleDelayMap.get(curFirstChild);
                result+= curFirstInf * Math.exp(-curFirstChildDelay / 9534.67);
                if(m_infMap.containsKey(curFirstChild)){
                    HashMap<Long, Double> secondStepChildrenMap = m_infMap.get(curFirstChild);
                    for(Map.Entry<Long, Double> secondEntry : secondStepChildrenMap.entrySet()){
                        long secondChild = secondEntry.getKey();
                        if(!m_onlineUserSet.contains(secondChild)){
                            continue;
                        }
                        double secondInf = secondEntry.getValue();
                        double secondDelay = m_singleDelayMap.get(secondChild);
                        result+= (1.0 / 28.389) * secondInf * Math.exp(-secondDelay / 9534.67);
                    }
                }
            }
        }
        return result;
    }
}
