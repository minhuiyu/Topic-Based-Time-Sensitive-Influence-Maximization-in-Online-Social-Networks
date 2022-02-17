package influencemax.TimeCriticalIM.InfMaxAlgorithm;

/**
 * Created by jack on 2018/4/29.
 */

import influencemax.TimeCriticalIM.MontoCarloClass;
import influencemax.util.DBHelper;
import influencemax.TimeCriticalIM.InfluenceMaxUtilMethod;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static influencemax.util.UtilMethod.sortLongDoubleMapByValueDesc;


/**
 * Created by jack on 2018/4/28.
 */
public class InfMax_TSDH implements InfMaxInterface{

   // private static double tParam = 100;
    public static void main(String[] args){
        double startTimeSecond = 36000;
        double maxDelay = 1500;//25分钟
        double theta = 1.0/320.0;
        for(int k = 0; k <= 50; k+=10){
            InfMaxInterface myAlgorithm = new InfMax_TSDH(startTimeSecond, maxDelay, theta, "");
            long startTime = System.currentTimeMillis();
            HashSet<Long> curSeed = myAlgorithm.getKSeeds(k);
            long endTime   = System.currentTimeMillis(); //程序结束记录时间
            long totalTime = endTime - startTime;       //总消耗时间
            MontoCarloClass monto = new MontoCarloClass(curSeed, startTimeSecond, maxDelay, 2, "");
            double infVal = monto.getMontoCarloInf();
            System.out.println("种子个数:" + k + "======" + myAlgorithm.getName() + "======影响力值======" + infVal + "======耗时======" + (totalTime) + "ms");
        }
//        //测试最适合的theta值，用10个节点的种子来计算
//        for(int i = 100; i < 1000; i+=100){
//            tParam = 1.0/i;
//            InfMaxInterface myAlgorithm = new InfMax_TSDH(startTimeSecond, maxDelay, theta);
//            long startTime = System.currentTimeMillis();
//            HashSet<Long> curSeed = myAlgorithm.getKSeeds(10);
//            long endTime   = System.currentTimeMillis(); //程序结束记录时间
//            long totalTime = endTime - startTime;       //总消耗时间
//            MontoCarloClass monto = new MontoCarloClass(curSeed, startTimeSecond, maxDelay, 2);
//            double infVal = monto.getMontoCarloInf();
//            System.out.println("时间参数" + i +  "种子个数:10======" + myAlgorithm.getName() + "======影响力值======" + infVal + "======耗时======" + (totalTime) + "ms");
//        }
//        for(int i = 1000; i < 30000; i+=1000){
//            tParam = 1.0/i;
//            InfMaxInterface myAlgorithm = new InfMax_TSDH(startTimeSecond, maxDelay, theta);
//            long startTime = System.currentTimeMillis();
//            HashSet<Long> curSeed = myAlgorithm.getKSeeds(10);
//            long endTime   = System.currentTimeMillis(); //程序结束记录时间
//            long totalTime = endTime - startTime;       //总消耗时间
//            MontoCarloClass monto = new MontoCarloClass(curSeed, startTimeSecond, maxDelay, 2);
//            double infVal = monto.getMontoCarloInf();
//            System.out.println("时间参数" + i +  "种子个数:10======" + myAlgorithm.getName() + "======影响力值======" + infVal + "======耗时======" + (totalTime) + "ms");
//        }
    }

    private double m_maxDelay = 0;
    private  double m_theta = 1/320.0;//time-critital influence maximization in social networks with time-delayed diffusion process
    //记录每个节点的时延
    private HashMap<Long, Double> m_singleDelayMap = new HashMap<>();

    //记录两两之间的时延信息，如<3-12,120>
    private HashMap<String, Double> m_delayMap = new HashMap<>();

    //传播的开始时间
    private double m_startTimeSecond = 0.0;

    @Override
    public String getName() {
        return "TSDH";
    }

    @Override
    public HashSet<Long> getKSeeds(int k) {
        //先过滤onlineUser，再从选出的sql里过滤
        Connection conn = DBHelper.getConnection();
        HashSet<Long> resultSet = new HashSet<>();
        try {
            Statement st = conn.createStatement();

            HashSet<Long> m_onlineUserSet = InfluenceMaxUtilMethod.filterOnlineUserSet(st, m_startTimeSecond, m_maxDelay);
            //存的是每个用户的ffval
            HashMap<Long, Double> infValMap = new HashMap<>();
            fetchOnlineFFVal(m_onlineUserSet, st, infValMap);
            //排序infValMap并选k个
            List<Map.Entry<Long, Double>> lst = sortLongDoubleMapByValueDesc(infValMap);
            for(int i = 0; i < k; i++){
                resultSet.add(lst.get(i).getKey());
            }
            return resultSet;
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            try {
                conn.close();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }

        return resultSet;
    }

    /**
     * 计算在线用户的ff(val)
     */
    public void fetchOnlineFFVal(HashSet<Long> onlineUserSet, Statement st, HashMap<Long, Double> infValMap){
        try {
            ResultSet rs = st.executeQuery("select uid,originUid,perTimeSecond from edges_1115_1228_maintopic_time");
            while (null != rs && rs.next()) {
                Long uid = rs.getLong("uid");
                Long originUid = rs.getLong("originUid");
                double perTimeSecond = rs.getDouble("perTimeSecond");
                double addInf = Math.exp(-perTimeSecond/2159.975);
                //double addInf = Math.exp(-perTimeSecond * tParam);//TODOdone 为了测试最好的时间系数
                if (!onlineUserSet.contains(uid)) {
                    continue;
                }
                if (uid == originUid) {
                    continue;
                }
                if(!infValMap.containsKey(uid)){
                    infValMap.put(uid, perTimeSecond);
                }
                else{
                    infValMap.put(uid, infValMap.get(uid) + addInf);//各边上的时延之和
                }
            }
        }catch (Exception ex){
            System.out.println(ex.toString());
        }
    }

    @Override
    public double getMaxDelay() {
        return 0;
    }

    @Override
    public double getStartTimeSecond() {
        return 0;
    }

    @Override
    public double getMaxInfVal() {
        return 0;
    }
    private String m_tableNameSuffix = "";
    /**
     *
     * @param startTime 开始的时间，以秒记
     * @param maxDelay 按秒计算的最大时延
     * @param theta 影响传播阈值
     */
    public InfMax_TSDH(double startTime, double maxDelay, double theta, String tableNameSuffix){
        this.m_maxDelay = maxDelay;
        this.m_theta = theta;
        this.m_startTimeSecond = startTime;
        this.m_tableNameSuffix = tableNameSuffix;
    }
}
