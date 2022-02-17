package influencemax.TimeCriticalIM.InfMaxAlgorithm;

import influencemax.TimeCriticalIM.MontoCarloClass;
import influencemax.TimeCriticalIM.InfluenceMaxUtilMethod;
import influencemax.util.DBHelper;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;

/**
 * Created by jack on 2018/4/28.
 */
public class InfMax_DH implements InfMaxInterface{
    public static void main(String[] args){
        double startTimeSecond = 36000;
        double maxDelay = 1500;//25分钟
        double theta = 1.0/320;
        for(int k = 0; k <= 50; k+=10){
            InfMaxInterface myAlgorithm = new InfMax_DH(startTimeSecond, maxDelay, theta, "");
            long startTime = System.currentTimeMillis();
            HashSet<Long> curSeed = myAlgorithm.getKSeeds(k);
            long endTime   = System.currentTimeMillis(); //程序结束记录时间
            long totalTime = endTime - startTime;       //总消耗时间
            MontoCarloClass monto = new MontoCarloClass(curSeed, startTimeSecond, maxDelay, 2, "");
            double infVal = monto.getMontoCarloInf();
            System.out.println("种子个数:" + k + "======" + myAlgorithm.getName() + "======影响力值======" + infVal + "======耗时======" + (totalTime) + "ms");
        }
    }

    private double m_maxDelay = 0;
    private  double m_theta = 1/320;//time-critital influence maximization in social networks with time-delayed diffusion process
    //记录每个节点的时延
    private HashMap<Long, Double> m_singleDelayMap = new HashMap<>();

    //记录两两之间的时延信息，如<3-12,120>
    private HashMap<String, Double> m_delayMap = new HashMap<>();

    //传播的开始时间
    private double m_startTimeSecond = 0.0;

    @Override
    public String getName() {
        return "DH";
    }

    @Override
    public HashSet<Long> getKSeeds(int k) {
        //先过滤onlineUser，再从选出的sql里过滤
        Connection conn = DBHelper.getConnection();
        HashSet<Long> resultSet = new HashSet<>();
        try {
            Statement st = conn.createStatement();

            HashSet<Long> m_onlineUserSet = InfluenceMaxUtilMethod.filterOnlineUserSet(st, m_startTimeSecond, m_maxDelay);
            String sqlStr = "select uid, count(*) as cnt from edges_1115_1228_infval_" + m_tableNameSuffix + " group by uid order by cnt desc;";
            ResultSet rs = st.executeQuery(sqlStr);

            while (rs.next()) {
                long cuid = rs.getLong("uid");
                if (m_onlineUserSet.contains(cuid)) {
                    resultSet.add(cuid);
                    if (resultSet.size() >= k) {
                        return resultSet;
                    }
                }
            }
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
    public InfMax_DH(double startTime, double maxDelay, double theta, String tableNameSuffix){
        this.m_maxDelay = maxDelay;
        this.m_theta = theta;
        this.m_startTimeSecond = startTime;
        this.m_tableNameSuffix = tableNameSuffix;
    }
}
