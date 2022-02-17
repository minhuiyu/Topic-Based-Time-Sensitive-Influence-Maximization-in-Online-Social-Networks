package influencemax.TimeCriticalIM.InfMaxAlgorithm;

import influencemax.TimeCriticalIM.MontoCarloClass;
import influencemax.util.DBHelper;
import influencemax.TimeCriticalIM.InfluenceMaxUtilMethod;

import java.sql.Connection;
import java.sql.Statement;
import java.util.*;

/**
 * Created by jack on 2018/2/28.
 * 随机的种子选取方法
 */
public class InfMax_RAND implements InfMaxInterface {
    private final double m_startTimeSecond;
    private final double m_maxDelay;
    private final double m_theta;

    public static void main(String[] args){
        //假设1小时
        InfMax_RAND test = new InfMax_RAND(36400, 3600, 1.0/ 320.0, "");
        //test.startCalcInfBetweenUsers();
        HashSet<Long> seedSet = test.getKSeeds(50);
        Iterator<Long> it = seedSet.iterator();
        while(null != it && it.hasNext()){
            long val = it.next();
            System.out.println(val);
        }
        //tododone 这里不同的算法在操作的过程中，起的数据库的表名字也要有区分，比如表名_方法名_种子数。（不需要区分表名，算法运行过程中无需新建表）
        //tododone 另外，这里还需要传入起始时间，用来过滤在线用户2018/2/23 22:27
        //tododone 实现以下方法
        MontoCarloClass gi = new MontoCarloClass(seedSet, test.getStartTimeSecond(), test.getMaxDelay(), test.getMaxInfVal(), "", 10000);
        double curTotalInf = gi.getMontoCarloInf();
        System.out.println("影响力模拟值为：" + curTotalInf);
    }
    private String m_tableNameSuffix = "";
    public InfMax_RAND(double startTimeSecond, double maxDelay, double theta, String tableNameSuffix){
        this.m_startTimeSecond = startTimeSecond;
        this.m_maxDelay = maxDelay;
        this.m_theta = theta;
        this.m_tableNameSuffix = tableNameSuffix;
    }
    @Override
    public double getStartTimeSecond(){
        return this.m_startTimeSecond;
    }
    @Override
    public double getMaxDelay(){
        return this.m_maxDelay;
    }
    @Override
    public double getMaxInfVal(){
        return 1.0;
    }

    @Override
    public String getName() {
        return "RAND";
    }

    @Override
    public HashSet<Long> getKSeeds(int n){
        Connection conn = DBHelper.getConnection();
        HashSet<Long> resultSet = new HashSet<>();
        try{
            Statement st = conn.createStatement();
            HashSet<Long> onlineSet = InfluenceMaxUtilMethod.filterOnlineUserSet(st, m_startTimeSecond, m_maxDelay);
            List<Long> onlineList = new ArrayList<>();
            Iterator<Long> it = onlineSet.iterator();
            while(null != it && it.hasNext()){
                onlineList.add(it.next());
            }
            for(int i = 0; i < n; i++){
                double rand = Math.random();
                int index = (int)(rand * onlineSet.size());
                long ranSeed = onlineList.get(index);
                if(!resultSet.contains(ranSeed)){
                    resultSet.add(ranSeed);
                }
                else{
                    i--;
                }
            }
        }
        catch (Exception ex){
            ex.printStackTrace();
        }
        return resultSet;
    }
}
