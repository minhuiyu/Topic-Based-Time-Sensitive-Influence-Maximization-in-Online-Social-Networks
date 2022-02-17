package influencemax.TimeCriticalIM.InfMaxAlgorithm;

import influencemax.TimeCriticalIM.MontoCarloClass;
import influencemax.TimeCriticalIM.UserInfluence;
import influencemax.TimeCriticalIM.comparationTest;
import influencemax.TimeCriticalIM.InfluenceMaxUtilMethod;
import influencemax.util.DBHelper;

import java.sql.Connection;
import java.sql.Statement;
import java.util.*;

/**
 * Created by jack on 2018/3/19.
 */
public class InfMax_MCT implements InfMaxInterface {

    private double m_maxDelay = 0;
    private  double m_theta = 1/320;//time-critital influence maximization in social networks with time-delayed diffusion process
    //存储所有的两两之间的影响力？比如("3-5",0.04)
    private HashMap<String, Double> m_singleInfMap = new HashMap<>();
    //比如3-<(2,0.4),(77,0.02))
    private HashMap<Long, HashMap<Long, Double>> m_infMap = new HashMap<>();

    //能影响到该节点的所有节点集合
    private HashMap<Long, HashSet<Long>> m_whoInfMeMap = new HashMap<>();

    //查询所跨越的时间段
    //private int[] m_onlineTimes;

    private HashSet<Long> m_onlineUserSet = new HashSet<>();

    //记录每个节点的时延
    private HashMap<Long, Double> m_singleDelayMap = new HashMap<>();

    //记录两两之间的时延信息，如<3-12,120>
    private HashMap<String, Double> m_delayMap = new HashMap<>();

    //传播的开始时间
    private double m_startTimeSecond = 0.0;

    //一跳的两两间的最大影响概率。该值的作用是防止monto carlo模拟的时候概率太小一直影响不成功
    private double m_maxInfVal = 1.0;

    //记录开始执行的时间
    long m_algrothimStartTime = System.currentTimeMillis();
    long m_endTime = System.currentTimeMillis();

    private static long m_startTime = 0;
    public static boolean isDebugging = false;
    public static void main(String[] args){
//        {
//            //just for testing 2018/05/31
//            String suffix = "20180518_150432";
//            InfMax_MCT.isDebugging = true;
//            InfMaxInterface mct = new InfMax_MCT(36000, 600, 1.0/320.0, suffix);
//            HashSet<Long> seedSet = mct.getKSeeds(30);
//        }
        double startTimeSecond = 36000;
        double maxDelay = 300;//5分钟
        double theta = 1.0/320.0;
        String text = "SadiqKhan last night in London 16 000 people mainly women did a great thing in raising money and awareness of breast cancer. This was worth an applause from the highest office and I feel that our Mayor and the media have not given this event the publicity it deserves ";
        isDebugging = true;
        comparationTest ct = new comparationTest(text, startTimeSecond, maxDelay, theta);
        String fix = InfluenceMaxUtilMethod.preCreateTables(text);
        System.out.println("50分钟");
        m_startTime = System.currentTimeMillis();
        InfMaxInterface mct = new InfMax_MCT(startTimeSecond, 3000, theta, fix);
        HashSet<Long> curSeed = mct.getKSeeds(30);//直接全部记录中间的10和20,然后打印

        System.out.println("10分钟");
        m_startTime = System.currentTimeMillis();
        mct = new InfMax_MCT(startTimeSecond, 600, theta, fix);//10分钟的
        HashSet<Long> curSeed1 = mct.getKSeeds(30);//直接全部记录中间的10和20,然后打印

        System.out.println("30分钟");
        m_startTime = System.currentTimeMillis();
        mct = new InfMax_MCT(startTimeSecond, 1800, theta, fix);//30分钟的
        HashSet<Long> curSeed2 = mct.getKSeeds(30);//直接全部记录中间的10和20,然后打印
    }

    private String m_tableNameSuffix = "";
    /**
     *
     * @param startTime 开始的时间，以秒记
     * @param maxDelay 按秒计算的最大时延
     * @param theta 影响传播阈值
     */
    public InfMax_MCT(double startTime, double maxDelay, double theta, String tableNameSuffix){
        this.m_maxDelay = maxDelay;
        this.m_theta = theta;
        this.m_startTimeSecond = startTime;
        this.m_tableNameSuffix = tableNameSuffix;
    }
    public InfMax_MCT(double maxDelay, double[] qDist){
        this.m_maxDelay = maxDelay;
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
        return "MCT";
    }
    @Override
    public HashSet<Long> getKSeeds(int k){
        long getStartTime = System.currentTimeMillis();
        //startCalcInfBetweenUsers();
        List<UserInfluence> infList = calcInitialInfluence();
        //首先计算所有节点的影响力值（使用Monto carlo模拟）
        //再利用CELF优化，否则运算速度太慢太慢太慢太慢太慢

        HashSet<Long> curSingleSet = new HashSet<>();
        MontoCarloClass monto = new MontoCarloClass(curSingleSet, m_startTimeSecond, m_maxDelay, 2, m_tableNameSuffix, 500);//TODO 你们接手之后，要是有时间的话就调成1000次,我要赶论文才调的500
        for(long cUid : m_onlineUserSet){
            curSingleSet.clear();
            curSingleSet.add(cUid);
            double curInfVal = monto.getMontoCarloInfSimutaneously(curSingleSet);
            infList.add(new UserInfluence(cUid, curInfVal));
        }
        Collections.sort(infList);

        //记录已经选好的种子节点集合及其对应的影响力值
        HashSet<Long> seedSet = new HashSet<>();
        long maxInfUid = -1;
        double maxInfVal = 0;
        double lastSeedInfluence = -1;//上一个种子节点加入之后，集合的影响力。该变量是为了计算上一个节点的margin，如果上一个节点的margin>当前节点的初始影响力，那么直接不用计算了，肯定在已经计算的节点里
        double maxMarginVal = -1;//到此为止已经计算到的最大的margin值
        for(int i = 0; i < k; i++){
            lastSeedInfluence = maxInfVal;
            System.out.println(i+1);
            for(int j = 0; j < infList.size(); j++){//TODOdone 这里改成从infList里取
                long cUid = infList.get(j).uid;
                if(!seedSet.contains(cUid)){
                    double curUserInitialInf = infList.get(j).infval;
                    if(curUserInitialInf < maxMarginVal){
                        //后面的直接不用计算了
                        break;
                    }
                    //计算加入之后 的影响力值
                    seedSet.add(cUid);
                    //这里只模拟1000次
                    double curInfVal = monto.getMontoCarloInfSimutaneously(seedSet);
                    if(curInfVal > maxInfVal){
                        maxInfVal = curInfVal;
                        maxInfUid = cUid;
                        maxMarginVal = curInfVal - lastSeedInfluence;//更新当前的已计算的margin值
                    }
                    seedSet.remove(cUid);
                }
            }
            seedSet.add(maxInfUid);
            if(isDebugging == true){//本文件内调试的时候才会调用
                if(seedSet.size() % 10 == 0){
                    long endTime   = System.currentTimeMillis(); //程序结束记录时间
                    long totalTime = endTime - getStartTime;       //总消耗时间
                    double curTotalInf = lastSeedInfluence + maxMarginVal;
                    System.out.println("种子个数:" + seedSet.size() + "======原始贪心======影响力值======" + curTotalInf + "======耗时======" + (totalTime) + "ms");
                    //这里要不要打印一下看看种子节点
                    String curSeed = "";
                    for(long s : seedSet){
                        curSeed+=s+",";
                    }
                    curSeed= curSeed.substring(0, curSeed.length() - 1);
                    System.out.println(curSeed +",");
                    //这里存储 2018/05/23
                    InfluenceMaxUtilMethod.storeSeedResult("MCT", m_startTimeSecond + "_" + m_maxDelay, seedSet.size(), m_tableNameSuffix, totalTime, seedSet);
                }
            }
            maxMarginVal = 0;//选择完一轮之后，下一个应该设置为0
        }
        return seedSet;
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
            m_onlineUserSet = InfluenceMaxUtilMethod.filterOnlineUserSet(st, m_startTimeSecond, m_maxDelay);
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
}
