package influencemax.TimeCriticalIM;

import influencemax.TimeCriticalIM.features.StringDistribution;
import influencemax.TimeCriticalIM.features.calcDelay;
import influencemax.TimeCriticalIM.features.calcOneStepInfluence;
import influencemax.util.DBHelper;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;

/**
 * Created by jack on 2018/2/27.
 */
public class InfluenceMaxUtilMethod {

    /**
     * 存储当前正在计算的状态
     * @param algname
     * @param duration
     * @param k
     * @param queryTableSuffix
     */
    public static void storeCalculating(String algname, String duration, int k, String queryTableSuffix){
        Connection conn = DBHelper.getConnection();
        Statement st;
        try {
            st = conn.createStatement();
            String insertCalSql = "insert into seedresult (algname,duration,k,seeds,topicsuffix) values('" + algname + "','" + duration + "'," + k + ",'calculating','" + queryTableSuffix + "')";
            st.execute(insertCalSql);
            st.close();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    /**
     * 更新数据库里的结果种子集合和耗时
     * @param algname
     * @param duration
     * @param k
     * @param queryTableSuffix
     * @param timecost
     * @param seeds
     */
    public static void storeSeedResult(String algname, String duration, int k, String queryTableSuffix, long timecost, HashSet<Long> seeds){
        String seedstr = "";
        for(long l : seeds){
            seedstr += l + ",";
        }
        seedstr = seedstr.substring(0, seedstr.length() - 1);
        Connection conn = DBHelper.getConnection();
        Statement st;
        try {
            st = conn.createStatement();
            // 更新数据库里的结果种子集合和耗时
            String updateCalSql = "update seedresult set seeds='" + seedstr + "', timecost=" + timecost
                    + " where algname='" + algname + "' and k=" + k + " and duration='" + duration
                    + "' and topicsuffix='" + queryTableSuffix + "'";
            st.execute(updateCalSql);
            st.close();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    /**
     * 开始对比之前确保已经建立好了相应的表
     * @param text 表示 要查询的句子
     * @return
     */
    public static String preCreateTables(String text){
        StringDistribution sd = new StringDistribution(text);
        double[] dists = sd.getDistribution();
        //检查存不存在该查询
        Connection conn = DBHelper.getConnection();
        String getSql = "select tablename from query_dist_name where ";
        //String topicValues = "";
        int[] topicArr = new int[dists.length];
        for(int i = 0; i < dists.length; i++){//精确到3位的值，乘以我1000后约整
            getSql+= "topic" + i + "=" + ((int)Math.round(dists[i]*1000+0.5)) + " and ";
            //topicValues+=((int)Math.round(dists[i]*1000+0.5)) + ",";
            topicArr[i] = (int)Math.round(dists[i]*1000+0.5);
        }
        getSql = getSql.substring(0, getSql.length() - 4);
        //topicValues = topicValues.substring(0, topicValues.length() - 1);
        String queryTableSuffix = "";
        try {
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery(getSql);
            //建相应的表
            if(rs.next()){
                queryTableSuffix = rs.getString("tablename");
            }
            else{
                //该主题从未被计算过，生成一个表名，插入相应的值
                Date dt = new Date();
                DateFormat df = new SimpleDateFormat("yyyyMMdd_HHmmss");
                String suffix = df.format(dt);
                java.sql.PreparedStatement stInsert = conn.prepareStatement("insert into query_dist_name (tablename,str,topic0,topic1,topic2,topic3,topic4,topic5,topic6,topic7,topic8) values (?,?,?,?,?,?,?,?,?,?,?)");
                stInsert.setString(1, suffix);
                stInsert.setString(2, text);
                for(int i = 0; i < topicArr.length; i++){
                    stInsert.setInt(i+3, topicArr[i]);
                }
                stInsert.execute();
                //插入之后 新建表
                calcOneStepInfluence cos = new calcOneStepInfluence(dists, suffix);
                cos.startCalcInfBetweenUsers();
                calcDelay cd = new calcDelay(dists, suffix);
                cd.startCalc();
                queryTableSuffix = suffix;
            }
        }
        catch(Exception ex){
            ex.printStackTrace();
        }
        //有表之后 查看对应的查询记录有没有
        return queryTableSuffix;
    }

    /**
     * 过滤在线用户存入m_onlineUserSet
     * @param st
     */
    public static HashSet<Long> filterOnlineUserSet(Statement st, double startTime, double maxDelay){
        //tododone 该方法提出成为static方法
        HashSet<Long> onlineUserSet = new HashSet<>();
        try {
//            int startHour = (int)startTime / 3600;
//            int endHour = (int)(startTime + maxDelay - 1) / 3600;
//            int[] onlineTimes = new int[endHour - startHour + 1];
//            for(int i = startHour; i <= endHour; i++){
//                onlineTimes[i - startHour] = i;
//            }
//            String onlineSql = "select uid from time_dist_1115_1228_main10 where h" + onlineTimes[0] + " > 0";
//            for (int i = 1; i < onlineTimes.length; i++){
//                onlineSql+= " or h" + onlineTimes[i] + " > 0";
//            }

            //2018/03/24 02:12 修改成只有在这段间隔内在线才算在线用户
            int startSecond = (int)startTime;
            int endSecond = (int)(startTime + maxDelay);
            String onlineSql = "select distinct(uid) from useronlinetime_1115_1228 where createAt>" + startSecond + " and createAt<" + endSecond;

            ResultSet onlineSet = null;
            onlineSet = st.executeQuery(onlineSql);
            //存入在线用户集合
            while(null != onlineSet && onlineSet.next()){
                onlineUserSet.add(onlineSet.getLong("uid"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return onlineUserSet;
    }

    /**
     * 本方法从数据库中取出单步的影响力、时延等因素，m_singleDelayMap和m_infMap等字典中
     * @param st
     */
    public static void fetchSingleDelayMap(HashSet<Long> onlineUserSet, Statement st, HashMap<Long, Double> singleDelayMap, HashMap<String, Double> singleInfMap, HashMap<String, Double> delayMap, HashMap<Long, HashMap<Long, Double>> infMap, String tableNameSuffix){
        try {
            ResultSet rs = st.executeQuery("select * from user_delay_1115_1228_" + tableNameSuffix);
            while(null != rs && rs.next()){
                Long uid = rs.getLong("uid");
                if(!onlineUserSet.contains(uid)){
                    continue;
                }
                singleDelayMap.put(uid, rs.getDouble("delay"));
            }
            //读取两两之间的影响力，并存储两两之间的时延
            rs = st.executeQuery("select * from edges_1115_1228_infval_" + tableNameSuffix);
            //rs = st.executeQuery("select * from edges_1115_1228_infval_new");
            //这里新建的话，传回去之后的Map值不会改变
            //singleInfMap = new HashMap<>();
            //delayMap = new HashMap<>();
            //infMap = new HashMap<>();
            while(null != rs && rs.next()){
                Long uid = rs.getLong("uid");
                Long originUid = rs.getLong("originUid");
                if(!onlineUserSet.contains(uid) || !onlineUserSet.contains(originUid)){
                    continue;
                }
                //fixed 加入下面一段过滤，如果源用户和目标用户相同，直接路过
                if(uid == originUid){
                    continue;
                }
                double infval = rs.getDouble("infval");
                //fixed 原来写成了uid + "-" + originUid
                singleInfMap.put(originUid + "-" + uid, infval);
                //存储两两之间的时延
                delayMap.put(originUid + "-" + uid, singleDelayMap.get(uid));

                if(!infMap.containsKey(originUid)){
                    HashMap<Long, Double> curInfValMap = new HashMap<>();
//                    infMap.put(uid, curInfValMap);
                    //fixed
                    infMap.put(originUid, curInfValMap);
                }
                HashMap<Long, Double> curInfValMap = infMap.get(originUid);
                //fixed原来是 originUid
                curInfValMap.put(uid, infval);
                infMap.put(originUid, curInfValMap);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
