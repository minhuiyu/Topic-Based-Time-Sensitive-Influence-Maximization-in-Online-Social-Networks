package influencemax.TimeCriticalIM.features;

import influencemax.util.DBHelper;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Created by jack on 2018/1/21.
 * 计算每个用户的在线时间分布(已完成)
 * 该方法不适用了（2018/05/06）
 */
public class calcTimeDistribute {
    public static void main(String[] args){
        calcTimeDistribute tmp = new calcTimeDistribute();
        tmp.startCalc();
    }
    public calcTimeDistribute(){

    }

    /**
     * 将用户所有有推文历史的时间全部作为用户的在线时间
     */
    public void startCalc(){
        //0表示不在线，1表示在线
        String createTableSql = "create table `time_dist_1115_1228_main10`(`uid` bigint(20) NOT NULL DEFAULT '0'," +
                "`h0` int default 0,`h1` int default 0,`h2` int default 0,`h3` int default 0,`h4` int default 0,`h5` int default 0,`h6` int default 0,"+
                "`h7` int default 0,`h8` int default 0,`h9` int default 0,`h10` int default 0,`h11` int default 0,`h12` int default 0,`h13` int default 0," +
                "`h14` int default 0,`h15` int default 0,`h16` int default 0,`h17` int default 0,`h18` int default 0,`h19` int default 0,`h20` int default 0," +
                "`h21` int default 0,`h22` int default 0,`h23` int default 0,primary key (`uid`))";
        Connection conn = DBHelper.getConnection();
        try{
            Statement st = conn.createStatement();
            st.execute("drop table if exists time_dist_1115_1228_main10");
            st.execute(createTableSql);
            st.execute("drop table if exists time_dist_1115_1228_tmp");
            st.execute("create table time_dist_1115_1228_tmp as select uid,DATE_FORMAT(createAt,'%H') as hourNum, count(*) as cnt from tweets1115_regular_15 group by uid, hourNum order by uid ,hourNum");
            ResultSet rs = st.executeQuery("select * from time_dist_1115_1228_tmp order by uid asc, hourNum asc");
            long curUid = -1;
            PreparedStatement stInsert = conn.prepareStatement("insert into time_dist_1115_1228_main10 (uid,h0,h1,h2,h3,h4,h5,h6,h7,h8,h9,h10,h11,h12,h13,h14,h15,h16,h17,h18,h19,h20,h21,h22,h23) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
            int lastId = 0;
            int batchCount = 0;
            boolean first = true;
            while(rs != null && rs.next()){
                long uid = rs.getLong("uid");
                int hour = Integer.valueOf(rs.getString("hourNum"));
                int cnt = rs.getInt("cnt");

                if(curUid != uid){
                    if(!first) {
                        while (lastId < 23) {
                            stInsert.setInt(lastId + 1 + 2, 0);
                            lastId++;
                        }
                        stInsert.addBatch();
                        batchCount++;
                    }
                    first = false;
                    if(batchCount > 1000){
                        stInsert.executeBatch();
                    }
                    //换一个用户
                    curUid = uid;
                    stInsert.setLong(1, uid);
                }
                while(lastId + 1 < hour){//lastId已经处理过，从lastId+1到hour-1
                    stInsert.setInt(lastId + 1 + 2, 0);
                    lastId++;
                }
                stInsert.setInt(hour + 2, cnt);
                lastId = hour;
            }
            while (lastId < 23) {
                stInsert.setInt(lastId + 1 + 2, 0);
                lastId++;
            }
            stInsert.addBatch();
            stInsert.executeBatch();
        }
        catch (Exception ex){
            ex.printStackTrace();
        }
    }
    /**
     * 下面函数只统计一个用记的10个在线时间段作为在线时间（废）
     */
    public void startCalc_10(){
        //0表示不在线，1表示在线
        String createTableSql = "create table `time_dist_1115_1228_main10`(`uid` bigint(20) NOT NULL DEFAULT '0'," +
                "`h0` int default 0,`h1` int default 0,`h2` int default 0,`h3` int default 0,`h4` int default 0,`h5` int default 0,`h6` int default 0,"+
                "`h7` int default 0,`h8` int default 0,`h9` int default 0,`h10` int default 0,`h11` int default 0,`h12` int default 0,`h13` int default 0," +
                "`h14` int default 0,`h15` int default 0,`h16` int default 0,`h17` int default 0,`h18` int default 0,`h19` int default 0,`h20` int default 0," +
                "`h21` int default 0,`h22` int default 0,`h23` int default 0,primary key (`uid`))";
        Connection conn = DBHelper.getConnection();
        try{
            Statement st = conn.createStatement();
            st.execute("drop table if exists time_dist_1115_1228_main10");
            st.execute(createTableSql);
            st.execute("drop table if exists time_dist_1115_1228_tmp");
            st.execute("create table time_dist_1115_1228_tmp as select uid,DATE_FORMAT(createAt,'%H') as hourNum, count(*) as cnt from tweets1115_regular_15 group by uid, hourNum order by uid ,hourNum");
            ResultSet rs = st.executeQuery("select * from time_dist_1115_1228_tmp order by uid asc, cnt desc, hourNum asc");
            long curUid = -1;
            int curCount = 25;
            StringBuilder sb = new StringBuilder("insert into time_dist_1115_1228_main10 (uid,");
            StringBuilder sb2 = new StringBuilder("values (");
            Statement stInsert = conn.createStatement();
            while(rs != null && rs.next()){
                long uid = rs.getLong("uid");
                int hour = Integer.valueOf(rs.getString("hourNum"));
                int cnt = rs.getInt("cnt");
                if(curCount >= 10 && curUid == uid){
                    //如果>=10只取10个，否则
                    continue;
                }
                else if(curCount >= 10 && curUid != uid){
                    //换一个用户
                    curCount = 0;
                    curUid = uid;
                    sb = new StringBuilder("insert into time_dist_1115_1228_main10 (uid,");
                    sb2 = new StringBuilder("values (");
                    sb2.append(curUid).append(",");
                }
                else if(curCount < 10 && curUid != uid){
                    //前面只有不到10条，切换了用户,把上一个执行完，启动下一个
                    executeInsert(stInsert, sb, sb2);
                    curUid = uid;
                    curCount = 0;
                    sb = new StringBuilder("insert into time_dist_1115_1228_main10 (uid,");
                    sb2 = new StringBuilder("values (");
                    sb2.append(curUid).append(",");
                }

                sb.append("h").append(hour).append(",");
                sb2.append("1").append(",");

                if(curCount == 9) {
                    executeInsert(stInsert, sb, sb2);
                }
                curCount++;
            }
        }
        catch (Exception ex){
            ex.printStackTrace();
        }
    }
    public void executeInsert(Statement stInsert, StringBuilder sb){
        sb.deleteCharAt(sb.length() - 1);
        sb.append(")");
        String testStr = sb.toString();
        try {
            stInsert.execute(testStr);
        }
        catch (Exception ex){
            ex.printStackTrace();
        }
    }
    public void executeInsert(Statement stInsert, StringBuilder sb, StringBuilder sb2){
        sb.deleteCharAt(sb.length() - 1);
        sb2.deleteCharAt(sb2.length() - 1);
        sb.append(")").append(sb2).append(")");
        String testStr = sb.toString();
        try {
            stInsert.execute(testStr);
        }
        catch (Exception ex){
            ex.printStackTrace();
        }
    }
}
