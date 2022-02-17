package influencemax.TimeCriticalIM;

import influencemax.util.DBHelper;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * Created by jack on 2018/5/28.
 */
public class computeSeedResultInf {
    public static Integer minId = 1564;
    public static void main(String[] args){
        List<computeSeedResultInfThread> lst = new ArrayList<>();
        for(int i = 0; i < 4; i++){
            //开10个线程
            lst.add(new computeSeedResultInfThread());
        }
        for(int i = 0; i < lst.size(); i++){
            lst.get(i).start();
        }
        //calcSimutaneously();
    }
    public static void calcSimutaneously(){
        //int updateCnt = 0;
        while(true) {
            Connection conn = DBHelper.getConnection();
            try {
                Statement st = conn.createStatement();
                ResultSet rs1 = st.executeQuery("select * from seedresult where seeds<>'calculating' and infval=0 limit 1");
                if(!rs1.next()){
                    break;//没有合适的了
                }
                int id = rs1.getInt("id");
                String seeds = rs1.getString("seeds");
                HashSet<Long> seedSet = new HashSet<>();
                String[] strs = seeds.split(",");
                for (String str : strs) {
                    seedSet.add(Long.parseLong(str));
                }
                String suffix = rs1.getString("topicsuffix");
                String duration = rs1.getString("duration");
                double startSecond = Double.parseDouble(duration.split("_")[0]);
                double last = Double.parseDouble(duration.split("_")[1]);
                MontoCarloClass monto = new MontoCarloClass(seedSet, startSecond, last, 2.0, suffix, 10000);
                PreparedStatement pst = conn.prepareStatement("update seedresult set infval=? where id=?");
                double infval = monto.getMontoCarloInfSimutaneously(seedSet);
                //todo 同样的suffix和duration，在线用户和用户间的影响力都是一样的
                //这里可以利用getMontoCarloInfSimutaneously处理相同的
                //同一段文本和时延，应该会有7种算法*10个集合+3个集合=73种。
                pst.setDouble(1, infval);
                pst.setInt(2, id);
                //updateCnt++;
                pst.addBatch();
                pst.executeBatch();
                PreparedStatement pst1 = conn.prepareStatement("select * from seedresult where topicsuffix=? and duration=? and id<>? and infval=0 and seeds<>'calculating'");
                try {
                    pst1.setString(1, suffix);
                    pst1.setString(2, duration);
                    pst1.setInt(3, id);
                    ResultSet rs = pst1.executeQuery();
                    while(rs.next()){
                        int curId = rs.getInt("id");
                        String curSeeds = rs.getString("seeds");
                        HashSet<Long> curSeedSet = new HashSet<>();
                        String[] curStrs = curSeeds.split(",");
                        for (String str : curStrs) {
                            curSeedSet.add(Long.parseLong(str));
                        }
                        infval = monto.getMontoCarloInfSimutaneously(curSeedSet);
                        pst.setDouble(1, infval);
                        pst.setInt(2, curId);
                        //updateCnt++;
                        pst.addBatch();
//                        if(updateCnt >= 10){
//                            updateCnt = 0;
//                            pst.executeBatch();
//                        }
                        //本来执行就慢，不如计算一个插入一次
                        pst.executeBatch();
                    }
                }
                catch (Exception ex){

                }
                //pst.executeBatch();
                pst.close();
                st.close();
            } catch (Exception ex) {

            } finally {
                try {
                    conn.close();
                } catch (Exception ex) {

                }
            }
        }
    }

    public synchronized static int getCurMin(){
        synchronized(minId){
            minId+= 300;
            return minId - 300;
        }
    }
    static class computeSeedResultInfThread extends Thread implements Runnable{
        private int m_minId = 0;
        public computeSeedResultInfThread(){
        }

        @Override
        public void run() {
            while(true){
                this.m_minId = getCurMin();
                int updateCnt = 0;
                while(true) {
                    Connection conn = DBHelper.getConnection();
                    try {
                        Statement st = conn.createStatement();
                        ResultSet rs1 = st.executeQuery("select * from seedresult where seeds<>'calculating' and infval=0 and id>=" + m_minId +" and id<" + (m_minId+300) +" limit 1");
                        if(!rs1.next()){
                            break;//没有合适的了
                        }
                        int id = rs1.getInt("id");
                        String seeds = rs1.getString("seeds");
                        HashSet<Long> seedSet = new HashSet<>();
                        String[] strs = seeds.split(",");
                        for (String str : strs) {
                            seedSet.add(Long.parseLong(str));
                        }
                        String suffix = rs1.getString("topicsuffix");
                        String duration = rs1.getString("duration");
                        double startSecond = Double.parseDouble(duration.split("_")[0]);
                        double last = Double.parseDouble(duration.split("_")[1]);
                        MontoCarloClass monto = new MontoCarloClass(seedSet, startSecond, last, 2.0, suffix, 10000);
                        PreparedStatement pst = conn.prepareStatement("update seedresult set infval=? where id=?");
                        double infval = monto.getMontoCarloInfSimutaneously(seedSet);
                        //todo 同样的suffix和duration，在线用户和用户间的影响力都是一样的
                        //这里可以利用getMontoCarloInfSimutaneously处理相同的
                        //同一段文本和时延，应该会有7种算法*10个集合+3个集合=73种。
                        pst.setDouble(1, infval);
                        pst.setInt(2, id);
                        updateCnt++;
                        pst.addBatch();
                        PreparedStatement pst1 = conn.prepareStatement("select * from seedresult where topicsuffix=? and duration=? and id<>? and infval=0 and seeds<>'calculating' and id>=" + m_minId +" and id<" + (m_minId+300));
                        try {
                            pst1.setString(1, suffix);
                            pst1.setString(2, duration);
                            pst1.setInt(3, id);
                            ResultSet rs = pst1.executeQuery();
                            while(rs.next()){
                                int curId = rs.getInt("id");
                                String curSeeds = rs.getString("seeds");
                                HashSet<Long> curSeedSet = new HashSet<>();
                                String[] curStrs = curSeeds.split(",");
                                for (String str : curStrs) {
                                    curSeedSet.add(Long.parseLong(str));
                                }
                                infval = monto.getMontoCarloInfSimutaneously(curSeedSet);
                                pst.setDouble(1, infval);
                                pst.setInt(2, curId);
                                updateCnt++;
                                pst.addBatch();
                                if(updateCnt >= 10){
                                    updateCnt = 0;
                                    pst.executeBatch();
                                }
                            }
                        }
                        catch (Exception ex){

                        }
                        pst.executeBatch();
                        pst.close();
                        st.close();
                    } catch (Exception ex) {

                    } finally {
                        try {
                            conn.close();
                        } catch (Exception ex) {

                        }
                    }
                }
            }
        }
    }
}
