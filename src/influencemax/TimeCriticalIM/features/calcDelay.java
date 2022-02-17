package influencemax.TimeCriticalIM.features;


import influencemax.util.DBHelper;

import java.sql.*;

/**
 * Created by jack on 2018/1/23.
 * 根据特定查询的主题分布，来计算各个用户的转发时延，最后结果 存在表user_delay_1115_1228中
 */
public class calcDelay {
    public static void main(String[] args){
        double[] arr = {0.37394955073543734,
            0.1960829551209526,
            2.1181912185206663E-4,
            0.018846630326105212,
            0.1604529269061093,
            0.06978022390439918,
            0.10649130485056166,
            0.06720273915964001,
            0.006981849874942686};
        calcDelay test = new calcDelay(arr, "");
        test.startCalc();
    }

    private double[] m_distArr;
    private String m_tableNameSuffix;
    public calcDelay(double[] distArr, String tableNameSuffix){
        m_distArr = distArr;
        m_tableNameSuffix = tableNameSuffix;
    }
    public void startCalc(){
        Connection conn = DBHelper.getConnection();
        try{
            //建一张表？
            String createDelayTable = "create table `user_delay_1115_1228_" + m_tableNameSuffix + "` (`uid` bigint(20), `delay` double, primary key(`uid`))";
            Statement st = conn.createStatement();
            st.execute("drop table if EXISTS  user_delay_1115_1228_" + m_tableNameSuffix);
            st.execute(createDelayTable);
            ResultSet rs = st.executeQuery("select * from edges_1115_1228_maintopic_time");
            PreparedStatement ps = conn.prepareStatement("insert into user_delay_1115_1228_" + m_tableNameSuffix + " (uid, delay) values (?, ?)");
            //计算每个边与m_distArr的相似度，加权平均其转发时延
            long lastUid = -1;
            double delta = 0;//m_distArr的方差
            for(int i = 0; i < m_distArr.length; i++){
                delta+= m_distArr[i] * m_distArr[i];
            }
            delta = Math.sqrt(delta);
            double similaritySum = 0;//相似度的和
            double weightedTimeSum = 0;
            int batchCount = 0;
            while(null != rs && rs.next()){
                long uid = rs.getLong("uid");
                double perTimeSecond = rs.getDouble("perTimeSecond");
                if(lastUid != uid && lastUid != -1){
                    //计算lastUid并存库
                    ps.setLong(1, lastUid);
                    ps.setDouble(2, (double)weightedTimeSum / similaritySum);
                    ps.addBatch();
                    batchCount++;
                    if(batchCount >= 1000){
                        ps.executeBatch();
                    }
                    weightedTimeSum = 0;
                    similaritySum = 0;
                    lastUid = uid;
                }
                double cosVal = 0;
                double deltaCur = 0;
                for(int i = 0; i < m_distArr.length; i++){
                    int val = rs.getInt("p" + i);
                    cosVal+= val * m_distArr[i];
                    deltaCur+= val * val;
                }
                deltaCur = Math.sqrt(delta);
                double similarity = cosVal /(delta * deltaCur);
                similaritySum+= similarity;
                weightedTimeSum+= similarity * perTimeSecond;
                lastUid = uid;
            }
            ps.setLong(1, lastUid);
            ps.setDouble(2, (double)weightedTimeSum / similaritySum);
            ps.addBatch();
            ps.executeBatch();
        }
        catch (Exception ex){
            ex.printStackTrace();
        }
        finally {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
