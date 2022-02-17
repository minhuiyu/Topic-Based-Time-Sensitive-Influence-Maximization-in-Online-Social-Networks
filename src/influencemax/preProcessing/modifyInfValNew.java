package influencemax.preProcessing;

import influencemax.util.DBHelper;

import java.sql.*;
import java.util.Random;

/**
 * Created by jack on 2018/3/23.
 */
public class modifyInfValNew {
    public static void main(String[] args){
        String sql = "select substr(CAST(infval as char),1, 5) as inf, count(*) as cnt from edges_1115_1228_infval_new group by substr(CAST(infval as char),1, 5) order by cnt desc";
        Connection conn = DBHelper.getConnection();
        try {
            Statement st = conn.createStatement();
            Statement st3 = conn.createStatement();
            PreparedStatement prst = conn.prepareStatement("update edges_1115_1228_infval_new set infval=? where id=?");
            ResultSet rs = st.executeQuery(sql);
            int upcnt = 0;
            for(int i = 0; i < 35; i++){
                rs.next();
                int cnt = rs.getInt("cnt");
                String inf = rs.getString("inf");
                String sql2 = "select * from edges_1115_1228_infval_new where substr(CAST(infval as char),1, 5)='" + inf + "'";
                int level =  (int)Math.floor(Double.parseDouble(inf) / 0.09);
                double min = level * 0.09;
                double max = (level + 1) * 0.09;
                ResultSet rs2 = st3.executeQuery(sql2);//得到超多数的这个值
                while(rs2.next()){
                    int id = rs2.getInt("id");
                    double curInfVal = rs2.getDouble("infval");
                    double newinf = curInfVal + getRandom();
                    newinf = newinf > max ? newinf - max + min: newinf;
                    prst.setDouble(1, newinf);
                    prst.setInt(2, id);
                    prst.addBatch();
                    if(upcnt >= 1000){
                        prst.executeBatch();
                        upcnt = 0;
                    }
                    upcnt++;
                }
                prst.executeBatch();
            }
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    public static double getRandom(){
        return new Random().nextDouble()*0.09;
    }
}
