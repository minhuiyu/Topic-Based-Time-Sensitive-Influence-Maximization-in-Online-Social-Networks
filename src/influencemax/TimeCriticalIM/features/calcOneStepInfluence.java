package influencemax.TimeCriticalIM.features;


import influencemax.util.DBHelper;

import java.sql.Connection;
import java.sql.Statement;

/**
 * Created by jack on 2018/2/27.
 */
public class calcOneStepInfluence {
    public static void main(String[] args){
        double[] qDist = {0.37394955073543734,
                0.1960829551209526,
                2.1181912185206663E-4,
                0.018846630326105212,
                0.1604529269061093,
                0.06978022390439918,
                0.10649130485056166,
                0.06720273915964001,
                0.006981849874942686};
        calcOneStepInfluence cosi = new calcOneStepInfluence(qDist, "edges_1115_1228_infval");
        cosi.startCalcInfBetweenUsers();
    }
    private double[] m_qDist;
    private String m_tableNameSuffix;
    public calcOneStepInfluence(double[] qDist, String tableNameSuffix){
        m_qDist = qDist;
        m_tableNameSuffix = tableNameSuffix;
    }
    /**
     * 计算用户两两之间的直接（一跳）影响概率，将结果存入数据库中：edges_1115_1228_infval
     * 该函数应该给出特定待传播消息之后，在所有算法之前调用
     */
    public void startCalcInfBetweenUsers(){
        Connection conn = DBHelper.getConnection();
        try{
            Statement st = conn.createStatement();
            String calcInfSql = "create table `edges_1115_1228_infval_" + m_tableNameSuffix + "` as select uid, originUid, ((p0 * " + m_qDist[0] + ")";//for循环写完sql,直接用sql计算出来后，返回hashmap
            for(int i = 1; i < m_qDist.length; i++){
                calcInfSql+= "+(p" + i + "*" + m_qDist[i] + ")";
            }
            calcInfSql+= ")/SQRT((p0*p0)";
            for(int i = 1; i < m_qDist.length; i++){
                calcInfSql+= "+(p" + i + "*p" + i + ")";
            }
            calcInfSql+= ") as infval, interCount, originCount from edges_1115_1228_maintopic_time";
            st.execute("drop table if exists edges_1115_1228_infval_" + m_tableNameSuffix);
            st.execute(calcInfSql);
            st.execute("delete from edges_1115_1228_infval_" + m_tableNameSuffix + " where infval is null");//有些数据，在非maintopic上有数据，但在maintopic上没有数据，这时候根号下为0，求出来的是null值
            //tododone 这里选出两两之间的最大影响概率，否则模拟的时候基本全部不通过 2018/2/26，可以选出，开始的时候是程序写错了
            //ResultSet rsMaxInf = st.executeQuery("select max(infval) as maxinf from " + m_tableName);
            //rsMaxInf.next();
            //double maxInfVal = rsMaxInf.getDouble("maxinf");
            //this.m_maxInfVal = maxInfVal;
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
    }
}
