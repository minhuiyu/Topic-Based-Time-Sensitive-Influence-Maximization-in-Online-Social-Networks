package influencemax;

import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;
import influencemax.util.SqlUtil;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by jack on 2017/12/7.
 * 本程序在tweets_1115_1129_regular_15_withorigin的基础上，其中有些推文有quoteTid但是quoteUid为-1，或者replyTid不为-1但replyUid为-1，
 * 此时需要爬取对应的quoteUid或replyUid的值
 * 注：该表中不存在原始推文，处理后需要将tweets_1115_1129_rugular_15中的原创推文合并过来
 * 以上说明作废 2017/12/30
 *  现改为以下说明
 *  本程序修改的是tweets1115_noduplicate中的数据
 */
public class processQuoteUidNull {
    public static int minId = 0;
    public static void main(String[] args){
        while(true){
            Connection conn = null;
            try {
//                conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/twicrawl?user=root&password=root&useUnicode=true");
//                Statement st = conn.createStatement();
//                ResultSet rs = st.executeQuery("select count(1) as cnt from tweets1115_1129_regular_15_withorigin where (length(replyTid)>2 and replyUid='-1') or (length(quoteTid)>2 and quoteUid='-1')");
//                rs.next();
//                int cnt = rs.getInt("cnt");
//                st.close();
//                if(cnt == 0){
//                    break;
//                }
                executeReplace();
            } catch (/*SQL*/Exception e) {
                e.printStackTrace();
            }
            finally {
                if(conn != null){
                    try {
                        conn.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }

        }
    }

    public static void executeReplace(){
        SqlUtil sqlU = new SqlUtil("com.mysql.jdbc.Driver", "jdbc:mysql://223.3.69.26:3306/twicrawl?user=root&password=root&useUnicode=true");
        String getsql = "select * from tweets1115_noduplicate where (id>" + minId + ") and ((replyTid>-1 and replyUid=-1) or (quoteTid>-1 and quoteUid=-1)) limit 10000";
        ResultSet res = sqlU.selectFromSql(getsql);
        if(res == null){
            return;
        }
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream("src/resrc/Twi.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        List<TwiAuthEntity> twiAuthEntities = new ArrayList<>();
        for (int i = 0; i < 217; i++) {
            twiAuthEntities.add(new TwiAuthEntity(properties.getProperty("consumerKey_" + i),
                    properties.getProperty("consumerSecret_" + i), properties.getProperty("accessToken_" + i),
                    properties.getProperty("accessTokenSecret_" + i)));
        }
        int curId = 0;
        String updateQuoteSql = "UPDATE tweets1115_noduplicate SET quoteUid = ? WHERE tid = ?";
        PreparedStatement psQuote = sqlU.getPs(updateQuoteSql);
        String updateReplySql = "UPDATE tweets1115_noduplicate SET replyUid = ? WHERE tid = ?";
        PreparedStatement psReply = sqlU.getPs(updateReplySql);
        int quoteUpdateCnt = 0, replyUpdateCnt = 0;
        int totalNum = 1;
        try {
            while(res.next()){
                if(curId > 216){
                    curId = 0;
                }
                int id = res.getInt("id");
                minId = Math.max(id, minId);
                long tid = res.getLong("tid");
                long quoteTid = res.getLong("quoteTid");
                long quoteUid = res.getLong("quoteUid");
                long replyTid = res.getLong("replyTid");
                long replyUid = res.getLong("replyUid");
                if(quoteTid > -1 && quoteUid ==-1){//需要爬取该quoteUid
                    ConfigurationBuilder cb = new ConfigurationBuilder();
                    TwiAuthEntity curAuthEntity = twiAuthEntities.get(curId);
                    cb.setOAuthAccessToken(curAuthEntity.getAccessToken());
                    cb.setOAuthAccessTokenSecret(curAuthEntity.getAccessTokenSecret());
                    cb.setOAuthConsumerKey(curAuthEntity.getConsumerKey());
                    cb.setOAuthConsumerSecret(curAuthEntity.getConsumerSecret());
                    cb.setHttpProxyHost("localhost");
                    cb.setHttpProxyPort(1080);
                    Twitter twitter = new TwitterFactory(cb.build()).getInstance();
                    try {
                        Status st = twitter.showStatus(quoteTid);
                        long qid = st.getUser().getId();
                        psQuote.setLong(1, qid);
                        psQuote.setLong(2, tid);
                        psQuote.addBatch();
                        quoteUpdateCnt++;
                        System.out.println("第"+(totalNum++)+"条,tid="+tid);
                    }
                    catch (Exception ex){
                        ex.printStackTrace();
                    }
                    curId++;
                }
                if(curId > 216){
                    curId = 0;
                }
                if(replyTid>-1 && replyUid == -1){//需要爬取该quoteUid
                    ConfigurationBuilder cb = new ConfigurationBuilder();
                    TwiAuthEntity curAuthEntity = twiAuthEntities.get(curId);
                    cb.setOAuthAccessToken(curAuthEntity.getAccessToken());
                    cb.setOAuthAccessTokenSecret(curAuthEntity.getAccessTokenSecret());
                    cb.setOAuthConsumerKey(curAuthEntity.getConsumerKey());
                    cb.setOAuthConsumerSecret(curAuthEntity.getConsumerSecret());
                    cb.setHttpProxyHost("localhost");
                    cb.setHttpProxyPort(1080);
                    Twitter twitter = new TwitterFactory(cb.build()).getInstance();
                    try {
                        Status st = twitter.showStatus(replyTid);
                        long qid = st.getUser().getId();
                        psReply.setLong(1, qid);
                        psReply.setLong(2, tid);
                        psReply.addBatch();
                        replyUpdateCnt++;
                        System.out.println("第"+(totalNum++)+"条,tid="+tid);
                    }
                    catch (Exception ex){
                        ex.printStackTrace();
                    }
                }
                if(quoteUpdateCnt > 1000){
                    psQuote.executeBatch();
                    quoteUpdateCnt = 0;
                }
                if(replyUpdateCnt > 1000){
                    psReply.executeBatch();
                    replyUpdateCnt = 0;
                }
            }
            psQuote.executeBatch();
            psReply.executeBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        sqlU.disconnnect();
    }

}
