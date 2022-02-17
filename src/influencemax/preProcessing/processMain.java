package influencemax.preProcessing;


import java.io.*;
import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import influencemax.hdpprocess.hdp.DOCState;
import influencemax.hdpprocess.hdp.HDPGibbsSampler;
import influencemax.hdpprocess.utils.CLDACorpus;
import influencemax.util.DBHelper;
import influencemax.util.LogFile;

import static influencemax.util.UtilMethod.sortMapByValueDesc;

/**
 * Created by jack on 2017/7/31.
 */
public class processMain {
    public static Integer startIndex = 0;//不包含
    public static Integer delta = 20000;
    //public static String dbIp = "localhost";
    public static String dbIp = "localhost";
    public static Integer endIndex = Integer.MAX_VALUE;//包含
    //public static Integer endIndex = 1833075;//包含
    public synchronized static int getAndSetStart(){
        synchronized (startIndex){
            startIndex += delta;
            return startIndex - delta;
        }
    }
    public static void main(String[] args){
        //TODO 注意：下面部分的代码有效，上面两句是为了单独写的存储字典和边。到时候应该放到最下边
        String fromTableName = "tweets1115_noduplicate";//最原始数据表（直接爬取的数据去重复推文后）
        //TODO 下面这条是有效的，只是为了临时改成情感分析才改的
        String regularizedTweets = "tweets1115_1228_regular";//替换完特殊字符，只保留关键词的数据
        String sentimentTweets = "tweets1115_1228_sentiment";
        String tablePrefix = "tweets1115_1228";
        if(args.length >= 1){
            dbIp = args[0];
        }
        if(args.length >= 2){
            startIndex = Integer.valueOf(args[1]);
        }
        if(args.length >= 3){
            endIndex = Integer.valueOf(args[2]);
        }
        /*这里需要把冗余的推文等信息删除*/
        Connection conn = DBHelper.getConnection();
        try {
            Statement stQuery = conn.createStatement();
            //创建关键词的推文表
            try {
                stQuery.execute("create table " + regularizedTweets + " like " + fromTableName);
                //下面是测试情感
//                stQuery.execute("create table `" + sentimentTweets + "`(`id` int(11) NOT NULL,`tid` bigint(20) DEFAULT NULL," +
//                                "`uid` bigint(20) DEFAULT NULL,`text` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,`sentiment` double DEFAULT NULL," +
//                                "`originUid` bigint(20) DEFAULT NULL, PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;");
            }catch (Exception ex){
                //log.writeLine("创新新表失败，有可能已存在" + ex.toString());
                ex.printStackTrace();
            }
            replaceIlegalChar(fromTableName, regularizedTweets);
            //replaceIlegalChar(fromTableName, sentimentTweets);

            ResultSet rs111 = stQuery.executeQuery("select count(1) as cnt from " + fromTableName);
            rs111.next();
            int cntOrigin = rs111.getInt("cnt");
            //每隔10分钟查询一次有没有插入完成
            while(true) {
                ResultSet rs = stQuery.executeQuery("select count(1) as cnt from " + regularizedTweets);
                rs.next();
                try {
                    int cnt = rs.getInt("cnt");
                    if(cntOrigin == cnt){
                        break;
                    }
                    Thread.sleep(1000 * 60 * 10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
//            TODO 下面这些暂时注释，现在重新提取关键词
//            try {
//                stQuery.execute("SET GLOBAL group_concat_max_len=1024000");
//                //将用户间交互的推文合并成一个文档
//                stQuery.execute("create table " + userInteractoinTableName + " as select uid, quoteuid, replyuid, GROUP_CONCAT(text SEPARATOR ' ') as text from " + regularizedTweets + "  group by uid, quoteuid, replyuid");
//            }catch(Exception ex){
//                ex.printStackTrace();
//            }
//            try {
//                //去除因为有些文档为空，连接后造成的连续两个空格
//                stQuery.execute("update " + userInteractoinTableName + " set text=REPLACE(text, '  ', ' ') where LOCATE('  ',text)>-1");
//            }catch(Exception ex){
//                ex.printStackTrace();
//            }
            try{
                //#统计每个用户的推文数
                stQuery.execute("create table " + tablePrefix + "_usertweetscount as select count(*) as tweetscount, uid from " + tablePrefix + "_regular group by uid");

                //#将推文数字段加入推文表（在这之前创建两个表的uid和tid索引?????答：不需要，现在速度还可以）
                stQuery.execute("create table " + tablePrefix + "_regular_15 as select t.*, u.tweetscount from " + tablePrefix + "_regular t, " + tablePrefix + "_usertweetscount u where u.uid=t.uid");

                //#选出其中tweetscount<15的所有uid，然后从tweets1115_1129_regular中删除
                stQuery.execute("delete from " + tablePrefix + "_regular_15 where tweetscount < 15");

                //#复制表tweets1115_1129_regular_15方便下一步操作
                stQuery.execute("create table " + tablePrefix + "_regular_15_copy like " + tablePrefix + "_regular_15");
                stQuery.execute(" insert into " + tablePrefix + "_regular_15_copy select * from " + tablePrefix + "_regular_15");

                //#分别在两个表上建索引
                stQuery.execute("alter table " + tablePrefix + "_regular_15 add index tidindex(tid)");
                stQuery.execute("alter table " + tablePrefix + "_regular_15_copy add index tidindex(tid)");

                //#新建表tweets1115_1129_regular_15_withorigin（如果某条推文为非原创，则origintext为原创推文）
                stQuery.execute("create table " + tablePrefix + "_regular_15_withoriginField as select t.*, c.text as origintext from " + tablePrefix + "_regular_15 t, " + tablePrefix + "_regular_15_copy c where length(t.quoteTid)>3 and c.tid=t.quoteTid");
                stQuery.execute("insert into " + tablePrefix + "_regular_15_withoriginField (select t.*, c.text as origintext from " + tablePrefix + "_regular_15 t, " + tablePrefix + "_regular_15_copy c where length(t.replyTid)>3 and c.tid=t.replyTid)");

                //####
                //#这里需要用程序爬取缺失的quoteUid和replyUid（有些推文的quoteTid有值，但其quoteUid为-1，replyTid和replyUid也一样）
                //#处理完成后，仍然有9030个replyTid不为空但replyUid为空的情况，这些是因为设置了权限爬取不到，这里作为其原始推文看待（即replyUid和quoteUid均为-1）
                //#已经将tweets1115_noduplicate中的漏掉的数据补上了
                //###//#

                //#合并单条推文内容和其originText
                stQuery.execute("create table " + tablePrefix + "_regular_15_withorigin as select id, tid, uid, createAt, CONCAT(text, origintext) as text, quoteTid, quoteUid, quoteCreateAt, replyTid, replyUid, replyCreateAt, tweetscount from " + tablePrefix + "_regular_15_withoriginField");
                stQuery.execute("insert into " + tablePrefix + "_regular_15_withorigin select * from " + tablePrefix + "_regular_15 where length(replyTid)<3 and length(quoteTid)<3");
                //#经过上述处理之后，推文数量为4104594，而原始tweets1115_1129_regular_15中有6359121条推文，这是因为前一步中留下的都是（原始推文在数据库内的推文）和（原创推文）
                //#这里需不需要将剩余的推文加进来？？？？？？？？？？？？？（答：不加了，因为要把所有的非原创推文统一处理）


                //#上面的部分已执行完成
                //#合并转发与原始推文。
                //#按uid, quoteuid, replyuid分组进行合并
                stQuery.execute("SET group_concat_max_len=1024000");
                stQuery.execute("create table " + tablePrefix + "_betweenusers_withduplicate as select uid, quoteUid as originUid, GROUP_CONCAT(text SEPARATOR ' ') as text from " + tablePrefix + "_regular_15_withorigin where quoteUid>0  group by uid, quoteUid");
                stQuery.execute("insert into " + tablePrefix + "_betweenusers_withduplicate select uid, replyUid as originUid, GROUP_CONCAT(text SEPARATOR ' ') as text from " + tablePrefix + "_regular_15_withorigin where replyUid>0  group by uid, replyUid");
                stQuery.execute("insert into " + tablePrefix + "_betweenusers_withduplicate select uid, -1 as originUid, GROUP_CONCAT(text SEPARATOR ' ') as text from " + tablePrefix + "_regular_15_withorigin where quoteUid=-1 and replyUid=-1 group by uid");
                //#上面处理完的关系有248335条
                //#下面合并相同之后剩余245600条
                stQuery.execute("create table " + tablePrefix + "_betweenusers as select uid, originUid, GROUP_CONCAT(text SEPARATOR ' ') as text from " + tablePrefix + "_betweenusers_withduplicate group by uid, originUid");

                //#删除其中的多少空格，防止多条为空的推文合并时出现问题
                stQuery.execute("update " + tablePrefix + "_betweenusers set text = REPLACE(text, '  ', ' ')");
                stQuery.execute("update " + tablePrefix + "_betweenusers set text = REPLACE(text, '  ', ' ')");
                stQuery.execute("update " + tablePrefix + "_betweenusers set text = REPLACE(text, '  ', ' ')");
                stQuery.execute("update " + tablePrefix + "_betweenusers set text = REPLACE(text, '  ', ' ')");
                stQuery.execute("update " + tablePrefix + "_betweenusers set text = REPLACE(text, '  ', ' ')");
                //#删除交互文字小于5个的关系边
                stQuery.execute("create table " + tablePrefix + "_betweenusers_5 as SELECT * from " + tablePrefix + "_betweenusers");
                stQuery.execute("delete from " + tablePrefix + "_betweenusers_5 where 1+(LENGTH(text)-length(replace(text,' ',''))) < 5");
            }
            catch(Exception ex){
                ex.printStackTrace();
            }
            processWithHdp(tablePrefix + "_betweenusers_5");
        } catch (Exception e) {
            e.printStackTrace();
        }

        //TODO 下面两条语句有效，后期加上的临时数据操作，但是需要在数据库的一系列操作之后
        storeDictionary("1115_1228");
        storeEdges("1115_1228");

    }
    public static void replaceIlegalChar(String fromTableName, String toTableName){
        //System.out.println(replaceStr("✈️❤6️⃣⬇️  878 dsfsd 878x sd878 87 sss 87 "));
        Connection conn = DBHelper.getConnection();
        long curTid = 0;
        LogFile log = new LogFile("output/去超链接和at20171106.error");
        //2017年11月6日，重新处理成tweetsNohttp3
        try {
            Statement stQuery = conn.createStatement();
            ResultSet rs = stQuery.executeQuery("select max(id) as maxId from "+fromTableName);
            if(rs != null && rs.next()) {
                int maxId = rs.getInt("maxId");
                //开16个线程处理数据
                influencemax.preProcessing.processThread[] prs= new influencemax.preProcessing.processThread[16];
                for(int i = 0; i < prs.length; i++){
                    prs[i] = new influencemax.preProcessing.processThread(20000, maxId, fromTableName, toTableName);
                }
                for(int i = 0; i < prs.length; i++){
                    prs[i].start();
                }
            }
            stQuery.close();
            rs.close();
            conn.close();
        }
        catch(Exception ex){
            log.writeLine("uid=" + curTid);
        }
        finally {
            log.closeFile();
        }
    }
    public static void processWithHdp(String tweetsTableName){
        String dictFile = "output/dictionary.txt";
        String topNWordFile = "output/topNWord.txt";
        String docTopicFile = "output/docTopic.txt";
        String wordTopicFile = "output/wordTopic.txt";
        String totalFile = "output/total.txt";
        int topWordCount = 10;
        try {
            HDPGibbsSampler hdp = new HDPGibbsSampler();
            CLDACorpus corpus = new CLDACorpus("jdbc:mysql://" + dbIp + ":3306/twicrawl?serverTimezone=UTC&characterEncoding=utf8&useUnicode=true&useSSL=false",
                    "root", "root", "select text from " + tweetsTableName + " order by uid asc", "text",
                    "select count(1) as cnt from " + tweetsTableName, "cnt", dictFile);
            hdp.addInstances(corpus.getDocuments(), corpus.getVocabularySize());

            System.out.println("sizeOfVocabulary = " + hdp.sizeOfVocabulary);
            System.out.println("totalNumberOfWords = " + hdp.totalNumberOfWords);
            System.out.println("NumberOfDocs = " + hdp.docStates.length);

            //TODO to be removed, for test only
            hdp.run(0, 3000, System.out);
            //hdp.run(0, 3000, System.out);

            try {
                PrintStream wordTopicPS = new PrintStream(wordTopicFile);
                for (int w = 0; w < hdp.sizeOfVocabulary; w++) {
                    for (int k = 0; k < hdp.numberOfTopics; k++) {
                        int wordCount = hdp.wordCountByTopicAndTerm[k][w];
                        wordTopicPS.format("%-8d ", (int) wordCount);
                    }
                    wordTopicPS.println();
                }
                wordTopicPS.close();
                PrintStream topNWordPS = new PrintStream(topNWordFile);
                for (int k = 0; k < hdp.numberOfTopics; k++) {
                    HashMap<Integer, Integer> orderdWordSet = new HashMap<Integer, Integer>();
                    for (int w = 0; w < hdp.sizeOfVocabulary; w++) {
                        int wordCount = hdp.wordCountByTopicAndTerm[k][w];
                        orderdWordSet.put(w, wordCount);
                    }

                /*下面的代码是为了提取每个主题的TopN的词*/
                    List<Map.Entry<Integer, Integer>> list = sortMapByValueDesc(orderdWordSet);
                    topNWordPS.println("Topic " + k + ":");
                    for (int i = 0; i < topWordCount; i++) {
                        int wordKey = list.get(i).getKey();
                        String wordStr = corpus.getWordByKey(wordKey);
                        if (wordStr != null) {
                            topNWordPS.println("\t" + wordStr + " " + list.get(i).getValue());
                        }
                    }
                    topNWordPS.println();
                }
                topNWordPS.close();
            }catch (Exception ex) {
                ex.printStackTrace();
            }

            try {
                PrintStream docTopicPS = new PrintStream(docTopicFile);
                double[][] theta = new double[hdp.docStates.length][hdp.numberOfTopics];
                for (int docIndex = 0; docIndex < hdp.docStates.length; docIndex++) {
                    for (int tabcnt = 0; tabcnt < hdp.docStates[docIndex].numberOfTables; tabcnt++) {
                        if (hdp.docStates[docIndex].wordCountByTable[tabcnt] > 0) {
                            int k = hdp.docStates[docIndex].tableToTopic[tabcnt];
                            theta[docIndex][k] += hdp.docStates[docIndex].wordCountByTable[tabcnt];
                        }
                    }
                }
                for (int d = 0; d < hdp.docStates.length; d++) {
                    for (int k = 0; k < hdp.numberOfTopics; k++) {
                        double val = theta[d][k];
                        try {
                            docTopicPS.format("%-10d ", (int)(val));
                        }
                        catch (Exception ex){
                            ex.printStackTrace();
                            docTopicPS.format("%-10s", "不通过"+val);
                        }
                    }
                    docTopicPS.println();
                }
                docTopicPS.close();
            }catch (Exception ex){
                ex.printStackTrace();
            }

            try {
                PrintStream topicWordPS = new PrintStream(totalFile);
                topicWordPS.println("d w z t");
                int t, docID;
                for (int d = 0; d < hdp.docStates.length; d++) {
                    DOCState docState = hdp.docStates[d];
                    docID = docState.docID;
                    for (int i = 0; i < docState.documentLength; i++) {
                        t = docState.words[i].tableAssignment;
                        topicWordPS.println(docID + " " + docState.words[i].termIndex + " " + docState.tableToTopic[t] + " " + t);
                    }
                }
                topicWordPS.close();
            }catch (Exception ex){
                ex.printStackTrace();
            }
        }
        catch (Exception ex){
            ex.printStackTrace();
        }
    }

    /**
     * 将字典入库
     * @param prefix , 例如1115_1228
     */
    public static void storeDictionary(String prefix){
        String dictTable = "dictionary_" + prefix;
        Connection conn = DBHelper.getConnection();
        try {
            Statement stQuery = conn.createStatement();
            //创建关键词的推文表
            try {
                stQuery.execute("drop table if exists " + dictTable);
                stQuery.execute("create table if not exists `" + dictTable + "` (`word` varchar(55) not null default ' '," +
                        " `id` int not null, `count` int not null, " +
                        "primary key(`word`));");

                FileReader fr = new FileReader("output/tweets" + prefix + "_betweenusers_5/dictionary.txt");
                BufferedReader br = new BufferedReader(fr);
                String line = br.readLine();
                PreparedStatement st = conn.prepareStatement("insert into " + dictTable + "(word, id, count) values(?,?,?)");
                int cacheCount = 0;
                while(line != null){
                    line = line.trim();
                    String[] arr = line.split(" +");
                    try {
                        int tmpIndex = 0;
                        String word = "";
                        if(arr.length > 2){
                            word = arr[tmpIndex++];
                        }
                        int id = Integer.valueOf(arr[tmpIndex++]);
                        int count = Integer.valueOf(arr[tmpIndex++]);
                        st.setString(1, word);
                        st.setInt(2, id);
                        st.setInt(3, count);
                        st.addBatch();
                        System.out.println(line);
                    }catch (Exception t){
                        t.printStackTrace();
                    }
                    cacheCount++;
                    if(cacheCount >= 1000){
                        st.executeBatch();
                        cacheCount = 0;
                    }
                    line = br.readLine();
                }
                if(cacheCount > 0){
                    st.executeBatch();
                }
                br.close();
                fr.close();
                st.close();
                stQuery.close();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
        catch(Exception ex){
            ex.printStackTrace();
        }
        finally{
            if(null != conn){
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    public static void storeEdges(String prefix){
        String edgeTable = "edges_" + prefix + "_tmp";
        String edgeMainTopicTable = "edges_" + prefix;
        String resultFolder = "tweets" + prefix + "_betweenusers_5";
        String tweetsBtwnUsersTable = "tweets" + prefix + "_betweenusers_5";
        Connection conn = DBHelper.getConnection();
        try {
            Statement stQuery = conn.createStatement();
            //创建关键词的推文表
            try {
                FileReader fr = new FileReader("output/" + resultFolder + "/docTopic.txt");
                BufferedReader br = new BufferedReader(fr);
                String line = br.readLine();
                String[] arr = line.split(" +");
                int topicCount = arr.length;
                stQuery.execute("drop table if exists " + edgeMainTopicTable);
                stQuery.execute("create table " + edgeMainTopicTable + " like " + edgeTable);
                stQuery.execute("insert into " + edgeMainTopicTable + " select * from " + edgeTable);
                HashMap<Integer, Integer> cntmap = new HashMap<>();
                for(int i = 0; i < topicCount; i++){
                    ResultSet rs = stQuery.executeQuery("select count(*) as cnt from " + edgeMainTopicTable + " where p" + i + ">0");
                    rs.next();
                    int cnt = rs.getInt("cnt");
                    cntmap.put(i, cnt);
                }
                //排序，去掉小于总数一定比例的列
                ResultSet totalRs = stQuery.executeQuery("select count(*) as cnt from " + edgeMainTopicTable);
                totalRs.next();
                int total = totalRs.getInt("cnt");
                //按1%算
                StringBuilder alterSql = new StringBuilder("alter table ");
                alterSql.append(edgeMainTopicTable);
                boolean bFirst = true;
                for(Map.Entry<Integer,Integer> entry : cntmap.entrySet()){
                    int cnt = entry.getValue();
                    if(cnt * 100 < total){
                        int id = entry.getKey();
                        if(!bFirst){
                            alterSql.append(",");
                        }
                        bFirst = false;
                        alterSql.append(" drop column p").append(id);
                    }
                }
                stQuery.execute(alterSql.toString());
                stQuery.close();

            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
        catch(Exception ex){
            ex.printStackTrace();
        }
        finally{
            if(null != conn){
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    public static void calcDelay(String prefix){
        Connection conn = DBHelper.getConnection();
        try {
            Statement stQuery = conn.createStatement();
            //计算转发间隔时，采用tweets1115_1228_regular_15中的内容，另外， 推文数量统计时也只采用这里的内容。因为该表中的内容才是有效内容
            //计算单条时间间隔
            //其中有很多quoteUid>-1,但quoteCreateAt=null的情况，用平均值代替
            //因此要统计出为null的条数，以及总的条数
            //注意：我这里的用户时延不在边上，而在于用户节点本身，因此，统计时
            //应该以uid为主线
            //该uid每一条非原创的时延*该条推文的主题分布,加和，除以数目，作为转发时延分布
//            -- 在tweets1115_1228_regular_15的基础上，计算出每条边的平均时延
//            drop table if EXISTS timedelta_1115_1228;
//            create table `timedelta_1115_1228`(`id` int(11) NOT NULL AUTO_INCREMENT ,`uid`  bigint(20) NULL DEFAULT NULL ,`originUid`  bigint(20) NULL DEFAULT NULL ,`timedelta` bigint(20),`notNullCount` int(11),primary key(`id`),INDEX `tidindex` (`uid`,`originUid`) USING BTREE ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARACTER SET=utf8mb4 COLLATE=utf8mb4_unicode_ci ROW_FORMAT=COMPACT;
//            insert into timedelta_1115_1228 select 0 as id, uid, quoteUid AS originUid, sum(TIMESTAMPDIFF(SECOND,quoteCreateAt,createAt)) as timedelta, count(*) as notNullCount from tweets1115_1228_regular_15 where quoteCreateAt is not null group by uid,quoteUid;
//            insert into timedelta_1115_1228 select 0 as id, uid, replyUid as originUid, sum(TIMESTAMPDIFF(SECOND,replyCreateAt,createAt)) as timedelta, count(*) as notNullCount from tweets1115_1228_regular_15 where replyCreateAt is not null group by uid,replyUid;
//            drop table if EXISTS timedelta_1115_1228_merge;
//            create table timedelta_1115_1228_merge as select uid, originUid, sum(timedelta) as sumTimeDelta, sum(notNullCount) as sumNotNullCount from timedelta_1115_1228 group by uid, originUid;
//            alter table timedelta_1115_1228_merge add column perTimeSecond double;
//            update timedelta_1115_1228_merge set perTimeSecond = sumTimeDelta/sumNotNullCount;


//            -- -- 下面是计算两个用户间的交互推文的数量
//                    -- drop table if EXISTS tweets1115_1228_regular_15_intercount;
//            -- drop table if EXISTS tweets1115_1228_regular_15_intercount_total;
//            -- create table tweets1115_1228_regular_15_intercount as select uid, quoteUid as originUid, count(*) as totalCount from tweets1115_1228_regular_15 where quoteUid>-1 group by uid, quoteUid;
//            -- insert into tweets1115_1228_regular_15_intercount select uid, replyUid as originUid, count(*) as totalCount from tweets1115_1228_regular_15 where replyUid>-1 group by uid, quoteUid;
//            -- create table tweets1115_1228_regular_15_intercount_total as select uid, originUid, sum(totalCount) as totalCountSum from tweets1115_1228_regular_15_intercount group by uid, originUid;
//            -- 上面有效
//
//                    -- 下面计算每个用户的所有推文数
//            create table tweets1115_1228_regular_15_totalCount as select uid, count(*) from tweets1115_1228_regular_15 group by uid;
//            alter table tweets1115_1228_regular_15_intercount_total add column originTotalCount int(11), PRIMARY key (`uid`);
//            alter table tweets1115_1228_regular_15_intercount_total add index originUidIndex('originUid');
//            update tweets1115_1228_regular_15_intercount_total i, tweets1115_1228_regular_15_totalCount t set i.

        } catch (SQLException e) {
            e.printStackTrace();
        }

    }
}
