package influencemax.preProcessing;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;
import influencemax.preProcessing.processMain;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static influencemax.preProcessing.processMain.dbIp;
import static influencemax.preProcessing.processMain.endIndex;

/**
 * Created by jack on 2017/11/7.
 */
public class processThread extends Thread implements Runnable {
    int delta = 0;
    Connection conn = null;
    int maxDbId = 0;
    String toTableName;
    String fromTableName;

    /**
     *
     * @param d，每个线程每次处理的推文数
     * @param m，原推文中最大的id
     */
    public processThread(int d, int m, String from, String to){
        delta = d;
        maxDbId = m;
        fromTableName = from;
        toTableName = to;
        try {
            conn = DriverManager.getConnection("jdbc:mysql://" + dbIp + ":3306/twicrawl?serverTimezone=UTC&characterEncoding=utf8&useUnicode=true&useSSL=false", "root", "root");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    public processThread(){}
    @Override
    public void run() {
        int start = processMain.getAndSetStart();
        //创建一个StanfordCoreNLP object tokenize(分词)、ssplit(断句)、 pos(词性标注)、lemma(词形还原)、* ner(命名实体识别)、parse(语法解析)、sentiment(情感分析)
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize, ssplit, pos, lemma, parse, sentiment");//2018/4/10，注：后期加入了情感分析，为了不破坏之前的逻辑，这里另建了一个表，统一跑的时候要改在一起
        StanfordCoreNLP pipeline = new StanfordCoreNLP(props);
        HashSet<String> stopSet = new HashSet<String>();
        String[] stopWordArr = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t","u","v","w","x","y","z","about","across","after","almost","also","am","among","an","and","any","are","as","at","be","because","been","but","by","can","cannot","could","dear","did","do","either","else","ever","every","for","from","get","have","he","her","hers","him","his","how","however","i","if","in","into","is","it","its","just","least","let","like","likely","may","me","might","most","must","my","neither","no","nor","not","of","off","often","on","only","or","other","our","own","rather","say","she","should","since","so","some","than","that","the","their","them","then","there","these","they","this","tis","to","too","twas","us","want","we","were","what","when","where","which","while","who","whom","why","will","with","would","yet","you","your", "http", "'d", "'ll", "n't", "'re"};
        Collections.addAll(stopSet, stopWordArr);

        while(start < maxDbId){
            int end = start + delta;
            if(end > endIndex){
                end = endIndex;
            }
            long curTid = 0;
            //2017年11月6日，重新处理成tweetsNohttp3
            try {
                Statement stQuery = conn.createStatement();
                //TODO 下面这句有效，注释掉只是为了处理情感 2018/04/10
                PreparedStatement stUpdate = conn.prepareStatement("insert into " + toTableName + "(tid,uid,createAt,text,quoteTid,quoteUid,quoteCreateAt,replyTid,replyUid,replyCreateAt,id) values (?,?,?,?,?,?,?,?,?,?,?)");
                PreparedStatement stSentiment = conn.prepareStatement("insert into " + toTableName + "(id,tid,uid,text,sentiment,originUid) values (?,?,?,?,?,?)");
                ResultSet rs = stQuery.executeQuery("select * from "+fromTableName+" where id>"+start + " and id<=" +end);
                int updateCount = 0;
                int sentimentUpdateCnt = 0;
                while(rs != null && rs.next()){
                    String text = rs.getString("text");
                    text = replaceStr(text);
                    //2017年11月6日，由于后期需要把用户间的交互集合作为一个文档，所以这里也不过滤
//                //这里也删除文字数量小于5个的推文
//                if(text.split(" ").length < 5){
//                    continue;
//                }
                    curTid = rs.getLong("tid");
                    long uid = rs.getLong("uid");
                    int id = rs.getInt("id");
                    Timestamp createAt = rs.getTimestamp("createAt");

                    long quoteTid = rs.getLong("quoteTid");
                    long quoteUid = rs.getLong("quoteUid");
                    Timestamp quoteCreateAt = rs.getTimestamp("quoteCreateAt");
                    long replyTid = rs.getLong("replyTid");
                    long replyUid = rs.getLong("replyUid");
                    Timestamp replyCreateAt = rs.getTimestamp("replyCreateAt");

                    //处理情感
                    if(!text.isEmpty() && !text.trim().isEmpty() && (quoteUid != -1 || replyUid != -1)){
                        stanfordSentiment(pipeline, text, stopSet, stSentiment, curTid, uid,
                                createAt, quoteTid, quoteUid, quoteCreateAt, replyTid, replyUid, replyCreateAt,id);
                        sentimentUpdateCnt++;
                        if (sentimentUpdateCnt > 1000) {
                            stSentiment.executeBatch();
                            stSentiment.clearBatch();
                            updateCount = 0;
                            System.out.println(new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").format(new Date()) + curTid);
                        }
                    }

//                    {//处理推文 TODO 这一部分 需要反注释，临时为了处理情感
//                        processRegular(pipeline, text, stopSet, stUpdate, curTid, uid,
//                        createAt, quoteTid, quoteUid, quoteCreateAt, replyTid, replyUid, replyCreateAt,id);
//                        updateCount++;
//                        if (updateCount > 1000) {
//                            stUpdate.executeBatch();
//                            stUpdate.clearBatch();
//                            updateCount = 0;
//                            System.out.println(new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").format(new Date()) + curTid);
//                        }
//                    }
                }
                stUpdate.executeBatch();
                stSentiment.executeBatch();
                stQuery.close();
                stUpdate.close();
                stSentiment.close();
                rs.close();
            }
            catch(Exception ex){
                System.out.println("uid=" + curTid);
            }
            start = processMain.getAndSetStart();
        }

        try {
            conn.close();
        } catch (SQLException e) {
            System.out.println(e.toString());
        }
    }

    /**
     * 该函数是为了将词提取成动、名、形容词的形式
     */
    public void processRegular(StanfordCoreNLP pipeline, String text, HashSet<String> stopSet, PreparedStatement stUpdate, long curTid, long uid,
                               Timestamp createAt, long quoteTid, long quoteUid, Timestamp quoteCreateAt, long replyTid, long replyUid, Timestamp replyCreateAt,
                               int id) throws Exception{
        String resultText = stanfortProcess(pipeline, text, stopSet);

        stUpdate.setLong(1, curTid);
        stUpdate.setLong(2, uid);
        stUpdate.setTimestamp(3, createAt);
        stUpdate.setString(4, resultText);
        stUpdate.setLong(5, quoteTid);
        stUpdate.setLong(6, quoteUid);
        stUpdate.setTimestamp(7, quoteCreateAt);
        stUpdate.setLong(8, replyTid);
        stUpdate.setLong(9, replyUid);
        stUpdate.setTimestamp(10, replyCreateAt);
        stUpdate.setInt(11, id);
        //2017年11月7日 改为批操作
        //stUpdate.executeUpdate();
        stUpdate.addBatch();
    }

    public void stanfordSentiment(StanfordCoreNLP pipeline, String text, HashSet<String> stopSet, PreparedStatement stSentiment, long curTid, long uid,
                                    Timestamp createAt, long quoteTid, long quoteUid, Timestamp quoteCreateAt, long replyTid, long replyUid, Timestamp replyCreateAt,
                                    int id) throws Exception{
        Annotation annotation = pipeline.process(text);
        int totalSentiment = 0;
        for (CoreMap sentence : annotation.get(CoreAnnotations.SentencesAnnotation.class)) {
            Tree tree = sentence.get(SentimentCoreAnnotations.SentimentAnnotatedTree.class);
            int sentiment = RNNCoreAnnotations.getPredictedClass(tree);
            totalSentiment+= sentiment;
        }
        double sentVal = (double)totalSentiment / (double)annotation.get(CoreAnnotations.SentencesAnnotation.class).size();

        long originUid = replyUid;
        if(quoteUid > -1){
            originUid = quoteUid;
        }

        stSentiment.setInt(1, id);
        stSentiment.setLong(2, curTid);
        stSentiment.setLong(3, uid);
        stSentiment.setString(4, text);
        stSentiment.setDouble(5, sentVal);
        stSentiment.setLong(6, originUid);

        stSentiment.addBatch();
    }


    public String stanfortProcess(StanfordCoreNLP pipeline, String text, HashSet<String> stopSet){
        try {
            //region stanfordNlp处理
            Annotation annotation = new Annotation(text);
            // run all the selected Annotators on this text
            pipeline.annotate(annotation);
            List<CoreMap> sentences = annotation.get(CoreAnnotations.SentencesAnnotation.class);
            StringBuilder sb = new StringBuilder();

            for (CoreMap sentence : sentences) {
                List<CoreLabel> tokens = sentence.get(CoreAnnotations.TokensAnnotation.class);
                for (int i = 0; i < tokens.size(); i++) {
                    CoreLabel token = tokens.get(i);
                    String word = token.get(CoreAnnotations.TextAnnotation.class);            // 获取分词
                    String pos = token.get(CoreAnnotations.PartOfSpeechAnnotation.class);     // 获取词性标注
                    String ne = token.get(CoreAnnotations.NamedEntityTagAnnotation.class);    // 获取命名实体识别结果
                    String lemma = token.get(CoreAnnotations.LemmaAnnotation.class).toLowerCase();          // 获取原形
                    String upperPos = pos.toUpperCase();
                    boolean add = false;
                    if (upperPos.startsWith("NN") || upperPos.startsWith("VB")) {
                        add = true;
                    }
                    else if (upperPos.startsWith("JJ")) {
                        //查看后面一个是不是名词，如果是名词，就保留当前词
                        if (i + 1 < tokens.size()) {
                            if (tokens.get(i + 1).get(CoreAnnotations.PartOfSpeechAnnotation.class).toUpperCase().startsWith("NN")) {
                                //NN,NNP,NNPS
                                add = true;
                            }
                        }
                        //当前词是最后一个词，或者当倒数第二个词但最后一个是标点符号
                        if (i == tokens.size() - 1 || i == tokens.size() - 2 && tokens.get(i + 1).get(CoreAnnotations.PartOfSpeechAnnotation.class).toUpperCase().startsWith("")) {
                            //如果形容词是最后一个，也加入
                            add = true;
                        }
                    }
                    Pattern p = Pattern.compile("\\d");
                    Matcher m = p.matcher(lemma);
                    if(add){
                        //2017/11/16 对于某些单词，其中夹杂了数字，比如说b4，3ku等，把这些词去掉
                        if(lemma.length() > 2 && !stopSet.contains(lemma) && m.find() == false){//2017-11-16加长度>2限制，因为太多单词<=2无实际意义
                            sb.append(lemma).append(" ");
                        }
                    }
                }
            }
            if (sb.length() > 0) {
                sb.deleteCharAt(sb.length() - 1);
            }
            return sb.toString();
        }
        catch (Exception ex){
            System.out.println(ex.toString());
            return "";
        }
    }
    public static void main(String[] args){
        //test code
        String a = "RT @scienmag: Georgia State physicist gets $400,000 grant to study solar energy conversion https://t.co/d4vZ4GyHe0 https://t.co/tSdCWDVZ6Y";
        String b = "RT @JeopardySports: \"Who are: the Los Angeles Lakers?\"#JeopardySports #Lakers https://t.co/VpiuZZZArn";
        String c = "@BillysAdvocate @UberFacts That doesn't change the fact that Jesus was a real person.\uD83D\uDE02 \uD83D\uDE02";
        String d = "@stui999 He is Greek!";
        processThread p = new processThread();
        String aa = p.replaceStr(a);
        String bb = p.replaceStr(b);
        String cc = p.replaceStr(c);
        String dd = p.replaceStr(d);
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize, ssplit, pos, lemma, sentiment");
        StanfordCoreNLP pipeline = new StanfordCoreNLP(props);
        HashSet<String> stopSet = new HashSet<String>();
        String[] stopWordArr = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t","u","v","w","x","y","z","about","across","after","almost","also","am","among","an","and","any","are","as","at","be","because","been","but","by","can","cannot","could","dear","did","do","either","else","ever","every","for","from","get","have","he","her","hers","him","his","how","however","i","if","in","into","is","it","its","just","least","let","like","likely","may","me","might","most","must","my","neither","no","nor","not","of","off","often","on","only","or","other","our","own","rather","say","she","should","since","so","some","than","that","the","their","them","then","there","these","they","this","tis","to","too","twas","us","want","we","were","what","when","where","which","while","who","whom","why","will","with","would","yet","you","your", "http", "'d", "'ll", "n't", "'re"};
        Collections.addAll(stopSet, stopWordArr);
        dd = p.stanfortProcess(pipeline, aa, stopSet);;
        System.out.println(dd);
    }

    /**
     * 去除特殊字符
     * @param origin
     * @return
     */
    public String replaceStr(String origin){
//        //去除at
//        origin = origin.replaceAll("(@([^@ ]+) )|(@[^ @]+$)", "");
//        System.out.println(origin);
//
//        //删除emoji✈️❤️
//        origin = origin.replaceAll("[\\ud800\\udc00-\\udbff\\udfff\\ud800-\\udfff]", "");
//        System.out.println(origin);
//
//        //删除超链接
//        origin = origin.replaceAll("http(s)?://([\\w-]+\\.)+[\\w-]+(/[\\w-./?%&=]*)?", "");
//        System.out.println(origin);

        //保留数字，方便以后的stanfordNlp处理
        ////删除纯数字
        //origin = origin.replaceAll("(^(\\d+) )|( (\\d+) )|( (\\d+)$)", " ");
        origin = origin.replaceAll(" +", " ");
        origin = origin.replaceAll("(^ )|( $)", "");
        origin = origin.replaceAll("((@([^@ ]+) )|(@[^ @]+$))|([\\ud800\\udc00-\\udbff\\udfff\\ud800-\\udfff])|(http(s)?://([\\w-]+\\.)+[\\w-]+(/[\\w-./?%&=]*)?)", "");
        //下面这句是后来新加的，因为推文爬取不完整，最后的单词会是***...
        origin = origin.replaceAll("[^\\s]*\\.\\.\\.$", "");
        origin = origin.replaceAll("[^\\s]*…$", "");
        //2017/11/27把省略号替换成.
        origin = origin.replaceAll("…", "");
        //2017/11/17去掉#主题
        origin = origin.replaceAll("(#([^# ]+) )|(#[^ #]+$)", "");
        //2017年11月30日添加，因为有些推文喜欢用换行来标识单词，因此换成空格，如928025355755147264
        origin = origin.replaceAll("\\n", " ");
        //2017年11月7日把除以下字符的所有字符全部删除,.!:;' "a-zA-Z0-9
        origin = origin.replaceAll("[^ \\.,';\\!a-zA-Z0-9]+", "");
        //2017/11/17把多个标点符号替换为一个标点符号
        origin = origin.replaceAll("[\\.,';\\!]+[\\.,';\\!]+", "");
        origin = origin.replaceAll(" +", " ");
        //2017年11月7日 把开头转推的RT去掉
        origin = origin.replaceAll("^RT ", "");
        //date 2017/11/10把长度》3的字母换成2个
        origin = origin.replaceAll("(\\w)\\1{3,}", "$1$1");
        //2017/11/16 对于某些单词，其中夹杂了数字，比如说b4，3ku等，放在stanfordnlp之后把这些词去掉
        //2017/11/27把下面的标点符号后面没有空格的，统一加上空格（因为后续词典出现了这种词：thing.if等）
        origin = origin.replaceAll("([\\.,;\\!])([^ $])", "$1 $2");//万一是i'm这种就不换了
        return origin;
    }
}
