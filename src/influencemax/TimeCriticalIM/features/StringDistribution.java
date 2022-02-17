package influencemax.TimeCriticalIM.features;

import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import influencemax.preProcessing.processThread;
import influencemax.util.DBHelper;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;

/**
 * Created by jack on 2018/1/22.
 * 给定一个查询，计算出其主题分布
 */
public class StringDistribution {
    public static void main(String[] args){
        String query = "Since Donald Trump took office, 863,000 new jobs have been filled by women and half a million American women have entered the work force. ";
        StringDistribution sd = new StringDistribution(query);
        double[] result = sd.getDistribution();
        for (int i = 0; i < result.length; i++){
            System.out.println(i + ":" + result[i]);
        }
    }
    private String m_Str;
    public StringDistribution(String str){
        m_Str = str;
    }
    public double[] getDistribution(){
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize, ssplit, pos, lemma");
        StanfordCoreNLP pipeline = new StanfordCoreNLP(props);
        HashSet<String> stopSet = new HashSet<String>();
        String[] stopWordArr = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t","u","v","w","x","y","z","about","across","after","almost","also","am","among","an","and","any","are","as","at","be","because","been","but","by","can","cannot","could","dear","did","do","either","else","ever","every","for","from","get","have","he","her","hers","him","his","how","however","i","if","in","into","is","it","its","just","least","let","like","likely","may","me","might","most","must","my","neither","no","nor","not","of","off","often","on","only","or","other","our","own","rather","say","she","should","since","so","some","than","that","the","their","them","then","there","these","they","this","tis","to","too","twas","us","want","we","were","what","when","where","which","while","who","whom","why","will","with","would","yet","you","your", "http", "'d", "'ll", "n't", "'re"};
        Collections.addAll(stopSet, stopWordArr);
        processThread pt = new processThread();
        String resultText = pt.stanfortProcess(pipeline, m_Str, stopSet);
        String[] words = resultText.split(" +");
        //处理完为9个主题
        int[] topicArr = new int[9];
        for(int i = 0; i < topicArr.length; i++){
            topicArr[i] = 0;
        }
        double[] distArr = new double[topicArr.length];
        try{
            Connection conn = DBHelper.getConnection();
            Statement st = conn.createStatement();
            for(int i = 0; i < words.length; i++){
                String curWord = words[i];
                String getTopicSql = "select wt.* from dictionary_1115_1228 dic, wordtopic_1115_1228 wt where word='" + curWord + "' and wt.id = dic.id";
                //得到id之后从wordtopic中取得其主题分布
                //所有单词的主题分布累加
                ResultSet rs = st.executeQuery(getTopicSql);
                while(null != rs && rs.next()){
                    int total = 0;
                    double[] curWordDist = new double[topicArr.length];
                    for(int j = 0; j < topicArr.length; j++){
                        curWordDist[j] = rs.getInt("t" + j);
                        total+= curWordDist[j];
                    }
                    for(int j = 0; j < topicArr.length; j++){
                        curWordDist[j] = (double)curWordDist[j]/total;
                    }
                    //加到目标分布
                    for(int j = 0; j < distArr.length; j++){
                        distArr[j]+= curWordDist[j];
                    }
                }
            }
            double total = 0;
            for(int i = 0; i < distArr.length; i++){
                total+= distArr[i];
            }
            if(total > 0){
                for(int j = 0; j < distArr.length; j++){
                    distArr[j] = (double)distArr[j] / total;
                }
            }
        }
        catch (Exception ex){
            ex.printStackTrace();
        }
        return distArr;
    }
}
