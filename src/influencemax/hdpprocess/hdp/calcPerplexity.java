package influencemax.hdpprocess.hdp;

import influencemax.hdpprocess.perplexity.GammaDistrn;
import influencemax.hdpprocess.perplexity.Self_Perplexity;
import influencemax.hdpprocess.utils.CLDACorpus;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.util.*;

/**
 * Created by jack on 2017/12/19.
 */
public class calcPerplexity {
    public static void main(String[] args){
        Self_Perplexity sperp = new Self_Perplexity();
        int K = 13;

//        HDPGibbsSampler hdp = new HDPGibbsSampler();
//        String dbIp = "localhost";
//        String tweetsTableName = "tweets1115_1213_betweenusers_5";
//        CLDACorpus corpus = new CLDACorpus("jdbc:mysql://" + dbIp + ":3306/twicrawl?serverTimezone=UTC&characterEncoding=utf8&useUnicode=true&useSSL=false",
//                "root", "root", "select text from " + tweetsTableName + " order by uid asc", "text",
//                "select count(1) as cnt from " + tweetsTableName, "cnt", null);
        PerplexityParameter result = getTotal("F:\\twitter\\preProcessWithStanfordNlp\\output\\tweets1115_1213_betweetnusers_5(only noun)\\total.txt");
        getPhi("F:\\twitter\\preProcessWithStanfordNlp\\output\\tweets1115_1213_betweetnusers_5(only noun)\\wordTopic.txt", result);
        //int V =
        double perplexity = sperp.getPerplexity(result.totalTopicNum, result.vocabularySize, (float)result.eta, result.wordCountsByTopic, result.alpha,
                result.gamma, result.totalTablesNum, result.tablesNumByTopic,
                result.docStates, result.sample_hyperparameter,
                (float)result.beta, result.totalWordNum, result.wordCountsByTopic, result.totalWordNum,
                result.tablesNumByTopic, result.wordCountsByTopic, result.phi,
                result.totalTablesNum);
        System.out.println(perplexity);
//        double perplexity = sperp.getPerplexity(int K, int V, float eta, int[] word_counts_by_z,
//        int[][] word_counts_by_zw, GammaDistrn alpha,
//                influencemax.hdpprocess.perplexity.GammaDistrn gamma, int totalTablesNum, int[] tablesNumByTopic,
//        DOCState[] docStates, boolean sample_hyperparameter, int[][] phi,
//        double beta, int totalWordsNum, int[] wordNumByTopic, int trainTotalWordsNum,
//        int[][] testDocs, DOCState[] docTestStates, List<int[][]> testDocsls,
//        int[] trn_tablesNumByTopic,int[] trn_wordNumByTopic, int[][] trn_phi,
//        int trn_totalTablesNum, int[][] testTrnDocs, int[][] testCalcDocs);
    }
    public static void getPhi(String filepath, PerplexityParameter result) {//得到主题-词矩阵
        //wordTopic
        try {
            FileReader fr = new FileReader(filepath);
            BufferedReader br = new BufferedReader(fr);
            String line = br.readLine();
            int zhutiCount = line.split(" +").length;
            int wordCount = 0;
            while (line != null) {
                wordCount++;
                line = br.readLine();
            }
            br.close();
            fr.close();
            int[][] phi = new int[zhutiCount][wordCount];
            for (int i = 0; i < phi.length; i++) {
                for (int j = 0; j < phi[0].length; j++) {
                    phi[i][j] = 0;
                }
            }
            fr = new FileReader(filepath);
            br = new BufferedReader(fr);
            line = br.readLine();
            int curWord = 0;
            while (line != null) {
                String[] arr = line.split(" +");
                for (int i = 0; i < zhutiCount; i++) {
                    phi[i][curWord] += Integer.valueOf(arr[i]);
                }
                curWord++;
                line = br.readLine();
            }
            br.close();
            fr.close();
            result.phi = phi;
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
    public static PerplexityParameter getTotal(String filepath){
        PerplexityParameter result = new PerplexityParameter();
        //total
        List<Integer> tablesNumByDocLst = new ArrayList<>();
        HashMap<Integer, Integer> tableNumsByTopicMap = new HashMap<>();
        HashMap<Integer, Integer> curTableNumbsByTopicMap = new HashMap<>();
        HashSet<Integer> vocabularySet = new HashSet<>();
        HashMap<Integer, Integer> wordNumByTopicMap = new HashMap<>();

        try{
            FileReader fr = new FileReader(filepath);
            BufferedReader br = new BufferedReader(fr);
            String line = br.readLine();line = br.readLine();
            int docCount = 0;
            int lastDocId = -1;
            int wordNum = 0;
            while(line != null && line.indexOf("d") == -1){
                if(Integer.valueOf(line.split(" +")[0]) != lastDocId){
                    lastDocId = Integer.valueOf(line.split(" +")[0]);
                    docCount++;
                }
                wordNum++;
                line = br.readLine();
            }
            br.close();
            fr.close();

            DOCState[] docStates = new DOCState[docCount];

            fr = new FileReader(filepath);
            br = new BufferedReader(fr);
            line = br.readLine();
            line = br.readLine();//直接读到数据
            int docId = -1;
            HashSet<Integer> tableSet = new HashSet<>();
            int curDocIndex = -1;//记录当前是第几个doc
            while(line != null){
                String[] arr = line.split(" +");
                int curDocId = Integer.valueOf(arr[0]);
                int curWordId = Integer.valueOf(arr[1]);
                int zhutiId = Integer.valueOf(arr[2]);
                int tableId = Integer.valueOf(arr[3]);
                if(!vocabularySet.contains(curWordId)){//加入词典为了下一步统计词典大小
                    vocabularySet.add(curWordId);
                }
                if(!wordNumByTopicMap.containsKey(zhutiId)){//统计每个主题中出现的词的数目
                    wordNumByTopicMap.put(zhutiId, 1);
                }
                else{
                    wordNumByTopicMap.put(zhutiId, wordNumByTopicMap.get(zhutiId) + 1);
                }
                if(curDocId != docId){
                    //一个新doc，结算并记录
                    curDocIndex++;
                    docStates[curDocIndex] = new DOCState(curDocId);//遇到第一行就建
                    if(curDocId > 0){
                        tablesNumByDocLst.add(tableSet.size());//到最后的时候计算上一个的
                        docStates[curDocIndex - 1].numberOfTables = tableSet.size();

                        for(Map.Entry<Integer, Integer> entry : curTableNumbsByTopicMap.entrySet()){//将当前文档中的主题-桌子数 统计进总主题-桌子数
                            int curTopicId = entry.getKey();
                            int curNums = entry.getValue();
                            if(tableNumsByTopicMap.containsKey(curTopicId)){//已经存在就加上
                                tableNumsByTopicMap.put(curTopicId, curNums + tableNumsByTopicMap.get(curTopicId));
                            }
                            else {
                                tableNumsByTopicMap.put(curTopicId, curNums);
                            }
                        }
                    }
                    tableSet = new HashSet<>();
                    curTableNumbsByTopicMap = new HashMap<>();
                }
                //将当前内容加入当前MAP的代码 统一写在后半部分，前半部分只新建当前MAP，以及将已经结束的内容统计到上一个
                docStates[curDocIndex].addWord(curWordId, zhutiId, tableId);
                if(!tableSet.contains(tableId)){
                    tableSet.add(tableId);
                    //只有该桌子没有出现的前提下，首次出现才计算其与主题的数量分布
                    if(curTableNumbsByTopicMap.containsKey(zhutiId)){//已经存在就加1
                        curTableNumbsByTopicMap.put(zhutiId, curTableNumbsByTopicMap.get(zhutiId) + 1);
                    }
                    else {
                        curTableNumbsByTopicMap.put(zhutiId, 1);
                    }
                }

                docId = curDocId;
                line = br.readLine();
                if(null == line){
                    //最后一行也要将数量加进来
                    tablesNumByDocLst.add(tableSet.size());//到最后的时候计算上一个的
                    docStates[curDocIndex].numberOfTables = tableSet.size();

                    for(Map.Entry<Integer, Integer> entry : curTableNumbsByTopicMap.entrySet()){//将当前文档中的主题-桌子数 统计进总主题-桌子数
                        int curTopicId = entry.getKey();
                        int curNums = entry.getValue();
                        if(tableNumsByTopicMap.containsKey(curTopicId)){//已经存在就加上
                            tableNumsByTopicMap.put(curTopicId, curNums + tableNumsByTopicMap.get(curTopicId));
                        }
                        else {
                            tableNumsByTopicMap.put(curTopicId, curNums);
                        }
                    }
                }
            }
            result.docStates = docStates;
            result.totalTopicNum = tableNumsByTopicMap.size();
            result.tablesNumByTopic = new int[tableNumsByTopicMap.size()];
            //从小到大记录topic中的桌子数
            for(int topicIndex = 0; topicIndex < tableNumsByTopicMap.size(); topicIndex++){
                result.tablesNumByTopic[topicIndex] = tableNumsByTopicMap.get(topicIndex);
            }
            result.wordCountsByTopic = new int[wordNumByTopicMap.size()];
            for(int topicIndex = 0; topicIndex < wordNumByTopicMap.size(); topicIndex++){
                result.wordCountsByTopic[topicIndex] = wordNumByTopicMap.get(topicIndex);
            }
            result.alpha = new GammaDistrn(2,1);
            //计算总共的桌子数
            for(int i = 0; i < tablesNumByDocLst.size(); i++){
                result.totalTablesNum+= tablesNumByDocLst.get(i);
            }
            result.sample_hyperparameter = false;
            result.vocabularySize = vocabularySet.size();
            result.eta = 0.01;
            result.beta = 0.01;
            result.gamma.m_gamma = result.gamma.getShape() * result.gamma.getScale();
            result.alpha.m_gamma = result.alpha.getShape() * result.alpha.getScale();
            result.totalWordNum = wordNum;
            System.out.println("end of computing");
        }
        catch (Exception ex){
            ex.printStackTrace();
        }
        return result;
    }
    static class PerplexityParameter{
        public int totalTopicNum;
        public int vocabularySize;
        public double eta = 0.01; // 主题-词 phi 的先验参数
        public int[] wordCountsByTopic;
        public GammaDistrn alpha = new GammaDistrn(2,1);
        public int totalTablesNum = 0;
        public int[] tablesNumByTopic;
        public DOCState[] docStates;
        public boolean sample_hyperparameter = false;
        public double beta = 0.01;
        public int totalWordNum = 0;
        public GammaDistrn gamma = new GammaDistrn(20,1);
        public int[][] phi;

        public PerplexityParameter(){

        }
    }
}
