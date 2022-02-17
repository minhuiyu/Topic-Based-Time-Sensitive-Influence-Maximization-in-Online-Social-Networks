package influencemax.test;
import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.stanford.nlp.io.*;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.*;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;
import influencemax.preProcessing.processThread;
import influencemax.util.DBHelper;

/**
 * Created by jack on 2017/11/7.
 */
public class StanfordCoreNlpDemo {
    public static void main(String[] args) throws IOException {
        PrintStream docTopicPS = new PrintStream("D:\\output.txt");
        String xx = "35";
        docTopicPS.format("%10d", 35);
        docTopicPS.format("%10d", 35);
        docTopicPS.close();
        String origin = "i'm a very ''''s \"!";
        origin = origin.replaceAll("[\\.,';\\!]+[\\.,';\\!]+", "");
        origin = "this is a big thing!he didn't consider that.";
        origin = origin.replaceAll("([\\.,;\\!])([^ $])", "$1 $2");//万一是i'm
        System.out.println(origin);
        String lemma = "helloss";
        Pattern p = Pattern.compile("\\d");
        Matcher m = p.matcher(lemma);
        System.out.println(m.find());
        PrintStream pss = new PrintStream("aa.txt");
        pss.println("你好");
        pss.close();
        String s = "sadfjeiusd gooooogle taaaake fuck fuuck youuuu";
        s = s.replaceAll("(\\w)\\1{3,}", "$1$1");
        s = "hello... there...";
        s = s.replaceAll("\\s[^\\s]*\\.\\.\\.$", "");
        System.out.println(s);
        Connection conn = DBHelper.getConnection();
        try {
            Statement stQuery = conn.createStatement();
            ResultSet ps = stQuery.executeQuery("select text from tweets1115 where tid=928025355755147264 limit 1");
            ps.next();
            origin = ps.getString("text");
            origin = origin.replaceAll("\\n", " ");
            System.out.println(origin);
            stQuery.close();
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        // Create a CoreNLP pipeline. To build the default pipeline, you can just use:
        //   StanfordCoreNLP pipeline = new StanfordCoreNLP(props);
        // Here's a more complex setup example:
        //   Properties props = new Properties();
        //   props.put("annotators", "tokenize, ssplit, pos, lemma, ner, depparse");
        //   props.put("ner.model", "edu/stanford/nlp/models/ner/english.all.3class.distsim.crf.ser.gz");
        //   props.put("ner.applyNumericClassifiers", "false");
        //   StanfordCoreNLP pipeline = new StanfordCoreNLP(props);

        /**
         * 创建一个StanfordCoreNLP object
         * tokenize(分词)、ssplit(断句)、 pos(词性标注)、lemma(词形还原)、* ner(命名实体识别)、parse(语法解析)、sentiment(情感分析)
         */
        // Add in sentiment
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize, ssplit, pos, lemma, parse, sentiment");
        StanfordCoreNLP pipeline = new StanfordCoreNLP(props);
        HashSet<String> stopSet = new HashSet<String>();
        String[] stopWordArr = {"a","about","across","after","almost","also","am","among","an","and","any","are","as","at","be","because","been","but","by","can","cannot","could","dear","did","do","either","else","ever","every","for","from","get","have","he","her","hers","him","his","how","however","i","if","in","into","is","it","its","just","least","let","like","likely","may","me","might","most","must","my","neither","no","nor","not","of","off","often","on","only","or","other","our","own","rather","say","she","should","since","so","some","than","that","the","their","them","then","there","these","they","this","tis","to","too","twas","us","want","we","were","what","when","where","which","while","who","whom","why","will","with","would","yet","you","your"};
        Collections.addAll(stopSet, stopWordArr);
        String text = "Jackie votes againt Trump, but he doesn't care about who will win. Finally the banana is hers. it has been a long time since we beat them.";
        //processThread ps = new processThread();
        //String r = ps.stanfortProcess(pipeline, text, stopSet);
       // System.out.println(r);
        // Initialize an Annotation with some text to be annotated. The text is the argument to the constructor.
//        Annotation annotation = new Annotation("Kosgi Santosh sent an email to Stanford University. He didn't get a reply.");
        Annotation ann2 = new Annotation("He is quite beautiful.");
//        // run all the selected Annotators on this text
        //pipeline.annotate(ann2);
        //pipeline.prettyPrint(ann2, System.out);

        text ="he did not pass the exam, and he is not happy at all. She is a little happy.";
        int mainSentiment = 0;
        int longest = 0;
        Annotation annotation = pipeline.process(text);
        for (CoreMap sentence : annotation.get(CoreAnnotations.SentencesAnnotation.class)) {
            Tree tree = sentence.get(SentimentCoreAnnotations.SentimentAnnotatedTree.class);
            int sentiment = RNNCoreAnnotations.getPredictedClass(tree);
            System.out.println(sentiment + "===========情感值");
            String partText = sentence.toString();
            if (partText.length() > longest) {
                mainSentiment = sentiment;
                longest = partText.length();
            }
        }
        if (mainSentiment == 2 || mainSentiment > 4 || mainSentiment < 0) {
            System.out.println("这个要return null?");
        }
        pipeline.prettyPrint(annotation, System.out);
        //TweetWithSentiment tweetWithSentiment = new TweetWithSentiment(line, toCss(mainSentiment));
//
//        List<CoreMap> sentences = ann2.get(CoreAnnotations.SentencesAnnotation.class);
//        for(CoreMap sentence: sentences) {
//            for (CoreLabel token : sentence.get(CoreAnnotations.TokensAnnotation.class)) {
//                String word = token.get(TextAnnotation.class);            // 获取分词
//                String pos = token.get(CoreAnnotations.PartOfSpeechAnnotation.class);     // 获取词性标注
//                String ne = token.get(CoreAnnotations.NamedEntityTagAnnotation.class);    // 获取命名实体识别结果
//                String lemma = token.get(CoreAnnotations.LemmaAnnotation.class);          // 获取词形还原结果
//            }
//        }
        IOUtils.closeIgnoringExceptions(System.out);

    }
}
