package influencemax.TimeCriticalIM;

import influencemax.TimeCriticalIM.InfMaxAlgorithm.InfMaxInterface;
import influencemax.TimeCriticalIM.InfMaxAlgorithm.InfMax_DH;
import influencemax.TimeCriticalIM.InfMaxAlgorithm.InfMax_ISP;
import influencemax.TimeCriticalIM.InfMaxAlgorithm.InfMax_MCT;
import influencemax.TimeCriticalIM.InfMaxAlgorithm.InfMax_MIAC;
import influencemax.TimeCriticalIM.InfMaxAlgorithm.InfMax_TTH;
import influencemax.TimeCriticalIM.features.StringDistribution;
import influencemax.TimeCriticalIM.features.calcDelay;
import influencemax.TimeCriticalIM.features.calcOneStepInfluence;
import influencemax.TimeCriticalIM.InfMaxAlgorithm.InfMax_TTG;
import influencemax.TimeCriticalIM.InfMaxAlgorithm.InfMax_RAND;
import influencemax.TimeCriticalIM.InfMaxAlgorithm.InfMax_TSDH;
import influencemax.util.DBHelper;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;

/**
 * Created by jack on 2018/3/7.
 */
public class comparationTest {
    public static void main(String[] args){
        int[] maxDelayArr = {300, 600, 1500, 1800, 3000};
        //文本4，2018/05/20
        for(int i = 0; i < maxDelayArr.length; i++){
//            calcCompare("We can live like Jack and Sally if we want, Where you can always find me, And we'll have Halloween on Christmas, And in the night we'll wish this never ends, We'll wish this never ends.",
//                    36000, maxDelayArr[i]);
            double theta = 1.0 / 320.0;
            comparationTest ct = new comparationTest("We can live like Jack and Sally if we want, Where you can always find me, And we'll have Halloween on Christmas, And in the night we'll wish this never ends, We'll wish this never ends.",
                    36000, maxDelayArr[i], theta);
            ct.startComapreMCT();
        }

        //文本5，2018/05/22
        for(int i = 0; i < maxDelayArr.length; i++){
//            calcCompare("Sharon Tan, the Senior QA Reviewer, shared @ISCA_Official initiatives to help SMPs in their ISQC 1 implementation journey, during the @kicpaa_official - @AFA_Accountants Conference 2018. She was also a panellist in the panel discussion on platform & tools to support ASEAN SMPs. ",
//                    36000, maxDelayArr[i]);
            double theta = 1.0 / 320.0;
            comparationTest ct = new comparationTest("Sharon Tan, the Senior QA Reviewer, shared @ISCA_Official initiatives to help SMPs in their ISQC 1 implementation journey, during the @kicpaa_official - @AFA_Accountants Conference 2018. She was also a panellist in the panel discussion on platform & tools to support ASEAN SMPs. ",
                    36000, maxDelayArr[i], theta);
            ct.startComapreMCT();
        }

        //文本3，2018/05/22
        for(int i = 0; i < maxDelayArr.length; i++){
//            calcCompare("Congratulations to winners from Jhargram district. It’s not easy to fight against TMC terror politics and win against all odds, real women power.  ",
//                    36000, maxDelayArr[i]);
            double theta = 1.0 / 320.0;
            comparationTest ct = new comparationTest("Congratulations to winners from Jhargram district. It’s not easy to fight against TMC terror politics and win against all odds, real women power.  ",
                    36000, maxDelayArr[i], theta);
            ct.startComapreMCT();
        }
        //XXX 使用以上三种文本


        //calcCompare("Since Donald Trump took office, 863,000 new jobs have been filled by women and half a million American women have entered the work force. ", 36000, 1500);
//        //文本1，25分钟
//        System.out.println("文本1，25分钟");
//        calcCompare("Since Donald Trump took office, 863,000 new jobs have been filled by women and half a million American women have entered the work force. ", 36000, 1500);
//        //文本2，25分钟
//        System.out.println("文本2，25分钟");
//        calcCompare("SadiqKhan last night in London 16 000 people mainly women did a great thing in raising money and awareness of breast cancer. This was worth an applause from the highest office and I feel that our Mayor and the media have not given this event the publicity it deserves ",
//36000, 1500);
        //文本2，5分钟
//        System.out.println("文本2，5分钟");
//        calcCompare("SadiqKhan last night in London 16 000 people mainly women did a great thing in raising money and awareness of breast cancer. This was worth an applause from the highest office and I feel that our Mayor and the media have not given this event the publicity it deserves ",
//                36000, 300);
//        //文本2，10分钟
//        System.out.println("文本2，10分钟");
//        calcCompare("SadiqKhan last night in London 16 000 people mainly women did a great thing in raising money and awareness of breast cancer. This was worth an applause from the highest office and I feel that our Mayor and the media have not given this event the publicity it deserves ",
//                36000, 600);
        //文本2，30分钟
//        System.out.println("文本2，30分钟");
//        calcCompare("SadiqKhan last night in London 16 000 people mainly women did a great thing in raising money and awareness of breast cancer. This was worth an applause from the highest office and I feel that our Mayor and the media have not given this event the publicity it deserves ",
//                36000, 1800);
//        //文本2，50分钟
//        System.out.println("文本2，50分钟");
//        calcCompare("SadiqKhan last night in London 16 000 people mainly women did a great thing in raising money and awareness of breast cancer. This was worth an applause from the highest office and I feel that our Mayor and the media have not given this event the publicity it deserves ",
//                36000, 3000);

//        //对比这一主题选出来的，在另外一个主题里的影响力
//        String text1 = "Since Donald Trump took office, 863,000 new jobs have been filled by women and half a million American women have entered the work force. ";
//        String text2 = "SadiqKhan last night in London 16 000 people mainly women did a great thing in raising money and awareness of breast cancer. This was worth an applause from the highest office and I feel that our Mayor and the media have not given this event the publicity it deserves ";
//        String suffixStr = ct.preCreateTables(text1);
//        String suffixStr2 = ct.preCreateTables(text2);
//        for(int i = 10; i <= 100; i+=10){
//	        InfMaxInterface myAlgorithm = new InfMax_TTG(36000, 1500, 1.0/320.0, suffixStr);
//	        HashSet<Long> curSeed = myAlgorithm.getKSeeds(i);
//	        MontoCarloClass monto = new MontoCarloClass(curSeed, 36000, 1500, 2, suffixStr2, 1000);//为了效率设置1000次吧？有篇论文都说够用了
//	        double infVal = monto.getMontoCarloInf();
//	        System.out.println(i + "==" + infVal);
//        }
    }

    /**
     * 该函数用户开始特定的比较（不包含MCT算法）
     * @param text
     * @param startTimeSecond
     * @param maxDelay
     */
    public static void calcCompare(String text, double startTimeSecond, double maxDelay){
        double theta = 1.0 / 320.0;
        comparationTest ct = new comparationTest(text, startTimeSecond, maxDelay, theta);
        ct.startCompare();
    }
    private String m_text = "";
    private double m_startTimeSecond = 36000;
    private double m_maxDelay = 1500;
    private double m_theta = 1.0/320.0;
    public comparationTest(String text, double startTimeSecond, double maxDelay, double theta){
        m_text = text;
        m_startTimeSecond = startTimeSecond;
        m_maxDelay = maxDelay;
        m_theta = theta;
    }

    /**
     * 对比MCT算法
     */
    public void startComapreMCT(){
        //这里要实时输出
        String suffixStr = InfluenceMaxUtilMethod.preCreateTables(m_text);
        InfMaxInterface mct = new InfMax_MCT(m_startTimeSecond, m_maxDelay, m_theta, suffixStr);
        InfMax_MCT.isDebugging = true;
        String durationStr = m_startTimeSecond + "_" + m_maxDelay;
        InfluenceMaxUtilMethod.storeCalculating(mct.getName(), durationStr, 10, suffixStr);
        InfluenceMaxUtilMethod.storeCalculating(mct.getName(), durationStr, 20, suffixStr);
        InfluenceMaxUtilMethod.storeCalculating(mct.getName(), durationStr, 30, suffixStr);
        long startTime = System.currentTimeMillis();
        //tododone，这里写成逐步存储结果
        HashSet<Long> curSeed = mct.getKSeeds(30);
        long endTime = System.currentTimeMillis(); //程序结束记录时间
        long totalTime = endTime - startTime;       //总消耗时间
        //存储结果
        //fixmefixed 应该写cnt
        //InfluenceMaxUtilMethod.storeSeedResult(mct.getName(), durationStr, 30, suffixStr, totalTime, curSeed);

        System.out.println(mct.getName() + "\t\t" + 30 + "\t\t" + (m_maxDelay / 60) + "min");
    }

    /**
     * 对比除MCT以外的运行算法
     */
    public void startCompare(){
        //这里要实时输出
        String suffixStr = InfluenceMaxUtilMethod.preCreateTables(m_text);
        for(int cnt = 10; cnt <= 100; cnt+=10){
            InfMaxInterface myAlgorithm = new InfMax_TTG(m_startTimeSecond, m_maxDelay, m_theta, suffixStr);
            InfMaxInterface random = new InfMax_RAND(m_startTimeSecond, m_maxDelay, m_theta, suffixStr);
            InfMaxInterface mia_c = new InfMax_MIAC(m_startTimeSecond, m_maxDelay, m_theta, suffixStr);
            InfMaxInterface heuristic = new InfMax_TTH(m_startTimeSecond, m_maxDelay, m_theta, suffixStr);
            InfMaxInterface isp = new InfMax_ISP(m_startTimeSecond, m_maxDelay, m_theta, suffixStr);
            InfMaxInterface dh = new InfMax_DH(m_startTimeSecond, m_maxDelay, m_theta, suffixStr);
            InfMaxInterface tsdh = new InfMax_TSDH(m_startTimeSecond, m_maxDelay, m_theta, suffixStr);
            InfMaxInterface mct = new InfMax_MCT(m_startTimeSecond, m_maxDelay, m_theta, suffixStr);//由于太耗时，暂时不测

            String durationStr = m_startTimeSecond + "_" + m_maxDelay;
            InfMaxInterface[] algorithmArr = {myAlgorithm, random, mia_c, heuristic, isp, dh, tsdh};
            for(int i = 0; i < algorithmArr.length; i++){
                InfMaxInterface curAlg = algorithmArr[i];
                //正在计算标签
                //fixmefixed 应该写cnt
                InfluenceMaxUtilMethod.storeCalculating(curAlg.getName(), durationStr, cnt, suffixStr);
                long startTime = System.currentTimeMillis();
                HashSet<Long> curSeed = curAlg.getKSeeds(cnt);
                long endTime   = System.currentTimeMillis(); //程序结束记录时间
                long totalTime = endTime - startTime;       //总消耗时间
                //存储结果
                //fixmefixed 应该写cnt
                InfluenceMaxUtilMethod.storeSeedResult(curAlg.getName(), durationStr, cnt, suffixStr, totalTime, curSeed);

                System.out.println(curAlg.getName() + "\t\t" + cnt + "\t\t" + (m_maxDelay/60) + "min");

                //后面的模拟值统一再计算
//                MontoCarloClass monto = new MontoCarloClass(curSeed, m_startTimeSecond, m_maxDelay, 2, suffixStr, 1000);//为了效率设置1000次吧？有篇论文都说够用了
//                double infVal = monto.getMontoCarloInf();
//                String result = "种子个数:(" + cnt + "),算法名(" + curAlg.getName() + "),影响力值(" + infVal + "),耗时ms(" + totalTime + "),种子集合(";
//                for(long seed : curSeed){
//                    result+= seed + ",";
//                }
//                result = result.substring(0, result.length() - 1);
//                result+= ")";
//                System.out.println(result);
            }
        }
    }
}
