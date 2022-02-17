package influencemax.TimeCriticalIM;

import influencemax.util.DBHelper;
import sun.nio.cs.HistoricallyNamedCharset;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;

/**
 * Created by jack on 2018/6/1.
 * 根据mysql中的历史运行时间和结果，生成画对比图的matlab代码
 */
public class generateMatlabCode {
    public static void main(String[] args){
        Connection conn = DBHelper.getConnection();
        try{
            //选出所有文本
            Statement stTopicSuffix = conn.createStatement();
            ResultSet rsTopicSuffix = stTopicSuffix.executeQuery("select distinct(topicsuffix) as suffixes from seedresult");
            int textIndex = 0;
            while (rsTopicSuffix.next()){
                textIndex++;
                String topicsuffix = rsTopicSuffix.getString("suffixes");
                //选出所有时间
                Statement stDelay = conn.createStatement();
                ResultSet rsDelay = stDelay.executeQuery("select DISTINCT(duration) as durations from seedresult where topicsuffix='" + topicsuffix + "'");
                while(rsDelay.next()){
                    String duration = rsDelay.getString("durations");
                    //按list< list<history>>排，同一个alg进同一个内部hist
                    double last = Double.parseDouble(duration.split("_")[1]);
                    //对于当前的时长，列出所有结果
                    Statement stHist = conn.createStatement();
                    ResultSet allhistsRs = stHist.executeQuery("select algname, k, infval, timecost from seedresult where topicsuffix='"+topicsuffix+"' and duration='"+duration+"' order by k asc");
                    HashMap<String, List<history>> maps = new HashMap<>();//算法名,list<影响力等>
                    while(allhistsRs.next()){
                        String alg = allhistsRs.getString("algname");
                        int time = allhistsRs.getInt("timecost");
                        double inf = allhistsRs.getDouble("infval");
                        int k = allhistsRs.getInt("k");
                        if(!maps.containsKey(alg)){
                            maps.put(alg, new ArrayList<history>());
                        }
                        maps.get(alg).add(new history(topicsuffix, alg, k, time, inf));
                    }
                    //不需要排序上面已经order过了
//                    //按种子大小排序单个算法中的所有记录
//                    for(Map.Entry<String, List<history>> entry : maps.entrySet()){
//                        Collections.sort(entry.getValue());
//                    }
                    //排序时间。因为有些时间比较短的算法的时间会由于机器运行时的状况不同而出现k大时间短的情况，因此排个序
                    for(Map.Entry<String, List<history>> entry : maps.entrySet()){
                        List<history> hist = entry.getValue();
                        for(int i = 0; i < hist.size(); i++){
                            for(int j = i + 1; j < hist.size(); j++){
                                if(hist.get(i).time > hist.get(j).time){
                                    int tmp = hist.get(i).time;
                                    hist.get(i).time = hist.get(j).time;
                                    hist.get(j).time = tmp;
                                }
                            }
                        }
                    }
                    String title = "文本" + textIndex + "_" + (int)(last/60) + "min";
                    String drawInfStr = drawInf(maps, title + "影响力对比");
                    String drawTimeStr = drawTime(maps, title + "时间对比");
                    String drawPartTimeStr = drawPartTime(maps, title + "部分时间对比");
                    System.out.println(drawTimeStr);
                    System.out.println(drawPartTimeStr);
                    System.out.println(drawInfStr);
                    System.out.println("====================================================");
                }
            }
        }
        catch (Exception ex){
            ex.printStackTrace();
        }
        finally {
            try{
                conn.close();
            }
            catch (Exception ex){

            }
        }
    }
    public static String drawPartTime(Map<String, List<history>> maps, String title) {
        StringBuilder sb = new StringBuilder();
        sb.append("xaxis=[10:10:100]").append("\n").append("mctxaxis=[10:10:30]").append("\n");
        String[] infs = new String[maps.size()];
        for(int i = 0; i < infs.length; i++){
            infs[i] = "inf" + (i+1) + "=[";
        }
        int index = 0;
        int mctIndex = -1;
        int ispIndex = -1;
        StringBuilder sbafter = new StringBuilder();

        int picIndex = 0;
        for(Map.Entry<String, List<history>> entry : maps.entrySet()){
            String alg = entry.getKey();
            if(alg.equalsIgnoreCase("MCT")){
                mctIndex = index;
            }
            if(alg.equalsIgnoreCase("ISP")){
                ispIndex = index;
            }
            if(!alg.equalsIgnoreCase("MCT") && !alg.equalsIgnoreCase("ISP")) {
                sbafter.append(getSetpartStr(alg, picIndex)).append("\n");
                picIndex++;
            }
            List<history> lst = entry.getValue();
            for(int i = 0; i < lst.size(); i++){
                infs[index]+=lst.get(i).time + " ";
            }
            infs[index]+="]";
            index++;
        }
        for(int i = 0; i < infs.length; i++){
            sb.append(infs[i]).append("\n");
        }
        sb.append("%CREATEFIGURE2(X1, YMATRIX1)\n" +
                "%  X1:  x 数据的矢量\n" +
                "%  YMATRIX1:  y 数据的矩阵\n" +
                "%  由 MATLAB 于 20-Mar-2018 10:33:05 自动生成\n" +
                "% 创建 figure\n" +
                "figure1 = figure('NumberTitle', 'off', 'Name', '" + title + "');\n" +
                "% 创建 axes\n" +
                "axes1 = axes('Parent',figure1,'FontSize',16,'Position',[0.0957142857142857 0.0785714285714286 0.857142857142857 0.854256360078271]);\n" +
                "xlim(axes1,[10,100]);\n" +
                "box(axes1,'on');\n" +
                "hold(axes1,'on');\n" +
                "% 使用 plot 的矩阵输入创建多行\n");
        sb.append("plot1=plot(");
        for(int i = 0; i < infs.length; i++){
            if(i != mctIndex && i != ispIndex){
                sb.append("xaxis, inf").append(i+1).append(",'-o',");
            }
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append(")\n");
        return sb.append(sbafter).toString();
    }
    public static String drawTime(Map<String, List<history>> maps, String title){
        StringBuilder sb = new StringBuilder();
        sb.append("xaxis=[10:10:100]").append("\n").append("mctxaxis=[10:10:30]").append("\n");
        String[] infs = new String[maps.size()];
        for(int i = 0; i < infs.length; i++){
            infs[i] = "inf" + (i+1) + "=[";
        }
        int index = 0;
        int mctIndex = -1;
        StringBuilder sbafter = new StringBuilder();
        for(Map.Entry<String, List<history>> entry : maps.entrySet()){
            String alg = entry.getKey();
            if(alg.equalsIgnoreCase("MCT")){
                mctIndex = index;
            }
            sbafter.append(getSetpartStr(alg, index)).append("\n");
            List<history> lst = entry.getValue();
            for(int i = 0; i < lst.size(); i++){
                infs[index]+=lst.get(i).time + " ";
            }
            infs[index]+="]";
            index++;
        }
        for(int i = 0; i < infs.length; i++){
            sb.append(infs[i]).append("\n");
        }
        sb.append("%CREATEFIGURE2(X1, YMATRIX1)\n" +
                "%  X1:  x 数据的矢量\n" +
                "%  YMATRIX1:  y 数据的矩阵\n" +
                "%  由 MATLAB 于 20-Mar-2018 10:33:05 自动生成\n" +
                "% 创建 figure\n" +
                "figure1 = figure('NumberTitle', 'off', 'Name', '" + title + "');\n" +
                "% 创建 axes\n" +
                "axes1 = axes('Parent',figure1,'FontSize',16,'Position',[0.0857142857142857 0.0785714285714286 0.857142857142857 0.854256360078271]);\n" +
                "xlim(axes1,[10,100]);\n" +
                "box(axes1,'on');\n" +
                "hold(axes1,'on');\n" +
                "% 使用 plot 的矩阵输入创建多行\n");
        sb.append("plot1=plot(");
        for(int i = 0; i < infs.length; i++){
            if(i == mctIndex){
                sb.append("mctxaxis, inf").append(i+1).append(",'-o',");
            }
            else {
                sb.append("xaxis, inf").append(i+1).append(",'-o',");
            }
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append(")\n");

//        sbafter.append("% 创建 xlabel\n" +
//                "xlabel('种子集合大小/个');\n" +
//                "\n" +
//                "% 创建 ylabel\n" +
//                "ylabel('算法运行时间/ms');\n" +
//                "% 创建 legend\n" +
//                "legend1 = legend(axes1,'show');\n" +
//                "set(legend1,...\n" +
//                "    'Position',[0.137071779952796 0.557314850764967 0.212051384813486 0.35309528449585]);");
        return sb.append(sbafter).toString();
    }

    public static String getSetpartStr(String alg, int index){
        String result = "";
        switch (alg){
            case "TTG":
                result = "set(plot1(" + (index+1) + "),'DisplayName','TTG','LineWidth', 2,'Marker','x','LineStyle','--', 'Color',[1 0 0]);";
                break;
            case "RAND":
                result = "set(plot1(" + (index+1) + "),'DisplayName','RAND','LineWidth', 2,'Marker','^','Color',[0 0 0]);";
                break;
            case "MIAC":
                result = "set(plot1(" + (index+1) + "),'DisplayName','MIAC','LineWidth', 2,'Marker','pentagram','LineStyle','-.','Color',[0 0.105882352941176 0.694117647058824]);";
                break;
            case "TTH":
                result = "set(plot1(" + (index+1) + "),'DisplayName','TTH','LineWidth', 2,'Marker','o','Color',[0 1 0.6]);";
                break;
            case "ISP":
                result = "set(plot1(" + (index+1) + "),'DisplayName','ISP','LineWidth', 2,'Marker','square','Color',[1 0 1]);";
                break;
            case "DH":
                result = "set(plot1(" + (index+1) + "),'DisplayName','DH','LineWidth', 2,'Marker','*','Color',[0 0 1]);";
                break;
            case "TSDH":
                result = "set(plot1(" + (index+1) + "),'DisplayName','TSDH','LineWidth', 2,'Marker','+','Color',[0.850980392156863 0.325490196078431 0.0980392156862745]);";
                break;
            case "MCT":
                result = "set(plot1(" + (index+1) + "),'DisplayName','MCT','LineWidth', 2,'Marker','diamond','Color',[0 0.6 0]);";
                break;
        }
        return result;
    }

    public static String drawInf(HashMap<String, List<history>> maps, String title){
        StringBuilder sb = new StringBuilder();
        sb.append("xaxis=[0:10:100]").append("\n").append("mctxaxis=[0:10:30]").append("\n");
        String[] infs = new String[maps.size()];
        for(int i = 0; i < infs.length; i++){
            infs[i] = "inf" + (i+1) + "=[0 ";
        }
        int index = 0;
        int mctIndex = -1;
        StringBuilder sbafter = new StringBuilder();
        for(Map.Entry<String, List<history>> entry : maps.entrySet()){
            String alg = entry.getKey();
            if(alg.equalsIgnoreCase("MCT")){
                mctIndex = index;
            }
            sbafter.append(getSetpartStr(alg, index)).append("\n");
            List<history> lst = entry.getValue();
            for(int i = 0; i < lst.size(); i++){
                infs[index]+=lst.get(i).inf + " ";
            }
            infs[index]+="]";
            index++;
        }
        for(int i = 0; i < infs.length; i++){
            sb.append(infs[i]).append("\n");
        }
        sb.append("%CREATEFIGURE2(X1, YMATRIX1)\n" +
                "%  X1:  x 数据的矢量\n" +
                "%  YMATRIX1:  y 数据的矩阵\n" +
                "%  由 MATLAB 于 20-Mar-2018 10:33:05 自动生成\n" +
                "% 创建 figure\n" +
                "figure1 = figure('NumberTitle', 'off', 'Name', '" + title + "');\n" +
                "% 创建 axes\n" +
                "axes1 = axes('Parent',figure1,'FontSize',16,'Position',[0.0857142857142857 0.0785714285714286 0.857142857142857 0.854256360078271]);\n" +
                "box(axes1,'on');\n" +
                "hold(axes1,'on');\n" +
                "% 使用 plot 的矩阵输入创建多行\n");
        sb.append("plot1=plot(");
        for(int i = 0; i < infs.length; i++){
            if(i == mctIndex){
                sb.append("mctxaxis, inf").append(i+1).append(",'-o',");
            }
            else {
                sb.append("xaxis, inf").append(i+1).append(",'-o',");
            }
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append(")\n");

//        sbafter.append("% 创建 xlabel\n" +
//                "xlabel('种子集合大小/个');\n" +
//                "\n" +
//                "% 创建 ylabel\n" +
//                "ylabel('影响范围');\n" +
//                "\n" +
//                "% 创建 legend\n" +
//                "legend1 = legend(axes1,'show');\n" +
//                "set(legend1,...\n" +
//                "    'Position',[0.137071779952796 0.557314850764967 0.212051384813486 0.35309528449585]);");
        return sb.append(sbafter).toString();
    }


    static class history implements Comparable{
        public String topic;
        public String alg;
        public int k;
        public int time;
        public double inf;
        public history(String _topic, String _alg, int _k, int _time, double _inf){
            topic = _topic;
            alg = _alg;
            k = _k;
            time = _time;
            inf = _inf;
        }

        @Override
        public int compareTo(Object o) {
            return new Integer(k).compareTo(((history)o).k);
        }
    }
}
