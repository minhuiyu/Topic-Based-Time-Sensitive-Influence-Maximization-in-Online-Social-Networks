package influencemax.TimeCriticalIM;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by jack on 2018/5/14.
 * 为了画图方便，将结果数据格式化一下
 */
public class processResult {
    public static void main(String[] args){
        String filePath = "F:\\twitter\\大论文\\实验结果数据和图\\根据老师意见修改实验后的数据\\100_2_30_粗糙.txt";
        File f = new File(filePath);
        HashMap<String, ArrayList<alg>> mp = new HashMap<>();
        try {
            FileInputStream fis = new FileInputStream(f);
            FileReader fr = new FileReader(f);
            BufferedReader br = new BufferedReader(fr);
            String str = "";
            while( (str = br.readLine())!= null){
                alg a = new alg(str);
                ArrayList<alg> lst = new ArrayList<>();
                if(!mp.containsKey(a.algName)){
                    mp.put(a.algName, lst);
                }
                mp.get(a.algName).add(a);
            }
        }
        catch (Exception ex){
            ex.printStackTrace();
        }
        int seedGrps = 0;
        //对于每一种算法，进行排序，然后重新赋值时间
        for(Map.Entry<String, ArrayList<alg>> entry : mp.entrySet()){
            String algName = entry.getKey();
            ArrayList<alg> lst = entry.getValue();
            int[] times = new int[lst.size()];
            for(int i = 0; i < times.length; i++){
                times[i] = lst.get(i).time;
            }
            seedGrps = times.length;
            Arrays.sort(times);//对时间排序
            for(int i = 0; i < times.length; i++){
                int curTime= times[i];
                for(int j = 0; j < lst.size(); j++){
                    if(lst.get(j).seedCnt == 10 * (i+1)){
                        lst.get(j).time = curTime;
                        break;
                    }
                }
            }
        }
        for(int i = 0; i < seedGrps; i++) {
            String[] names = {"TTG", "RAND", "MIAC", "TTH", "ISP", "DH", "TSDH"};
            for(int j = 0; j < names.length; j++){
                String name = names[j];
                ArrayList<alg> lst = mp.get(name);
                alg a = lst.get(i);
                System.out.println(a.print());
            }
        }
//        //以下是按算法打印
//        for(Map.Entry<String, ArrayList<alg>> entry : mp.entrySet()){
//            String algName = entry.getKey();
//            System.out.println(algName + ":===");
//            ArrayList<alg> lst = entry.getValue();
//            for(int i = 0; i < lst.size(); i++){
//                alg a = lst.get(i);
//                System.out.println(a.print());
//            }
//        }
    }
    static class alg{
        public int seedCnt = 0;
        public ArrayList<Long> seedList = new ArrayList<>();
        public double infval = 0;
        public int time = 0;
        public String algName = "";
        public alg(){

        }
        public alg(String str){
            seedCnt = Integer.parseInt(str.substring(str.indexOf("种子个数:(") + 6, str.indexOf(")", str.indexOf("种子个数:("))));
            algName = str.substring(str.indexOf("算法名(") + 4, str.indexOf(")", str.indexOf("算法名(")));
            infval = Double.parseDouble(str.substring(str.indexOf("影响力值(") + 5, str.indexOf(")", str.indexOf("影响力值("))));
            time = Integer.parseInt(str.substring(str.indexOf("耗时ms(") + 5, str.indexOf(")", str.indexOf("耗时ms("))));
            String lstStr = str.substring(str.indexOf("种子集合(") + 5, str.indexOf(")", str.indexOf("种子集合(")));
            String[] arr = lstStr.split(",");
            for(int i = 0; i < arr.length; i++){
                seedList.add(Long.parseLong(arr[i]));
            }
        }
        public String print(){
            String result =  seedCnt + "          " +  algName
                    + "          " +  seedCnt
                    + "          " +  infval
                    + "          " +  time
                    + "          ";
            for(int i = 0; i < seedList.size(); i++){
                result+= seedList.get(i) + ",";
            }
            result = result.substring(0, result.length() - 1);
            return result;
        }
    }
}
