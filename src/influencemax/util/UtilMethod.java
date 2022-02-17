package influencemax.util;

import java.util.*;

/**
 * Created by jack on 2017/11/9.
 */
public class UtilMethod {
    public static void swap(int[][] arr, int arg1, int arg2) {
        int[] t = arr[arg1];
        arr[arg1] = arr[arg2];
        arr[arg2] = t;
    }

    public static void swap(int[] arr, int arg1, int arg2){
        int t = arr[arg1];
        arr[arg1] = arr[arg2];
        arr[arg2] = t;
    }
    public static List<Map.Entry<Integer,Integer>> sortMapByValueDesc(Map<Integer, Integer> mp){
        //这里将map.entrySet()转换成list
        List<Map.Entry<Integer,Integer>> list = new ArrayList<Map.Entry<Integer,Integer>>(mp.entrySet());
        //然后通过比较器来实现排序
        Collections.sort(list,new Comparator<Map.Entry<Integer, Integer>>() {
            //降序排序
            @Override
            public int compare(Map.Entry<Integer, Integer> o1,
                               Map.Entry<Integer, Integer> o2) {
                return o2.getValue().compareTo(o1.getValue());
            }
        });
        return list;
    }

    public static List<Map.Entry<Long,Double>> sortLongDoubleMapByValueDesc(Map<Long, Double> mp){
        //这里将map.entrySet()转换成list
        List<Map.Entry<Long,Double>> list = new ArrayList<Map.Entry<Long,Double>>(mp.entrySet());
        //然后通过比较器来实现排序
        Collections.sort(list,new Comparator<Map.Entry<Long, Double>>() {
            //降序排序
            @Override
            public int compare(Map.Entry<Long, Double> o1,
                               Map.Entry<Long, Double> o2) {
                return o2.getValue().compareTo(o1.getValue());
            }
        });
        return list;
    }
}
