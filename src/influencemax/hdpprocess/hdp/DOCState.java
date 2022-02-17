package influencemax.hdpprocess.hdp;

import influencemax.util.UtilMethod;

import java.util.HashMap;
import java.util.HashSet;

import static influencemax.util.UtilMethod.swap;

/**
 * Created by jack on 2017/11/9.
 */
public class DOCState {
    public int docID, documentLength, numberOfTables;
    public int[] tableToTopic;
    public int[] wordCountByTable;
    public influencemax.hdpprocess.hdp.WordState[] words;

    public DOCState(int[] instance, int docID) {
        this.docID = docID;
        numberOfTables = 0;
        documentLength = instance.length;
        words = new influencemax.hdpprocess.hdp.WordState[documentLength];
        wordCountByTable = new int[2];
        tableToTopic = new int[2];
        for (int position = 0; position < documentLength; position++)
            words[position] = new influencemax.hdpprocess.hdp.WordState(instance[position], -1);
    }

    /**
     * 只有在计算最终困惑度的时候会用到
     * @param docID
     */
    public DOCState(int docID){
        this.docID = docID;
        numberOfTables = 0;//最后再设置
        documentLength = 0;
        words = new influencemax.hdpprocess.hdp.WordState[0];
        wordCountByTable = new int[0];
        tableToTopic = new int[0];
    }

//    /**
//     * 记录tableId对应的词在wordCountByTable中的index值
//     */
//    private HashMap<Integer, Integer> tableMap = new HashMap<>();
    /**
     * 只有在计算最终困惑度的时候会用到
     * @param wordIndex
     * @param topicIndex
     * @param tableIndex
     */
    public void addWord(int wordIndex, int topicIndex, int tableIndex){
        documentLength++;
        influencemax.hdpprocess.hdp.WordState[] newwords = new WordState[documentLength];
        for(int i = 0; i < words.length; i++){
            newwords[i] = words[i];
        }
        newwords[documentLength - 1] = new WordState(wordIndex, tableIndex);
        words = newwords;
        if(numberOfTables <= tableIndex){//因为对于每一个文档，桌子号都是从0开始的连续编号
            numberOfTables = tableIndex + 1;//设置桌子数能容纳下当前的tableIndex
        }
        if(wordCountByTable.length < numberOfTables){
            wordCountByTable = resizeArray(wordCountByTable, numberOfTables);
        }
        if(tableToTopic.length < numberOfTables){
            tableToTopic = resizeArray(tableToTopic, numberOfTables);
        }
        wordCountByTable[tableIndex] = wordCountByTable[tableIndex] + 1;
        tableToTopic[tableIndex] = topicIndex;
        /*if(!tableMap.containsKey(tableIndex)){
            tableMap.put(tableIndex, numberOfTables);
            numberOfTables++;
            //TODO 复制新的wordCountByTable
            //TODO 这里不对，应该按桌子的ID来算
            if(wordCountByTable.length < numberOfTables){
                wordCountByTable = resizeArray(wordCountByTable, numberOfTables);
            }
            wordCountByTable[numberOfTables - 1] = 1;//新桌子，词数为1
        }
        else{
            wordCountByTable[tableMap.get(tableIndex)] = wordCountByTable[tableMap.get(tableIndex)] + 1;
        }*/
    }

    private int[] resizeArray(int[] origin, int newSize){
        if(newSize <= origin.length){
            return origin;
        }
        else{
            int[] newArr = new int[newSize];
            for(int i = 0; i < origin.length; i++){
                newArr[i] = origin[i];
            }
            return newArr;
        }
    }


    public void defragment(int[] kOldToKNew) {
        int[] tOldToTNew = new int[numberOfTables];
        int t, newNumberOfTables = 0;
        for (t = 0; t < numberOfTables; t++){
            if (wordCountByTable[t] > 0){
                tOldToTNew[t] = newNumberOfTables;
                tableToTopic[newNumberOfTables] = kOldToKNew[tableToTopic[t]];
                UtilMethod.swap(wordCountByTable, newNumberOfTables, t);
                newNumberOfTables ++;
            } else
                tableToTopic[t] = -1;
        }
        numberOfTables = newNumberOfTables;
        for (int i = 0; i < documentLength; i++)
            words[i].tableAssignment = tOldToTNew[words[i].tableAssignment];
    }

}