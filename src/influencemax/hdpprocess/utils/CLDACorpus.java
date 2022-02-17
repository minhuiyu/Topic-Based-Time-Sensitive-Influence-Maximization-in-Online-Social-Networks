package influencemax.hdpprocess.utils;

import influencemax.util.DBHelper;

import java.io.*;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

public class CLDACorpus {

	private int[][] documents;
	private int vocabularySize = 0;
    WordDictionary dict = new WordDictionary();

	public CLDACorpus(String mysqlConnStr, String userName, String pwd, String getDataStr, String dataFieldName, String getCntStr, String cntFieldName, String dictFile){
//		Connection conn = DBHelper.getConnection(mysqlConnStr, userName, pwd);
//		Statement stQuery = null;
//		try {
//			stQuery = conn.createStatement();
//			//TODO 这里的getCount和getData都应该写成非空的形式
//			ResultSet rs = stQuery.executeQuery(getCntStr);
//			if(rs != null && rs.next()) {
//				int docCount = rs.getInt(cntFieldName);
//				if(docCount == 0){
//					return;
//				}
//				rs.close();
//				documents = new int[docCount][];
//				ResultSet rsData = stQuery.executeQuery(getDataStr);
//				int i = 0;
//				while(i < docCount && rsData.next()) {
//					String content = rsData.getString(dataFieldName);
//					List<Integer> ids = new ArrayList<Integer>();//ids代表当前文档包含的词id列表
//					String words[] = content.split(" ");
//					for(int tmp = 0; tmp < words.length; tmp++){
//					    String word = words[tmp];
//					    int id = dict.addword(word);
//					    ids.add(id);
//                    }
//                    documents[i] = new int[ids.size()];
//                    for(int j = 0; j < ids.size(); j++){
//                        documents[i][j] = ids.get(j);
//                    }
//                    i++;
//				}
//			}
//			rs.close();
//			stQuery.close();
//			conn.close();
//            vocabularySize = dict.size();
//            dict.writeDictToFile(dictFile);
//		} catch (SQLException e) {
//			e.printStackTrace();
//		}
        initCLDACorpus(mysqlConnStr, userName, pwd, getDataStr, dataFieldName, getCntStr, cntFieldName, dictFile, null);
        dict.optimizeDict(5, 25);
        initCLDACorpus(mysqlConnStr, userName, pwd, getDataStr, dataFieldName, getCntStr, cntFieldName, dictFile, dict);
	}

	/**
	 *
	 * @param mysqlConnStr
	 * @param userName
	 * @param pwd
	 * @param getDataStr
	 * @param dataFieldName
	 * @param getCntStr
	 * @param cntFieldName
	 * @param dictFile
	 * @param initDict 如果不为空，说明是已经优化好词典，重新建模文本集
	 */
	private void initCLDACorpus(String mysqlConnStr, String userName, String pwd, String getDataStr, String dataFieldName, String getCntStr, String cntFieldName, String dictFile, WordDictionary initDict){
		Connection conn = DBHelper.getConnection(mysqlConnStr, userName, pwd);
		Statement stQuery = null;
		try {
			stQuery = conn.createStatement();
			//TODO 这里的getCount和getData都应该写成非空的形式
			ResultSet rs = stQuery.executeQuery(getCntStr);
			if(rs != null && rs.next()) {
				int docCount = rs.getInt(cntFieldName);
				if(docCount == 0){
					return;
				}
				rs.close();
				documents = new int[docCount][];
				ResultSet rsData = stQuery.executeQuery(getDataStr);
				int i = 0;
				while(i < docCount && rsData.next()) {
					String content = rsData.getString(dataFieldName);
					List<Integer> ids = new ArrayList<Integer>();//ids代表当前文档包含的词id列表
					String words[] = content.split(" ");
					for(int tmp = 0; tmp < words.length; tmp++){
						String word = words[tmp];
						if(null == initDict) {
							int id = dict.addword(word);
							ids.add(id);
						}
						else{
						    int id = dict.getid(word);
						    if(id != -1) {
                                ids.add(id);
                            }
						}
					}
					documents[i] = new int[ids.size()];
					for(int j = 0; j < ids.size(); j++){
						documents[i][j] = ids.get(j);
					}
					i++;
				}
			}
			rs.close();
			stQuery.close();
			conn.close();
			vocabularySize = dict.size();
			if(null == initDict){
			    dictFile = dictFile.substring(0, dictFile.lastIndexOf("/") + 1) + "未optimized" + dictFile.substring(dictFile.lastIndexOf("/") + 1);
            }
            if(null != dictFile) {
				dict.writeDictToFile(dictFile);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 从词典中删除只出现一次的词，如果单词拼写错误，那么很有可能只出现一两次
	 * 处理过程如下
	 * 1.删除只出现一次的单词，重新从1开始构建连续词典
	 * 2.重新遍历文档，构建id化的文档列表
	 */
	public void RemoveOnceWord(){

	}

	/**
	 * 作用就是把documents填满用数字代表的词
	 * @param is
	 * @throws IOException
	 */
	public CLDACorpus(InputStream is) throws IOException {
		int length, word, counts;//统计用的临时变量
		List<List<Integer>> docList = new ArrayList<List<Integer>>();
		List<Integer> doc;
		BufferedReader br = new BufferedReader(new InputStreamReader(is,
				"UTF-8"));
		String line = null;
		while ((line = br.readLine()) != null) {
			try {//2 3434:35 4052:29代表该行有两个字，第3434个字有35个
				doc = new ArrayList<Integer>();// 4052,4052,4052...,3434,3434,3434...,3434
				String[] fields = line.split(" ");
				length = Integer.parseInt(fields[0]);
				for (int n = 0; n < length; n++) {//遍历每一对
					String[] wordCounts = fields[n + 1].split(":");
					word = Integer.parseInt(wordCounts[0]);
					counts = Integer.parseInt(wordCounts[1]);
					for (int i = 0; i < counts; i++)
						doc.add(word);//还原成原有的词
					if (word >= vocabularySize)//word的编号是从0开始的
						vocabularySize = word + 1;
				}
				docList.add(doc);
			} catch (Exception e) {
				System.err.println(e.getMessage() + "\n");
			}
		}
		documents = new int[docList.size()][];
		for (int j = 0; j < docList.size(); j++) {
			doc = docList.get(j);
			documents[j] = new int[doc.size()];
			 for (int i = 0; i < doc.size(); i++) {
			 documents[j][i] = doc.get(i);
			 }
		}
	}

	public String getWordByKey(int key){
	    return dict.getword(key);
    }

	public int[][] getDocuments() {
		return documents;
	}

	public int getVocabularySize() {
		return vocabularySize;
	}
}

class WordDictionary{
	/// <summary>词-编号词典</summary>
    private HashMap<String, Integer> Word2Id = new HashMap<String, Integer>();
	/// <summary>编号-词词典</summary>
    private HashMap<Integer, String> Id2Word = new HashMap<Integer, String>();
	/// <summary>
	/// ytf.编号-出现次数词典
	/// </summary>
	private HashMap<Integer, Integer>Id2Count = new HashMap<Integer, Integer>();

	/// <summary>初始化词-编号和编号-词两个词典</summary>
	public WordDictionary(){ }
	/// <summary>根据编号获取词</summary>
	public String getword(int id)
	{
		return Id2Word.get(id);
	}
	/// <summary>根据词获取其编号（无则返货-1）</summary>
	public int getid(String word)
	{
		return Word2Id.containsKey(word) ? Word2Id.get(word) : -1;
	}
	/// <summary>检测是否存在词的编号</summary>
	public boolean contains(int id)
	{
		return Id2Word.containsKey(id);
	}
	/// <summary>检测是否存在词</summary>
	public boolean contains(String word)
	{
		return Word2Id.containsKey(word);
	}
	/// <summary>添加词（如果词典中午该词），最后并返回其编号，这里自带的查重机制</summary>
	public int addword(String word)
	{
		if (!contains(word))
		{
			int id = Word2Id.size();
			Word2Id.put(word, id);
			Id2Word.put(id, word);
			Id2Count.put(id, 1);
			return id;
		}
		int wordId = Word2Id.get(word);
		Id2Count.put(Word2Id.get(word), Id2Count.get(wordId) + 1);
		return wordId;
	}

	/// <summary>
	/// 将词-编号词典中的内容写入词图文件
	/// </summary>
	/// <param name="wordMapFile"></param>
	/// <returns></returns>
	public boolean writeDictToFile(String wordMapFile)
	{
		try
		{
			//TODO 这里改成从多到少进行排序
			PrintStream ps = new PrintStream(wordMapFile);
//			for(Map.Entry<String, Integer> entry : Word2Id.entrySet()){
//			   //ps.write(entry.getKey() + " " + entry.getValue() + " " + Id2Count.get(entry.getValue()) + "\n");
//			    ps.format("%30s", entry.getKey());
//				ps.format("%10d", entry.getValue());
//				ps.format("%10d",  Id2Count.get(entry.getValue()));
//				ps.println();
//            }
            LinkedHashMap<Integer, Integer> newId2Count = (LinkedHashMap<Integer, Integer>) sortByValue(Id2Count);
            for(Map.Entry<Integer, Integer> entry : newId2Count.entrySet()){
				//ps.write(entry.getKey() + " " + entry.getValue() + " " + Id2Count.get(entry.getValue()) + "\n");
				ps.format("%30s", Id2Word.get(entry.getKey()));
				ps.format("%10d", entry.getKey());
				ps.format("%10d",  entry.getValue());
				ps.println();
			}
            ps.close();
            return true;
		}
		catch (Exception ex)
		{
			System.out.println("Error while writing dictionary :" + ex.toString());
			return false;
		}
	}
	public <K, V extends Comparable<? super V>> Map<K, V> sortByValue(Map<K, V> map) {
		List<Map.Entry<K, V>> list = new LinkedList<>(map.entrySet());
		Collections.sort(list, new Comparator<Map.Entry<K, V>>() {
			@Override
			public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
				return (o2.getValue()).compareTo(o1.getValue());
			}
		});

		Map<K, V> result = new LinkedHashMap<>();
		for (Map.Entry<K, V> entry : list) {
			result.put(entry.getKey(), entry.getValue());
		}
		return result;
	}

    public int size() {
	    return Word2Id.size();
    }

	/**
	 * 将出现次数小于等于count的词删除，重新构建index从0开始的词典类
	 * @param count
     * @param wordMaxLen
	 */
	public void optimizeDict(int count, int wordMaxLen){
		int newIndex = 0;
		int oldSize = Word2Id.size();
		int oldIndex = 0;
		int newSize = 0;
		while(oldIndex < oldSize){
			if(Id2Count.get(oldIndex)< count || Id2Word.get(oldIndex).length() > wordMaxLen){
				Id2Count.remove(oldIndex);
			}
			oldIndex++;//旧词典的id是连续的
		}
		newSize = Id2Count.size();
		oldIndex = 0;
		//接下来重新构建新词典
		HashMap<String, Integer> newWord2Id = new HashMap<String, Integer>();
		HashMap<Integer, String> newId2Word = new HashMap<Integer, String>();
		HashMap<Integer, Integer> newId2Count = new HashMap<Integer, Integer>();
		while(newIndex < newSize){
			if(Id2Count.containsKey(oldIndex)){
				int cnt = Id2Count.get(oldIndex);
				String wd = Id2Word.get(oldIndex);
				newId2Count.put(newIndex, cnt);
				newWord2Id.put(wd, newIndex);
				newId2Word.put(newIndex, wd);
				newIndex++;
			}
			oldIndex++;
		}
		Word2Id = newWord2Id;
		Id2Word = newId2Word;
		Id2Count = newId2Count;
	}
}
