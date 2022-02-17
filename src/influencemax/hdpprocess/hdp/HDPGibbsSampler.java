/*
 * Copyright 2011 Arnim Bleier, Andreas Niekler and Patrick Jaehnichen
 * Licensed under the GNU Lesser General Public License.
 * http://www.gnu.org/licenses/lgpl.html
 */

package influencemax.hdpprocess.hdp;

import influencemax.hdpprocess.hdp.DOCState;
import influencemax.hdpprocess.hdp.WordState;
import influencemax.hdpprocess.utils.CLDACorpus;
import influencemax.util.UtilMethod;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static influencemax.util.UtilMethod.swap;

/**
 * Hierarchical Dirichlet Processes  
 * Chinese Restaurant Franchise Sampler
 * 
 * For more information on the algorithm see:
 * Hierarchical Bayesian Nonparametric Models with Applications. 
 * Y.W. Teh and M.I. Jordan. Bayesian Nonparametrics, 2010. Cambridge University Press.
 * http://www.gatsby.ucl.ac.uk/~ywteh/research/npbayes/TehJor2010a.pdf
 * 
 * For other known implementations see README.txt
 * 
 * @author <a href="mailto:arnim.bleier+hdp@gmail.com">Arnim Bleier</a>
 */
public class HDPGibbsSampler { 


	public double beta  = 0.5; // default only
	public double gamma = 1.5;
	public double alpha = 1.0;
	
	private Random random = new Random();
	private double[] p;
	/**
	 * 当前词属于第k个主题的概率
	 */
	private double[] f;
	
	public DOCState[] docStates;
	protected int[] numberOfTablesByTopic;
	/**
	 * 每个主题下的词数量
	 */
	protected int[] wordCountByTopic;
	/**
	 * 主题-词的二维矩阵，即phi
	 */
	public int[][] wordCountByTopicAndTerm;
	
	
	public int sizeOfVocabulary;
	public int totalNumberOfWords;
	public int numberOfTopics = 1;
	public int totalNumberOfTables;
	

	/**
	 * Initially assign the words to tables and topics初始化分配词到桌子和主题
	 * 
	 * @param documentsInput {@link CLDACorpus} on which to fit the model
	 */
	public void addInstances(int[][] documentsInput, int V) {
		sizeOfVocabulary = V;
		totalNumberOfWords = 0;//所有文档的总词数（含重复的）
		docStates = new DOCState[documentsInput.length];//文档状态集合
		for (int d = 0; d < documentsInput.length; d++) {
			docStates[d] = new DOCState(documentsInput[d], d);
			totalNumberOfWords += documentsInput[d].length;
		}
		int k, i, j;
		DOCState docState;
		p = new double[20]; 
		f = new double[20];
		/*下面3个向量虽然每次都是固定了大小（相对于当前已有的主题个数），但由于每次都会初始化大小，
		 * 所以可以理解为随着采样迭代，每次都在变化*/
		numberOfTablesByTopic = new int[numberOfTopics+1];;//numberOfTopics是预设的主题数（初始为1），这里+1是为了扩展topicnew
		wordCountByTopic = new  int[numberOfTopics+1];
		wordCountByTopicAndTerm = new int[numberOfTopics+1][];
		for (k = 0; k <= numberOfTopics; k++) 	// var initialization done
			wordCountByTopicAndTerm[k] = new int[sizeOfVocabulary];//为主题-词矩阵分配空间，由于numberOfTopics在变化，所以每次都要重新分配
		/*下面两个for循环是将所有文档中的词分配到目前的桌子和主题（不额外增加桌子或主题）
		 第一个for (k = 0; k < numberOfTopics; k++)是为了保证每个主题和桌子都能够分配到词（顾客）
		 第二个for (j = numberOfTopics; j < docStates.Length; j++)是保证剩余的所有文档中的词能随机分配到某个桌子和主题*/
		for (k = 0; k < numberOfTopics; k++) {
			/*初始化阶段，所有主题分配一个文件
			 * 注意docStates的大小是输入文档集合的数量，所以与numberOfTopics无关（但一般numberOfTopics<=文档总数）*/
			docState = docStates[k];
			for (i = 0; i < docState.documentLength; i++) //在这一层k是不变的，文档是固定的，该文档所有的词都分配给一个主题k
				addWord(docState.docID, i, 0, k);//将文档的所有的词添加到“！某一个！”餐厅，桌子，主题（这里不会产生新的桌子（全是0）或主题（都在numberOfTopics内））
		} // all topics have now one document初始化阶段，所有主题分配一个文件
		for (j = numberOfTopics; j < docStates.length; j++) {//前numberOfTopics个文档分配给numberOfTopics个主题，剩余的文档随机地分配给各个主题，但不增加主题和桌子
			docState = docStates[j]; 
			k = random.nextInt(numberOfTopics);
			for (i = 0; i < docState.documentLength; i++)
				addWord(docState.docID, i, 0, k);
		} // the words in the remaining documents are now assigned too
	}

	
	/**
	 * Step one step ahead一步一步向前执行Gibbs Sampling
	 * 
	 */
	protected void nextGibbsSweep() {
		int table;
		for (int d = 0; d < docStates.length; d++) {//逐个文档的进行处理
			for (int i = 0; i < docStates[d].documentLength; i++) {//对文档d中的每个词进行采样
				removeWord(d, i); // remove the word i from the state// 去除当前采样词对应的桌子、主题、phi，如果当前词所在的桌子仅有当前词一个词，则连同桌子及其对应的主题全部去掉
				table = sampleTable(d, i);//分配桌子，可能从已有的中选取，也可能选一个新的
				if (table == docStates[d].numberOfTables) // new Table//如果是一个新的桌子 new Table
					addWord(d, i, table, sampleTopic()); // sampling its Topic
				else
					addWord(d, i, table, docStates[d].tableToTopic[table]); // existing Table
			}
		}
		defragment();
	}

	
	/**
	 * Decide at which topic the table should be assigned to决定分配的主题k，主要用于采样到了新的桌子时使用
	 * 
	 * @return the index of the topic
	 */
	private int sampleTopic() {
		double u, pSum = 0.0;
		int k;
		p = ensureCapacity(p, numberOfTopics);
		for (k = 0; k < numberOfTopics; k++) {
			pSum += numberOfTablesByTopic[k] * f[k];
			p[k] = pSum;
		}
		pSum += gamma / sizeOfVocabulary;
		p[numberOfTopics] = pSum;
		u = random.nextDouble() * pSum;
		for (k = 0; k <= numberOfTopics; k++)
			if (u < p[k])
				break;
		return k;
	}

	/// <summary>
	/// 词抽样，决定将词赋予哪个桌子（主题）
	/// </summary>
	/// <param name="docID"></param>
	/// <param name="i"> </param>
	/// <returns> </returns>
	/**	 
	 * Decide at which table the word should be assigned to
	 * 
	 * @param docID the index of the document of the current word
	 * @param i the index of the current word
	 * @return the index of the table
	 */
	int sampleTable(int docID, int i) {	
		int k, j;
		double pSum = 0.0, vb = sizeOfVocabulary * beta, fNew, u;
		DOCState docState = docStates[docID];
		f = ensureCapacity(f, numberOfTopics);//扩充概率密度向量大小
		p = ensureCapacity(p, docState.numberOfTables);//扩充概向量率大小
		//fNew表示
		fNew = gamma / sizeOfVocabulary;
		/*求密度函数*/
		for (k = 0; k < numberOfTopics; k++) {
			f[k] = (wordCountByTopicAndTerm[k][docState.words[i].termIndex] + beta) /
					(wordCountByTopic[k] + vb);
			fNew += numberOfTablesByTopic[k] * f[k];
		}
		//分配桌子
		for (j = 0; j < docState.numberOfTables; j++) {//这一级是针对一个餐厅docStates[docID]内（文档）的桌子，桌子的编号是j，这里不要带入后验公式中的j
			if (docState.wordCountByTable[j] > 0) //桌子上一定要有顾客（词）
				pSum += docState.wordCountByTable[j] * f[docState.tableToTopic[j]];//这个是菜（主题）后验分布的公式m.k*f(k)，只针对当前餐厅，但是没有分母的部分
			p[j] = pSum;
		}
		//TODO 在这里改新主题的选择方式，
		pSum += alpha * fNew / (totalNumberOfTables + gamma); // Probability for t = tNew // 产生一个新桌子的概率，但是为什么乘以alpha而不是Gamma Probability for t = tNew
		p[docState.numberOfTables] = pSum;
		u = random.nextDouble() * pSum;
		for (j = 0; j <= docState.numberOfTables; j++)
			if (u < p[j]) 
				break;	// decided which table the word i is assigned to // 决定词到主题的分配
		return j;
	}


	/// <summary>
	/// 调用模型的方法，Gibbs采样
	/// </summary>
	/// <param name="shuffleLag">用于标记随机打乱文档状态集合的时机，为0则表示从不打乱，一般iter % shuffleLag == 0时打乱一次</param>
	/// <param name="maxIter">最大迭代次数</param>
	public void run(int shuffleLag, int maxIter, PrintStream log) 
	throws IOException {
		for (int iter = 0; iter < maxIter; iter++) {
			System.out.println("开始第" + iter + "次迭代");
			if ((shuffleLag > 0) && (iter > 0) && (iter % shuffleLag == 0))
				doShuffle();//随机打乱文档状态集合docStates及其WordState的顺序，保证随机分布。
			nextGibbsSweep();//Gibbs采样
			//log.println("iter = " + iter + " #topics = " + numberOfTopics + ", #tables = "+ totalNumberOfTables );
			System.out.println("iter = " + iter + " #topics = " + numberOfTopics + ", #tables = "+ totalNumberOfTables );
		}
	}
		
	
	/**
	 * Removes a word from the bookkeeping
	 * 
	 * @param docID the id of the document the word belongs to 
	 * @param i the index of the word
	 */
	protected void removeWord(int docID, int i){
		DOCState docState = docStates[docID];
		int table = docState.words[i].tableAssignment;//当前词在文档中桌子编号
		int k = docState.tableToTopic[table];//当前词所属桌子对应的主题号
		/*去除当前采样词对应的桌子、主题、phi
             这里只去除了当前词，没有动同桌的其它词*/
		docState.wordCountByTable[table]--; 
		wordCountByTopic[k]--; 		
		wordCountByTopicAndTerm[k][docState.words[i].termIndex] --;
		//如果当前词所在的桌子仅有当前词一个词，则连同桌子，对应的主题全部去掉（必要忘记总桌数也要减1）
		if (docState.wordCountByTable[table] == 0) { // table is removed
			totalNumberOfTables--; 
			numberOfTablesByTopic[k]--; 
			docState.tableToTopic[table] --; 
		}
	}



	/// <summary>
	/// 添加词，并在该词对应的
	/// wordCountByTable：当某个词分配到（某个餐厅/文档）的某个桌子时，那该桌子的人数（词数）就要+1；
	/// wordCountByTopic：由于每个桌子都会对应一个主题，则该主题也要+1；
	/// phi：主题-词分布中也要对应的+1；
	/// 如果添加到了一个新桌子，对应的主题数等也要相应地增加，对应的数组也要扩展，在此基础上如果出现新的主题，也要做相应的处理。
	/// （这里实际上可能有个问题，DOCState docState = docStates[docID];之后所有的操作都是docState的，跟docStates[docID]没有半毛钱关系）
	/// </summary>
	/// <param name="docID">文档编号 docID</param>
	/// <param name="i">词在文档中的索引号（位置）</param>
	/// <param name="table"> 该字分配给的桌子</param>
	/// <param name="k">分配给该词的主题</param>
	protected void addWord(int docID, int i, int table, int k) {
		DOCState docState = docStates[docID];
		docState.words[i].tableAssignment = table; //为当前词分配桌子
		docState.wordCountByTable[table]++; //当某个词分配到（某个餐厅/文档）的某个桌子时，那该桌子的人数（词数）就要+1
		wordCountByTopic[k]++; //由于每个桌子都会对应一个主题，则该主题也要+1，注意table与k的是不是固定对应关系，所以k要作为输入参数
		try {
			//TODO DONE 2017/12/13 出现-1的原因，在词典中不存在这一个词，已经删除，因此需要重新调整取词的程序
			wordCountByTopicAndTerm[k][docState.words[i].termIndex]++;//主题-词分布中也要对应的+1
		}catch (Exception ex){
			ex.printStackTrace();
		}
		if (docState.wordCountByTable[table] == 1) { // a new table is created//当该+1的完成后，如果当前桌子只有一个顾客（词）说明是一张新的桌子（菜/主题还未分配）
			docState.numberOfTables++;//有新的桌子，所以当前文档（餐厅）的桌子数要+1
			docState.tableToTopic[table] = k;//并为新的桌子分配主题k
			totalNumberOfTables++;
			numberOfTablesByTopic[k]++; 
			docState.tableToTopic = ensureCapacity(docState.tableToTopic, docState.numberOfTables);//扩充 桌子的主题 数组
			docState.wordCountByTable = ensureCapacity(docState.wordCountByTable, docState.numberOfTables);//扩充 桌子的词数 数组
			if (k == numberOfTopics) { // a new topic is created//k从0开始计数，所以k == numberOfTopics说明出现了新的主题
				numberOfTopics++; 
				numberOfTablesByTopic = ensureCapacity(numberOfTablesByTopic, numberOfTopics); 
				wordCountByTopic = ensureCapacity(wordCountByTopic, numberOfTopics);
				wordCountByTopicAndTerm = add(wordCountByTopicAndTerm, new int[sizeOfVocabulary], numberOfTopics);
			}
		}
	}

	
	/**
	 * Removes topics from the bookkeeping that have no words assigned to将没有一个词的 主题移走
	 */
	protected void defragment() {
		int[] kOldToKNew = new int[numberOfTopics];
		int k, newNumberOfTopics = 0;
		for (k = 0; k < numberOfTopics; k++) {
			if (wordCountByTopic[k] > 0) {
				kOldToKNew[k] = newNumberOfTopics;
				UtilMethod.swap(wordCountByTopic, newNumberOfTopics, k);
				UtilMethod.swap(numberOfTablesByTopic, newNumberOfTopics, k);
				UtilMethod.swap(wordCountByTopicAndTerm, newNumberOfTopics, k);
				newNumberOfTopics++;
			} 
		}
		numberOfTopics = newNumberOfTopics;
		for (int j = 0; j < docStates.length; j++) 
			docStates[j].defragment(kOldToKNew);
	}
	
	
	/**
	 * Permute the ordering of documents and words in the bookkeeping 随机打乱文档状态集合docStates及其WordState的顺序，保证随机分布。
	 */
	protected void doShuffle(){
		List<DOCState> h = Arrays.asList(docStates);//将docStates转化为链表
		Collections.shuffle(h);
		docStates = h.toArray(new DOCState[h.size()]);
		for (int j = 0; j < docStates.length; j ++){
			List<WordState> h2 = Arrays.asList(docStates[j].words);
			Collections.shuffle(h2);
			docStates[j].words = h2.toArray(new WordState[h2.size()]);
		}
	}
	/// <summary>
	/// 将数组放大（原来位置的值保持不变），min的2倍，确保不越界
	/// </summary>
	/// <param name="arr"></param>
	/// <param name="min"></param>
	/// <returns></returns>
	public static double[] ensureCapacity(double[] arr, int min){
		int length = arr.length;
		if (min < length)
			return arr;
		double[] arr2 = new double[min*2];
		for (int i = 0; i < length; i++) 
			arr2[i] = arr[i];
		return arr2;
	}
	/// <summary>
	/// 将数组放大（原来位置的值保持不变），min的2倍，确保不越界
	/// </summary>
	/// <param name="arr"></param>
	/// <param name="min"></param>
	/// <returns></returns>
	public static int[] ensureCapacity(int[] arr, int min) {
		int length = arr.length;
		if (min < length)
			return arr;
		int[] arr2 = new int[min*2];
		for (int i = 0; i < length; i++) 
			arr2[i] = arr[i];
		return arr2;
	}

	public static int[][] add(int[][] arr, int[] newElement, int index) {
		int length = arr.length;
		if (length <= index){
			int[][] arr2 = new int[index*2][];
			for (int i = 0; i < length; i++) 
				arr2[i] = arr[i];
			arr = arr2;
		}
		arr[index] = newElement;
		return arr;
	}
	
	
	public static void main(String[] args) throws IOException {

	}

}