package influencemax.hdpprocess.perplexity;

import java.util.List;

import influencemax.hdpprocess.hdp.DOCState;


/**
 * 计算perplexity
 * @author Administrator
 *
 */
public class Self_Perplexity {

	int K;//主题数
	int V;//词个数
	double eta;//phi~Dir(eta)
	int[] word_counts_by_z;//每个topic含有的词个数
	int[][] word_counts_by_zw;//[each topic, each word]词个数
	GammaDistrn alpha;//Gj|G0 ~ DP(a;G0);与 Teh et al. (2005) 论文一致
	GammaDistrn gamma;
	double beta; //beta~Dir(gamma)
	int totalTablesNum;//总的桌子数
	public int[] tablesNumByTopic;//一个主题拥有的桌子数
	DOCState[] docStates;//文档状态
	boolean sample_hyperparameter;	//是否抽样超参数
	int[][] phi;//主题-词矩阵, 主题中词的个数
	int trianWordsNum;//语料中 总词数，注意与字典词V的不同
	private double[] p;//概率
	private double[] f;//概率密度 函数
	int[] wordNumByTopic;
	int testTotalWordsNum;	
	DOCState[] docTestStates;
	List<int[][]> testDocsls;
	int[][] testTrnDocs;//测试部分的训练部分
	int[][] testCalcDocs;//测试部分的计算部分
	
	public int[] trn_tablesNumByTopic;//一个主题拥有的桌子数
	public int[] trn_wordNumByTopic;//一个主题中的词个数
	public int[][] trn_phi;//训练后的主题-词矩阵, 主题中词的个数,
	public int trn_totalTablesNum;//总的桌子数
	
	/**
	 * 计算每个词的perplexity
	 * @param K 主题数
	 * @param V 词典中的单词数
	 * @param eta 主题-词 phi 的先验参数 0.01
	 * @param word_counts_by_z 主题中的词的个数
	 * @param word_counts_by_zw 主题-词分布矩阵
	 * @param alpha 0, 0, 0
	 * @param gamma 0, 0, 0
	 * @param totalTablesNum 总计桌子数
	 * @param tablesNumByTopic 每个主题中桌子的个数
	 * @param docStates
	 * @param sample_hyperparameter 是否抽样超参数false
	 * @param phi
	 * @param beta
	 * @param totalWordsNum 总词数
	 * @param wordNumByTopic 主题中的词的个数
	 * @param trainTotalWordsNum 总词数
	 * @param testDocs null
	 * @param docTestStates 同上的docStates
	 * @param testDocsls null
	 * @param trn_tablesNumByTopic 同上的tablesNumByTopic
	 * @param trn_wordNumByTopic 同上的wordNumByTopic
	 * @param trn_phi 同上的phi
	 * @param trn_totalTablesNum 同上的totalTablesNum
	 * @param testTrnDocs
	 * @param testCalcDocs
	 * @return
	 */
	public double getPerplexity(int K, int V, float eta, int[] word_counts_by_z,
								GammaDistrn alpha,
								GammaDistrn gamma, int totalTablesNum, int[] tablesNumByTopic,
								DOCState[] docStates, boolean sample_hyperparameter,
								double beta, int totalWordsNum, int[] wordNumByTopic, int trainTotalWordsNum,
								int[] trn_tablesNumByTopic,int[] trn_wordNumByTopic, int[][] trn_phi,
								int trn_totalTablesNum){
		this.K = K;
		this.V = V;
		this.eta = eta;
		this.word_counts_by_z = word_counts_by_z;
		this.alpha = alpha;
		this.gamma = gamma;
		this.totalTablesNum = totalTablesNum;
		this.tablesNumByTopic = tablesNumByTopic;
		this.docStates = docStates;
		this.sample_hyperparameter = sample_hyperparameter;
		this.trianWordsNum = totalWordsNum;
		this.beta = beta;
		this.trn_tablesNumByTopic = trn_tablesNumByTopic;
		this.trn_wordNumByTopic = trn_wordNumByTopic;
		this.trn_phi = trn_phi;
		this.trn_totalTablesNum = trn_totalTablesNum;

		return perplexity();
	}
	
	public double perplexity() {

		wordNumByTopic = trn_wordNumByTopic;
		totalTablesNum = trn_totalTablesNum;
		tablesNumByTopic = trn_tablesNumByTopic;
			
		double[][] theta = estimateTheta();
		double[][] phi = estimatePhi();
		
		int total_length = 0;
		double perplexity = 0.0;
		
		for (int d = 0; d < docStates.length; d++) {
			total_length += docStates[d].documentLength;
			for (int w = 0, len2 = docStates[d].documentLength; w < len2; w++) {
				double prob = 0.0;
				for (int k = 0; k < K; k++) {
					prob += theta[d][k] * phi[k][docStates[d].words[w].termIndex];
				}
				perplexity += Math.log(prob);
			}
		}
		
		System.out.println("perplexity: " + perplexity + "\ttotal_length" + total_length );
		
		perplexity = Math.exp(-1 * perplexity / total_length);		
		
		return perplexity;
	}
	
	public double[][] estimateTheta() {
		int dLen = docStates.length;
		double[][] theta = new double[dLen][K];
		
		for (int d = 0; d < dLen; d++) {
	    	for(int t=0 ; t < docStates[d].numberOfTables; t++){
	    		if(docStates[d].wordCountByTable[t] > 0){
	    			int k = docStates[d].tableToTopic[t];
	    			theta[d][k] += docStates[d].wordCountByTable[t];
	    		}
	    	}
	    	for(int k = 0; k < K; k++) {
	    		if(word_counts_by_z[k] > 0){
	    		theta[d][k] += alpha.getValue() * tablesNumByTopic[k] / 
	    						(gamma.getValue() + totalTablesNum);
	    		theta[d][k] /= docStates[d].documentLength + alpha.getValue() ;
	    		}
	    	}
		}
		return theta;
	}
	
	public double[][] estimatePhi() {
		double[][] phi = new double[K][V];
		double v_eta = V * eta;
		for (int k = 0; k < K; k++) {
			for (int w = 0; w < V; w++) {
				phi[k][w] = (trn_phi[k][w] + eta) /
					(word_counts_by_z[k] + v_eta);
			}
		}
		return phi;
	}
}
