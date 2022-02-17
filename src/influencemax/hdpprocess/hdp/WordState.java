package influencemax.hdpprocess.hdp;

/**
 * Created by jack on 2017/11/9.
 */
public class WordState {
    /**
     * 词在词典中的ID
     */
    public int termIndex;
    public int tableAssignment;

    public WordState(int wordIndex, int tableAssignment){
        this.termIndex = wordIndex;
        this.tableAssignment = tableAssignment;
    }
}