package influencemax.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Date;

/**
 * Created by jack on 2017/7/30.
 */
public class LogFile {
    private void createFile(String fileName){
        m_file = new File(fileName);
        if(!m_file.exists()){
            try {
                m_file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    private void getFileStream(){
        try {
            m_stream = new FileOutputStream(m_file);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }
    private File m_file;
    private FileOutputStream m_stream;
    public LogFile(String fileName){
        createFile(fileName);
        getFileStream();
    }
    public void writeLine(String line){
        if(m_stream == null){
            getFileStream();
        }
        if(m_stream == null){
            return;
        }
        try {
            m_stream.write((new Date().toString() + ":" + line).getBytes("GBK"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public void closeFile(){
        if(m_stream != null){
            try {
                m_stream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
