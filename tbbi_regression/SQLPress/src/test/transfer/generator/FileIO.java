package test.transfer.generator;


import java.io.File;
import java.io.FileOutputStream;

public class FileIO {   

    public static void main(String[] args) {  
    	FileIO.append("./result.txt", "test");
    	FileIO.append("./result.txt", "test2");
    	
    }
    public static void truncate(String filename){
    	 FileOutputStream out = null;    
    	 try {   
    		 System.out.println("truncate result file,filename:"+filename);
             out = new FileOutputStream(new File("./result.txt"));
             if(out!=null){
        		 out.close();
        	 }
    	 } catch (Exception e) {   
             e.printStackTrace();   
         }   
         finally {
         }   
    }
    public static void append(String filename,String content){
        FileOutputStream out = null;    
        int count=1;//写文件行数   
        try {   
            out = new FileOutputStream(new File("./result.txt"),true);   
            for (int i = 0; i < count; i++) {   
                out.write(content.getBytes());
            }   
            out.close();   
        } catch (Exception e) {   
            e.printStackTrace();   
        }   
        finally {   
        }   

    }   

}