
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

public class Utility{
	  public static Set<String> enList=null;
	  public static HashMap<String,String> argMap=null;
	  public static HashMap<String, String> genDict(String filename,String splitchar){
		  Utility.argMap=new HashMap<String,String>();
		  ArrayList<String> items=Utility.readFileByLines(filename);
		  //System.out.println("items length:"+items.size());
		  for(int i=0;i<items.size();i++){
			  if(items.get(i).length()==0) continue;
			  int index= items.get(i).indexOf(splitchar);
			  if(index==-1) continue;
			  //System.out.println("item:"+items.get(i)+"\tindex:"+index);
			  String key=items.get(i).substring(0, index);
			  String val=items.get(i).substring(index+1,items.get(i).length());
			  //System.out.println("key="+key+"\tval="+val);
			  Utility.argMap.put(key,val);
		  }
		  enList= argMap.keySet();
		  return argMap;
	  }
	  
	  public static String transItem(String item){
		  Iterator<String> it = enList.iterator();  
		  while (it.hasNext()) {  
		    String str = it.next();  
		//	CharSequence target=new CharSequence("hello");
			CharSequence replacement,target;
			target=str;
			replacement=argMap.get(target);
//			System.out.println("str:"+str+"\ttarget:"+target+"\treplacement:"+replacement);
			item=item.replace(target, replacement);
		  }
		  return item;
	  }
	  public static void writeFile(String filename,ArrayList<String> outSqls) throws IOException{
			 FileOutputStream out = null; 
			 try {
				out = new FileOutputStream(new File(filename)); 
			    for (int i = 0; i < outSqls.size(); i++) {   
	        	out.write(outSqls.get(i).getBytes());
			    } 
			 }catch (FileNotFoundException e1) {
	 			// TODO Auto-generated catch block
	 			e1.printStackTrace();
	 			}
	         out.close();   
		} 
		public static ArrayList<String> readFileByLines(String fileName) {
	        ArrayList<String> sqls = new ArrayList<String>();
			File file = new File(fileName);
	        BufferedReader reader = null;
	        try {
	            System.out.println("read file");
	            reader = new BufferedReader(new FileReader(file));
	            String tempString = "",sql="";
	            int line = 1;
	            while ((tempString = reader.readLine()) != null) {
	                /*
	                sql=sql+tempString;
	            	if(sql.endsWith(";")){
	            		sqls.add(sql.substring(0,sql.length()-1));
	            		sql="";
	            	}
	                System.out.println("line " + line + ": " + tempString);
	                */
	                sqls.add(tempString);
	                line++;
	            }
	            reader.close();
	        } catch (IOException e) {
	            e.printStackTrace();
	        } finally {
	            if (reader != null) {
	                try {
	                    reader.close();
	                } catch (IOException e1) {
	                }
	            }
	        }
	        return sqls;
	    }
}
