package test.transfer.parse;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;

import test.transfer.generator.FieldSchema;

public class Utility{
	private static final Log LOG = LogFactory.getLog(DDLSemanticAnalyzer.class);
	  public static Set<String> enList=null;
	  public static HashMap<String,String> en2cn=null;
	  
	  public static void genDict(String filename){
		  en2cn=new HashMap<String,String>();
		  ArrayList<String> items=Utility.readFileByLines(filename);
		  System.out.println("items length:"+items.size());
		  for(int i=0;i<items.size();i++){
			  String key=items.get(i).split("\\s+")[0];
			  String val=items.get(i).split("\\s+")[1];
			  System.out.println("key="+key+"\tval="+val);
			  en2cn.put(key,val);
		  }
		  enList= en2cn.keySet();
	  }
	  
	  public static String transItem(String item){
		  Iterator<String> it = enList.iterator();  
		  while (it.hasNext()) {  
		    String str = it.next();  
		//	CharSequence target=new CharSequence("hello");
			CharSequence replacement,target;
			target=str;
			replacement=en2cn.get(target);
//			System.out.println("str:"+str+"\ttarget:"+target+"\treplacement:"+replacement);
			item=item.replace(target, replacement);
		  }
		  return item;
	  }
	  public static void writeFile(String filename,ArrayList<String> outSqls) throws IOException{
			 FileOutputStream out = null; 
			 try {
				out = new FileOutputStream(new File(filename));
			    long begin = System.currentTimeMillis();   
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
	            System.out.println("����Ϊ��λ��ȡ�ļ����ݣ�һ�ζ�һ���У�");
	            reader = new BufferedReader(new FileReader(file));
	            String tempString = "",sql="";
	            int line = 1;
	            while ((tempString = reader.readLine()) != null) {
	                sql=sql+tempString;
	            	if(sql.endsWith(";")){
	            		sqls.add(sql.substring(0,sql.length()-1));
	            		sql="";
	            	}
	                System.out.println("line " + line + ": " + tempString);
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
		
		public static void traverse(ASTNode tree){
			ArrayList<Node> a = tree.getChildren();
			if (a==null)
				return;
			for (int j=0;j<a.size();j++){
				ASTNode node=(ASTNode)a.get(j);
				if(node.getToken().getType() == HiveParser.TOK_TABCOL){
					System.out.println(node.getChild(0).getText());
				}
				traverse((ASTNode)a.get(j));
			}
		}	
		
		public static String arraylistToString(ArrayList<FieldSchema> fields,String fieldSplitor) {
			return Utility.listToString(fields);
		}
		public static String arraylistToString(ArrayList<FieldSchema> fields) {
			return Utility.arraylistToString(fields,"\t");
		}

		public static String listToString(List<? extends Comparable>  fields) {
			return Utility.listToString(fields,"\t");
		}

		private static String listToString(List<? extends Comparable>  fields,String fieldSplitor) {
			if(fields==null) return "";
			String text = "";
			Collections.sort(fields);
			for (int i = 0; i < fields.size(); i++) {
				text = text+fieldSplitor + fields.get(i).toString();
			}
			return text;
		}
}
