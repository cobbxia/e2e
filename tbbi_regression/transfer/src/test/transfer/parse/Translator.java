package test.transfer.parse;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;

import org.antlr.runtime.tree.Tree;
import org.apache.hadoop.hive.ql.parse.SemanticException;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseDriver;

import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.serde.Constants;

import com.testyun.apsara.odps.parser.InSubqueryAnalyzer;
import com.testyun.odps.common.ErrorMsg;

//import org.apache.hadoop.hive.ql.parse.SemanticAnalyzerFactory;




public class Translator{
	public static void main(String args[]) throws SemanticException, ParseException, Exception {
	String sql="";
	//sql=new String("CREATE TABLE IF NOT EXISTS sale_detail(shop_name     STRING,customer_id   STRING,total_price   DOUBLE)PARTITIONED BY (sale_date STRING,region STRING)");
	String dictFilename="./cnMap.txt";
	Utility.genDict(dictFilename);
	//sql="ALTER TABLE xxootest CHANGE COLUMN id RENAME TO new_id;";
	sql=" select * from v1 a join v2 b on a.c_int_a=b.c_int_b;";
	if(args.length!=3){
		System.out.println("Usage:hiveConf	inFileName outFileName");
		System.exit(0);
	}
	Translator trans=new Translator();
	trans.process(args[0],args[1],args[2]);
	//Translator.test(args);
	}
	public ArrayList<String> process(String hiveConf,String inFileName,String outFileName) throws SemanticException, ParseException, org.apache.hadoop.hive.ql.parse.ParseException, IOException{
		ArrayList<String> inSqls=new ArrayList<String>(),outSqls=new ArrayList<String>();
		inSqls=Utility.readFileByLines(inFileName);
		for(int i=0;i<inSqls.size();i++){
			System.out.println("process i="+i);
			outSqls.add(processSQL(hiveConf, inSqls.get(i)));
		}
		Utility.writeFile(outFileName, outSqls);
		for(int i=0;i<outSqls.size();i++){
			System.out.println(outSqls.get(i));
		}
		return outSqls;
	}
	  private static void dfsFindColsForView(ASTNode node, Set<String> cols) {
		  if(node==null) return ;
		    if (node.getType() == HiveParser.TOK_TABLE_OR_COL) {
		      ASTNode idNode = (ASTNode) node.getChild(0);
		      assert idNode.getType() == HiveParser.Identifier;
		      System.out.println("idNode\t"+idNode.dump());
		      String colName = idNode.getText();
		    System.out.println("idNode col\t"+BaseSemanticAnalyzer.unescapeIdentifier(colName));
		      cols.add(BaseSemanticAnalyzer.unescapeIdentifier(colName));  // Fill
		      return;
		    }   
		    System.out.println(node.dump());
		    if(node.getChildCount()==0) return ;
		    for (Node child : node.getChildren()) {
		    	System.out.println("dfsFindColsForView\tchild\t"+child);
		      dfsFindColsForView((ASTNode) child, cols);
		      
		    }   
		  } 
	
	public  String processSQL(String arg,String sql) throws ParseException, SemanticException, org.apache.hadoop.hive.ql.parse.ParseException{
		//String sql=new String("create table test(id string);");
		HiveConf conf=new HiveConf();
		System.out.println(arg);
		conf.addResource(arg);
	    System.out.println("hive.metastore.rawstore.impl"+"\t"+conf.get("hive.metastore.rawstore.impl"));
	
		sql="select  id from b where id=3";
		String[] tokens = sql.split("\\s+");
		ParseDriver pd1=new ParseDriver();
		System.out.println(sql);
		ASTNode tree1=pd1.parse(sql);
		//System.out.println(tree1.dump());
		tree1 = ParseUtils.findRootNonNullToken(tree1);
		System.out.println(tree1.dump());
		//System.exit(0);
		for(int i=0;i<tree1.getChildCount();i++){
			System.out.println("i="+i+"\tchild\t"+(ASTNode)tree1.getChild(i));
		}
		//System.out.println(tree1.getChild(0).getChild(1).getChild(1).getChild(1).getChild(0).getChild(0).dupTree());

		System.out.println(((ASTNode) tree1.getChild(1)).dump());
		System.out.println(((ASTNode) tree1.getChild(1).getChild(0)).dump());
		System.out.println(((ASTNode) tree1.getChild(1).getChild(0).getChild(0)).dump());
		System.out.println(((ASTNode) tree1.getChild(1).getChild(0).getChild(0).getChild(0)).dump());
		System.exit(0);
		for (Node selExpr : tree1.getChildren()) {
	//		System.out.println("child\t"+(ASTNode)selExpr);
			Set<String> cols = new HashSet< String>();
			dfsFindColsForView((ASTNode)selExpr,cols);
			if(cols==null) return "";
			for(String col : cols)
				System.out.println("col\t"+col);
		}
		
		Map<String, ASTNode> astMap = InSubqueryAnalyzer.splitSubquery(tree1);
		BaseSemanticAnalyzer sem =  SemanticAnalyzerFactory.get(conf, tree1);
		sem.set(sql);
	//	traverse(tree1);
		sem.translate(tree1);
		//SemanticAnalyzer sem1=new SemanticAnalyzer(conf);
		//System.out.println("sem1.enCol2cnCol.size()\t"+sem1.enCol2cnCol.size());
	//	System.exit(0);
		return sem.newSQL;
		//exit
		
		/*
		sem1.analyzeInternal(tree1);
		Map<String,String> map=sem1.enCol2cnCol;
		for(Map.Entry<String, String> entry:map.entrySet())
		{
			System.out.println("key="+entry.getKey()+"\tval="+entry.getValue());
			sql=sql.replace(entry.getKey(), entry.getValue());
		}
		System.out.println(sql);
		return sql;
	
		for(int i=0;i<tokens.length;i++)
			System.out.println(tokens[i]);
		System.out.println(tokens);
		*/
		/*
		//System.out.println("SemanticAnalyzer Translator:\t"+sem1.getTable2Cols());
		int childCount=tree1.getChildCount();
		traverse(tree1);
		ArrayList<Node> a = tree1.getChildren();
		for (int j=0;j<a.size();j++){
			System.out.println(a.get(j).toString());
		}
		for(int j=0;j<childCount;j++){
			if(((ASTNode)tree1.getChild(j)).getToken().getType() == HiveParser.TOK_TABCOL){
				System.out.println();
			}
		}
		*/
		
		
		
	/*	sem.analyzeInternal(tree1);
		List<FieldSchema> fields=Translator.getTranslator((ASTNode)tree1.getChild(1), false);
		for(int i=0;i<fields.size();i++)
		{
			System.out.println("column name:"+i+"\t"+fields.get(i).name+"type\t"+fields.get(i).type);
		}/*
		for(Map.Entry<String, ASTNode> entry:astMap.entrySet()){
	        if(entry.getKey().startsWith("SQL") || entry.getKey().startsWith("PLAN-")){
	        	Driver pd=new Driver();
	            int ret=pd.compileForAst(sql,entry.getValue(),true);
	            if(ret==0){
	              //this.plans.put(entry.getKey(), getPlanDesc());
	            } else {
	              //return ret;
	            }
	          }else{
	            
	            entry.getValue().toStringTree();
	            //this.plans.put(entry.getKey(), JsonUtil.objToJson(entry.getValue()));
	          }   
		}*/
	
	
	
	
	}
	
	  private void traverse(Tree tree) {
		// TODO Auto-generated method stub
		System.out.println("traverse:\t"+tree.getText());
		for(int i=0;i<tree.getChildCount();i++){
			Tree child=tree.getChild(i);
			traverse(child);
		}
		
	}
	@SuppressWarnings("nls")
	  public static String unescapeSQLString(String b) {

	    Character enclosure = null;

	    // Some of the strings can be passed in as unicode. For example, the
	    // delimiter can be passed in as \002 - So, we first check if the
	    // string is a unicode number, else go back to the old behavior
	    StringBuilder sb = new StringBuilder(b.length());
	    for (int i = 0; i < b.length(); i++) {

	      char currentChar = b.charAt(i);
	      if (enclosure == null) {
	        if (currentChar == '\'' || b.charAt(i) == '\"') {
	          enclosure = currentChar;
	        }
	        // ignore all other chars outside the enclosure
	        continue;
	      }

	      if (enclosure.equals(currentChar)) {
	        enclosure = null;
	        continue;
	      }

	      if (currentChar == '\\' && (i + 4 < b.length())) {
	        char i1 = b.charAt(i + 1);
	        char i2 = b.charAt(i + 2);
	        char i3 = b.charAt(i + 3);
	        if ((i1 >= '0' && i1 <= '1') && (i2 >= '0' && i2 <= '7')
	            && (i3 >= '0' && i3 <= '7')) {
	          byte bVal = (byte) ((i3 - '0') + ((i2 - '0') * 8) + ((i1 - '0') * 8 * 8));
	          byte[] bValArr = new byte[1];
	          bValArr[0] = bVal;
	          String tmp = new String(bValArr);
	          sb.append(tmp);
	          i += 3;
	          continue;
	        }
	      }

	      if (currentChar == '\\' && (i + 2 < b.length())) {
	        char n = b.charAt(i + 1);
	        switch (n) {
	        case '0':
	          sb.append("\0");
	          break;
	        case '\'':
	          sb.append("'");
	          break;
	        case '"':
	          sb.append("\"");
	          break;
	        case 'b':
	          sb.append("\b");
	          break;
	        case 'n':
	          sb.append("\n");
	          break;
	        case 'r':
	          sb.append("\r");
	          break;
	        case 't':
	          sb.append("\t");
	          break;
	        case 'Z':
	          sb.append("\u001A");
	          break;
	        case '\\':
	          sb.append("\\");
	          break;
	        // The following 2 lines are exactly what MySQL does
	        // case '%':
	        //   sb.append("\\%");
	        //   break;
	        // case '_':
	        //   sb.append("\\_");
	        //   break;
	        default:
	          sb.append(n);
	        }
	        i++;
	      } else {
	        sb.append(currentChar);
	      }
	    }
	    return sb.toString();
	  }
	  
	  public static String unescapeIdentifier(String val) {
		    if (val == null) {
		      return null;
		    }
		    if (val.charAt(0) == '`' && val.charAt(val.length() - 1) == '`') {
		      val = val.substring(1, val.length() - 1);
		    }
		    return val;
		  }

	  private static String getStructTypeStringFromAST(ASTNode typeNode)
	      throws SemanticException {
	    String typeStr = Constants.STRUCT_TYPE_NAME + "<";
	    typeNode = (ASTNode) typeNode.getChild(0);
	    int children = typeNode.getChildCount();
	    if (children <= 0) {
	      throw new SemanticException(ErrorMsg.GENERIC_SEMANTIC_ERROR.getMsg("empty struct not allowed."));
	    }
	    StringBuilder buffer = new StringBuilder(typeStr);
	    for (int i = 0; i < children; i++) {
	      ASTNode child = (ASTNode) typeNode.getChild(i);
	      buffer.append(unescapeIdentifier(child.getChild(0).getText())).append(":");
	      buffer.append(getTypeStringFromAST((ASTNode) child.getChild(1)));
	      if (i < children - 1) {
	        buffer.append(",");
	      }
	    }

	    buffer.append(">");
	    return buffer.toString();
	  }

	  private static String getUnionTypeStringFromAST(ASTNode typeNode)
	      throws SemanticException {
	    String typeStr = Constants.UNION_TYPE_NAME + "<";
	    typeNode = (ASTNode) typeNode.getChild(0);
	    int children = typeNode.getChildCount();
	    if (children <= 0) {
	      throw new SemanticException(ErrorMsg.GENERIC_SEMANTIC_ERROR.getMsg("empty union not allowed."));
	    }
	    StringBuilder buffer = new StringBuilder(typeStr);
	    for (int i = 0; i < children; i++) {
	      buffer.append(getTypeStringFromAST((ASTNode) typeNode.getChild(i)));
	      if (i < children - 1) {
	        buffer.append(",");
	      }
	    }
	    buffer.append(">");
	    typeStr = buffer.toString();
	    return typeStr;
	  }

	  protected static String getTypeStringFromAST(ASTNode typeNode)
		      throws SemanticException {
		  System.out.println(typeNode.token);
		    switch (typeNode.getType()) {
		    case HiveParser.TOK_LIST:
		      return Constants.LIST_TYPE_NAME + "<"
		          + getTypeStringFromAST((ASTNode) typeNode.getChild(0)) + ">";
		    case HiveParser.TOK_MAP:
		      return Constants.MAP_TYPE_NAME + "<"
		          + getTypeStringFromAST((ASTNode) typeNode.getChild(0)) + ","
		          + getTypeStringFromAST((ASTNode) typeNode.getChild(1)) + ">";
		    case HiveParser.TOK_STRUCT:
		      return getStructTypeStringFromAST(typeNode);
		    case HiveParser.TOK_UNIONTYPE:
		      return getUnionTypeStringFromAST(typeNode);
		    default:
		      return DDLSemanticAnalyzer.getTypeName(typeNode.getType());
		    }
		  }
	  
	  public static List<FieldSchema> getTranslator(ASTNode ast, boolean lowerCase) throws SemanticException {
		    List<FieldSchema> colList = new ArrayList<FieldSchema>();
		    int numCh = ast.getChildCount();
		    System.out.println("numCh:\t"+numCh);
		    for (int i = 0; i < numCh; i++) {
		      FieldSchema col = new FieldSchema();
		      ASTNode child = (ASTNode) ast.getChild(i);

		      String name = child.getChild(0).getText();
		      System.out.println("colname:\t"+name);
		      if(lowerCase) {
		        name = name.toLowerCase();
		      }
		      // child 0 is the name of the column
		      col.setName(Translator.unescapeIdentifier(name));
		      // child 1 is the type of the column
		      ASTNode typeChild = (ASTNode) (child.getChild(1));
		      col.setType(Translator.getTypeStringFromAST(typeChild));

		      // child 2 is the optional comment of the column
		      if (child.getChildCount() == 3) {
		        col.setComment(unescapeSQLString(child.getChild(2).getText()));
		      }
		      colList.add(col);
		    }
		    return colList;
		  }
	    
	public static void test(String args[]){
		HiveConf conf=new HiveConf();
	    System.out.println(args.length+"\t"+args[0]);
	    conf.addResource(args[0]);
	    String sql=new String("CREATE TABLE IF NOT EXISTS sale_detail(shop_name     STRING,customer_id   STRING,total_price   DOUBLE)PARTITIONED BY (sale_date STRING,region STRING)");
       // conf.addResource("C:\Users\mingchao.xiamc\Desktop\hiveserver_release_64\hive\dist\conf");	   
 	   // conf.addResource("C:/Users/mingchao.xiamc/Desktop/hiveserver_release_64/hive/dist/conf/hive-site.xml");
	    //conf.addResource("./hive-site.xml");
 	    System.out.println("hive.metastore.rawstore.impl"+"\t"+conf.get("hive.metastore.rawstore.impl"));
	    Driver pd=new Driver(conf);
		int tree =pd.compile(sql);
		
	}
}	 
	    
	    
	    
		/*
		HiveConf conf =new HiveConf();
		CommandProcessor proc = CommandProcessorFactory.get(tokens[0]);
		Driver qp = (Driver) proc;
		 SemanticAnalyzerFactory sfactory = new SemanticAnalyzerFactory(conf);
		BaseSemanticAnalyzer sem = sfactory.get(tree);
		*/
