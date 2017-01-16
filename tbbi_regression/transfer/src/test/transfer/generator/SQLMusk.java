package test.transfer.generator;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.hive.ql.parse.HiveParser;


public class SQLMusk {
	  String tablename="";
	  ArrayList<FieldSchema> fields=null; 
	  int SQLType=0;
	  HashMap<String,String>sqlHarness=new HashMap();
	  public static void  main(String[] args) throws Exception{
		  String tablename="musktest";
		  int SQLType=HiveParser.TOK_ALTERTABLE_ADDCOLS;
		  SQLMusk sqlmusk=new SQLMusk(SQLType, tablename, FieldHarness.allTypecol());
		  sqlmusk.mainProcess();
	  }
	  public  SQLMusk(int SQLType,String tablename,ArrayList<FieldSchema> fields){
		  this.SQLType=SQLType;
		  this.tablename=tablename;
		  this.fields=fields;
	  }
	  public void mainProcess() throws Exception {
		  System.out.println("TEST EXECUTION STARTS");
		  FileIO.truncate(GlobalConf.resultFile);
		  SQLTestSuite testcases=new SQLTestSuite(tablename,fields);
		  for(int i=0;i<GlobalConf.sqlTokList.length;i++){
			  testcases.prepare(SQLTestSuiteFactory(GlobalConf.sqlTokList[i]));
		  }
		  testcases.exec();
		  testcases.verify();
		  testcases.report();		
		  System.out.println("TEST EXECUTION OVER");
	  }

	  SQLGenerator SQLTestSuiteFactory(int type){
		//System.out.println(type);		
		      switch(type) {
		      case HiveParser.TOK_DROPTABLE:
		      case HiveParser.TOK_DROPVIEW:
		      case HiveParser.TOK_DESCTABLE:
		      case HiveParser.TOK_DESCFUNCTION:
		      case HiveParser.TOK_MSCK:
		      case HiveParser.TOK_SHOWDATABASES:
		      case HiveParser.TOK_SHOWTABLES:
		      case HiveParser.TOK_SHOW_TABLESTATUS:
		      case HiveParser.TOK_SHOWFUNCTIONS:
		      case HiveParser.TOK_SHOWPARTITIONS:
		      case HiveParser.TOK_SHOWINDEXES:
		      case HiveParser.TOK_SHOWLOCKS:
		      case HiveParser.TOK_CREATEINDEX:
		      case HiveParser.TOK_DROPINDEX:
		      case HiveParser.TOK_ALTERTABLE_ADDCOLS:
		    	  return new AlterTableAddColSQL(tablename,fields);
		      case HiveParser.TOK_ALTERTABLE_RENAMECOL:
		    	  return new AlterTableRenameColSQL(tablename,fields);
		      case HiveParser.TOK_ALTERTABLE_REPLACECOLS:
		      case HiveParser.TOK_ALTERTABLE_RENAME:
		    	  return new AlterTableRenameSQL(tablename,fields);
		      case HiveParser.TOK_ALTERTABLE_DROPPARTS:
		    	  return new AlterTableDropParts(tablename,fields);
		      case HiveParser.TOK_ALTERTABLE_ADDPARTS:
		    	  return new AlterTableAddParts(tablename,fields);
		      case HiveParser.TOK_ALTERTABLE_PROPERTIES:
		    	  return new AlterTableProperties(tablename,fields);
		      case HiveParser.TOK_ALTERTABLE_SERIALIZER:
		      case HiveParser.TOK_ALTERTABLE_SERDEPROPERTIES:
		      case HiveParser.TOK_ALTERINDEX_REBUILD:
		      case HiveParser.TOK_ALTERINDEX_PROPERTIES:
		     // case HiveParser.TOK_ALTERVIEW_PROPERTIES:
		      case HiveParser.TOK_ALTERVIEW_ADDPARTS:
		      case HiveParser.TOK_ALTERVIEW_DROPPARTS:
		      case HiveParser.TOK_ALTERVIEW_RENAME:
		      case HiveParser.TOK_ALTERTABLE_CLUSTER_SORT:
		      case HiveParser.TOK_ALTERTABLE_TOUCH:
		      case HiveParser.TOK_ALTERTABLE_ARCHIVE:
		      case HiveParser.TOK_ALTERTABLE_UNARCHIVE:
		      case HiveParser.TOK_LOCKTABLE:
		      case HiveParser.TOK_UNLOCKTABLE:
		      case HiveParser.TOK_CREATEROLE:
		      case HiveParser.TOK_DROPROLE:
		      case HiveParser.TOK_GRANT:
		      case HiveParser.TOK_REVOKE:
		      case HiveParser.TOK_SHOW_GRANT:
		      case HiveParser.TOK_GRANT_ROLE:
		      case HiveParser.TOK_REVOKE_ROLE:
		      case HiveParser.TOK_SHOW_ROLE_GRANT:
		      case HiveParser.TOK_ALTERDATABASE_PROPERTIES:
		    //    return new DDLSemanticAnalyzer(conf);
		    //	  return ddlType;
		      case HiveParser.TOK_ALTERTABLE_FILEFORMAT:
		      case HiveParser.TOK_ALTERTABLE_ALTERPARTS_PROTECTMODE:
		      case HiveParser.TOK_ALTERTABLE_LOCATION:
		    	  return new AlterTableLocation(tablename,fields);
		      case HiveParser.TOK_ALTERTABLE_ALTERPARTS_MERGEFILES:
		    	  return new AlterTableAlterPartsMergefiles();
		      case HiveParser.TOK_ALTERTABLE_PARTITIONPROPERTIES:
		    	  return null;
		      case HiveParser.TOK_ALTERTABLE_RENAMEPART:
		    	  return null;
		      case HiveParser.TOK_ALTERPARTITION_LIFECYCLE:
		      	  return null;
		      case HiveParser.TOK_CREATEFUNCTION:
		      case HiveParser.TOK_DROPFUNCTION:
	//	        return new FunctionSemanticAnalyzer(conf);
		      default:
//		        return new SemanticAnalyzer(conf);
		    	  return new DdlSQL(tablename, fields);
		    }
		      }
}
 