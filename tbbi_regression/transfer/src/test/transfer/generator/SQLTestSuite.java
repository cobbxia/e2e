package test.transfer.generator;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

import org.junit.Test;

import junit.framework.Assert;
import junit.framework.TestCase;


class SQLTestSuite extends TestCase{
	ArrayList<CaseUnit> cunits=null;
	private int casenum=0;
//	Result r;
	SQLGenerator generator=null;
	  String tablename;
	  ArrayList<FieldSchema> fields;
	  SQLTestSuite(String tablename, ArrayList<FieldSchema> fields){
		  this.tablename=tablename;
		  this.fields=fields;
//		  r=new Result();
		  cunits=new ArrayList<CaseUnit>();
	}
	public void setGenerator(SQLGenerator generator){
		this.generator=generator;
	}
	protected int prepare(SQLGenerator sqlGenerator){
		sqlGenerator.setFeilds(this.fields);
		sqlGenerator.setTablename(this.tablename);
		this.cunits.addAll(sqlGenerator.genCases());
		this.casenum=GlobalConf.getSize(this.cunits);
		return 0;
		 
	}
	protected int exec() throws Exception{
		for(int i=0;i<this.casenum;i++){
			String ret=SqlExecutor.exec(this.cunits.get(i).sql);
			this.cunits.get(i).result.genText();
		}
		return 0;
	}
	@Test
	public int verify(){
		for(int i=0;i<this.casenum;i++){
			if(this.cunits.get(i).base.toString().equals(this.cunits.get(i).result.toString())){
				FileIO.append(GlobalConf.resultFile,this.cunits.get(i).sql+"\tPASS\tcomment\t"+
			this.cunits.get(i).comment+"\n");
			}else{
				FileIO.append(GlobalConf.resultFile,this.cunits.get(i).sql+"\tFAIL\tcomment\t"+
			this.cunits.get(i).comment+
			"\tbase\t"+this.cunits.get(i).base.toString()+"\tresult\t"+
			this.cunits.get(i).result.toString()+"\n");
			}
			try{
				Assert.assertEquals("comment\t"+this.cunits.get(i).comment, 
					this.cunits.get(i).base.toString(),this.cunits.get(i).result.toString());
			}catch(Exception e){
			}
		}
		return 0;
	}
	public void report() {
	}
 }