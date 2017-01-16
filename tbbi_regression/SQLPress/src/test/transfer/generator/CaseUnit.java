package test.transfer.generator;
/*testcase has sql,result and base 
 * 
 */
class CaseUnit{
	String sql="",comment="";
	Task result=null,base=null;
	CaseUnit(){
		result=new Task();
		base=new Task();
	}
	CaseUnit(String sql,Task result,Task base){
		this.sql=sql;
		this.result=result;
		this.base=base;
	}
	CaseUnit(String sql,Task result,Task base,String comment){
		this.sql=sql;
		this.result=result;
		this.base=base;
		this.comment=comment;
	}
}
