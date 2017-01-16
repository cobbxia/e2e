package test.transfer.generator;

import java.util.ArrayList;

 class SQLGenerator {
	String tablename;
	ArrayList<FieldSchema> fields;
	FieldHarness fh;

	interface Wrapper{public ArrayList<CaseUnit> gencases();}
	ArrayList<Wrapper> wrappers=null;
	
	SQLGenerator(String tablename, ArrayList<FieldSchema> fields) {
		this.tablename = tablename;
		this.fields = fields;
		fh = new FieldHarness();
//	cases = new ArrayList<CaseUnit>();
		wrappers=new ArrayList<Wrapper>();
	}

	public void setTablename(String tablename) {
		this.tablename = tablename;
	}

	public void setFeilds(ArrayList<FieldSchema> fields) {
		this.fields = fields;
	}

	SQLGenerator() {

	}

	public ArrayList<CaseUnit> genCases() {
		ArrayList<CaseUnit> cases =  new ArrayList<CaseUnit>();
		for(int i=0;i<this.wrappers.size();i++){
			cases.addAll(this.wrappers.get(i).gencases());
		}
		System.out.println("SQLGenerator genSQL");
		return cases;
	}
}
class DdlSQL extends SQLGenerator {
	public DdlSQL(String tablename, ArrayList<FieldSchema> fields) {
		// TODO Auto-generated constructor stub
	}
	
}

