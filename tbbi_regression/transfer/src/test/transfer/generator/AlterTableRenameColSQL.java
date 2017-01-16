package test.transfer.generator;

import java.util.ArrayList;

import test.transfer.parse.Utility;

public class AlterTableRenameColSQL extends SQLGenerator {
//长度、 中文变成英文
	public AlterTableRenameColSQL(String tablename,
			ArrayList<FieldSchema> fields) {
		super(tablename,fields);
		this.wrappers.add(new Wrapper(){
			public ArrayList<CaseUnit> gencases(){
				return renameSQL();
			}
		});
		// TODO Auto-generated constructor stub
	}
	public ArrayList<CaseUnit> renameSQL(){
		System.out.println(Thread.currentThread().getStackTrace()[1].getMethodName());
		String fulltablename = this.tablename
				+ Integer.toString(GlobalConf.getTableIndex());
		ArrayList<CaseUnit> cases=new ArrayList<CaseUnit>();
		for(int i=0;i<fields.size();i++){
			//ALTER TABLE table_name CHANGE COLUMN old_col_name RENAME TO new_col_name;
			String oldName=fields.get(i).name,type=fields.get(i).type;
			String newName=oldName+FieldHarness.getIncFieldName();
			String renameSQL = "drop table IF EXISTS " + fulltablename + ";"
				+ fh.getCreateTableSQL(fulltablename, fields) + "alter table "
				+ fulltablename + " change COLUMN " + oldName + " RENAME TO "+ newName+";";
			fields.set(i,new FieldSchema(newName,type));
			Task result = new DescTask(fulltablename);
			Task base = new DescTask();
			base.setText(Utility.arraylistToString(fields));
			cases.add(new CaseUnit(renameSQL, result, base, Thread.currentThread()
					.getStackTrace()[1].getMethodName()));
		}
		
		return cases;
	}
	
	
	
/*
	public ArrayList<CaseUnit> genCases() {
		cases.addAll(this.renameSQL());
		return this.cases;
	}
	*/
}
