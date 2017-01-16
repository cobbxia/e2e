package test.transfer.generator;

import java.util.ArrayList;

import test.transfer.generator.SQLGenerator.Wrapper;
import test.transfer.parse.Utility;

public class AlterTableRenameSQL extends SQLGenerator {

	public AlterTableRenameSQL(String tablename, ArrayList<FieldSchema> fields) {
		super(tablename,fields);
		// TODO Auto-generated constructor stub
		this.wrappers.add(new Wrapper(){
			public ArrayList<CaseUnit> gencases(){
				return renameTableSQL();
			}
		});
	}
	
	//ALTER TABLE sale_detail_rename1 RENAME TO sale_detail_rename2;
	public ArrayList<CaseUnit>  renameTableSQL(){
		System.out.println(Thread.currentThread()
				.getStackTrace()[1].getMethodName());
		String newtablename=tablename+ Integer.toString(GlobalConf.getTableIndex());
		String SQL = "drop table IF EXISTS " + tablename + ";"
				+fh.getCreateTableSQL(tablename, fields) + "alter table "
				+ tablename + " RENAME TO " + newtablename + ");";
		Task result = new DescTask(newtablename);
		Task base = new DescTask();
		base.setText(Utility.arraylistToString(fields));
		ArrayList<CaseUnit> cases =  new ArrayList<CaseUnit>();;
		cases.add(new CaseUnit(SQL, result, base, Thread.currentThread()
					.getStackTrace()[1].getMethodName()));
		return cases;
	}

}
