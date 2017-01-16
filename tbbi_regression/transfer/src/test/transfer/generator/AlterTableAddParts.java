package test.transfer.generator;

import java.util.ArrayList;
import java.util.List;

import test.transfer.generator.SQLGenerator.Wrapper;
import test.transfer.parse.Utility;

public class AlterTableAddParts extends SQLGenerator {

	public AlterTableAddParts(String tablename, ArrayList<FieldSchema> fields) {
		super(tablename, fields);
		// TODO Auto-generated constructor stub
		this.wrappers.add(new Wrapper(){
			public ArrayList<CaseUnit> gencases(){
				return addPartsSQL();
			}
		});
	}
	public ArrayList<CaseUnit> addPartsSQL(){
		ArrayList<CaseUnit> x = new ArrayList<CaseUnit>();
		String fulltablename = this.tablename
				+ Integer.toString(GlobalConf.getTableIndex());
		String partname=GlobalConf.defaultPartName+
				"="+Integer.toString(GlobalConf.getPartColIndex());
		List<String> partList=null;
		ArrayList<CaseUnit> cases=new ArrayList<CaseUnit>();
		String sql="drop table IF EXISTS "+fulltablename+";"+
		fh.getCreateDefaultPartedTableSQL(tablename, fields)+
		"alter table add partition("+partname+");";
		Task result = new PartTask(fulltablename);
		Task base = new PartTask();
	    partList.add(partname);
		base.setText(Utility.listToString(partList));
		x.add(new CaseUnit(sql, result, base, Thread.currentThread()
				.getStackTrace()[1].getMethodName()));
		return x;
	}
}
