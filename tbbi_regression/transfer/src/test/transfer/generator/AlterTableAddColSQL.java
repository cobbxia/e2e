package test.transfer.generator;


import java.util.ArrayList;

import test.transfer.parse.Utility;
import  com.testyun.odps.TableSchema;
import com.testyun.odps.Column;


class AlterTableAddColSQL extends SQLGenerator {
	String baseSql = "";
	 com.testyun.odps.TableSchema t=null;
	AlterTableAddColSQL(String tablename, ArrayList<FieldSchema> fields) {
		
		super(tablename, fields);
		t=new TableSchema();
		Column column = t.getColumn(0);
		column.getType();
		System.out.println("AlterTableAddColSQL");
		this.wrappers.add(new Wrapper(){
			public ArrayList<CaseUnit> gencases(){
				return maxAllTypeField();
			}
		});
		this.wrappers.add(new Wrapper(){
			public ArrayList<CaseUnit> gencases(){
				return maxLenFieldPair();
			}
		});
		this.wrappers.add(new Wrapper(){
			public ArrayList<CaseUnit> gencases(){
				return maxHalfLenFieldPair();
			}
		});
	}

	AlterTableAddColSQL() {

	}

	ArrayList<CaseUnit> maxHalfLenFieldPair() {
		System.out.println("maxHalfLenFieldPair");
		return LenFieldPair(fh.halfCharactr());
	}

	ArrayList<CaseUnit> maxLenFieldPair() {
		System.out.println("maxLenFieldPair");
		return LenFieldPair(fh.maxLenField());
	}

	ArrayList<CaseUnit> LenFieldPair(String maxLenField) {
		ArrayList<CaseUnit> x = new ArrayList<CaseUnit>();
		String[] allcols = GlobalConf.getAllColType();
		for (int i = 0; i < allcols.length; i++) {
			String fulltablename = this.tablename
					+ Integer.toString(GlobalConf.getTableIndex());
            String maxLenSQL = "drop table IF EXISTS " + fulltablename + ";"
					+ fh.getCreateTableSQL(fulltablename, fields) + "alter table "
					+ fulltablename + " add COLUMNS (" + maxLenField + " "
					+ allcols[i] + ");";
            String maxResultSQL = fulltablename;
			ArrayList<FieldSchema> ofs = new ArrayList<FieldSchema>();
			ofs.addAll(fields);
			ofs.add(new FieldSchema(maxLenField, allcols[i]));
			Task result = new DescTask(maxResultSQL);
			Task base = new DescTask();
			base.setText(Utility.arraylistToString(ofs));
			x.add(new CaseUnit(maxLenSQL, result, base, Thread.currentThread()
					.getStackTrace()[1].getMethodName()));
		}
		return x;
	}

	ArrayList<CaseUnit> maxAllTypeField() {
		System.out.println("maxAllTypeField");
		String allcol = "";
		ArrayList<FieldSchema> ofs = new ArrayList<FieldSchema>();
		ArrayList<CaseUnit> x = new ArrayList<CaseUnit>();
		//System.out.println("maxAllTypeField   fields:\t"+fields.size());
		ofs.addAll(fields);
		String[] allTypes=GlobalConf.getAllColType();
		for (int i = 0; i < allTypes.length; i++) {
			FieldSchema fs = new FieldSchema(fh.getIncFieldName(),
					allTypes[i]);
			ofs.add(fs);
	//		System.out.println(Integer.toString(i) + "\t"
	//				+ Integer.toString(allTypes.length));
			if (allcol == "") {
				allcol = fs.toString();
			} else {
				allcol = allcol + "," + fs.toString();
			}
//			System.out.println("allcol:\t"+allcol);
		}
		String fulltablename = this.tablename
				+ Integer.toString(GlobalConf.getTableIndex());
		String maxLenSQL = "drop table IF EXISTS " + fulltablename + ";"
				+ fh.getCreateTableSQL(fulltablename, fields) + "alter table "
				+ fulltablename + " add COLUMNS (" + allcol + ");";
		Task result = new DescTask(fulltablename);
		Task base = new DescTask();
		base.setText(Utility.arraylistToString(ofs));
		x.add(new CaseUnit(maxLenSQL, result, base, Thread.currentThread()
				.getStackTrace()[1].getMethodName()));
		return x;
	}
/*
	public ArrayList<CaseUnit> genCases() {
		System.out.println("AlterTableAddColSQL genSQL");
	//	cases.addAll(this.maxLenFieldPair());
	//	cases.addAll(this.maxHalfLenFieldPair());
		cases.addAll(this.maxAllTypeField());
		return cases;
	}
*/
}
