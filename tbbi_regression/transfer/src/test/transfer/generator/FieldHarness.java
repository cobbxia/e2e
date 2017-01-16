package test.transfer.generator;

import java.util.ArrayList;
import java.util.Collections;

import test.transfer.generator.GlobalConf;

class Pair {
	String verifiedSQL, executedSQL;
	Pair(String executedSQL, String verifiedSQL) {
		this.executedSQL = executedSQL;
		this.verifiedSQL = verifiedSQL;
	}
}

public class FieldHarness {
	String fieldsrand = "";
	ArrayList<FieldSchema> fields,partedFields;
	int fieldnum = 0;// 算子，用生成最大数目的列
	int fieldlen = 0;// 算子，用来生成最大长度的列

	public void init(int fieldnum, int fieldlen) {
		fields = new ArrayList<FieldSchema>();
		partedFields= new ArrayList<FieldSchema>();
		this.fieldnum = fieldnum;
		if (GlobalConf.iscn == true) {
			this.fieldsrand = GlobalConf.cnfieldsrand;
			this.fieldlen = fieldlen / 2;
		} else {
			this.fieldsrand = GlobalConf.fieldsrand;
			this.fieldlen = fieldlen;
		}
	}

	public FieldHarness(int fieldnum, int fieldlen) {
		init(fieldnum, fieldlen);
	}

	public FieldHarness() {
		init(GlobalConf.maxFieldNum, GlobalConf.maxFieldLen);
	}

	public void setSrand(String fieldSrand) {
		this.fieldsrand = fieldSrand;
	}

	public String maxLenField() {
		String retf = "";
		for (int i = 0; i < this.fieldlen; i++)
			retf = retf + this.fieldsrand;
		return retf;
	}

	public static ArrayList<FieldSchema> allTypecol() {
		ArrayList<FieldSchema> fields = new ArrayList<FieldSchema>();
		for (int i = 0; i < GlobalConf.TypeList.length; i++) {
			FieldSchema fs = new FieldSchema(FieldHarness.getIncFieldName(),
					GlobalConf.TypeList[i]);
			fields.add(fs);
		}
		for (int i = 0; i < GlobalConf.ComplexTypeList.length; i++) {
			FieldSchema fs = new FieldSchema(FieldHarness.getIncFieldName(),
					GlobalConf.ComplexTypeList[i]);
			fields.add(fs);
		}
		return fields;
	}

	public static String getIncFieldName() {
		return GlobalConf.fieldsrand + String.valueOf(GlobalConf.getColIndex());
	}

	public String halfCharactr() {
		String retf = "a";
		for (int i = 0; i < this.fieldlen; i++)
			retf = retf + this.fieldsrand;
		return retf;
	}

	public String getCreateTableSQL(String tablename) {
		String strfield = "";
//		System.out.println("strfield getctSQL size:\t" + fields.size());
		for (int i = 0; i < this.fields.size(); i++) {
			strfield = strfield + this.fields.get(i).name + " "
					+ this.fields.get(i).type + ",";
		}
		strfield = strfield.substring(0, strfield.length() - 1);
//		System.out.println("strfield:\t" + strfield);
		String strct="";
		if(this.partedFields.size()==0){
			strct = "create table " + tablename + "(" + strfield + ")";
		}else{
			strct=strct+" partitioned  by(";
			
			for(int i=0;i<this.partedFields.size();i++)
				strct =strct+this.partedFields.get(i).name+" "+this;
		}
		strct=strct+";";
	//	System.out.println("strct:\t" + strct);
		return strct;
	}
	
	public String getCreateDefaultPartedTableSQL(String tablename, ArrayList<FieldSchema> fields) {
		this.fields = fields;
		FieldSchema fs=new FieldSchema(tablename, GlobalConf.defaultPartName);
		this.partedFields.add(fs);
	//	System.out.println("getctSQL size:\t" + fields.size());
		return getCreateTableSQL(tablename);
	}

	public String getCreateTableSQL(String tablename, ArrayList<FieldSchema> fields) {
		this.fields = fields;
//		System.out.println("getctSQL size:\t" + fields.size());
		return getCreateTableSQL(tablename);
	}
	public String getCreateTableSQL(String tablename, ArrayList<FieldSchema> fields,ArrayList<FieldSchema> partedFields) {
		this.fields = fields;
		this.partedFields=partedFields;
//		System.out.println("getctSQL size:\t" + fields.size());
		return getCreateTableSQL(tablename);
	}
}