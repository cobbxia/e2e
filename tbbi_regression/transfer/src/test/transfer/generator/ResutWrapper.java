package test.transfer.generator;

import java.util.ArrayList;
import java.util.List;

/*
 string and fieldList stored
 */
public class ResutWrapper {
	public String str="";
	public ArrayList<FieldSchema> fields=null;
	public List<String> parts=null;
	public ResutWrapper(String sql,ArrayList<FieldSchema> fields){
		this.fields=fields;
		this.str=sql;
	}
	public  ArrayList<FieldSchema> getFields (){
		return this.fields;
	}
}


/*
public class Base {
	public String str="";
	public ArrayList<FieldSchema> fields=null;
	public Base(String sql,ArrayList<FieldSchema> fields){
		this.fields=fields;
		this.str=sql;
	}
	public  ArrayList<FieldSchema> getFields (){
		return this.fields;
	}
	public Base() {
		// TODO Auto-generated constructor stub
	}
	public String toString(){
		String retstr="";
		if(this.str!=""){
			retstr=this.str;
		}else if (this.fields!=null){
			for(int i=0;i<this.fields.size();i++){
				retstr=retstr+this.fields.toString();
			}
		}
		return retstr;
	}
}
*/