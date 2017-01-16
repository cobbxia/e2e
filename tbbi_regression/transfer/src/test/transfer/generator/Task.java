package test.transfer.generator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import test.transfer.parse.Utility;

import com.testyun.openservices.ClientException;
import com.testyun.openservices.odps.ODPSException;

public class Task {
	String cmd = "", text = "";

	public Task(String cmd) {
		this.cmd = cmd;
	}

	public Task() {
		// TODO Auto-generated constructor stub
	}

	public void execmd() {
	}

	public void setText(String text) {
		this.text = text;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof Task) {
			return text.equals(((Task) obj).text);
		} else {
			return false;
		}
	}

	@Override
	public String toString() {
		return this.text;
	}

	public void genText() throws Exception {

	}
}

class SqlTask extends Task{
	public SqlTask(String cmd) {
		super(cmd);
		// TODO Auto-generated constructor stub
	}
	@Override
	public void genText(){
		this.text=SqlExecutor.exec(cmd);
	}
}

class PartTask extends Task{
	public PartTask(String newtablename) {
		super(newtablename);
		// TODO Auto-generated constructor stub
	}

	public PartTask() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public void genText() throws ODPSException, ClientException{
		List<FieldSchema> fields=null;
		try{
			fields=SqlExecutor.getPart(cmd).getFields();
		}catch(Exception e){
			e.printStackTrace();
		}
		this.text=Utility.listToString((ArrayList<FieldSchema>) fields);
	}
}

class DescTask extends Task{
	public DescTask(String newtablename) {
		super(newtablename);
		// TODO Auto-generated constructor stub
	}

	public DescTask() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public void genText() throws ODPSException, ClientException{
		List<FieldSchema> fields=null;
		try{
			fields=SqlExecutor.getFieldFromTable(cmd).getFields();
		}catch(Exception e){
			e.printStackTrace();
		}
		this.text=Utility.listToString((ArrayList<FieldSchema>) fields);
	}
}
