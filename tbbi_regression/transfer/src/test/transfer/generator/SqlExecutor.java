package test.transfer.generator;


import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;

import test.transfer.parse.Utility;


import antlr.collections.List;

import com.testyun.openservices.ClientConfiguration;
import com.testyun.openservices.ClientException;
import com.testyun.openservices.odps.ODPSConnection;
import com.testyun.openservices.odps.ODPSException;
import com.testyun.openservices.odps.Project;
import com.testyun.openservices.odps.jobs.Job;
import com.testyun.openservices.odps.jobs.JobInstance;
import com.testyun.openservices.odps.jobs.SqlTask;
import com.testyun.openservices.odps.jobs.Task;
import com.testyun.openservices.odps.jobs.TaskStatus;
import com.testyun.openservices.odps.jobs.WaitSettings;
import com.testyun.openservices.odps.tables.Column;
import com.testyun.openservices.odps.tables.Table;
import com.testyun.openservices.odps.tables.TableInfo;

public class SqlExecutor {
    ODPSConnection connectioin =null;
	public static void main(String[] args) throws Exception, ClientException{
		Utility.listToString(SqlExecutor.getPart("mztest", "partedkv"));
		SqlExecutor executor=new SqlExecutor();
		System.out.println(SqlExecutor.exec("mztest", "create table if not exists partedkv(id string) partitioned by(pt string);"));
		System.out.println(SqlExecutor.exec("mztest", "alter table partedkv add partition(pt=\"20140626\");"));
		System.out.println(SqlExecutor.exec("mztest", "show partitions partedkv;"));
		System.exit(0);
		System.out.println(executor.execSQL("mztest", "desc t1;"));
		System.out.println(SqlExecutor.exec("mztest", "desc t1;"));
		SqlExecutor.getFieldFromTable("mztest", "t1");
	}
	public ODPSConnection init(String endpoint,String access_id,String access_key){
		 ClientConfiguration config = new ClientConfiguration();
	     return connectioin = new ODPSConnection(endpoint, access_id, access_key, config);
	}
	public SqlExecutor(String endpoint,String access_id,String access_key){
		  connectioin =init(endpoint,access_id,access_key);
	}
	public SqlExecutor(){
		connectioin=init(GlobalConf.endpoint,GlobalConf.access_id,GlobalConf.access_key);
	}
	public static ResutWrapper getFieldFromTable(String tablename) throws ODPSException, ClientException{
		return getFieldFromTable(GlobalConf.defaultProjectName,tablename);
	}
	public static ResutWrapper getFieldFromTable(String projectName,String tablename) throws ODPSException, ClientException{
        ClientConfiguration config = new ClientConfiguration();
        String endpoint = GlobalConf.endpoint;
        String access_id = GlobalConf.access_id;
        String access_key = GlobalConf.access_key;
        ODPSConnection connectioin = new ODPSConnection(endpoint, access_id, access_key, config);
		ArrayList<FieldSchema> fields=new ArrayList<FieldSchema>();
		Project project = new Project(connectioin, projectName);
		Table table = new Table(project,tablename);
		table.load();
		java.util.List<Column> cols=table.getColumns();
		for(int i=0;i<cols.size();i++){
	//		System.out.println("name\t"+cols.get(i).getName()+"\ttype\t"+cols.get(i).getType());
			FieldSchema fs=new FieldSchema(cols.get(i).getName(), cols.get(i).getType());
			fields.add(fs);
		}
		//System.out.println(table.getSchema().toJson());
		return new ResutWrapper("",fields);
	}
	
	public String execSQL(String projectName, String sql) {
	        Project project = new Project(connectioin, projectName);
	        String taskName = "SqlTask";
	        Task task = new SqlTask(taskName, sql);
	        String result = null;
	        try {
	            JobInstance instance = Job.run(project, task);
	            WaitSettings setting = new WaitSettings();
	            setting.setMaxErrors(10);
	            instance.waitForCompletion(setting, null);
	            Map<String, String> resultMap = instance.getResult();
	            result = resultMap.get(task.getName());
	            for (Entry<String, String> status : resultMap.entrySet()) {
	             //   System.out.println("key\t"+status.getKey());
	              //  System.out.println("value\t"+status.getValue());
	            }
	            TaskStatus taskStatus = instance.getTaskStatus().get(taskName);
	            if (TaskStatus.Status.FAILED.equals(taskStatus.getStatus())) {
	                throw new Exception(result);
	            }
	        } catch (Exception e) {
	            throw new RuntimeException(e);
	        }
	        return result;
	}
  
	public static ResutWrapper exec(String projectName, String sql) {
	        ClientConfiguration config = new ClientConfiguration();
	        String endpoint = GlobalConf.endpoint;
	        String access_id = GlobalConf.access_id;
	        String access_key = GlobalConf.access_key;
	        ODPSConnection connectioin = new ODPSConnection(endpoint, access_id, access_key, config);
	        Project project = new Project(connectioin, projectName);
	        String taskName = "SqlTask";
	        TableInfo ti=new TableInfo();
	        
	        Task task = new SqlTask(taskName, sql);
	        String result = null;
	        try {
	            JobInstance instance = Job.run(project, task);
	            WaitSettings setting = new WaitSettings();
	            setting.setMaxErrors(10);
	            instance.waitForCompletion(setting, null);
	            Map<String, String> resultMap = instance.getResult();
	            result = resultMap.get(task.getName());
	            for (Entry<String, String> status : resultMap.entrySet()) {
	            //    System.out.println("key\t"+status.getKey());
	            //s    System.out.println("value\t"+status.getValue());
	            }
	            TaskStatus taskStatus = instance.getTaskStatus().get(taskName);
	            if (TaskStatus.Status.FAILED.equals(taskStatus.getStatus())) {
	                throw new Exception(result);
	            }
	        } catch (Exception e) {
	          //  throw new RuntimeException(e);
	        	e.printStackTrace();
	        }
	        return new ResutWrapper(result,null);
	    }
		 
	 
	 public static String exec(String sql){
			String[] sqls=sql.split(";");
			String ret="";
			SqlExecutor executor=new SqlExecutor();
			for(int i=0;i<sqls.length;i++)
			{
				if(sqls[i]==";") continue;
				sqls[i]=sqls[i]+";";
				System.out.println("sql:"+sqls[i]);
				try{
					ret=ret+executor.execSQL(GlobalConf.defaultProjectName, sqls[i]).toString();
				}catch(Exception e){
					System.out.println("Exception SQL:\t"+sqls[i]);
					e.printStackTrace();
				}
			}
			return ret;
		}
	 
		public static  ODPSConnection getDefaultConnection(){
			 ClientConfiguration config = new ClientConfiguration();
		        String endpoint = GlobalConf.endpoint;
		        String access_id = GlobalConf.access_id;
		        String access_key = GlobalConf.access_key;
			return new ODPSConnection(endpoint, access_id, access_key, config);
		}
		
		public static java.util.List<String>  getPart(String projectName,String tableName){
			java.util.List<String> parts = null;
			ODPSConnection connectioin =SqlExecutor.getDefaultConnection();
			Project project = new Project(connectioin, projectName);
			Table table = new Table(project,tableName);
			try {
				table.load();
				parts=table.listPartitions();
			} catch (ODPSException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClientException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			return parts;
		}
		public static ResutWrapper getPart(String cmd) {
			// TODO Auto-generated method stub
			return null;
		}
}






