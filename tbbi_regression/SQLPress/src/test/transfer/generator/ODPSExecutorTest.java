package test.transfer.generator;

import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;
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

public class ODPSExecutorTest {
    ODPSConnection connectioin =null;
	public static void main(String[] args) throws Exception, ClientException{
		ODPSExecutorTest executor=new ODPSExecutorTest();
	}
	public ODPSConnection init(String endpoint,String access_id,String access_key){
		 ClientConfiguration config = new ClientConfiguration();
	     return connectioin = new ODPSConnection(endpoint, access_id, access_key, config);
	}
	public ODPSExecutorTest(String endpoint,String access_id,String access_key){
		  connectioin =init(endpoint,access_id,access_key);
	}
	public ODPSExecutorTest(){
		connectioin=init(GlobalConf.endpoint,GlobalConf.access_id,GlobalConf.access_key);
	}
	public static ArrayList<FieldSchema> getFieldFromTable(String tablename) throws ODPSException, ClientException{
		return getFieldFromTable(GlobalConf.defaultProjectName,tablename);
	}

	public static ArrayList<FieldSchema>  getFieldFromTable(String projectName,String tableName) throws ODPSException, ClientException{
        ClientConfiguration config = new ClientConfiguration();
        String endpoint = GlobalConf.endpoint;
        String access_id = GlobalConf.access_id;
        String access_key = GlobalConf.access_key;
        ODPSConnection connectioin = new ODPSConnection(endpoint, access_id, access_key, config);
		ArrayList<FieldSchema> fields=new ArrayList<FieldSchema>();
		Project project = new Project(connectioin, projectName);
		Table table = new Table(project,tableName);
		table.load();
		java.util.List<Column> cols=table.getColumns();
		for(int i=0;i<cols.size();i++){
			FieldSchema fs=new FieldSchema(cols.get(i).getName(), cols.get(i).getType());
			fields.add(fs);
		}
		return fields;
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
//	                System.out.println("key\t"+status.getKey());
//	                System.out.println("value\t"+status.getValue());
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
  
	public static String exec(String projectName, String sql) {
	        ClientConfiguration config = new ClientConfiguration();
	        String endpoint = GlobalConf.endpoint;
	        String access_id = GlobalConf.access_id;
	        String access_key = GlobalConf.access_key;
	        ODPSConnection connectioin = new ODPSConnection(endpoint, access_id, access_key, config);
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
//	                System.out.println("key\t"+status.getKey());
//	                System.out.println("value\t"+status.getValue());
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
	 
	 public static String exec(String sql){
			String[] sqls=sql.split(";");
			String ret="";
			System.out.println(sql);
			for(int i=0;i<sqls.length;i++)
			{
				if(sqls[i]==";") continue;
				sqls[i]=sqls[i]+";";
				try{
					ret=ret+ODPSExecutorTest.exec(GlobalConf.defaultProjectName, sqls[i]);
				}catch(Exception e){
					e.printStackTrace();
				}
			}
			return ret;
		}
}
