import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
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
import com.testyun.openservices.odps.tables.Table;


class Controller implements Runnable{
	public static boolean isRunning=true;
	public int duration=0;
	public Controller(int duration){
		this.duration=duration;
	}
	@Override
	public void run() {
		try {
			Thread.sleep(duration*1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Controller.isRunning=false;
		// TODO Auto-generated method stub
	}
	
}


class SqlExecutor  implements Runnable{
    ODPSConnection connectioin =null;
    ClientConfiguration config =null;
    Project project = null;
    String taskName = "SqlTask";
    String endpoint ="";
    String access_id ="";
    String access_key ="";
    String projectName= "";
	private String tablename;
	public static void main(String[] args) throws Exception, ClientException{		
		int threadcount=128;
		if(args.length==1){
			//threadcount=Integer.parseInt(args[0]);
			System.out.println("0:"+args[0]);
			Utility.genDict(args[0], "=");
			if(Utility.argMap.containsKey("threadcount"))
			{System.out.println("threadcount:"+Utility.argMap.get("threadcount"));
			 threadcount=Integer.parseInt(Utility.argMap.get("threadcount"));
			}
		}else{
			System.out.println("using default value from GlobalConf.\naccess_id="+GlobalConf.access_id+"\naccess_key="+GlobalConf.access_key+"\nendpoint="+GlobalConf.endpoint);
			Utility.argMap=new HashMap<String,String>();
			Utility.argMap.put(new String("access_id"), GlobalConf.access_id);
			Utility.argMap.put("access_key", GlobalConf.access_key);
			Utility.argMap.put("endpoint", GlobalConf.endpoint);			
			Utility.argMap.put("durationSeconds", GlobalConf.durationSeconds);		
			//System.exit(0);
		}
	    /*	
		SqlExecutor exec=new SqlExecutor(0);
		exec.execSQL("drop table mztest;");
		exec.execSQL("create table mztest(id string) partitioned by(pt string);");
		System.exit(0);
		*/
		SqlExecutor[] executors=new SqlExecutor[threadcount];
		for(int i=0;i<threadcount;i++){
			executors[i]=new SqlExecutor(i);
			System.out.println("thradcount:"+threadcount+"\ti:"+i);
		}
		//Controller controller=null;
		new Thread((new Controller(Integer.parseInt(Utility.argMap.get("durationSeconds"))))).start();
		for(int i=0;i<threadcount;i++){
			new Thread(executors[i],"thrad"+String.valueOf(i)).start();
		}
	}
	
	public SqlExecutor(int index){
		this.tablename=GlobalConf.defaultTableName+Integer.valueOf(index);
		init(Utility.argMap.get("endpoint"),Utility.argMap.get("access_id"),Utility.argMap.get("access_key"));
	}

	public SqlExecutor(int index,String endpoint,String access_id,String access_key){
		this.tablename=GlobalConf.defaultTableName+Integer.valueOf(index);
		System.out.println(this.tablename);
		init(endpoint,access_id,access_key);
	}
	public void setProject(){
		if(this.projectName=="")	this.projectName=GlobalConf.defaultProjectName;
		this.project = new Project(this.connectioin,this.projectName);
	}
	
	public void init(String endpoint,String access_id,String access_key){
		System.out.println("endpoint:\t"+endpoint);
		this.endpoint = endpoint;
	    this.access_id = access_id;
	    this.access_key = access_key;
	    connectioin = new ODPSConnection(this.endpoint, this.access_id, this.access_key, config);
		config = new ClientConfiguration();
	}
		
	public String execSQL(String sql) {   
		System.out.println(sql);
	    String result = null; 
		try {
			 Task task = new SqlTask(taskName, sql);   
			 if(project==null) setProject();
	         JobInstance instance = Job.run(this.project, task);
	         WaitSettings setting = new WaitSettings();
	         setting.setMaxErrors(10);
	         instance.waitForCompletion(setting, null);
	         Map<String, String> resultMap = instance.getResult();
	         result = resultMap.get(task.getName());
	         for (Entry<String, String> status : resultMap.entrySet()) {
	            System.out.println("key:"+status.getKey()+"\tval:"+status.getValue());
	         }
	         TaskStatus taskStatus = instance.getTaskStatus().get(taskName);
	         if (TaskStatus.Status.FAILED.equals(taskStatus.getStatus())) {
	        	 throw new Exception(result);
	         }
	    } catch (Exception e) {
	         //throw new RuntimeException(e);
	    	e.printStackTrace();
	    }
	    return result;
	}
  	 
	public  java.util.List<String>  getPart(String projectName,String tableName){
			java.util.List<String> parts = null;
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

		@Override
		public void run() {
			while(true){
			//while(i<4096){	
			//String sql="create table if not exists "+this.tablename+"(id string);";
			String sql="alter table "+GlobalConf.defaultTableName+" add partition(pt=\"1\");";
			System.out.println(sql);
			execSQL(sql);
			//sql="drop table if exists "+this.tablename+";";
			sql="alter table "+GlobalConf.defaultTableName+" drop partition(pt=\"1\");";
			execSQL(sql);
			if(Controller.isRunning==false){
				System.out.println("is not running "+Controller.isRunning);
				break;	
			}	
		}
	}
}







