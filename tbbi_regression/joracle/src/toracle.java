import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

class TypeMap{
	
}

 class DBConnection {
		    public static void dbConn(String name, String pass,String sql) {
		       Connection c = null;
		       try {
		           Class.forName("oracle.jdbc.driver.OracleDriver");
		           // 要是导入驱动没有成功的话都是会出现classnotfoundException.自己看看是不是哪里错了,例如classpath这些设置
		       } catch (ClassNotFoundException e) {
		           System.out.println(e.toString());
		           return ;
		       }
		       //phoenix_prod@10.232.31.194:1521:ark【oracle】
		       try {
		           c = DriverManager.getConnection(
		                  "jdbc:oracle:thin:@10.232.31.194:1521:ark", name, pass);
		
	//	           String sql=new String("select status,count(*) from phoenix_task_inst where bizdate=to_date('2014-12-21','yyyy-mm-dd') and dag_type=0 and app_id=17470  and task_type=0 group by status");
		           //String sql=new String("select TASK_INST_ID from phoenix_task_inst where bizdate=to_date('2014-12-21','yyyy-mm-dd') and dag_type=0 and app_id=17470  and task_type=0 and status=5");
		           /*
		           String sql=new String("select task_inst_id,node_def_id from phoenix_task_inst  where bizdate=to_date('2014-12-21','yyyy-mm-dd') and dag_type=0 and app_id=17470  and task_type=0  and status="+status);
		           */
		           
		           System.err.println("sql:"+sql);
		           Statement stmt=  c.createStatement();
		           boolean ret=stmt.execute(sql);
		           int cnt=0;
		           if(ret==true){
		        	   ResultSet rs=stmt.getResultSet();
		        	   while (rs.next()) {
		                   int  task_inst_id = rs.getInt(1);
		                   int node_def_id = rs.getInt(2);
		                   cnt=cnt+1;
		                   System.out.println("task_inst_id:"+task_inst_id+"\tnode_def_id:"+node_def_id);
		        	   }
		           }
		           System.err.println("cnt:"+cnt);
		           // 连接数据的方法有四种, 这个属于最简单的,一般用网页程序
		           // "jdbc:oracle:thin:@计算机名称:监听端口:系统实例名", username, password,
		           // 计算机名称,要是自己不知道可以在计算机属性查知.
		           // 监听端口一般默认是1521, 要是改变了就看自己的监听文件listener.ora
		           // 系统实例名一般是默认orcl, 要是不是的话就用 select name from v$database; 看看当前的实例名.
		           // username,password,就是登陆数据库的用户名和密码.
		       } catch (SQLException e) {
		           System.out.println(e.toString());
		           return ;
		       }
		       
		    }
 }
 
public  class toracle {
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		System.err.println("begin to process");
		// TODO Auto-generated method stub
		String name="phoenix_prod",pass="phoenix_prod";
		if(args.length!= 1){
			System.err.println("usage:toralce.jar sql-string");
			System.exit(0);
		}
			
		System.err.println(args[0]);
		DBConnection.dbConn(name,pass,args[0]);
		System.err.println("process over");
		}
}
