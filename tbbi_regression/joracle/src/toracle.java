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
		           // Ҫ�ǵ�������û�гɹ��Ļ����ǻ����classnotfoundException.�Լ������ǲ����������,����classpath��Щ����
		       } catch (ClassNotFoundException e) {
		           System.out.println(e.toString());
		           return ;
		       }
		       //phoenix_prod@10.232.31.194:1521:ark��oracle��
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
		           // �������ݵķ���������, ���������򵥵�,һ������ҳ����
		           // "jdbc:oracle:thin:@���������:�����˿�:ϵͳʵ����", username, password,
		           // ���������,Ҫ���Լ���֪�������ڼ�������Բ�֪.
		           // �����˿�һ��Ĭ����1521, Ҫ�Ǹı��˾Ϳ��Լ��ļ����ļ�listener.ora
		           // ϵͳʵ����һ����Ĭ��orcl, Ҫ�ǲ��ǵĻ����� select name from v$database; ������ǰ��ʵ����.
		           // username,password,���ǵ�½���ݿ���û���������.
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
