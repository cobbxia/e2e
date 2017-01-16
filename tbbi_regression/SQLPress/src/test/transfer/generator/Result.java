package test.transfer.generator;
/*
 * unused
 */
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

 class Result{
	String projectName="";
	String verifiedSQLorTable="";
	public void setProj(String projectName){this.projectName=projectName;}
	public ResutWrapper getResult(String prjectName,String sql){
		return SqlExecutor.exec(projectName, sql);
	}
	public void setSQL(String verifiedSQLorTable) {
		this.verifiedSQLorTable=verifiedSQLorTable;
		// TODO Auto-generated method stub
	}
	public Result(String verifiedSQLorTable){
		setSQL(verifiedSQLorTable);
	}
	public Result() {
		// TODO Auto-generated constructor stub
	}
	
	public ResutWrapper getResult(String methodname) throws SecurityException, NoSuchMethodException, IllegalArgumentException, IllegalAccessException, InvocationTargetException, ClassNotFoundException {
		// TODO Auto-generated method stub
		System.out.println(methodname);
		Class<?> c=Class.forName("test.transfer.generator.SqlExecutor");
		Method m=c.getMethod(methodname,new Class[]{String.class} );
		return (ResutWrapper) m.invoke(null,verifiedSQLorTable);
	//	return SqlExecutor.exec(verifiedSQL);
	}
}


