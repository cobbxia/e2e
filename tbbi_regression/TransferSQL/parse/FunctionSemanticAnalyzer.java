package test.hiveserver.parse;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.parse.ASTNode;
//import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticException;

public class FunctionSemanticAnalyzer extends BaseSemanticAnalyzer {

	public FunctionSemanticAnalyzer(HiveConf conf) throws SemanticException {
		super(conf);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void analyzeInternal(ASTNode ast) throws SemanticException {
		// TODO Auto-generated method stub

	}

}
