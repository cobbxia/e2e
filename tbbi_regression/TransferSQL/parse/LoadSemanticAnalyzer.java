package test.hiveserver.parse;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.parse.SemanticException;

public class LoadSemanticAnalyzer extends DDLSemanticAnalyzer {

	public LoadSemanticAnalyzer(HiveConf conf) throws SemanticException {
		super(conf);
		// TODO Auto-generated constructor stub
	}

}
