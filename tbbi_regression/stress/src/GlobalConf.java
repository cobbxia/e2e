/*
类型	描述	取值范围
Bigint	8字节有符号整型。请不要使用整型的最小值 (-9223372036854775808)，这是系统保留值。	-9223372036854775807 ~ 9223372036854775807
String	字符串，支持utf-8编码。其他编码的字符行为未定义。 STRING类型允许最长为2M字节。	 
Boolean	布尔型。	True/False
Double	8字节双精度浮点数。	-1.0 * 10^308 ~ 1.0 * 10^308
Datetime	日期类型。	0001-01-01 00:00:00 ~ 9999-12-31 23:59:59
DECIMAL	DECIMAL类型整数部分不限长， 小数部分保留十位有效数字	 
ARRAY<T> | 数组类型, T为BIGINT/BOOLEAN/DOUBLE/STRING	 
MAP<T1, T2> | 字典类型, T1为BIGINT/STRING,
T2为BIGINT/DOUBLE/STRING
* */
class GlobalConf{
	
	public static String resultFile="result.txt";
    public static String access_id = "";
    public static String access_key = "";
	public static String endpoint = "";

	public static String[] TypeList={"bigint","string","boolean","double","datetime","decimal"};
	//BIGINT/BOOLEAN/DOUBLE/STRING
	//T1为BIGINT/STRING,
	//T2为BIGINT/DOUBLE/STRING
	public static String[] ComplexTypeList={"array<bigint>","array<BOOLEAN>","array<DOUBLE>",
		"array<STRING>",
		"map<BIGINT,BIGINT>","map<BIGINT,DOUBLE>","map<BIGINT,STRING>",
		"map<STRING,BIGINT>","map<STRING,DOUBLE>","map<STRING,STRING>"};
	public static String[] getAllColType(){
		String[] allcols=new String[GlobalConf.TypeList.length+GlobalConf.ComplexTypeList.length];
		int i=0,j=0;
		for(;i<GlobalConf.TypeList.length;i++){
			allcols[i]=GlobalConf.TypeList[i];
		}
		for(;j<GlobalConf.ComplexTypeList.length;j++){
			allcols[i+j]=GlobalConf.ComplexTypeList[j];
		}
		return allcols;
	}

	//public static int sqlTokList[]={HiveParser.TOK_ALTERTABLE_RENAMECOL};
	public static int maxFieldNum=1024;
	//256+1 is ok,256 +2 error
	public static int maxFieldLen=156+1;
	public static String cnfieldsrand="列";
	public static String fieldsrand="中文列";
	private static int tableIndex=0;
	private static int colIndex=0;
	private static int partColIndex=20140101;
	public static boolean iscn=true;
	public static String defaultProjectName="";
	public static String defaultPartName="";
	public static String defaultTableName="";
	public static String durationSeconds="1";
	public static int getTableIndex(){
		tableIndex=tableIndex+1;
		return tableIndex;
	}
	public static int getColIndex(){
		colIndex=colIndex+1;
		return colIndex;
	}
	public static int getPartColIndex() {
		partColIndex=partColIndex+1;
		// TODO Auto-generated method stub
		return partColIndex;
	}
}
