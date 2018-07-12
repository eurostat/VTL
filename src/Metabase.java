
public class Metabase {
	
/*
  drop lines starting with:
	 -- 
	 /
	 /*
	 set
	 replace "\r\n" with " "
	 replace TAB with " "
	 split by ";"
	 ignore empty line
 */

public static void createMetabase ( ) throws VTLError 
{
	StringBuffer	cmd = new StringBuffer () ;
	boolean 		createFunction = false ;			// create function, procedure, package
	boolean 		createTable = false ;				// create/alter table or view
	String[]		lines ;
	ListString		ls = new ListString () ;
	String			all = audit + "\r\n" + ddl + "\r\n" ; // avoid conflicts with other schemas + dcl ;
	
	lines = all.split(System.getProperty("line.separator"));
	for (String s : lines) {
		s = s.replace('\t', ' ') ;
		// do not trim, END must be at the beginning of the line
		if ( createFunction && ( s.startsWith( "END;" ) ) ) {	
			// NB: all functions and packages must end with the line "END;"
			ls.add ( cmd.append ( s.trim () ).toString() ) ;
			cmd.setLength(0);
			createFunction = false ;
			continue ;
		}
		
		if ( createTable && s.endsWith( ";" )) {
			ls.add ( cmd.append( s ).toString() ) ;
			cmd.setLength(0);			
			createTable =  false ;
			continue ;
		}

		s = s.trim() ;
		if ( s.length() == 0 || s.equals( "/" ) || s.startsWith( "COMMIT;" ) 
				|| s.startsWith( "/*" ) || s.startsWith( "--" ) || s.startsWith( "SET " ) )
			continue ;			

		if ( s.indexOf( " --") > 0 )
			s = s.substring(0, s.indexOf( " --") ) ;		// comments
		
		if ( s.startsWith( "CREATE OR REPLACE PACKAGE" ) || s.startsWith( "CREATE OR REPLACE FUNCTION" ) 
				|| s.startsWith( "CREATE OR REPLACE PACKAGE BODY" ) || s.startsWith( "CREATE OR REPLACE TYPE BODY" ) 
				|| s.startsWith( "CREATE OR REPLACE PROCEDURE") ) {
			if ( s.endsWith( ";" ) ) {
				ls.add ( s ) ;
			}
			else {
				cmd.append( s ) ;
				createFunction =  true ;
			}
		}
		else if ( createFunction || createTable ) {
			cmd.append ( ' ' ).append( s ).append( ' ' ) ;
		}
		else if ( s.startsWith( "DROP " ) || s.startsWith( "INSERT INTO " ) || s.startsWith( "GRANT " )) {
			if ( s.endsWith( ";" ) ) {
				ls.add ( s ) ;
			}
			else {
				cmd.append( s ) ;
				createTable = true ;				
			}
		}
		else if ( s.startsWith( "CREATE TABLE" ) || s.startsWith( "ALTER TABLE" ) || s.startsWith( "CREATE OR REPLACE VIEW" )
				|| s.startsWith( "CREATE OR REPLACE TYPE" ) || s.startsWith( "CREATE SEQUENCE" )
				|| s.startsWith( "CREATE INDEX" ) || s.startsWith( "CREATE UNIQUE INDEX" ) || s.startsWith( "CALL " )) {
			if ( s.endsWith( ";" ) ) {
				ls.add ( s ) ;
			}
			else {
				cmd.append( s ) ;
				createTable = true ;				
			}
		}
		else
			Sys.printStdOut ( "*********** ERROR at line: " + s ) ;
	}

	
	for ( String s : ls ) {
		if ( s.endsWith( ";" ) && ! s.endsWith( "END;" ))
			s = s.substring(0, s.length() - 1 ) ;
		Sys.printStdOut( s );					// Db.sql_exec( cmd.toString() ) ;
		// Sys.println( "_____________" );
		// sql " select * from user_objects where status='INVALID' "
		// sql " select * from user_errors" 
		try {
			Db.sqlExec( s ) ;
		}
		catch ( VTLError e ) {
			if ( s.startsWith( "DROP " ) )
				;	// ignore
			else
				throw e ;
		}
	}
}

static String audit = 
	"--	EXEC HIST table stores the different script executions with timestamp\r\n" + 
	"DROP TABLE EXEC_HIST CASCADE CONSTRAINTS PURGE;\r\n" + 
	"CREATE TABLE EXEC_HIST (" + 
	"	SCRIPT_NAME VARCHAR2(255)," + 
	"	EXEC_TIME TIMESTAMP (6) WITH TIME ZONE," + 
	"	VERSION VARCHAR2(100)," + 
	"	DESCRIPTION VARCHAR2(255)" + 
	");\r\n" + 
	"--	Procedures to insert an entry to log the script execution.\r\n" + 
	"CREATE OR REPLACE PROCEDURE EXEC_SCRIPT(SCRIPT_NAME IN VARCHAR2, VERSION IN VARCHAR2, DESCRIPTION IN VARCHAR2)\r\n" + 
	"IS\r\n" + 
	"BEGIN\r\n" + 
	"	INSERT INTO EXEC_HIST (SCRIPT_NAME, VERSION, EXEC_TIME, DESCRIPTION) VALUES (SCRIPT_NAME, VERSION, CURRENT_TIMESTAMP, DESCRIPTION);\r\n" + 
	"END;\r\n" + 
	"--	Log this audit sql script as first script being executed.\r\n" + 
	"CALL EXEC_SCRIPT('audit.sql', '4', 'Audit Script to allow script executions to be logged.');\r\n" ;

/*
 * NB: create view can be execute after the creation of the base table but its status is INVALID 
 * (the status is set to VALID after any select from the view)
 */
static String ddl =
	"CALL EXEC_SCRIPT('mdt-tool-ddl.sql', '15.02.28', 'MDT Tool DDL language.');\r\n" + 
	"/*  Drop                                                */\r\n" + 
	"--  Packages\r\n" + 
	"DROP PACKAGE \"MDT_UTILS\";\r\n" + 
	"--  Functions\r\n" + 
	"DROP FUNCTION \"MDT_MERGE_FLAGS\";\r\n" + 
	"DROP FUNCTION \"MDT_AGG_FLAGS\";\r\n" + 
	"DROP FUNCTION \"BLOB_TO_CLOB\";\r\n" + 
	"--  Views\r\n" + 
	"DROP VIEW \"MDT_USER_AUDITS\" CASCADE CONSTRAINTS;\r\n" + 
	"DROP VIEW \"MDT_USER_AUDITS_INS\" CASCADE CONSTRAINTS;\r\n" + 
	"DROP VIEW \"MDT_USER_DATA_OBJECTS\" CASCADE CONSTRAINTS;\r\n" + 
	"DROP VIEW \"MDT_USER_DEPENDENCIES\" CASCADE CONSTRAINTS;\r\n" + 
	"DROP VIEW \"MDT_USER_DIMENSIONS\" CASCADE CONSTRAINTS;\r\n" + 
	"DROP VIEW \"MDT_USER_DROPPED_OBJECTS\" CASCADE CONSTRAINTS;\r\n" + 
	"DROP VIEW \"MDT_USER_EQUATIONS\" CASCADE CONSTRAINTS;\r\n" + 
	"DROP VIEW \"MDT_USER_EQUATIONS_ITEMS\" CASCADE CONSTRAINTS;\r\n" + 
	"DROP VIEW \"MDT_USER_EQUATIONS_TREE\" CASCADE CONSTRAINTS;\r\n" + 
	"DROP VIEW \"MDT_USER_HISTORY\" CASCADE CONSTRAINTS;\r\n" + 
	"DROP VIEW \"MDT_USER_MODIFICATIONS\" CASCADE CONSTRAINTS;\r\n" + 
	"DROP VIEW \"MDT_USER_MODIFICATIONS_INS\" CASCADE CONSTRAINTS;\r\n" + 
	"DROP VIEW \"MDT_USER_NOTES\" CASCADE CONSTRAINTS;\r\n" + 
	"DROP VIEW \"MDT_USER_OBJECTS\" CASCADE CONSTRAINTS;\r\n" + 
	"DROP VIEW \"MDT_USER_OBJECTS_COMMENTS\" CASCADE CONSTRAINTS;\r\n" + 
	"DROP VIEW \"MDT_USER_OBJECTS_INS\" CASCADE CONSTRAINTS;\r\n" + 
	"DROP VIEW \"MDT_USER_POSITIONS\" CASCADE CONSTRAINTS;\r\n" + 
	"DROP VIEW \"MDT_USER_POSITIONS_INS\" CASCADE CONSTRAINTS;\r\n" + 
	"DROP VIEW \"MDT_USER_PRIVILEGES\" CASCADE CONSTRAINTS;\r\n" + 
	"DROP VIEW \"MDT_USER_PROFILES\" CASCADE CONSTRAINTS;\r\n" + 
	"DROP VIEW \"MDT_USER_RENAMED_OBJECTS\" CASCADE CONSTRAINTS;\r\n" + 
	"DROP VIEW \"MDT_USER_SESSIONS\" CASCADE CONSTRAINTS;\r\n" + 
	"DROP VIEW \"MDT_USER_SOURCES\" CASCADE CONSTRAINTS;\r\n" + 
	"DROP VIEW \"MDT_USER_SYNTAX_TREES\" CASCADE CONSTRAINTS;\r\n" + 
	"DROP VIEW \"MDT_USER_VALIDATION_CONDITIONS\" CASCADE CONSTRAINTS;\r\n" + 
	"DROP VIEW \"MDT_USER_VALIDATION_RULES\" CASCADE CONSTRAINTS;\r\n" + 
	"--  Tables\r\n" + 
	"DROP TABLE \"MDT_AUDITS\" CASCADE CONSTRAINTS;\r\n" + 
	"DROP TABLE \"MDT_DATA_OBJECTS\" CASCADE CONSTRAINTS;\r\n" + 
	"DROP TABLE \"MDT_DEPENDENCIES\" CASCADE CONSTRAINTS;\r\n" + 
	"DROP TABLE \"MDT_DIMENSIONS\" CASCADE CONSTRAINTS;\r\n" + 
	"DROP TABLE \"MDT_DROPPED_OBJECTS\" CASCADE CONSTRAINTS;\r\n" + 
	"DROP TABLE \"MDT_EQUATIONS\" CASCADE CONSTRAINTS;\r\n" + 
	"DROP TABLE \"MDT_EQUATIONS_ITEMS\" CASCADE CONSTRAINTS;\r\n" + 
	"DROP TABLE \"MDT_EQUATIONS_TREE\" CASCADE CONSTRAINTS;\r\n" + 
	"DROP TABLE \"MDT_HISTORY\" CASCADE CONSTRAINTS;\r\n" + 
	"DROP TABLE \"MDT_MODIFICATIONS\" CASCADE CONSTRAINTS;\r\n" + 
	"DROP TABLE \"MDT_NOTES\" CASCADE CONSTRAINTS;\r\n" + 
	"DROP TABLE \"MDT_OBJECTS\" CASCADE CONSTRAINTS;\r\n" + 
	"DROP TABLE \"MDT_OBJECTS_COMMENTS\" CASCADE CONSTRAINTS;\r\n" + 
	"DROP TABLE \"MDT_POSITIONS\" CASCADE CONSTRAINTS;\r\n" + 
	"DROP TABLE \"MDT_PRIVILEGES\" CASCADE CONSTRAINTS;\r\n" + 
	"DROP TABLE \"MDT_PROFILES\" CASCADE CONSTRAINTS;\r\n" + 
	"DROP TABLE \"MDT_RENAMED_OBJECTS\" CASCADE CONSTRAINTS;\r\n" + 
	"DROP TABLE \"MDT_SESSIONS\" CASCADE CONSTRAINTS;\r\n" + 
	"DROP TABLE \"MDT_SOURCES\" CASCADE CONSTRAINTS;\r\n" + 
	"DROP TABLE \"MDT_SYNTAX_TREES\" CASCADE CONSTRAINTS;\r\n" + 
	"DROP TABLE \"MDT_VALIDATION_CONDITIONS\" CASCADE CONSTRAINTS;\r\n" + 
	"DROP TABLE \"MDT_VALIDATION_RULES\" CASCADE CONSTRAINTS;\r\n" + 
	"--  Sequences for primary keys\r\n" + 
	"DROP SEQUENCE \"MDT_SESSION_ID\";\r\n" + 
	"DROP SEQUENCE \"MDT_HISTORY_ID\";\r\n" + 
	"DROP SEQUENCE \"MDT_CHANGE_ID\";\r\n" + 
	"DROP SEQUENCE \"MDT_CREATEINDEX_ID\";\r\n" + 
	"DROP SEQUENCE \"MDT_NOTE_ID\";\r\n" + 
	"DROP SEQUENCE \"MDT_OBJECT_ID\";\r\n" + 
	"DROP SEQUENCE \"MDT_RECYCLEBIN_ID\";\r\n" + 
	"DROP SEQUENCE \"MDT_UPDATE_ID\";\r\n" + 
	"--  Types\r\n" + 
	"DROP TYPE \"TYP_AGGR_FLAGS\";\r\n" + 
	"DROP TYPE \"TABLE_FLAG_COMBINATION\";\r\n" + 
	"DROP TYPE \"STRUCT_FLAG_COMBINATION\";\r\n" + 
	"DROP TYPE \"TABLE_VARCHAR2\";\r\n" + 
	"DROP TYPE \"MDT_COLLECT_TABLE\";\r\n" + 
	"DROP TYPE \"MDT_BLOB\";\r\n" + 
	"/*  Create                                              */\r\n" + 
	"--  Types\r\n" + 
	"CREATE OR REPLACE TYPE \"MDT_BLOB\" AS OBJECT (blob_file blob, blob_filename VARCHAR ( 100 ));\r\n" + 
	"CREATE OR REPLACE TYPE \"MDT_COLLECT_TABLE\" AS TABLE OF VARCHAR (4000);\r\n" + 
	"CREATE OR REPLACE TYPE \"TABLE_VARCHAR2\" AS TABLE OF VARCHAR2(200 CHAR);\r\n" + 
	"CREATE OR REPLACE TYPE \"STRUCT_FLAG_COMBINATION\" AS OBJECT (flag VARCHAR2(1 CHAR), related_flags TABLE_VARCHAR2, result_option VARCHAR2(1 CHAR));\r\n" + 
	"CREATE OR REPLACE TYPE \"TABLE_FLAG_COMBINATION\" AS TABLE OF STRUCT_FLAG_COMBINATION;\r\n" + 
	"CREATE OR REPLACE TYPE \"TYP_AGGR_FLAGS\" AS OBJECT (\r\n" + 
	"	l_flags          VARCHAR2(200 CHAR),\r\n" + 
	"	l_flags_tbl      TABLE_VARCHAR2,\r\n" + 
	"	l_flags_counter  NUMBER,\r\n" + 
	"	static function ODCIAggregateInitialize (actx in out TYP_AGGR_FLAGS) return NUMBER,\r\n" + 
	"	member function ODCIAggregateIterate (self in out TYP_AGGR_FLAGS, val in VARCHAR2) return NUMBER,\r\n" + 
	"	member function ODCIAggregateTerminate (self in TYP_AGGR_FLAGS, returnValue out VARCHAR2, flags in NUMBER) return NUMBER,\r\n" + 
	"	member function ODCIAggregateMerge (self in out TYP_AGGR_FLAGS, ctx2 in TYP_AGGR_FLAGS) return NUMBER\r\n" + 
	");\r\n" + 
	"CREATE OR REPLACE TYPE BODY \"TYP_AGGR_FLAGS\" as\r\n" + 
	"	static function ODCIAggregateInitialize (actx in out TYP_AGGR_FLAGS) return NUMBER IS\r\n" + 
	"	BEGIN\r\n" + 
	"		if (actx is null) then\r\n" + 
	"			actx := typ_aggr_flags('', table_varchar2(), 0);\r\n" + 
	"		else\r\n" + 
	"			actx.l_flags := '';\r\n" + 
	"			actx.l_flags_tbl.delete();\r\n" + 
	"			actx.l_flags_counter := 0;\r\n" + 
	"		end if;\r\n" + 
	"		return ODCIConst.Success;\r\n" + 
	"	END;\r\n" + 
	"	member function ODCIAggregateIterate (self in out TYP_AGGR_FLAGS, val in VARCHAR2) return NUMBER\r\n" + 
	"	IS\r\n" + 
	"		l_found boolean := false;\r\n" + 
	"		l_value VARCHAR2(200 char);\r\n" + 
	"		l_char  VARCHAR2(1 char);\r\n" + 
	"	BEGIN\r\n" + 
	"		if (val is not null) then\r\n" + 
	"			l_value := trim(val);\r\n" + 
	"			for k in 1..length(l_value)\r\n" + 
	"			loop\r\n" + 
	"				l_char := substr(l_value, k, 1);\r\n" + 
	"				for i in 1..self.l_flags_counter\r\n" + 
	"				loop\r\n" + 
	"					if (l_char = self.l_flags_tbl(i)) then\r\n" + 
	"						l_found := true;\r\n" + 
	"						exit;\r\n" + 
	"					end if;\r\n" + 
	"				end loop;\r\n" + 
	"				if (not l_found) then\r\n" + 
	"					self.l_flags_tbl.extend(1);\r\n" + 
	"					self.l_flags_counter := self.l_flags_counter + 1;\r\n" + 
	"					self.l_flags_tbl(self.l_flags_counter) := l_char;\r\n" + 
	"				end if;\r\n" + 
	"				l_found := false;\r\n" + 
	"			end loop;\r\n" + 
	"		end if;\r\n" + 
	"		return ODCIConst.Success;\r\n" + 
	"	END;\r\n" + 
	"	member function ODCIAggregateTerminate (self in TYP_AGGR_FLAGS, returnValue out VARCHAR2, flags in NUMBER) return NUMBER\r\n" + 
	"	IS\r\n" + 
	"		l_tbl  TABLE_VARCHAR2;\r\n" + 
	"	BEGIN\r\n" + 
	"		select a.column_value\r\n" + 
	"		bulk collect into l_tbl\r\n" + 
	"		from table(self.l_flags_tbl) a\r\n" + 
	"		order by 1;\r\n" + 
	"		returnValue := '';\r\n" + 
	"		for i in 1..l_tbl.count()\r\n" + 
	"		loop\r\n" + 
	"			returnValue := returnValue || l_tbl(i);\r\n" + 
	"		end loop;\r\n" + 
	"		return ODCIConst.Success;\r\n" + 
	"	END;\r\n" + 
	"	member function ODCIAggregateMerge (self in out TYP_AGGR_FLAGS, ctx2 in TYP_AGGR_FLAGS) return NUMBER IS\r\n" + 
	"	BEGIN\r\n" + 
	"		return ODCIConst.Success;\r\n" + 
	"	END;\r\n" + 
	"END;\r\n" + 
	"--  Sequences for primary keys\r\n" + 
	"CREATE SEQUENCE \"MDT_SESSION_ID\"         MINVALUE 2000 MAXVALUE 999999999999999999999999999 INCREMENT BY 1 START WITH 2000 CACHE 20 NOORDER NOCYCLE;\r\n" + 
	"CREATE SEQUENCE \"MDT_HISTORY_ID\"         MINVALUE 2000 MAXVALUE 999999999999999999999999999 INCREMENT BY 1 START WITH 2000 CACHE 20 NOORDER NOCYCLE;\r\n" + 
	"CREATE SEQUENCE \"MDT_CHANGE_ID\"          MINVALUE 2000 MAXVALUE 999999999999999999999999999 INCREMENT BY 1 START WITH 2000 CACHE 20 NOORDER NOCYCLE;\r\n" + 
	"CREATE SEQUENCE \"MDT_CREATEINDEX_ID\"     MINVALUE 2000 MAXVALUE 999999999999999999999999999 INCREMENT BY 1 START WITH 2000 CACHE 20 NOORDER NOCYCLE;\r\n" + 
	"CREATE SEQUENCE \"MDT_NOTE_ID\"            MINVALUE 2000 MAXVALUE 999999999999999999999999999 INCREMENT BY 1 START WITH 2000 CACHE 20 NOORDER NOCYCLE;\r\n" + 
	"CREATE SEQUENCE \"MDT_OBJECT_ID\"          MINVALUE 2000 MAXVALUE 999999999999999999999999999 INCREMENT BY 1 START WITH 2000 CACHE 20 NOORDER NOCYCLE;\r\n" + 
	"CREATE SEQUENCE \"MDT_RECYCLEBIN_ID\"      MINVALUE 2000 MAXVALUE 999999999999999999999999999 INCREMENT BY 1 START WITH 2000 CACHE 20 NOORDER NOCYCLE;\r\n" + 
	"CREATE SEQUENCE \"MDT_UPDATE_ID\"          MINVALUE 2000 MAXVALUE 999999999999999999999999999 INCREMENT BY 1 START WITH 2000 CACHE 20 NOORDER NOCYCLE;\r\n" + 
	"--  Tables\r\n" + 
	"--  DDL for Table MDT_AUDITS\r\n" + 
	"CREATE TABLE \"MDT_AUDITS\" (\r\n" + 
	"	\"USER_NAME\" VARCHAR2(30 BYTE),\r\n" + 
	"	\"OBJECT_ID\" NUMBER(19,0),\r\n" + 
	"	\"TIMESTAMP\" DATE,\r\n" + 
	"	\"SES_ID\" NUMBER(19,0),\r\n" + 
	"	\"CMD_ID\" NUMBER(19,0),\r\n" + 
	"	\"UPD_ID\" NUMBER(19,0),\r\n" + 
	"	\"CMD_TEXT\" VARCHAR2(2000 BYTE),\r\n" + 
	"	\"N_OPS\" NUMBER(19,0),\r\n" + 
	"	\"CELLS_INSERTED\" NUMBER(19,0),\r\n" + 
	"	\"CELLS_DELETED\" NUMBER(19,0),\r\n" + 
	"	\"ELAPSED_TIME\" NUMBER,\r\n" + 
	"	\"PERIOD_MIN\" VARCHAR2(10 BYTE),\r\n" + 
	"	\"PERIOD_MAX\" VARCHAR2(10 BYTE),\r\n" + 
	"	\"AUDIT_COMMENT\" VARCHAR2(1000 BYTE),\r\n" + 
	"	\"AUDIT_BLOB\" \"MDT_BLOB\" ,\r\n" + 
	"	\"AUDIT_CLOB\" CLOB\r\n" + 
	");\r\n" + 
	"--  DDL for Table MDT_DATA_OBJECTS\r\n" + 
	"CREATE TABLE \"MDT_DATA_OBJECTS\" (\r\n" + 
	"	\"OBJECT_ID\" NUMBER(19,0),\r\n" + 
	"	\"DV_MODE\" CHAR(1 BYTE),\r\n" + 
	"	\"DV_KEEP\" CHAR(1 BYTE),\r\n" + 
	"	\"DV_KEEP_NUMBER\" NUMBER(19,0)\r\n" + 
	");\r\n" + 
	"--  DDL for Table MDT_DEPENDENCIES\r\n" + 
	"CREATE TABLE \"MDT_DEPENDENCIES\" (\r\n" + 
	"	\"OBJECT_ID\" NUMBER(19,0),\r\n" + 
	"	\"REF_OBJECT_OWNER\" VARCHAR2(30 BYTE),\r\n" + 
	"	\"REF_OBJECT_NAME\" VARCHAR2(30 BYTE),\r\n" + 
	"	\"REF_TYPE\" CHAR(1 BYTE)\r\n" + 
	");\r\n" + 
	"--  DDL for Table MDT_DIMENSIONS\r\n" + 
	"CREATE TABLE \"MDT_DIMENSIONS\" (\r\n" + 
	"	\"OBJECT_ID\" NUMBER(19,0),\r\n" + 
	"	\"DIM_NAME\" VARCHAR2(30 BYTE),\r\n" + 
	"	\"DIM_INDEX\" NUMBER(19,0),\r\n" + 
	"	\"DIM_TYPE\" CHAR(1 BYTE),\r\n" + 
	"	\"DIM_NULL\" CHAR(1 BYTE),\r\n" + 
	"	\"DIM_WIDTH\" NUMBER(19,0),\r\n" + 
	"	\"DIM_CONST\" VARCHAR2(50 BYTE),\r\n" + 
	"	\"COLUMN_TYPE\" VARCHAR2(61 BYTE),\r\n" + 
	"	\"LD_CL_STATIC\" CHAR(1 BYTE),\r\n" + 
	"	\"LD_CL_SORT\" CHAR(1 BYTE),\r\n" + 
	"	\"LD_DIM_NAMES\" VARCHAR2(100 BYTE),\r\n" + 
	"	\"LD_TRANSCOD\" VARCHAR2(61 BYTE),\r\n" + 
	"	\"LD_REFER_LIST\" VARCHAR2(61 BYTE),\r\n" + 
	"	\"LD_VALIDATION\" CHAR(1 BYTE),\r\n" + 
	"	\"DIM_HIERARCHY\" VARCHAR2(61 BYTE),\r\n" + 
	"	\"DIM_PRECISION\" NUMBER(19,0),\r\n" + 
	"	\"DIM_SCALE\" NUMBER(19,0)\r\n" + 
	");\r\n" + 
	"--  DDL for Table MDT_DROPPED_OBJECTS\r\n" + 
	"CREATE TABLE \"MDT_DROPPED_OBJECTS\" (\r\n" + 
	"	\"USER_NAME\" VARCHAR2(30 BYTE),\r\n" + 
	"	\"OBJECT_NAME\" VARCHAR2(30 BYTE),\r\n" + 
	"	\"OBJECT_TYPE\" CHAR(1 BYTE),\r\n" + 
	"	\"TIMESTAMP\" DATE\r\n" + 
	");\r\n" + 
	"--  DDL for Table MDT_EQUATIONS\r\n" + 
	"CREATE TABLE \"MDT_EQUATIONS\" (\r\n" + 
	"	\"OBJECT_ID\" NUMBER(19,0),\r\n" + 
	"	\"LEFT_PART\" VARCHAR2(50 BYTE),\r\n" + 
	"	\"EQUATION_TYPE\" VARCHAR2(10 BYTE),\r\n" + 
	"	\"CONSTANT_PART\" NUMBER,\r\n" + 
	"	\"COMPUTATION_LEVEL\" NUMBER(19,0),\r\n" + 
	"	\"NUM_ITEMS\" NUMBER(19,0),\r\n" + 
	"	\"EQUATION_NUMBER\" NUMBER(19,0),\r\n" + 
	"	\"COND_LOWERBOUND\" VARCHAR2(50 BYTE),\r\n" + 
	"	\"COND_UPPERBOUND\" VARCHAR2(50 BYTE),\r\n" + 
	"	\"EQUATION_COMMENT\" VARCHAR2(100 BYTE)\r\n" + 
	");\r\n" + 
	"--  DDL for Table MDT_EQUATIONS_ITEMS\r\n" + 
	"CREATE TABLE \"MDT_EQUATIONS_ITEMS\" (\r\n" + 
	"	\"OBJECT_ID\" NUMBER(19,0),\r\n" + 
	"	\"POS_CODE\" VARCHAR2(50 BYTE),\r\n" + 
	"	\"FACTOR\" NUMBER,\r\n" + 
	"	\"ITEM_NUMBER\" NUMBER(19,0),\r\n" + 
	"	\"EQUATION_NUMBER\" NUMBER(19,0)\r\n" + 
	");\r\n" + 
	"--  DDL for Table MDT_EQUATIONS_TREE\r\n" + 
	"CREATE TABLE \"MDT_EQUATIONS_TREE\" (\r\n" + 
	"	\"OBJECT_ID\" NUMBER(19,0),\r\n" + 
	"	\"POS_INDEX\" NUMBER(19,0),\r\n" + 
	"	\"POS_CODE\" VARCHAR2(50 BYTE),\r\n" + 
	"	\"POS_LEVEL\" NUMBER(19,0),\r\n" + 
	"	\"POS_CLASS\" NUMBER(19,0),\r\n" + 
	"	\"EQUATION_NUMBER\" NUMBER(19,0)\r\n" + 
	");\r\n" + 
	"--  DDL for Table MDT_HISTORY\r\n" + 
	"CREATE TABLE \"MDT_HISTORY\" (\r\n" + 
	"	\"HISTORY_ID\" NUMBER(19,0),\r\n" + 
	"	\"SESSION_ID\" NUMBER(19,0),\r\n" + 
	"	\"STATEMENT\" VARCHAR2(4000 BYTE),\r\n" + 
	"	\"ELAPSED_TIME\" NUMBER(19,0),\r\n" + 
	"	\"CUSTOM_MESSAGE\" VARCHAR2(4000 BYTE),\r\n" + 
	"	\"JAVA_EXCEPTION_MESSAGE\" VARCHAR2(4000 BYTE),\r\n" + 
	"	\"JAVA_STACK_TRACE\" VARCHAR2(4000 BYTE),\r\n" + 
	"	\"LOG_TYPE\" CHAR(1 BYTE)\r\n" + 
	");\r\n" + 
	"--  DDL for Table MDT_MODIFICATIONS\r\n" + 
	"CREATE TABLE \"MDT_MODIFICATIONS\" (\r\n" + 
	"	\"OBJECT_ID\" NUMBER(19,0),\r\n" + 
	"	\"TIMESTAMP\" DATE,\r\n" + 
	"	\"SES_ID\" NUMBER(19,0),\r\n" + 
	"	\"CMD_ID\" NUMBER(19,0),\r\n" + 
	"	\"DIM_NAME\" VARCHAR2(30 BYTE),\r\n" + 
	"	\"OP_TYPE\" CHAR(1 BYTE),\r\n" + 
	"	\"POS_CODE\" VARCHAR2(50 BYTE)\r\n" + 
	");\r\n" + 
	"--  DDL for Table MDT_NOTES\r\n" + 
	"CREATE TABLE \"MDT_NOTES\" (\r\n" + 
	"	\"OBJECT_ID\" NUMBER(19,0),\r\n" + 
	"	\"NOTE_INDEX\" NUMBER(19,0),\r\n" + 
	"	\"FIELD_INDEX\" NUMBER(19,0),\r\n" + 
	"	\"DIM_INDEX\" NUMBER(19,0),\r\n" + 
	"	\"POS_CODE\" VARCHAR2(50 BYTE)\r\n" + 
	");\r\n" + 
	"--  DDL for Table MDT_OBJECTS\r\n" + 
	"CREATE TABLE \"MDT_OBJECTS\" (\r\n" + 
	"	\"USER_NAME\" VARCHAR2(30 BYTE),\r\n" + 
	"	\"OBJECT_ID\" NUMBER(19,0),\r\n" + 
	"	\"OBJECT_NAME\" VARCHAR2(30 BYTE),\r\n" + 
	"	\"OBJECT_TYPE\" CHAR(1 BYTE),\r\n" + 
	"	\"STATUS\" CHAR(1 BYTE),\r\n" + 
	"	\"CREATED\" DATE,\r\n" + 
	"	\"LAST_MODIFIED\" DATE,\r\n" + 
	"	\"CHANGE_ID\" NUMBER(19,0),\r\n" + 
	"	\"SYNONYM_FOR\" VARCHAR2(61 BYTE),\r\n" + 
	"	\"DROP_TIME\" DATE,\r\n" + 
	"	\"DROP_ORIGINAL_NAME\" VARCHAR2(30 BYTE),\r\n" + 
	"	\"PARENT_FOLDER\" NUMBER(19,0)\r\n" + 
	");\r\n" + 
	"--  DDL for Table MDT_OBJECTS_COMMENTS\r\n" + 
	"CREATE TABLE \"MDT_OBJECTS_COMMENTS\" (\r\n" + 
	"	\"OBJECT_ID\" NUMBER(19,0),\r\n" + 
	"	\"OBJECT_COMMENT\" VARCHAR2(500 BYTE),\r\n" + 
	"	\"TITLE_EN\" VARCHAR2(500 BYTE),\r\n" + 
	"	\"TITLE_FR\" VARCHAR2(500 BYTE),\r\n" + 
	"	\"TITLE_DE\" VARCHAR2(500 BYTE),\r\n" + 
	"	\"LAST_EXPORTED\" DATE,\r\n" + 
	"	\"DOMAIN\" VARCHAR2(50 BYTE)\r\n" + 
	");\r\n" + 
	"--  DDL for Table MDT_POSITIONS\r\n" + 
	"CREATE TABLE \"MDT_POSITIONS\" (\r\n" + 
	"	\"OBJECT_ID\" NUMBER(19,0),\r\n" + 
	"	\"DIM_INDEX\" NUMBER(19,0),\r\n" + 
	"	\"POS_INDEX\" NUMBER(19,0),\r\n" + 
	"	\"POS_CODE\" VARCHAR2(50 BYTE)\r\n" + 
	");\r\n" + 
	"--  DDL for Table MDT_PRIVILEGES\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"\r\n" + 
	"CREATE TABLE \"MDT_PRIVILEGES\" (\r\n" + 
	"	\"OBJECT_ID\" NUMBER(19,0),\r\n" + 
	"	\"GRANTEE\" VARCHAR2(30 BYTE),\r\n" + 
	"	\"PRIVILEGE\" CHAR(1 BYTE)\r\n" + 
	");\r\n" + 
	"\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"--  DDL for Table MDT_PROFILES\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"\r\n" + 
	"CREATE TABLE \"MDT_PROFILES\" (\r\n" + 
	"	\"USER_NAME\" VARCHAR2(30 BYTE),\r\n" + 
	"	\"DEFAULT_SELECT_PRIV_ROLE\" VARCHAR2(30 BYTE),\r\n" + 
	"	\"DEFAULT_SELECT_PRIV_USER\" VARCHAR2(30 BYTE),\r\n" + 
	"	\"IN_REAL_LIFE\" VARCHAR2(100 BYTE),\r\n" + 
	"	\"DESCRIPTION\" VARCHAR2(100 BYTE),\r\n" + 
	"	\"AUDIT_LEVEL\" NUMBER(19,0),\r\n" + 
	"	\"SEARCH_USER\" VARCHAR2(30 BYTE),\r\n" + 
	"	\"DICTIONARY_TEMPLATE\" VARCHAR2(61 BYTE),\r\n" + 
	"	\"TABLE_TEMPLATE\" VARCHAR2(30 BYTE),\r\n" + 
	"	\"TABLES_HAVE_PRIMARY_KEY\" VARCHAR2(3 BYTE),\r\n" + 
	"	\"TABLES_HAVE_BITMAP_INDEXES\" VARCHAR2(3 BYTE),\r\n" + 
	"	\"TIME_PERIOD_NAME\" VARCHAR2(30 BYTE),\r\n" + 
	"	\"TIME_PERIOD_ANNUAL_FORMAT\" VARCHAR2(1 BYTE),\r\n" + 
	"	\"LIST_DISPLAY_PROPERTIES\" VARCHAR2(200 BYTE),\r\n" + 
	"	\"LIST_DISPLAY_GRID\" VARCHAR2(3 BYTE),\r\n" + 
	"	\"DIGIT_GROUPING_SYMBOL\" CHAR(1 BYTE),\r\n" + 
	"	\"DICTIONARY_MODIFY_EDIT\" VARCHAR2(3 BYTE),\r\n" + 
	"	\"BROWSE_SELECT_TIME_AUTO\" NUMBER(19,0),\r\n" + 
	"	\"DECIMAL_PLACES\" NUMBER(19,0),\r\n" + 
	"	\"DISPLAY_LABEL\" VARCHAR2(30 BYTE),\r\n" + 
	"	\"LABEL_NAME\" VARCHAR2(30 BYTE),\r\n" + 
	"	\"LABEL_NAME2\" VARCHAR2(30 BYTE),\r\n" + 
	"	\"DATE_FORMAT\" VARCHAR2(30 BYTE),\r\n" + 
	"	\"AUTO_SAVE_PRESENTATION\" VARCHAR2(3 BYTE),\r\n" + 
	"	\"CHART_AUTO_TIME\" VARCHAR2(30 BYTE),\r\n" + 
	"	\"CHART_AUTO_OTHER\" VARCHAR2(30 BYTE),\r\n" + 
	"	\"CHART_CATEGORY_AXIS\" VARCHAR2(30 BYTE),\r\n" + 
	"	\"CHART_LEGEND\" VARCHAR2(30 BYTE),\r\n" + 
	"	\"FONT_SIZE\" NUMBER(19,0)\r\n" + 
	");\r\n" + 
	"\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"--  DDL for Table MDT_RENAMED_OBJECTS\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"\r\n" + 
	"CREATE TABLE \"MDT_RENAMED_OBJECTS\" (\r\n" + 
	"	\"OBJECT_ID\" NUMBER(19,0),\r\n" + 
	"	\"TIMESTAMP\" DATE,\r\n" + 
	"	\"SES_ID\" NUMBER(19,0),\r\n" + 
	"	\"PREVIOUS_OBJECT_NAME\" VARCHAR2(30 BYTE)\r\n" + 
	");\r\n" + 
	"\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"--  DDL for Table MDT_SESSIONS\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"\r\n" + 
	"CREATE TABLE \"MDT_SESSIONS\" (\r\n" + 
	"	\"SESSION_ID\" NUMBER(19,0),\r\n" + 
	"	\"USER_NAME\" VARCHAR2(30 BYTE),\r\n" + 
	"	\"OS_USERID\" VARCHAR2(30 BYTE),\r\n" + 
	"	\"TIME_LOGON\" DATE,\r\n" + 
	"	\"TIME_LOGOFF\" DATE,\r\n" + 
	"	\"VERSION\" VARCHAR2(50 BYTE),\r\n" + 
	"	\"ECAS_USERNAME\" VARCHAR2(50 BYTE)\r\n" + 
	");\r\n" + 
	"\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"--  DDL for Table MDT_SOURCES\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"\r\n" + 
	"CREATE TABLE \"MDT_SOURCES\" (\r\n" + 
	"	\"OBJECT_ID\" NUMBER(19,0),\r\n" + 
	"	\"BUFFER_INDEX\" NUMBER(19,0),\r\n" + 
	"	\"BUFFER_TEXT\" VARCHAR2(4000 BYTE)\r\n" + 
	");\r\n" + 
	"\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"--  DDL for Table MDT_SYNTAX_TREES\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"\r\n" + 
	"CREATE TABLE \"MDT_SYNTAX_TREES\" (\r\n" + 
	"	\"OBJECT_ID\" NUMBER(19,0),\r\n" + 
	"	\"NODE_INDEX\" NUMBER(19,0),\r\n" + 
	"	\"NODE_NUM_CHILDREN\" NUMBER(19,0),\r\n" + 
	"	\"NODE_NAME\" NUMBER(19,0),\r\n" + 
	"	\"NODE_INFO\" NUMBER(19,0),\r\n" + 
	"	\"NODE_VALUE_1\" VARCHAR2(4000 BYTE),\r\n" + 
	"	\"NODE_VALUE_2\" VARCHAR2(4000 BYTE)\r\n" + 
	");\r\n" + 
	"\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"--  DDL for Table MDT_VALIDATION_CONDITIONS\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"\r\n" + 
	"CREATE TABLE \"MDT_VALIDATION_CONDITIONS\" (\r\n" + 
	"	\"OBJECT_ID\" NUMBER(19,0),\r\n" + 
	"	\"LINE_NUMBER\" NUMBER(19,0),\r\n" + 
	"	\"PRECONDITION\" VARCHAR2(4000 BYTE),\r\n" + 
	"	\"CONDITION\" VARCHAR2(4000 BYTE),\r\n" + 
	"	\"ERROR_MESSAGE\" VARCHAR2(4000 BYTE),\r\n" + 
	"	\"SEVERITY_CODE\" VARCHAR2(50 BYTE),\r\n" + 
	"	\"SQL_PRECONDITION\" VARCHAR2(4000 BYTE),\r\n" + 
	"	\"SQL_CONDITION\" VARCHAR2(4000 BYTE)\r\n" + 
	");\r\n" + 
	"\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"--  DDL for Table MDT_VALIDATION_RULES\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"\r\n" + 
	"CREATE TABLE \"MDT_VALIDATION_RULES\" (\r\n" + 
	"	\"OBJECT_ID\" NUMBER(19,0),\r\n" + 
	"	\"DATA_OBJECT_NAME\" VARCHAR2(1000 BYTE),\r\n" + 			// changed 20.11.2017
	"	\"SEVERITY_DICTIONARY\" VARCHAR2(50 BYTE)\r\n" + 
	");\r\n" + 
	"--  Indexes\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"--  DDL for Index MDT_AUDITS_INDEX\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"\r\n" + 
	"CREATE INDEX \"MDT_AUDITS_INDEX\" ON \"MDT_AUDITS\" (\"OBJECT_ID\");\r\n" + 
	"\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"--  DDL for Index MDT_DATA_OBJECTS_PRIMARY_KEY\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"\r\n" + 
	"CREATE UNIQUE INDEX \"MDT_DATA_OBJECTS_PRIMARY_KEY\" ON \"MDT_DATA_OBJECTS\" (\"OBJECT_ID\");\r\n" + 
	"\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"--  DDL for Index MDT_DEPENDENCIES_PRIMARY_KEY\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"\r\n" + 
	"CREATE UNIQUE INDEX \"MDT_DEPENDENCIES_PRIMARY_KEY\" ON \"MDT_DEPENDENCIES\" (\"OBJECT_ID\", \"REF_OBJECT_OWNER\", \"REF_OBJECT_NAME\");\r\n" + 
	"\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"--  DDL for Indexes on MDT_DIMENSIONS\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"\r\n" + 
	"CREATE UNIQUE INDEX \"MDT_DIMENSIONS_PRIMARY_KEY\" ON \"MDT_DIMENSIONS\" (\"OBJECT_ID\", \"DIM_INDEX\");\r\n" + 
	"CREATE UNIQUE INDEX \"MDT_DIMENSIONS_UNIQUE_NAME\" ON \"MDT_DIMENSIONS\" (\"OBJECT_ID\", \"DIM_TYPE\", \"DIM_NAME\");\r\n" + 
	"CREATE INDEX \"I0_MDT_DIMENSIONS\" ON \"MDT_DIMENSIONS\" (\"OBJECT_ID\", \"DIM_NAME\", \"DIM_TYPE\", \"COLUMN_TYPE\");\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"--  DDL for Index MDT_EQUATIONS_PRIMARY_KEY\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"\r\n" + 
	"CREATE UNIQUE INDEX \"MDT_EQUATIONS_PRIMARY_KEY\" ON \"MDT_EQUATIONS\" (\"OBJECT_ID\", \"EQUATION_NUMBER\");\r\n" + 
	"\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"--  DDL for Index MDT_EQUAT_ITEMS_PRIMARY_KEY\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"\r\n" + 
	"CREATE UNIQUE INDEX \"MDT_EQUAT_ITEMS_PRIMARY_KEY\" ON \"MDT_EQUATIONS_ITEMS\" (\"OBJECT_ID\", \"EQUATION_NUMBER\", \"ITEM_NUMBER\");\r\n" + 
	"\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"--  DDL for Index MDT_EQUAT_TREE_PRIMARY_KEY\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"\r\n" + 
	"CREATE UNIQUE INDEX \"MDT_EQUAT_TREE_PRIMARY_KEY\" ON \"MDT_EQUATIONS_TREE\" (\"OBJECT_ID\", \"POS_INDEX\");\r\n" + 
	"\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"--  DDL for Index MDT_HISTORY_PKEY\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"\r\n" + 
	"CREATE UNIQUE INDEX \"MDT_HISTORY_PKEY\" ON \"MDT_HISTORY\" (\"HISTORY_ID\");\r\n" + 
	"\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"--  DDL for Index MDT_MODIFICATIONS_INDEX\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"\r\n" + 
	"CREATE INDEX \"MDT_MODIFICATIONS_INDEX\" ON \"MDT_MODIFICATIONS\" (\"OBJECT_ID\");\r\n" + 
	"\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"--  DDL for Index MDT_NOTES_PRIMARY_KEY\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"\r\n" + 
	"CREATE UNIQUE INDEX \"MDT_NOTES_PRIMARY_KEY\" ON \"MDT_NOTES\" (\"OBJECT_ID\", \"DIM_INDEX\", \"NOTE_INDEX\", \"FIELD_INDEX\");\r\n" + 
	"\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"--  DDL for Indexes on MDT_OBJECTS\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"\r\n" + 
	"CREATE UNIQUE INDEX \"MDT_OBJECTS_PRIMARY_KEY\" ON \"MDT_OBJECTS\" (\"OBJECT_ID\");\r\n" + 
	"CREATE UNIQUE INDEX \"MDT_OBJECTS_UNIQUE_NAME\" ON \"MDT_OBJECTS\" (\"USER_NAME\", \"OBJECT_NAME\");\r\n" + 
	"CREATE INDEX \"I0_MDT_OBJECTS\" ON \"MDT_OBJECTS\" (\"OBJECT_ID\", \"USER_NAME\", \"OBJECT_NAME\", \"OBJECT_TYPE\");\r\n" + 
	"\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"--  DDL for Index MDT_OBJ_COMMENTS_PRIMARY_KEY\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"\r\n" + 
	"CREATE UNIQUE INDEX \"MDT_OBJ_COMMENTS_PRIMARY_KEY\" ON \"MDT_OBJECTS_COMMENTS\" (\"OBJECT_ID\");\r\n" + 
	"\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"--  DDL for Index MDT_POSITIONS_PRIMARY_KEY\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"\r\n" + 
	"CREATE UNIQUE INDEX \"MDT_POSITIONS_PRIMARY_KEY\" ON \"MDT_POSITIONS\" (\"OBJECT_ID\", \"DIM_INDEX\", \"POS_CODE\");\r\n" + 
	"\r\n" + 
	"--  DDL for Index MDT_PROFILES_PRIMARY_KEY\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"\r\n" + 
	"CREATE UNIQUE INDEX \"MDT_PROFILES_PRIMARY_KEY\" ON \"MDT_PROFILES\" (\"USER_NAME\");\r\n" + 
	"\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"--  DDL for Index MDT_RENAMED_OBJECTS_INDEX\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"\r\n" + 
	"CREATE INDEX \"MDT_RENAMED_OBJECTS_INDEX\" ON \"MDT_RENAMED_OBJECTS\" (\"OBJECT_ID\"); \r\n" + 
	"\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"--  DDL for Index MDT_SOURCES_PRIMARY_KEY\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"\r\n" + 
	"CREATE UNIQUE INDEX \"MDT_SOURCES_PRIMARY_KEY\" ON \"MDT_SOURCES\" (\"OBJECT_ID\", \"BUFFER_INDEX\");\r\n" + 
	"\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"--  DDL for Index MDT_SYNTAX_TREES_PRIMARY_KEY\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"\r\n" + 
	"CREATE UNIQUE INDEX \"MDT_SYNTAX_TREES_PRIMARY_KEY\" ON \"MDT_SYNTAX_TREES\" (\"OBJECT_ID\", \"NODE_INDEX\"); \r\n" + 
	"\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"--  DDL for Index MDT_USER_SESSIONS_PRIMARY_KEY\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"\r\n" + 
	"CREATE UNIQUE INDEX \"MDT_USER_SESSIONS_PRIMARY_KEY\" ON \"MDT_SESSIONS\" (\"SESSION_ID\");\r\n" + 
	"\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"--  DDL for Index MDT_VALIDATION_CONDS_PRIM_KEY\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"\r\n" + 
	"CREATE UNIQUE INDEX \"MDT_VALIDATION_CONDS_PRIM_KEY\" ON \"MDT_VALIDATION_CONDITIONS\" (\"OBJECT_ID\", \"LINE_NUMBER\");\r\n" + 
	"\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"--  DDL for Index MDT_VALIDATION_RULES_PRIM_KEY\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"\r\n" + 
	"CREATE UNIQUE INDEX \"MDT_VALIDATION_RULES_PRIM_KEY\" ON \"MDT_VALIDATION_RULES\" (\"OBJECT_ID\", \"DATA_OBJECT_NAME\");\r\n" + 
	"\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"--  Constraints\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"  --------------------------------------------------------\r\n" + 
	"--  Constraints for Table MDT_AUDITS\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"\r\n" + 
	"ALTER TABLE \"MDT_AUDITS\" MODIFY (\"ELAPSED_TIME\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_AUDITS\" MODIFY (\"CELLS_DELETED\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_AUDITS\" MODIFY (\"CELLS_INSERTED\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_AUDITS\" MODIFY (\"N_OPS\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_AUDITS\" MODIFY (\"CMD_TEXT\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_AUDITS\" MODIFY (\"UPD_ID\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_AUDITS\" MODIFY (\"CMD_ID\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_AUDITS\" MODIFY (\"SES_ID\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_AUDITS\" MODIFY (\"TIMESTAMP\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_AUDITS\" MODIFY (\"OBJECT_ID\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_AUDITS\" MODIFY (\"USER_NAME\" NOT NULL ENABLE);\r\n" + 
	"\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"--  Constraints for Table MDT_DATA_OBJECTS\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"\r\n" + 
	"ALTER TABLE \"MDT_DATA_OBJECTS\" ADD CONSTRAINT \"MDT_DATA_OBJECTS_PRIMARY_KEY\" PRIMARY KEY (\"OBJECT_ID\") USING INDEX ENABLE;\r\n" + 
	"ALTER TABLE \"MDT_DATA_OBJECTS\" ADD CONSTRAINT \"MDT_DATA_OBJECTS_DV_KEEP\" CHECK ( dv_keep IN ( 'V', 'M' ) ) ENABLE;\r\n" + 
	"ALTER TABLE \"MDT_DATA_OBJECTS\" ADD CONSTRAINT \"MDT_DATA_OBJECTS_DV_MODE\" CHECK ( dv_mode IN ( 'D', 'A', 'O' ) ) ENABLE;\r\n" + 
	"ALTER TABLE \"MDT_DATA_OBJECTS\" MODIFY (\"OBJECT_ID\" NOT NULL ENABLE);\r\n" + 
	"\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"--  Constraints for Table MDT_DEPENDENCIES\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"\r\n" + 
	"ALTER TABLE \"MDT_DEPENDENCIES\" MODIFY (\"REF_TYPE\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_DEPENDENCIES\" MODIFY (\"REF_OBJECT_NAME\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_DEPENDENCIES\" MODIFY (\"REF_OBJECT_OWNER\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_DEPENDENCIES\" MODIFY (\"OBJECT_ID\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_DEPENDENCIES\" ADD CONSTRAINT \"MDT_DEPENDENCIES_PRIMARY_KEY\" PRIMARY KEY (\"OBJECT_ID\", \"REF_OBJECT_OWNER\", \"REF_OBJECT_NAME\") USING INDEX ENABLE;\r\n" + 
	"\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"--  Constraints for Table MDT_DIMENSIONS\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"\r\n" + 
	"ALTER TABLE \"MDT_DIMENSIONS\" MODIFY (\"DIM_TYPE\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_DIMENSIONS\" MODIFY (\"DIM_INDEX\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_DIMENSIONS\" MODIFY (\"DIM_NAME\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_DIMENSIONS\" MODIFY (\"OBJECT_ID\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_DIMENSIONS\" ADD CONSTRAINT \"MDT_DIMENSIONS_UNIQUE_NAME\" UNIQUE (\"OBJECT_ID\", \"DIM_TYPE\", \"DIM_NAME\") USING INDEX ENABLE;\r\n" + 
	"ALTER TABLE \"MDT_DIMENSIONS\" ADD CONSTRAINT \"MDT_DIMENSIONS_PRIMARY_KEY\" PRIMARY KEY (\"OBJECT_ID\", \"DIM_INDEX\") USING INDEX ENABLE;\r\n" + 
	"ALTER TABLE \"MDT_DIMENSIONS\" ADD CONSTRAINT \"MDT_DIMENSIONS_LD_CL_STATIC\" CHECK ( ld_cl_static IN ( 'S', 'A', 'D' )) ENABLE;\r\n" + 
	"ALTER TABLE \"MDT_DIMENSIONS\" ADD CONSTRAINT \"MDT_DIMENSIONS_LD_CL_SORT\" CHECK ( ld_cl_sort IN ( 'D', 'A', 'C', 'L' )) ENABLE;\r\n" + 
	"ALTER TABLE \"MDT_DIMENSIONS\" ADD CONSTRAINT \"MDT_DIMENSIONS_DIM_TYPE\" CHECK (dim_type IN ( 'D', 'C', 'X', 'N', 'R', 'G' )) ENABLE;\r\n" + 
	"--  Constraints for Table MDT_DROPPED_OBJECTS\r\n" + 
	"ALTER TABLE \"MDT_DROPPED_OBJECTS\" MODIFY (\"TIMESTAMP\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_DROPPED_OBJECTS\" MODIFY (\"OBJECT_TYPE\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_DROPPED_OBJECTS\" MODIFY (\"OBJECT_NAME\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_DROPPED_OBJECTS\" MODIFY (\"USER_NAME\" NOT NULL ENABLE);\r\n" + 
	"\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"--  Constraints for Table MDT_EQUATIONS\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"\r\n" + 
	"ALTER TABLE \"MDT_EQUATIONS\" MODIFY (\"EQUATION_NUMBER\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_EQUATIONS\" MODIFY (\"NUM_ITEMS\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_EQUATIONS\" MODIFY (\"COMPUTATION_LEVEL\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_EQUATIONS\" MODIFY (\"EQUATION_TYPE\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_EQUATIONS\" MODIFY (\"LEFT_PART\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_EQUATIONS\" MODIFY (\"OBJECT_ID\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_EQUATIONS\" ADD CONSTRAINT \"MDT_EQUATIONS_PRIMARY_KEY\" PRIMARY KEY (\"OBJECT_ID\", \"EQUATION_NUMBER\") USING INDEX ENABLE;\r\n" + 
	"\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"--  Constraints for Table MDT_EQUATIONS_ITEMS\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"\r\n" + 
	"ALTER TABLE \"MDT_EQUATIONS_ITEMS\" MODIFY (\"EQUATION_NUMBER\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_EQUATIONS_ITEMS\" MODIFY (\"ITEM_NUMBER\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_EQUATIONS_ITEMS\" MODIFY (\"FACTOR\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_EQUATIONS_ITEMS\" MODIFY (\"POS_CODE\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_EQUATIONS_ITEMS\" MODIFY (\"OBJECT_ID\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_EQUATIONS_ITEMS\" ADD CONSTRAINT \"MDT_EQUAT_ITEMS_PRIMARY_KEY\" PRIMARY KEY (\"OBJECT_ID\", \"EQUATION_NUMBER\", \"ITEM_NUMBER\") USING INDEX ENABLE;\r\n" + 
	"\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"--  Constraints for Table MDT_EQUATIONS_TREE\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"\r\n" + 
	"ALTER TABLE \"MDT_EQUATIONS_TREE\" MODIFY (\"EQUATION_NUMBER\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_EQUATIONS_TREE\" MODIFY (\"POS_CLASS\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_EQUATIONS_TREE\" MODIFY (\"POS_LEVEL\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_EQUATIONS_TREE\" MODIFY (\"POS_CODE\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_EQUATIONS_TREE\" MODIFY (\"POS_INDEX\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_EQUATIONS_TREE\" MODIFY (\"OBJECT_ID\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_EQUATIONS_TREE\" ADD CONSTRAINT \"MDT_EQUAT_TREE_PRIMARY_KEY\" PRIMARY KEY (\"OBJECT_ID\", \"POS_INDEX\") USING INDEX ENABLE;\r\n" + 
	"\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"--  Constraints for Table MDT_HISTORY\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"\r\n" + 
	"ALTER TABLE \"MDT_HISTORY\" ADD CONSTRAINT \"MDT_HISTORY_PKEY\" PRIMARY KEY (\"HISTORY_ID\") USING INDEX ENABLE;\r\n" + 
	"ALTER TABLE \"MDT_HISTORY\" MODIFY (\"LOG_TYPE\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_HISTORY\" MODIFY (\"SESSION_ID\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_HISTORY\" MODIFY (\"HISTORY_ID\" NOT NULL ENABLE);\r\n" + 
	"\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"--  Constraints for Table MDT_MODIFICATIONS\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"\r\n" + 
	"ALTER TABLE \"MDT_MODIFICATIONS\" MODIFY (\"OP_TYPE\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_MODIFICATIONS\" MODIFY (\"DIM_NAME\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_MODIFICATIONS\" MODIFY (\"CMD_ID\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_MODIFICATIONS\" MODIFY (\"SES_ID\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_MODIFICATIONS\" MODIFY (\"TIMESTAMP\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_MODIFICATIONS\" MODIFY (\"OBJECT_ID\" NOT NULL ENABLE);\r\n" + 
	"\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"--  Constraints for Table MDT_NOTES\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"\r\n" + 
	"ALTER TABLE \"MDT_NOTES\" MODIFY (\"POS_CODE\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_NOTES\" MODIFY (\"DIM_INDEX\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_NOTES\" MODIFY (\"FIELD_INDEX\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_NOTES\" MODIFY (\"NOTE_INDEX\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_NOTES\" MODIFY (\"OBJECT_ID\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_NOTES\" ADD CONSTRAINT \"MDT_NOTES_PRIMARY_KEY\" PRIMARY KEY (\"OBJECT_ID\", \"DIM_INDEX\", \"NOTE_INDEX\", \"FIELD_INDEX\") USING INDEX ENABLE;\r\n" + 
	"\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"--  Constraints for Table MDT_OBJECTS\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"\r\n" + 
	"ALTER TABLE \"MDT_OBJECTS\" MODIFY (\"CREATED\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_OBJECTS\" MODIFY (\"OBJECT_TYPE\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_OBJECTS\" MODIFY (\"OBJECT_NAME\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_OBJECTS\" MODIFY (\"OBJECT_ID\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_OBJECTS\" MODIFY (\"USER_NAME\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_OBJECTS\" ADD CONSTRAINT \"MDT_OBJECTS_UNIQUE_NAME\" UNIQUE (\"USER_NAME\", \"OBJECT_NAME\") USING INDEX ENABLE;\r\n" + 
	"ALTER TABLE \"MDT_OBJECTS\" ADD CONSTRAINT \"MDT_OBJECTS_TYPE\" CHECK (object_type IN ('A', 'D', 'F', 'M', 'N', 'R', 'S', 'T', 'V')) ENABLE;\r\n" + 
	"ALTER TABLE \"MDT_OBJECTS\" ADD CONSTRAINT \"MDT_OBJECTS_PRIMARY_KEY\" PRIMARY KEY (\"OBJECT_ID\") USING INDEX ENABLE;\r\n" + 
	"\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"--  Constraints for Table MDT_OBJECTS_COMMENTS\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"\r\n" + 
	"ALTER TABLE \"MDT_OBJECTS_COMMENTS\" MODIFY (\"OBJECT_COMMENT\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_OBJECTS_COMMENTS\" MODIFY (\"OBJECT_ID\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_OBJECTS_COMMENTS\" ADD CONSTRAINT \"MDT_OBJ_COMMENTS_PRIMARY_KEY\" PRIMARY KEY (\"OBJECT_ID\") USING INDEX ENABLE;\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"--  Constraints for Table MDT_POSITIONS\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"\r\n" + 
	"ALTER TABLE \"MDT_POSITIONS\" MODIFY (\"POS_CODE\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_POSITIONS\" MODIFY (\"POS_INDEX\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_POSITIONS\" MODIFY (\"DIM_INDEX\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_POSITIONS\" MODIFY (\"OBJECT_ID\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_POSITIONS\" ADD CONSTRAINT \"MDT_POSITIONS_PRIMARY_KEY\" PRIMARY KEY (\"OBJECT_ID\", \"DIM_INDEX\", \"POS_CODE\") USING INDEX ENABLE;\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"--  Constraints for Table MDT_PRIVILEGES\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"\r\n" + 
	"ALTER TABLE \"MDT_PRIVILEGES\" MODIFY (\"PRIVILEGE\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_PRIVILEGES\" MODIFY (\"GRANTEE\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_PRIVILEGES\" MODIFY (\"OBJECT_ID\" NOT NULL ENABLE);\r\n" + 
	"\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"--  Constraints for Table MDT_PROFILES\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"\r\n" + 
	"ALTER TABLE \"MDT_PROFILES\" MODIFY (\"USER_NAME\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_PROFILES\" ADD CONSTRAINT \"MDT_PROFILES_PRIMARY_KEY\" PRIMARY KEY (\"USER_NAME\") USING INDEX ENABLE;\r\n" + 
	"\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"--  Constraints for Table MDT_RENAMED_OBJECTS\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"\r\n" + 
	"ALTER TABLE \"MDT_RENAMED_OBJECTS\" MODIFY (\"PREVIOUS_OBJECT_NAME\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_RENAMED_OBJECTS\" MODIFY (\"SES_ID\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_RENAMED_OBJECTS\" MODIFY (\"TIMESTAMP\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_RENAMED_OBJECTS\" MODIFY (\"OBJECT_ID\" NOT NULL ENABLE);\r\n" + 
	"\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"--  Constraints for Table MDT_SESSIONS\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"\r\n" + 
	"ALTER TABLE \"MDT_SESSIONS\" MODIFY (\"TIME_LOGON\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_SESSIONS\" MODIFY (\"OS_USERID\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_SESSIONS\" MODIFY (\"USER_NAME\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_SESSIONS\" MODIFY (\"SESSION_ID\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_SESSIONS\" MODIFY (\"ECAS_USERNAME\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_SESSIONS\" ADD CONSTRAINT \"MDT_USER_SESSIONS_PRIMARY_KEY\" PRIMARY KEY (\"SESSION_ID\") USING INDEX ENABLE;\r\n" + 
	"\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"--  Constraints for Table MDT_SOURCES\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"\r\n" + 
	"ALTER TABLE \"MDT_SOURCES\" MODIFY (\"BUFFER_TEXT\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_SOURCES\" MODIFY (\"BUFFER_INDEX\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_SOURCES\" MODIFY (\"OBJECT_ID\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_SOURCES\" ADD CONSTRAINT \"MDT_SOURCES_PRIMARY_KEY\" PRIMARY KEY (\"OBJECT_ID\", \"BUFFER_INDEX\") USING INDEX ENABLE;\r\n" + 
	"\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"--  Constraints for Table MDT_SYNTAX_TREES\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"\r\n" + 
	"ALTER TABLE \"MDT_SYNTAX_TREES\" MODIFY (\"NODE_INFO\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_SYNTAX_TREES\" MODIFY (\"NODE_NAME\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_SYNTAX_TREES\" MODIFY (\"NODE_NUM_CHILDREN\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_SYNTAX_TREES\" MODIFY (\"NODE_INDEX\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_SYNTAX_TREES\" MODIFY (\"OBJECT_ID\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_SYNTAX_TREES\" ADD CONSTRAINT \"MDT_SYNTAX_TREES_PRIMARY_KEY\" PRIMARY KEY (\"OBJECT_ID\", \"NODE_INDEX\") USING INDEX ENABLE;\r\n" + 
	"\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"--  Constraints for Table MDT_VALIDATION_CONDITIONS\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"\r\n" + 
	"ALTER TABLE \"MDT_VALIDATION_CONDITIONS\" MODIFY (\"PRECONDITION\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_VALIDATION_CONDITIONS\" MODIFY (\"LINE_NUMBER\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_VALIDATION_CONDITIONS\" MODIFY (\"OBJECT_ID\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_VALIDATION_CONDITIONS\" ADD CONSTRAINT \"MDT_VALIDATION_CONDS_PRIM_KEY\" PRIMARY KEY (\"OBJECT_ID\", \"LINE_NUMBER\") USING INDEX ENABLE;\r\n" + 
	"\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"--  Constraints for Table MDT_VALIDATION_RULES\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"\r\n" + 
	"ALTER TABLE \"MDT_VALIDATION_RULES\" MODIFY (\"DATA_OBJECT_NAME\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_VALIDATION_RULES\" MODIFY (\"OBJECT_ID\" NOT NULL ENABLE);\r\n" + 
	"ALTER TABLE \"MDT_VALIDATION_RULES\" ADD CONSTRAINT \"MDT_VALIDATION_RULES_PRIM_KEY\" PRIMARY KEY (\"OBJECT_ID\", \"DATA_OBJECT_NAME\") USING INDEX ENABLE;\r\n" + 
	"\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"--  Ref Constraints for Table MDT_AUDITS\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"\r\n" + 
	"ALTER TABLE \"MDT_AUDITS\" ADD CONSTRAINT \"MDT_AUDIT_FOREIGN_KEY\" FOREIGN KEY (\"OBJECT_ID\")\r\n" + 
	"	REFERENCES \"MDT_OBJECTS\" (\"OBJECT_ID\") ENABLE;\r\n" + 
	"\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"--  Ref Constraints for Table MDT_DATA_OBJECTS\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"\r\n" + 
	"ALTER TABLE \"MDT_DATA_OBJECTS\" ADD CONSTRAINT \"MDT_DATA_OBJECTS_FOREIGN_KEY\" FOREIGN KEY (\"OBJECT_ID\")\r\n" + 
	"	REFERENCES \"MDT_OBJECTS\" (\"OBJECT_ID\") ENABLE;\r\n" + 
	"\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"--  Ref Constraints for Table MDT_DEPENDENCIES\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"\r\n" + 
	"ALTER TABLE \"MDT_DEPENDENCIES\" ADD CONSTRAINT \"MDT_DEPENDENCIES_FOREIGN_KEY\" FOREIGN KEY (\"OBJECT_ID\")\r\n" + 
	"	REFERENCES \"MDT_OBJECTS\" (\"OBJECT_ID\") ON DELETE CASCADE ENABLE;\r\n" + 
	"\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"--  Ref Constraints for Table MDT_DIMENSIONS\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"\r\n" + 
	"ALTER TABLE \"MDT_DIMENSIONS\" ADD CONSTRAINT \"MDT_DIMENSIONS_FOREIGN_KEY\" FOREIGN KEY (\"OBJECT_ID\")\r\n" + 
	"	REFERENCES \"MDT_OBJECTS\" (\"OBJECT_ID\") ENABLE;\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"--  Ref Constraints for Table MDT_EQUATIONS\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"\r\n" + 
	"ALTER TABLE \"MDT_EQUATIONS\" ADD CONSTRAINT \"MDT_EQUATIONS_FOREIGN_KEY\" FOREIGN KEY (\"OBJECT_ID\")\r\n" + 
	"	REFERENCES \"MDT_OBJECTS\" (\"OBJECT_ID\") ENABLE;\r\n" + 
	"\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"--  Ref Constraints for Table MDT_EQUATIONS_ITEMS\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"\r\n" + 
	"ALTER TABLE \"MDT_EQUATIONS_ITEMS\" ADD CONSTRAINT \"MDT_EQUAT_ITEMS_FOREIGN_KEY\" FOREIGN KEY (\"OBJECT_ID\", \"EQUATION_NUMBER\")\r\n" + 
	"	REFERENCES \"MDT_EQUATIONS\" (\"OBJECT_ID\", \"EQUATION_NUMBER\") ENABLE;\r\n" + 
	"\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"--  Ref Constraints for Table MDT_EQUATIONS_TREE\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"\r\n" + 
	"ALTER TABLE \"MDT_EQUATIONS_TREE\" ADD CONSTRAINT \"MDT_EQUAT_TREE_FOREIGN_KEY\" FOREIGN KEY (\"OBJECT_ID\", \"EQUATION_NUMBER\")\r\n" + 
	"	REFERENCES \"MDT_EQUATIONS\" (\"OBJECT_ID\", \"EQUATION_NUMBER\") ENABLE;\r\n" + 
	"\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"--  Ref Constraints for Table MDT_HISTORY\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"\r\n" + 
	"ALTER TABLE \"MDT_HISTORY\" ADD CONSTRAINT \"MDT_HISTORY_FKEY\" FOREIGN KEY (\"SESSION_ID\")\r\n" + 
	"	REFERENCES \"MDT_SESSIONS\" (\"SESSION_ID\") ENABLE;\r\n" + 
	"\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"--  Ref Constraints for Table MDT_MODIFICATIONS\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"\r\n" + 
	"ALTER TABLE \"MDT_MODIFICATIONS\" ADD CONSTRAINT \"MDT_MODIFICATIONS_FOREIGN_KEY\" FOREIGN KEY (\"OBJECT_ID\")\r\n" + 
	"	REFERENCES \"MDT_OBJECTS\" (\"OBJECT_ID\") ON DELETE CASCADE ENABLE;\r\n" + 
	"\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"--  Ref Constraints for Table MDT_NOTES\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"\r\n" + 
	"ALTER TABLE \"MDT_NOTES\" ADD CONSTRAINT \"MDT_NOTES_FOREIGN_KEY\" FOREIGN KEY (\"OBJECT_ID\", \"DIM_INDEX\")\r\n" + 
	"	REFERENCES \"MDT_DIMENSIONS\" (\"OBJECT_ID\", \"DIM_INDEX\") ON DELETE CASCADE ENABLE;\r\n" + 
	"\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"--  Ref Constraints for Table MDT_OBJECTS\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"\r\n" + 
	"ALTER TABLE \"MDT_OBJECTS\" ADD CONSTRAINT \"MDT_OBJECTS_PARENT_FK\" FOREIGN KEY (\"PARENT_FOLDER\")\r\n" + 
	"	REFERENCES \"MDT_OBJECTS\" (\"OBJECT_ID\") ENABLE;\r\n" + 
	"\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"--  Ref Constraints for Table MDT_OBJECTS_COMMENTS\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"\r\n" + 
	"ALTER TABLE \"MDT_OBJECTS_COMMENTS\" ADD CONSTRAINT \"MDT_OBJ_COMMENTS_FOREIGN_KEY\" FOREIGN KEY (\"OBJECT_ID\")\r\n" + 
	"	REFERENCES \"MDT_OBJECTS\" (\"OBJECT_ID\") ENABLE;\r\n" + 
	"\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"--  Ref Constraints for Table MDT_POSITIONS\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"\r\n" + 
	"ALTER TABLE \"MDT_POSITIONS\" ADD CONSTRAINT \"MDT_POSITIONS_FOREIGN_KEY\" FOREIGN KEY (\"OBJECT_ID\", \"DIM_INDEX\")\r\n" + 
	"	REFERENCES \"MDT_DIMENSIONS\" (\"OBJECT_ID\", \"DIM_INDEX\") ENABLE;\r\n" + 
	"\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"--  Ref Constraints for Table MDT_PRIVILEGES\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"\r\n" + 
	"ALTER TABLE \"MDT_PRIVILEGES\" ADD CONSTRAINT \"MDT_PRIVILEGES_FK\" FOREIGN KEY (\"OBJECT_ID\")\r\n" + 
	"	REFERENCES \"MDT_OBJECTS\" (\"OBJECT_ID\") ON DELETE CASCADE ENABLE;\r\n" + 
	"\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"--  Ref Constraints for Table MDT_RENAMED_OBJECTS\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"\r\n" + 
	"ALTER TABLE \"MDT_RENAMED_OBJECTS\" ADD CONSTRAINT \"MDT_RENAMED_OBJECTS_FK\" FOREIGN KEY (\"OBJECT_ID\")\r\n" + 
	"	REFERENCES \"MDT_OBJECTS\" (\"OBJECT_ID\") ON DELETE CASCADE ENABLE;\r\n" + 
	"\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"--  Ref Constraints for Table MDT_SOURCES\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"\r\n" + 
	"ALTER TABLE \"MDT_SOURCES\" ADD CONSTRAINT \"MDT_SOURCES_FOREIGN_KEY\" FOREIGN KEY (\"OBJECT_ID\")\r\n" + 
	"	REFERENCES \"MDT_OBJECTS\" (\"OBJECT_ID\") ENABLE;\r\n" + 
	"\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"--  Ref Constraints for Table MDT_SYNTAX_TREES\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"\r\n" + 
	"ALTER TABLE \"MDT_SYNTAX_TREES\" ADD CONSTRAINT \"MDT_SYNTAX_TREES_FOREIGN_KEY\" FOREIGN KEY (\"OBJECT_ID\")\r\n" + 
	"	REFERENCES \"MDT_OBJECTS\" (\"OBJECT_ID\") ON DELETE CASCADE ENABLE;\r\n" + 
	"\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"--  Ref Constraints for Table MDT_VALIDATION_CONDITIONS\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"\r\n" + 
	"ALTER TABLE \"MDT_VALIDATION_CONDITIONS\" ADD CONSTRAINT \"MDT_VALIDATION_CONDS_FORE_KEY\" FOREIGN KEY (\"OBJECT_ID\")\r\n" + 
	"	REFERENCES \"MDT_OBJECTS\" (\"OBJECT_ID\") ENABLE;\r\n" + 
	"	\r\n" + 
	"\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"--  Ref Constraints for Table MDT_VALIDATION_RULES\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"\r\n" + 
	"ALTER TABLE \"MDT_VALIDATION_RULES\" ADD CONSTRAINT \"MDT_VALIDATION_RULES_FORE_KEY\" FOREIGN KEY (\"OBJECT_ID\")\r\n" + 
	"	REFERENCES \"MDT_OBJECTS\" (\"OBJECT_ID\") ENABLE;\r\n" + 
	"\r\n" + 
	"--  Views\r\n" + 
	"--  DDL for View MDT_USER_AUDITS\r\n" + 
	"CREATE OR REPLACE VIEW \"MDT_USER_AUDITS\" (\"USER_NAME\", \"OBJECT_ID\", \"TIMESTAMP\", \"SES_ID\", \"CMD_ID\", \"UPD_ID\", \"CMD_TEXT\", \"N_OPS\", \"CELLS_INSERTED\", \"CELLS_DELETED\", \"ELAPSED_TIME\", \"PERIOD_MIN\", \"PERIOD_MAX\", \"AUDIT_COMMENT\", \"AUDIT_BLOB\", \"AUDIT_CLOB\") AS\r\n" + 
	"SELECT user_name, object_id, timestamp, ses_id, cmd_id, upd_id,	cmd_text, n_ops, cells_inserted, cells_deleted,	elapsed_time, period_min, period_max, audit_comment, audit_blob, audit_clob\r\n" + 
	"FROM mdt_audits\r\n" + 
	"WHERE object_id in ( select object_id from mdt_objects where user_name = USER ) WITH CHECK OPTION  CONSTRAINT \"CHECK_MDT_USER_AUDITS\";\r\n" + 
	"--  DDL for View MDT_USER_AUDITS_INS\r\n" + 
	"CREATE OR REPLACE VIEW \"MDT_USER_AUDITS_INS\" (\"USER_NAME\", \"OBJECT_ID\", \"TIMESTAMP\", \"SES_ID\", \"CMD_ID\", \"UPD_ID\", \"CMD_TEXT\", \"N_OPS\", \"CELLS_INSERTED\", \"CELLS_DELETED\", \"ELAPSED_TIME\", \"PERIOD_MIN\", \"PERIOD_MAX\", \"AUDIT_COMMENT\", \"AUDIT_BLOB\", \"AUDIT_CLOB\") AS\r\n" + 
	"SELECT user_name, object_id, timestamp, ses_id, cmd_id, upd_id,	cmd_text, n_ops, cells_inserted, cells_deleted,	elapsed_time, period_min, period_max, audit_comment, audit_blob, audit_clob\r\n" + 
	"FROM mdt_audits\r\n" + 
	"WHERE object_id IN ( select object_id from mdt_objects where user_name = USER ) OR object_id IN ( select object_id from mdt_privileges where  privilege = 'U'  AND ( grantee = USER OR grantee in ( SELECT role FROM session_roles )))\r\n" + 
	"WITH CHECK OPTION CONSTRAINT \"CHECK_MDT_USER_AUDITS_INS\";\r\n" + 
	"--  DDL for View MDT_USER_DATA_OBJECTS\r\n" + 
	"CREATE OR REPLACE VIEW \"MDT_USER_DATA_OBJECTS\" (\"OBJECT_ID\", \"DV_MODE\", \"DV_KEEP\", \"DV_KEEP_NUMBER\") AS\r\n" + 
	"SELECT object_id, dv_mode, dv_keep, dv_keep_number\r\n" + 
	"FROM mdt_data_objects\r\n" + 
	"WHERE object_id in ( select object_id from mdt_objects where user_name = USER )\r\n" + 
	"WITH CHECK OPTION  CONSTRAINT \"CHECK_MDT_DATA_OBJECTS\";\r\n" + 
	"--  DDL for View MDT_USER_DEPENDENCIES\r\n" + 
	"CREATE OR REPLACE VIEW \"MDT_USER_DEPENDENCIES\" (\"OBJECT_ID\", \"REF_OBJECT_NAME\", \"REF_OBJECT_OWNER\", \"REF_TYPE\") AS\r\n" + 
	"SELECT object_id,ref_object_name, ref_object_owner, ref_type\r\n" + 
	"FROM mdt_dependencies\r\n" + 
	"WHERE object_id in ( select object_id from mdt_objects where user_name = USER )\r\n" + 
	"WITH CHECK OPTION CONSTRAINT \"CHECK_MDT_USER_DEPENDENCIES\";\r\n" + 
	"--  DDL for View MDT_USER_DIMENSIONS\r\n" + 
	"CREATE OR REPLACE VIEW \"MDT_USER_DIMENSIONS\" (\"OBJECT_ID\", \"DIM_NAME\", \"DIM_INDEX\", \"DIM_TYPE\", \"DIM_NULL\", \"DIM_WIDTH\", \"DIM_CONST\", \"COLUMN_TYPE\", \"DIM_HIERARCHY\", \"LD_CL_STATIC\", \"LD_CL_SORT\", \"LD_DIM_NAMES\", \"LD_TRANSCOD\", \"LD_REFER_LIST\", \"LD_VALIDATION\", \"DIM_PRECISION\", \"DIM_SCALE\") AS\r\n" + 
	"SELECT object_id, dim_name, dim_index, dim_type, dim_null, dim_width, dim_const, column_type, dim_hierarchy, ld_cl_static , ld_cl_sort , ld_dim_names, ld_transcod, ld_refer_list, ld_validation, dim_precision, dim_scale\r\n" + 
	"FROM mdt_dimensions\r\n" + 
	"WHERE object_id in ( select object_id from mdt_objects where user_name = USER )\r\n" + 
	"WITH CHECK OPTION CONSTRAINT \"CHECK_MDT_USER_DIMENSIONS\";\r\n" + 
	"--  DDL for View MDT_USER_DROPPED_OBJECTS\r\n" + 
	"CREATE OR REPLACE VIEW \"MDT_USER_DROPPED_OBJECTS\" (\"USER_NAME\", \"OBJECT_NAME\", \"OBJECT_TYPE\", \"TIMESTAMP\") AS\r\n" + 
	"SELECT user_name, object_name, object_type, timestamp\r\n" + 
	"FROM mdt_dropped_objects\r\n" + 
	"WHERE user_name = USER\r\n" + 
	"WITH CHECK OPTION CONSTRAINT \"CHECK_MDT_USER_DROPPED_OBJECTS\";\r\n" + 
	"--  DDL for View MDT_USER_EQUATIONS\r\n" + 
	"CREATE OR REPLACE VIEW \"MDT_USER_EQUATIONS\" (\"OBJECT_ID\", \"LEFT_PART\", \"EQUATION_TYPE\", \"CONSTANT_PART\", \"COMPUTATION_LEVEL\", \"NUM_ITEMS\", \"EQUATION_NUMBER\", \"COND_LOWERBOUND\", \"COND_UPPERBOUND\", \"EQUATION_COMMENT\") AS\r\n" + 
	"SELECT object_id, left_part, equation_type, constant_part, computation_level, num_items, equation_number, cond_lowerbound, cond_upperbound, equation_comment\r\n" + 
	"FROM mdt_equations\r\n" + 
	"WHERE object_id in ( select object_id from mdt_objects where user_name = USER )\r\n" + 
	"WITH CHECK OPTION CONSTRAINT \"CHECK_MDT_USER_EQUATIONS\";\r\n" + 
	"--  DDL for View MDT_USER_EQUATIONS_ITEMS\r\n" + 
	"CREATE OR REPLACE VIEW \"MDT_USER_EQUATIONS_ITEMS\" (\"OBJECT_ID\", \"POS_CODE\", \"FACTOR\", \"ITEM_NUMBER\", \"EQUATION_NUMBER\") AS\r\n" + 
	"SELECT object_id, pos_code, factor, item_number, equation_number\r\n" + 
	"FROM mdt_equations_items\r\n" + 
	"WHERE object_id in ( select object_id from mdt_objects where user_name = USER )\r\n" + 
	"WITH CHECK OPTION CONSTRAINT \"CHECK_MDT_USER_EQUATIONS_ITEMS\";\r\n" + 
	"--  DDL for View MDT_USER_EQUATIONS_TREE\r\n" + 
	"CREATE OR REPLACE VIEW \"MDT_USER_EQUATIONS_TREE\" (\"OBJECT_ID\", \"POS_INDEX\", \"POS_CODE\", \"POS_LEVEL\", \"POS_CLASS\", \"EQUATION_NUMBER\") AS\r\n" + 
	"SELECT object_id, pos_index, pos_code, pos_level, pos_class, equation_number\r\n" + 
	"FROM mdt_equations_tree\r\n" + 
	"WHERE object_id in ( select object_id from mdt_objects where user_name = USER )\r\n" + 
	"WITH CHECK OPTION CONSTRAINT \"CHECK_MDT_USER_EQUATIONS_TREE\";\r\n" + 
	"--  DDL for View MDT_USER_HISTORY\r\n" + 
	"CREATE OR REPLACE VIEW \"MDT_USER_HISTORY\" (\"HISTORY_ID\", \"SESSION_ID\", \"STATEMENT\", \"ELAPSED_TIME\", \"CUSTOM_MESSAGE\", \"JAVA_EXCEPTION_MESSAGE\", \"JAVA_STACK_TRACE\", \"LOG_TYPE\") AS\r\n" + 
	"SELECT history_id, session_id, statement, elapsed_time, custom_message, java_exception_message, java_stack_trace, log_type\r\n" + 
	"FROM mdt_history\r\n" + 
	"WHERE session_id in ( select session_id from mdt_sessions where user_name = USER )\r\n" + 
	"WITH CHECK OPTION CONSTRAINT \"CHECK_MDT_USER_HISTORY\";\r\n" + 
	"--  DDL for View MDT_USER_MODIFICATIONS\r\n" + 
	"CREATE OR REPLACE VIEW \"MDT_USER_MODIFICATIONS\" (\"OBJECT_ID\", \"TIMESTAMP\", \"SES_ID\", \"CMD_ID\", \"DIM_NAME\", \"OP_TYPE\", \"POS_CODE\") AS\r\n" + 
	"SELECT object_id, timestamp, ses_id, cmd_id, dim_name, op_type, pos_code\r\n" + 
	"FROM mdt_modifications\r\n" + 
	"WHERE object_id in ( select object_id from mdt_objects where user_name = USER )\r\n" + 
	"WITH CHECK OPTION CONSTRAINT \"CHECK_MDT_USER_MODIFICATIONS\";\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"--  DDL for View MDT_USER_MODIFICATIONS_INS\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"CREATE OR REPLACE VIEW \"MDT_USER_MODIFICATIONS_INS\" (\"OBJECT_ID\", \"TIMESTAMP\", \"SES_ID\", \"CMD_ID\", \"DIM_NAME\", \"OP_TYPE\", \"POS_CODE\") AS\r\n" + 
	"SELECT object_id, timestamp, ses_id, cmd_id, dim_name, op_type, pos_code\r\n" + 
	"FROM mdt_modifications\r\n" + 
	"WHERE object_id in ( select object_id from mdt_objects where user_name = USER ) OR object_id IN ( select object_id from mdt_privileges where grantee = USER AND privilege = 'P' ) OR object_id IN ( select object_id from mdt_privileges where grantee in ( SELECT role FROM session_roles ) AND privilege = 'P' )\r\n" + 
	"WITH CHECK OPTION  CONSTRAINT \"CHECK_MDT_USER_MODIF_INS\";\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"--  DDL for View MDT_USER_NOTES\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"CREATE OR REPLACE VIEW \"MDT_USER_NOTES\" (\"OBJECT_ID\", \"NOTE_INDEX\", \"FIELD_INDEX\", \"DIM_INDEX\", \"POS_CODE\") AS\r\n" + 
	"SELECT object_id, note_index, field_index, dim_index, pos_code\r\n" + 
	"FROM mdt_notes\r\n" + 
	"WHERE object_id in ( select object_id from mdt_objects where user_name = USER )\r\n" + 
	"WITH CHECK OPTION CONSTRAINT \"CHECK_MDT_USER_NOTES\";\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"--  DDL for View MDT_USER_OBJECTS\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"CREATE OR REPLACE VIEW \"MDT_USER_OBJECTS\" (\"USER_NAME\", \"OBJECT_ID\", \"OBJECT_NAME\", \"OBJECT_TYPE\", \"STATUS\", \"CREATED\", \"LAST_MODIFIED\", \"CHANGE_ID\", \"SYNONYM_FOR\", \"DROP_TIME\", \"DROP_ORIGINAL_NAME\", \"PARENT_FOLDER\") AS\r\n" + 
	"SELECT user_name, object_id, object_name, object_type, status, created, last_modified, change_id, synonym_for, drop_time, drop_original_name, parent_folder\r\n" + 
	"FROM mdt_objects\r\n" + 
	"WHERE user_name = USER\r\n" + 
	"WITH CHECK OPTION CONSTRAINT \"CHECK_MDT_USER_OBJECTS\";\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"--  DDL for View MDT_USER_OBJECTS_COMMENTS\r\n" + 
	"--------------------------------------------------------\r\n" +  
	"CREATE OR REPLACE VIEW \"MDT_USER_OBJECTS_COMMENTS\" (\"OBJECT_ID\", \"OBJECT_COMMENT\", \"TITLE_EN\", \"TITLE_FR\", \"TITLE_DE\", \"LAST_EXPORTED\", \"DOMAIN\") AS\r\n" + 
	"SELECT \"OBJECT_ID\",\"OBJECT_COMMENT\",\"TITLE_EN\",\"TITLE_FR\",\"TITLE_DE\",\"LAST_EXPORTED\",\"DOMAIN\"\r\n" + 
	"FROM MDT_OBJECTS_COMMENTS\r\n" + 
	"WHERE OBJECT_ID IN ( SELECT OBJECT_ID FROM MDT_OBJECTS WHERE USER_NAME = USER ) \r\n" + 
	"WITH CHECK OPTION  CONSTRAINT \"CHECK_MDT_USER_OBJ_COMMENTS\";\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"--  DDL for View MDT_USER_OBJECTS_INS\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"CREATE OR REPLACE VIEW \"MDT_USER_OBJECTS_INS\" (\"USER_NAME\", \"OBJECT_ID\", \"OBJECT_NAME\", \"OBJECT_TYPE\", \"STATUS\", \"CREATED\", \"LAST_MODIFIED\", \"CHANGE_ID\", \"SYNONYM_FOR\") AS\r\n" + 
	"SELECT user_name, object_id, object_name, object_type, status, created, last_modified, change_id, synonym_for\r\n" + 
	"FROM mdt_objects\r\n" + 
	"WHERE user_name = USER OR object_id IN ( select object_id from mdt_privileges where grantee = USER AND privilege = 'P' ) OR object_id IN ( select object_id from mdt_privileges where grantee in ( SELECT role FROM session_roles ) AND privilege = 'P' )\r\n" + 
	"WITH CHECK OPTION CONSTRAINT \"CHECK_MDT_USER_OBJECTS_INS\";\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"--  DDL for View MDT_USER_POSITIONS\r\n" + 
	"--------------------------------------------------------\r\n" +  
	"CREATE OR REPLACE VIEW \"MDT_USER_POSITIONS\" (\"OBJECT_ID\", \"DIM_INDEX\", \"POS_INDEX\", \"POS_CODE\") AS\r\n" + 
	"SELECT object_id, dim_index, pos_index, pos_code\r\n" + 
	"FROM mdt_positions\r\n" + 
	"WHERE object_id in ( select object_id from mdt_objects where user_name = USER )\r\n" + 
	"WITH CHECK OPTION CONSTRAINT \"CHECK_MDT_USER_POSITIONS\";\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"--  DDL for View MDT_USER_POSITIONS_INS\r\n" + 
	"--------------------------------------------------------\r\n" +  
	"CREATE OR REPLACE VIEW \"MDT_USER_POSITIONS_INS\" (\"OBJECT_ID\", \"DIM_INDEX\", \"POS_INDEX\", \"POS_CODE\") AS\r\n" + 
	"SELECT object_id, dim_index, pos_index, pos_code\r\n" + 
	"FROM mdt_positions\r\n" + 
	"WHERE object_id in ( select object_id from mdt_objects where user_name = USER ) OR object_id IN ( select object_id from mdt_privileges where grantee = USER AND privilege = 'P' ) OR object_id IN ( select object_id from mdt_privileges where grantee in ( SELECT role FROM session_roles ) AND privilege = 'P' )\r\n" + 
	"WITH CHECK OPTION  CONSTRAINT \"CHECK_MDT_USER_POSITIONS_INS\";\r\n" + 
	"--  DDL for View MDT_USER_PRIVILEGES\r\n" + 
	"CREATE OR REPLACE VIEW \"MDT_USER_PRIVILEGES\" (\"OBJECT_ID\", \"GRANTEE\", \"PRIVILEGE\") AS \r\n" + 
	"SELECT object_id, grantee, privilege\r\n" + 
	"FROM mdt_privileges\r\n" + 
	"WHERE object_id IN ( select object_id from mdt_objects where user_name = USER )\r\n" + 
	"WITH CHECK OPTION CONSTRAINT \"CHECK_MDT_USER_PRIVILEGES\";\r\n" + 
	"--  DDL for View MDT_USER_PROFILES\r\n" + 
	"CREATE OR REPLACE VIEW \"MDT_USER_PROFILES\" (\"USER_NAME\", \"DEFAULT_SELECT_PRIV_ROLE\", \"DEFAULT_SELECT_PRIV_USER\", \"IN_REAL_LIFE\", \"DESCRIPTION\", \"AUDIT_LEVEL\", \"SEARCH_USER\", \"DICTIONARY_TEMPLATE\", \"TABLE_TEMPLATE\", \"TABLES_HAVE_PRIMARY_KEY\", \"TABLES_HAVE_BITMAP_INDEXES\", \"TIME_PERIOD_NAME\", \"TIME_PERIOD_ANNUAL_FORMAT\", \"LIST_DISPLAY_PROPERTIES\", \"LIST_DISPLAY_GRID\", \"DIGIT_GROUPING_SYMBOL\", \"DICTIONARY_MODIFY_EDIT\", \"BROWSE_SELECT_TIME_AUTO\", \"DECIMAL_PLACES\", \"DISPLAY_LABEL\", \"LABEL_NAME\", \"LABEL_NAME2\", \"DATE_FORMAT\", \"AUTO_SAVE_PRESENTATION\", \"CHART_AUTO_TIME\", \"CHART_AUTO_OTHER\", \"CHART_CATEGORY_AXIS\", \"CHART_LEGEND\", \"FONT_SIZE\") AS\r\n" + 
	"SELECT user_name, default_select_priv_role, default_select_priv_user, in_real_life, description, audit_level, search_user, dictionary_template, table_template, tables_have_primary_key, tables_have_bitmap_indexes, time_period_name, time_period_annual_format,   list_display_properties,list_display_grid,digit_grouping_symbol,dictionary_modify_edit, browse_select_time_auto,   decimal_places,display_label,label_name,label_name2,date_format,   auto_save_presentation,chart_auto_time,chart_auto_other,chart_category_axis,chart_legend, font_size\r\n" + 
	"FROM mdt_profiles\r\n" + 
	"WHERE user_name = USER\r\n" + 
	"WITH CHECK OPTION CONSTRAINT \"CHECK_MDT_USER_PROFILES\";\r\n" + 
	"--  DDL for View MDT_USER_RENAMED_OBJECTS\r\n" + 
	"CREATE OR REPLACE VIEW \"MDT_USER_RENAMED_OBJECTS\" (\"OBJECT_ID\", \"TIMESTAMP\", \"SES_ID\", \"PREVIOUS_OBJECT_NAME\") AS\r\n" + 
	"SELECT object_id, timestamp, ses_id, previous_object_name\r\n" + 
	"FROM mdt_renamed_objects\r\n" + 
	"WHERE object_id in ( select object_id from mdt_objects where user_name = USER )\r\n" + 
	"WITH CHECK OPTION CONSTRAINT \"CHECK_MDT_USER_RENAMED_OBJECTS\";\r\n" + 
	"--  DDL for View MDT_USER_SESSIONS\r\n" + 
	"CREATE OR REPLACE VIEW \"MDT_USER_SESSIONS\" (\"SESSION_ID\", \"USER_NAME\", \"OS_USERID\", \"TIME_LOGON\", \"TIME_LOGOFF\", \"VERSION\", \"ECAS_USERNAME\") AS\r\n" + 
	"SELECT session_id, user_name, os_userid, time_logon, time_logoff, version, ecas_username\r\n" + 
	"FROM mdt_sessions\r\n" + 
	"WHERE user_name = USER\r\n" + 
	"WITH CHECK OPTION CONSTRAINT \"CHECK_MDT_USER_SESSIONS\";\r\n" + 
	"--  DDL for View MDT_USER_SOURCES\r\n" + 
	"CREATE OR REPLACE VIEW \"MDT_USER_SOURCES\" (\"OBJECT_ID\", \"BUFFER_INDEX\", \"BUFFER_TEXT\") AS\r\n" + 
	"SELECT object_id, buffer_index, buffer_text\r\n" + 
	"FROM mdt_sources\r\n" + 
	"WHERE object_id in ( select object_id from mdt_objects where user_name = USER )\r\n" + 
	"WITH CHECK OPTION CONSTRAINT \"CHECK_MDT_USER_SOURCES\";\r\n" + 
	"--  DDL for View MDT_USER_SYNTAX_TREES\r\n" + 
	"CREATE OR REPLACE VIEW \"MDT_USER_SYNTAX_TREES\" (\"OBJECT_ID\", \"NODE_INDEX\", \"NODE_NUM_CHILDREN\", \"NODE_NAME\", \"NODE_INFO\", \"NODE_VALUE_1\", \"NODE_VALUE_2\") AS\r\n" + 
	"SELECT object_id, node_index, node_num_children, node_name, node_info, node_value_1, node_value_2\r\n" + 
	"FROM mdt_syntax_trees\r\n" + 
	"WHERE object_id in ( select object_id from mdt_objects where user_name = USER )\r\n" + 
	"WITH CHECK OPTION CONSTRAINT \"CHECK_MDT_USER_SYNTAX_TREES\";\r\n" + 
	"--  DDL for View MDT_USER_VALIDATION_CONDITIONS\r\n" + 
	"CREATE OR REPLACE VIEW \"MDT_USER_VALIDATION_CONDITIONS\" (\"OBJECT_ID\", \"LINE_NUMBER\", \"PRECONDITION\", \"CONDITION\", \"ERROR_MESSAGE\", \"SEVERITY_CODE\", \"SQL_PRECONDITION\", \"SQL_CONDITION\") AS\r\n" + 
	"SELECT object_id, line_number, precondition, condition, error_message, severity_code, sql_precondition, sql_condition\r\n" + 
	"FROM mdt_validation_conditions\r\n" + 
	"WHERE object_id in ( select object_id from mdt_objects where user_name = USER )\r\n" + 
	"WITH CHECK OPTION CONSTRAINT \"CHECK_MDT_USER_VALID_CONDS\";\r\n" + 
	"--  DDL for View MDT_USER_VALIDATION_RULES\r\n" + 
	"CREATE OR REPLACE VIEW \"MDT_USER_VALIDATION_RULES\" (\"OBJECT_ID\", \"DATA_OBJECT_NAME\", \"SEVERITY_DICTIONARY\") AS\r\n" + 
	"SELECT object_id, data_object_name, severity_dictionary\r\n" + 
	"FROM mdt_validation_rules\r\n" + 
	"WHERE object_id in ( select object_id from mdt_objects where user_name = USER )\r\n" + 
	"WITH CHECK OPTION CONSTRAINT \"CHECK_MDT_USER_VALID_RULES\";\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"--  Functions\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"--  DDL for Function BLOB_TO_CLOB\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"\r\n" + 
	"CREATE OR REPLACE FUNCTION \"BLOB_TO_CLOB\" (temp_blob BLOB) RETURN CLOB\r\n" + 
	"AS\r\n" + 
	"  temp_clob CLOB;\r\n" + 
	"  dest_offset NUMBER  := 1;\r\n" + 
	"  src_offset NUMBER  := 1;\r\n" + 
	"  amount INTEGER := dbms_lob.lobmaxsize;\r\n" + 
	"  blob_csid NUMBER  := dbms_lob.default_csid;\r\n" + 
	"  lang_ctx INTEGER := dbms_lob.default_lang_ctx;\r\n" + 
	"  warning INTEGER;\r\n" + 
	"BEGIN\r\n" + 
	"  DBMS_LOB.CREATETEMPORARY(lob_loc=>temp_clob, cache=>TRUE, dur=>dbms_lob.SESSION);\r\n" + 
	"  DBMS_LOB.CONVERTTOCLOB(temp_clob,temp_blob,amount,dest_offset,src_offset,blob_csid,lang_ctx,warning);\r\n" + 
	"  RETURN ( temp_clob ) ;\r\n" + 
	"END;\r\n" + 
	"/\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"--  Packages\r\n" + 
	"--------------------------------------------------------\r\n" + 
	"CREATE OR REPLACE PACKAGE \"MDT_UTILS\" AS\r\n" + 
	"	function aggr_flags (i_flagA in VARCHAR2, i_flagB in VARCHAR2 default null) return VARCHAR2;\r\n" + 
	"END;\r\n" + 
	"CREATE OR REPLACE PACKAGE BODY \"MDT_UTILS\" AS\r\n" + 
	"	--constants\r\n" + 
	"	AGGR		constant varchar2(1 char) := 'A'; --aggregate\r\n" + 
	"	FLAG_1		constant varchar2(1 char) := '1'; --first flag only\r\n" + 
	"	FLAG_2		constant varchar2(1 char) := '2'; --second flag only\r\n" + 
	"	NV			constant varchar2(1 char) := 'N'; --null value\r\n" + 
	"	--groups of flags (with the flags sorted)\r\n" + 
	"	G1			constant table_varchar2 := table_varchar2('B', 'I', 'R');\r\n" + 
	"	G2			constant table_varchar2 := table_varchar2('M', 'N', 'Z');\r\n" + 
	"	G3			constant table_varchar2 := table_varchar2('E', 'F', 'P', 'S', 'U');\r\n" + 
	"	G123_EX_M	constant table_varchar2 := table_varchar2('B', 'E', 'F', 'I', 'N', 'P', 'R', 'S', 'U', 'Z'); --G1 + G2 + G3 except M flag\r\n" + 
	"	--special flags that don't aggregate with nothing, they just return NULL. what ever the combination may be, if at least one of these flags is present, the entire result is NULL\r\n" + 
	"	G_NULLIFY	constant table_varchar2 := table_varchar2('L');\r\n" + 
	"	--globals\r\n" + 
	"	g_flag_combination	table_flag_combination := table_flag_combination();\r\n" + 
	"	g_flag_comb_count	number := 0;\r\n" + 
	"	--package public methods\r\n" + 
	"	function aggr_flags (i_flagA in varchar2,i_flagB in varchar2 default null) return varchar2\r\n" + 
	"	IS\r\n" + 
	"		l_ret_value			varchar2(200 char) := '';\r\n" + 
	"		l_flagA				varchar2(200 char) := '';\r\n" + 
	"		l_flagB				varchar2(200 char) := '';\r\n" + 
	"		l_flag_coll			table_varchar2;\r\n" + 
	"		l_flag_coll_sorted	table_varchar2;\r\n" + 
	"		---------------------------------------------------\r\n" + 
	"		l_leading_zero		boolean := false;\r\n" + 
	"		L_ZERO				constant varchar2(1 char) := '0';\r\n" + 
	"		---------------------------------------------------\r\n" + 
	"		function aggr_flags --aggregate the two flags based on the rule\r\n" + 
	"		(\r\n" + 
	"			i_flagA				in varchar2,\r\n" + 
	"			i_flagB				in varchar2,\r\n" + 
	"			i_flag_combination	in varchar2\r\n" + 
	"		)\r\n" + 
	"		return varchar2\r\n" + 
	"		is\r\n" + 
	"		begin\r\n" + 
	"			return \r\n" + 
	"				case i_flag_combination\r\n" + 
	"					when AGGR		then i_flagA || i_flagB\r\n" + 
	"					when FLAG_1		then i_flagA\r\n" + 
	"					when FLAG_2		then i_flagB\r\n" + 
	"					when NV			then null\r\n" + 
	"				end;\r\n" + 
	"		end aggr_flags;\r\n" + 
	"		--\r\n" + 
	"		procedure merge_flags --merge flags into a collection\r\n" + 
	"		(\r\n" + 
	"			i_flag	in varchar2,\r\n" + 
	"			io_coll	in out table_varchar2\r\n" + 
	"		)\r\n" + 
	"		is\r\n" + 
	"			l_flag_len	number := 0;\r\n" + 
	"			l_coll_len	number := 0;\r\n" + 
	"		begin\r\n" + 
	"			l_flag_len := length(i_flag);\r\n" + 
	"			if (l_flag_len > 0) then\r\n" + 
	"				l_coll_len := io_coll.count();\r\n" + 
	"				io_coll.extend(l_flag_len);\r\n" + 
	"				\r\n" + 
	"				for i in 1..l_flag_len\r\n" + 
	"				loop\r\n" + 
	"					io_coll(i + l_coll_len) := substr(i_flag, i, 1);\r\n" + 
	"				end loop;\r\n" + 
	"			end if;\r\n" + 
	"		end merge_flags;\r\n" + 
	"		--\r\n" + 
	"		function sort_flags --sort and distinct collection of flags\r\n" + 
	"		(\r\n" + 
	"			i_flag_coll	in table_varchar2\r\n" + 
	"		)\r\n" + 
	"		return table_varchar2\r\n" + 
	"		is\r\n" + 
	"			l_coll	table_varchar2;\r\n" + 
	"		begin\r\n" + 
	"			select distinct a.column_value\r\n" + 
	"			bulk collect into l_coll\r\n" + 
	"			from table(i_flag_coll) a\r\n" + 
	"			order by 1;\r\n" + 
	"			\r\n" + 
	"			return l_coll;\r\n" + 
	"		end sort_flags;\r\n" + 
	"		--\r\n" + 
	"		\r\n" + 
	"		function aggr_flags_simple --aggregates two flags with the rules defined\r\n" + 
	"		(\r\n" + 
	"			i_flagA in varchar2,\r\n" + 
	"			i_flagB	in varchar2\r\n" + 
	"		)\r\n" + 
	"		return varchar2\r\n" + 
	"		is\r\n" + 
	"			l_ret_value		varchar2(200 char) := '';\r\n" + 
	"			l_found			boolean := false;\r\n" + 
	"			l_flag_count	number := 0;\r\n" + 
	"		begin\r\n" + 
	"			if (i_flagA is null or i_flagB is null) then\r\n" + 
	"				l_ret_value := coalesce(i_flagA, i_flagB);\r\n" + 
	"			else\r\n" + 
	"				--search for flag A in the rule collection\r\n" + 
	"				for i_combs in 1 .. g_flag_comb_count\r\n" + 
	"				loop\r\n" + 
	"					if (i_flagA = g_flag_combination(i_combs).flag) then\r\n" + 
	"						l_flag_count := g_flag_combination(i_combs).related_flags.count();\r\n" + 
	"						\r\n" + 
	"						--search for flag B in the flags related collection for the rule\r\n" + 
	"						for i_flag_combs in 1 .. l_flag_count\r\n" + 
	"						loop\r\n" + 
	"							if (i_flagB = g_flag_combination(i_combs).related_flags(i_flag_combs)) then\r\n" + 
	"								l_ret_value := l_ret_value || aggr_flags(i_flagA, i_flagB, g_flag_combination(i_combs).result_option);\r\n" + 
	"								l_found := true;\r\n" + 
	"								exit;\r\n" + 
	"							end if;\r\n" + 
	"						end loop;\r\n" + 
	"		\r\n" + 
	"					end if;\r\n" + 
	"		\r\n" + 
	"					if (l_found) then\r\n" + 
	"						exit;\r\n" + 
	"					end if;\r\n" + 
	"				end loop;\r\n" + 
	"			end if;\r\n" + 
	"\r\n" + 
	"			return l_ret_value;\r\n" + 
	"\r\n" + 
	"		end aggr_flags_simple;\r\n" + 
	"		--\r\n" + 
	"\r\n" + 
	"		function combine_multiple_flags\r\n" + 
	"		(\r\n" + 
	"			i_flag_coll	in table_varchar2\r\n" + 
	"		)\r\n" + 
	"		return varchar2\r\n" + 
	"		is\r\n" + 
	"			l_coll_len		number := 0;\r\n" + 
	"			l_aux_flag1		varchar2(200 char) := '';\r\n" + 
	"			l_aux_flag2		varchar2(200 char) := '';\r\n" + 
	"			l_ret_aux		varchar2(200 char) := '';\r\n" + 
	"			l_ret_value		varchar2(200 char) := '';\r\n" + 
	"			l_do_aggr		boolean := true;\r\n" + 
	"		begin\r\n" + 
	"			l_coll_len := i_flag_coll.count();\r\n" + 
	"			\r\n" + 
	"			for i in 1..l_coll_len\r\n" + 
	"			loop\r\n" + 
	"				l_do_aggr := true;\r\n" + 
	"				\r\n" + 
	"				for j in 1..l_coll_len\r\n" + 
	"				loop\r\n" + 
	"					if (i != j) then\r\n" + 
	"						--sort flags\r\n" + 
	"						if (i_flag_coll(i) > i_flag_coll(j)) then\r\n" + 
	"							l_aux_flag1 := i_flag_coll(j);\r\n" + 
	"							l_aux_flag2 := i_flag_coll(i);\r\n" + 
	"						else\r\n" + 
	"							l_aux_flag1 := i_flag_coll(i);\r\n" + 
	"							l_aux_flag2 := i_flag_coll(j);\r\n" + 
	"						end if;\r\n" + 
	"						\r\n" + 
	"						l_ret_aux := aggr_flags_simple(l_aux_flag1, l_aux_flag2);\r\n" + 
	"						if (l_ret_aux is null or instr(l_ret_aux, i_flag_coll(i)) = 0) then\r\n" + 
	"							l_do_aggr := false;\r\n" + 
	"							exit;\r\n" + 
	"						end if;\r\n" + 
	"					end if;\r\n" + 
	"				end loop;\r\n" + 
	"\r\n" + 
	"				if (l_do_aggr) then\r\n" + 
	"					l_ret_value := l_ret_value || i_flag_coll(i);\r\n" + 
	"				end if;\r\n" + 
	"			end loop;\r\n" + 
	"		\r\n" + 
	"			return l_ret_value;\r\n" + 
	"		\r\n" + 
	"		end combine_multiple_flags;\r\n" + 
	"		function check_nullify_flags\r\n" + 
	"		(\r\n" + 
	"			i_tbl_flags	in table_varchar2\r\n" + 
	"		)\r\n" + 
	"		return boolean\r\n" + 
	"		is\r\n" + 
	"			l_found	boolean := false;\r\n" + 
	"		begin\r\n" + 
	"			for i in 1 .. i_tbl_flags.count()\r\n" + 
	"			loop\r\n" + 
	"				for j in 1 .. G_NULLIFY.count()\r\n" + 
	"				loop\r\n" + 
	"					if (instr(G_NULLIFY(j), i_tbl_flags(i)) != 0) then\r\n" + 
	"						l_found := true;\r\n" + 
	"						exit;\r\n" + 
	"					end if;\r\n" + 
	"				end loop;\r\n" + 
	"\r\n" + 
	"				if (l_found) then\r\n" + 
	"					exit;\r\n" + 
	"				end if;\r\n" + 
	"			end loop;\r\n" + 
	"			return l_found;\r\n" + 
	"		end check_nullify_flags;\r\n" + 
	"	begin\r\n" + 
	"		l_flagA := upper(trim(i_flagA));\r\n" + 
	"		l_flagB := upper(trim(i_flagB));\r\n" + 
	"		if (instr(l_flagA, L_ZERO) != 0) then\r\n" + 
	"			l_flagA := replace(l_flagA, L_ZERO);\r\n" + 
	"			l_leading_zero := true;\r\n" + 
	"		end if;\r\n" + 
	"		if (instr(l_flagB, L_ZERO) != 0) then\r\n" + 
	"			l_flagB := replace(l_flagB, L_ZERO);\r\n" + 
	"			l_leading_zero := true;\r\n" + 
	"		end if;\r\n" + 
	"		--*** check if there are special nullify flags\r\n" + 
	"		if (check_nullify_flags(table_varchar2(l_flagA, l_flagB))) then\r\n" + 
	"			l_ret_value := null;\r\n" + 
	"		else\r\n" + 
	"			if (l_flagA is null and l_flagB is null) then\r\n" + 
	"				l_ret_value := null;\r\n" + 
	"			elsif (length(l_flagA) > 1 or length(l_flagB) > 1) then --multiple flags\r\n" + 
	"				l_flag_coll := table_varchar2();\r\n" + 
	"				merge_flags(i_flag => l_flagA, io_coll => l_flag_coll);\r\n" + 
	"				merge_flags(i_flag => l_flagB, io_coll => l_flag_coll);\r\n" + 
	"				l_flag_coll_sorted := sort_flags(i_flag_coll => l_flag_coll);\r\n" + 
	"				l_ret_value := combine_multiple_flags(i_flag_coll => l_flag_coll_sorted);\r\n" + 
	"			else --single flags\r\n" + 
	"				l_flag_coll := table_varchar2(l_flagA, l_flagB);\r\n" + 
	"				l_flag_coll_sorted := sort_flags(i_flag_coll => l_flag_coll);\r\n" + 
	"				l_flagA := l_flag_coll_sorted(1);\r\n" + 
	"				if (l_flag_coll_sorted.count() > 1) then\r\n" + 
	"					l_flagB := l_flag_coll_sorted(2);\r\n" + 
	"				else\r\n" + 
	"					l_flagB := l_flag_coll_sorted(1);\r\n" + 
	"				end if;\r\n" + 
	"				l_ret_value := aggr_flags_simple(i_flagA => l_flagA, i_flagB => l_flagB);\r\n" + 
	"			end if;\r\n" + 
	"		end if;\r\n" + 
	"		if (l_leading_zero) then\r\n" + 
	"			l_ret_value := L_ZERO || l_ret_value;\r\n" + 
	"		end if;\r\n" + 
	"		return l_ret_value;\r\n" + 
	"	exception\r\n" + 
	"	when others then\r\n" + 
	"		--log error (TODO)\r\n" + 
	"		raise;\r\n" + 
	"	end aggr_flags;\r\n" + 
	"begin\r\n" + 
	"	--inicialize flag combination rules\r\n" + 
	"	g_flag_combination.extend(26);\r\n" + 
	"	--B\r\n" + 
	"	g_flag_combination(1) := struct_flag_combination('B', table_varchar2('B'), FLAG_1);\r\n" + 
	"	g_flag_combination(2) := struct_flag_combination('B', G123_EX_M, AGGR);\r\n" + 
	"	g_flag_combination(3) := struct_flag_combination('B', table_varchar2('M'), FLAG_1);\r\n" + 
	"	--C (obs_conf)\r\n" + 
	"	g_flag_combination(4) := struct_flag_combination('C', table_varchar2('C', 'N'), FLAG_1);\r\n" + 
	"	--E\r\n" + 
	"	g_flag_combination(5) := struct_flag_combination('E', table_varchar2('F'), FLAG_2);\r\n" + 
	"	g_flag_combination(6) := struct_flag_combination('E', table_varchar2('E', 'M', 'N', 'S', 'U', 'Z'), FLAG_1);\r\n" + 
	"	g_flag_combination(7) := struct_flag_combination('E', table_varchar2('I', 'P', 'R'), AGGR);\r\n" + 
	"	--F\r\n" + 
	"	g_flag_combination(8) := struct_flag_combination('F', G1, AGGR);\r\n" + 
	"	g_flag_combination(9) := struct_flag_combination('F', G2, FLAG_1);\r\n" + 
	"	g_flag_combination(10) := struct_flag_combination('F', G3, FLAG_1);\r\n" + 
	"	--I\r\n" + 
	"	g_flag_combination(11) := struct_flag_combination('I', table_varchar2('I'), FLAG_1);\r\n" + 
	"	g_flag_combination(12) := struct_flag_combination('I', G123_EX_M, AGGR);\r\n" + 
	"	g_flag_combination(13) := struct_flag_combination('I', table_varchar2('M'), FLAG_1);\r\n" + 
	"	--M\r\n" + 
	"	g_flag_combination(14) := struct_flag_combination('M', G123_EX_M, FLAG_2);\r\n" + 
	"	g_flag_combination(15) := struct_flag_combination('M', table_varchar2('M'), NV); -- MM = null ??? TODO: check this!\r\n" + 
	"	--N\r\n" + 
	"	g_flag_combination(16) := struct_flag_combination('N', G1, AGGR);\r\n" + 
	"	g_flag_combination(17) := struct_flag_combination('N', G2, FLAG_1);\r\n" + 
	"	g_flag_combination(18) := struct_flag_combination('N', G3, FLAG_2);\r\n" + 
	"	--P\r\n" + 
	"	g_flag_combination(19) := struct_flag_combination('P', table_varchar2('S'), FLAG_2);\r\n" + 
	"	g_flag_combination(20) := struct_flag_combination('P', table_varchar2('R'), AGGR);\r\n" + 
	"	g_flag_combination(21) := struct_flag_combination('P', table_varchar2('P', 'U', 'Z'), FLAG_1);\r\n" + 
	"	--R\r\n" + 
	"	g_flag_combination(22) := struct_flag_combination('R', table_varchar2('R', 'M'), FLAG_1);\r\n" + 
	"	g_flag_combination(23) := struct_flag_combination('R', G123_EX_M, AGGR); --groups 1, 2 (except M flag), 3\r\n" + 
	"	--S\r\n" + 
	"	g_flag_combination(24) := struct_flag_combination('S', table_varchar2('S', 'U', 'Z'), FLAG_1);\r\n" + 
	"	--U\r\n" + 
	"	g_flag_combination(25) := struct_flag_combination('U', table_varchar2('U', 'Z'), FLAG_1);\r\n" + 
	"	--Z\r\n" + 
	"	g_flag_combination(26) := struct_flag_combination('Z', table_varchar2('Z'), FLAG_1);\r\n" + 
	"	g_flag_comb_count := g_flag_combination.count();\r\n" + 
	"END;\r\n" +
	"CREATE OR REPLACE FUNCTION \"MDT_AGG_FLAGS\" (nt_in in mdt_collect_table) return varchar2\r\n" + 
	"is\r\n" + 
	"	v_idx PLS_INTEGER;\r\n" + 
	"	v_str VARCHAR2(32767) := '';\r\n" + 
	"begin\r\n" + 
	"	v_idx := nt_in.FIRST;\r\n" + 
	"	WHILE v_idx IS NOT NULL LOOP\r\n" + 
	"		v_str := v_str || nt_in(v_idx);\r\n" + 
	"		v_idx := nt_in.NEXT(v_idx);\r\n" + 
	"	END LOOP;\r\n" + 
	"	v_str := mdt_utils.aggr_flags(v_str);\r\n" + 
	"	RETURN v_str;\r\n" + 
	"END;\r\n" + 
	"CREATE OR REPLACE FUNCTION \"MDT_MERGE_FLAGS\" (string_flags IN VARCHAR, new_flags IN VARCHAR) RETURN VARCHAR IS\r\n" + 
	"begin\r\n" + 
	"	return mdt_utils.aggr_flags(string_flags, new_flags);\r\n" + 
	"END;\r\n" ; 

static String dml =
	"CALL EXEC_SCRIPT('mdt-tool-dml.sql', '15.02.28', 'MDT Tool DML language.');\r\n" + 
	"INSERT INTO MDT_PROFILES (USER_NAME, DEFAULT_SELECT_PRIV_ROLE, SEARCH_USER, TABLES_HAVE_PRIMARY_KEY, TABLES_HAVE_BITMAP_INDEXES, TIME_PERIOD_NAME, BROWSE_SELECT_TIME_AUTO, LABEL_NAME, LABEL_NAME2) VALUES ('$SYS', '$ROLE', '$ROLE', 'Y', 'N', 'time', 12, 'label_en', 'label_en');\r\n" + 
	"INSERT INTO MDT_PROFILES (USER_NAME, DEFAULT_SELECT_PRIV_ROLE, SEARCH_USER, DECIMAL_PLACES, LABEL_NAME, DATE_FORMAT) VALUES ('$CL', '$ROLE', '$CL', 4, 'label_en', 'dd.MM.yyyy HH:mm');\r\n" + 
	"INSERT INTO MDT_PROFILES (USER_NAME, DEFAULT_SELECT_PRIV_ROLE, SEARCH_USER, DECIMAL_PLACES, DISPLAY_LABEL, LABEL_NAME, LABEL_NAME2, DATE_FORMAT, FONT_SIZE) VALUES ('$DATA', '$ROLE', '$DATA', 2, 'code', 'label_en', 'label_fr', 'dd.MM.yyyy HH:mm', 13);\r\n" + 
	"COMMIT;" ;

static String dcl = 
	"CALL EXEC_SCRIPT('mdt-tool-dcl.sql', '15.02.28', 'MDT Tool DCL language.');\r\n" + 
	"GRANT SELECT ON MDT_AUDITS                    TO PUBLIC;\r\n" + 
	"GRANT SELECT ON MDT_DATA_OBJECTS              TO PUBLIC;\r\n" + 
	"GRANT SELECT ON MDT_DEPENDENCIES              TO PUBLIC;\r\n" + 
	"GRANT SELECT ON MDT_DIMENSIONS                TO PUBLIC;\r\n" + 
	"GRANT SELECT ON MDT_DROPPED_OBJECTS           TO PUBLIC;\r\n" + 
	"GRANT SELECT ON MDT_EQUATIONS                 TO PUBLIC;\r\n" + 
	"GRANT SELECT ON MDT_EQUATIONS_ITEMS           TO PUBLIC;\r\n" + 
	"GRANT SELECT ON MDT_EQUATIONS_TREE            TO PUBLIC;\r\n" + 
	"GRANT SELECT ON MDT_HISTORY                   TO PUBLIC;\r\n" + 
	"GRANT SELECT ON MDT_MODIFICATIONS             TO PUBLIC;\r\n" + 
	"GRANT SELECT ON MDT_NOTES                     TO PUBLIC;\r\n" + 
	"GRANT SELECT ON MDT_OBJECTS                   TO PUBLIC;\r\n" + 
	"GRANT SELECT ON MDT_OBJECTS_COMMENTS          TO PUBLIC;\r\n" + 
	"GRANT SELECT ON MDT_POSITIONS                 TO PUBLIC;\r\n" + 
	"GRANT SELECT ON MDT_PRIVILEGES                TO PUBLIC;\r\n" + 
	"GRANT SELECT ON MDT_PROFILES                  TO PUBLIC;\r\n" + 
	"GRANT SELECT ON MDT_RENAMED_OBJECTS           TO PUBLIC;\r\n" + 
	"GRANT SELECT ON MDT_SESSIONS                  TO PUBLIC;\r\n" + 
	"GRANT SELECT ON MDT_SOURCES                   TO PUBLIC;\r\n" + 
	"GRANT SELECT ON MDT_SYNTAX_TREES              TO PUBLIC;\r\n" + 
	"GRANT SELECT ON MDT_VALIDATION_CONDITIONS     TO PUBLIC;\r\n" + 
	"GRANT SELECT ON MDT_VALIDATION_RULES          TO PUBLIC;\r\n" + 
	"GRANT SELECT, INSERT, UPDATE, DELETE ON MDT_USER_AUDITS                  TO PUBLIC;\r\n" + 
	"GRANT SELECT, INSERT                 ON MDT_USER_AUDITS_INS              TO PUBLIC;\r\n" + 
	"GRANT SELECT, INSERT, UPDATE, DELETE ON MDT_USER_DATA_OBJECTS            TO PUBLIC;\r\n" + 
	"GRANT SELECT, INSERT, UPDATE, DELETE ON MDT_USER_DEPENDENCIES            TO PUBLIC;\r\n" + 
	"GRANT SELECT, INSERT, UPDATE, DELETE ON MDT_USER_DIMENSIONS              TO PUBLIC;\r\n" + 
	"GRANT SELECT, INSERT                 ON MDT_USER_DROPPED_OBJECTS         TO PUBLIC;\r\n" + 
	"GRANT SELECT, INSERT, UPDATE, DELETE ON MDT_USER_EQUATIONS               TO PUBLIC;\r\n" + 
	"GRANT SELECT, INSERT, UPDATE, DELETE ON MDT_USER_EQUATIONS_ITEMS         TO PUBLIC;\r\n" + 
	"GRANT SELECT, INSERT, UPDATE, DELETE ON MDT_USER_EQUATIONS_TREE          TO PUBLIC;\r\n" + 
	"GRANT SELECT, INSERT, UPDATE, DELETE ON MDT_USER_HISTORY                 TO PUBLIC;\r\n" + 
	"GRANT SELECT, INSERT, UPDATE, DELETE ON MDT_USER_MODIFICATIONS           TO PUBLIC;\r\n" + 
	"GRANT SELECT, INSERT                 ON MDT_USER_MODIFICATIONS_INS       TO PUBLIC;\r\n" + 
	"GRANT SELECT, INSERT, UPDATE, DELETE ON MDT_USER_NOTES                   TO PUBLIC;\r\n" + 
	"GRANT SELECT, INSERT, UPDATE, DELETE ON MDT_USER_OBJECTS                 TO PUBLIC;\r\n" + 
	"GRANT SELECT, INSERT, UPDATE, DELETE ON MDT_USER_OBJECTS_COMMENTS        TO PUBLIC;\r\n" + 
	"GRANT SELECT, UPDATE                 ON MDT_USER_OBJECTS_INS             TO PUBLIC;\r\n" + 
	"GRANT SELECT, INSERT, UPDATE, DELETE ON MDT_USER_POSITIONS               TO PUBLIC;\r\n" + 
	"GRANT SELECT, INSERT, UPDATE         ON MDT_USER_POSITIONS_INS           TO PUBLIC;\r\n" + 
	"GRANT SELECT, INSERT, UPDATE, DELETE ON MDT_USER_PRIVILEGES              TO PUBLIC;\r\n" + 
	"GRANT SELECT, INSERT, UPDATE, DELETE ON MDT_USER_PROFILES                TO PUBLIC;\r\n" + 
	"GRANT SELECT, INSERT, UPDATE, DELETE ON MDT_USER_RENAMED_OBJECTS         TO PUBLIC;\r\n" + 
	"GRANT SELECT, INSERT, UPDATE, DELETE ON MDT_USER_SESSIONS                TO PUBLIC;\r\n" + 
	"GRANT SELECT, INSERT, UPDATE, DELETE ON MDT_USER_SOURCES                 TO PUBLIC;\r\n" + 
	"GRANT SELECT, INSERT, UPDATE, DELETE ON MDT_USER_SYNTAX_TREES            TO PUBLIC;\r\n" + 
	"GRANT SELECT, INSERT, UPDATE, DELETE ON MDT_USER_VALIDATION_CONDITIONS   TO PUBLIC;\r\n" + 
	"GRANT SELECT, INSERT, UPDATE, DELETE ON MDT_USER_VALIDATION_RULES        TO PUBLIC;\r\n" + 
	"GRANT SELECT ON MDT_CHANGE_ID       TO PUBLIC;\r\n" + 
	"GRANT SELECT ON MDT_CREATEINDEX_ID  TO PUBLIC;\r\n" + 
	"GRANT SELECT ON MDT_HISTORY_ID      TO PUBLIC;\r\n" + 
	"GRANT SELECT ON MDT_NOTE_ID         TO PUBLIC;\r\n" + 
	"GRANT SELECT ON MDT_OBJECT_ID       TO PUBLIC;\r\n" + 
	"GRANT SELECT ON MDT_RECYCLEBIN_ID   TO PUBLIC;\r\n" + 
	"GRANT SELECT ON MDT_SESSION_ID      TO PUBLIC;\r\n" + 
	"GRANT SELECT ON MDT_UPDATE_ID       TO PUBLIC;\r\n" + 
	
	"GRANT EXECUTE ON MDT_UTILS          TO PUBLIC;\r\n" + 
	"GRANT EXECUTE ON MDT_MERGE_FLAGS    TO PUBLIC;\r\n" + 
	"GRANT EXECUTE ON MDT_AGG_FLAGS      TO PUBLIC;\r\n" + 
	"GRANT EXECUTE ON BLOB_TO_CLOB       TO PUBLIC;\r\n" + 
	"GRANT EXECUTE ON MDT_BLOB           TO PUBLIC;\r\n" + 
	"GRANT EXECUTE ON MDT_COLLECT_TABLE  TO PUBLIC;\r\n" ;
}
