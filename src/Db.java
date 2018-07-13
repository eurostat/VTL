

/*
   DB interface: 
      SQL primitives 
      Operations on mdt system tables.
*/

import java.util.* ;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.sql.* ;

public class Db {

public static Connection 	DbConnection = null ;
static boolean 				Connected = false ;
public static Statement 	sql_statement ;
public static String  		db_username    ; 
public static boolean 		db_readonly ;
public static String  		db_url    ; 
public static String  		db_password    ; 
public static String 		owner_of_system_tables ;
public static int			db_session_id ;

public static String mdt_object_id 			;
public static String mdt_change_id 			;
public static String mdt_session_id			;
public static String mdt_createindex_id		;
public static String mdt_recyclebin_id 		;
public static String mdt_blob_id 			;
public static String mdt_update_id 			;
public static String mdt_metabase_version	 	;
public static String mdt_blob_type				;
public static String mdt_history_id				;

public static String mdt_objects      		;
public static String mdt_objects_comments   ;
public static String mdt_dimensions   		;
public static String mdt_positions   		;
public static String mdt_equations   		;
public static String mdt_equations_items   	;
public static String mdt_equations_tree   	;
public static String mdt_sources   	        ;
public static String mdt_audits   	        ;
public static String mdt_profiles   	    ;
public static String mdt_modifications 	 	;
public static String mdt_dependencies 	 	;
public static String mdt_syntax_trees 	 	;
public static String mdt_validation_rules 	;
public static String mdt_validation_conditions 	;
public static String mdt_privileges 	;
public static String mdt_merge_flags 	;
public static String mdt_history				;

public static String mdt_user_objects 			;
public static String mdt_user_objects_comments 	;
public static String mdt_user_positions   		;
public static String mdt_user_dimensions   		;
public static String mdt_user_equations   		;
public static String mdt_user_equations_items 	;
public static String mdt_user_equations_tree  	;
public static String mdt_user_sources  			;
public static String mdt_user_audits  			;
public static String mdt_user_profiles  		;
public static String mdt_user_modifications 	;
public static String mdt_user_dependencies 	 	;
public static String mdt_user_renamed_objects 	;
public static String mdt_user_dropped_objects 	;
public static String mdt_user_syntax_trees 	 	;
public static String mdt_user_sessions		 	;
public static String mdt_user_validation_rules 	;
public static String mdt_user_validation_conditions 	;
public static String mdt_user_privileges 		;
public static String mdt_user_audits_ins		;
public static String mdt_user_modifications_ins	;
public static String mdt_user_positions_ins		;
public static String mdt_user_objects_ins ;
public static String mdt_user_history			;

/*
 * Find owner of MDT system tables on remote database.
 */
static final boolean dbConnected ( )
{
	return ( Connected ) ;
}

/*
 * Find owner of MDT system tables on remote database.
 */
static String findOwnerOfSystemTables ( String db_link ) throws VTLError
{
	ListString 	items ;
	String		owner_of_system_tables ;

	items = sqlFillArray ( "SELECT owner FROM all_tables" + db_link + " WHERE table_name='MDT_OBJECTS'" ) ;
	
	if ( items.size() == 0 )
		VTLError . RunTimeError ( "Cannot find owner of MDT system tables (mdt_objects)" ) ;

	if ( items.size() > 1 )
		VTLError . RunTimeError ( "I have found more than 1 owner of MDT system tables (mdt_objects)" ) ;

	owner_of_system_tables = items . get ( 0 ) ;
	
	return ( owner_of_system_tables ) ;
}


/*
 * is current User the Owner Of System Tables.
 */
public static boolean isUserOwnerOfSystemTables ( ) throws VTLError
{
	return ( db_username.compareToIgnoreCase( owner_of_system_tables ) == 0 ) ;
}

/*
 * Find owner of MDT system tables.
 */
public static void getOwnerOfSystemTables ( ) throws VTLError
{
	ListString items ;
  
	// items = sql_fill_array ( "SELECT owner FROM all_tables WHERE table_name='MDT_OBJECTS'" ) ;
	items = sqlFillArray ( "SELECT '" + db_username + "' FROM user_tables WHERE table_name='MDT_OBJECTS'" ) ;
	// owner_of_system_tables = sql_get_value ( "SELECT owner FROM all_tables WHERE table_name='MDT_OBJECTS'" ) ;
  
	/* start
	if ( items.size() > 1 ) {
		owner_of_system_tables = UIConsole.chooseFromList("owners", items) ;
	}
	else 
		owner_of_system_tables = items.get ( 0 ) ; // end */
	if ( items.size() == 0 )
		VTLError . RunTimeError ( "Cannot find owner of VTL system tables (mdt_objects)" ) ;

	if ( items.size() > 1 && ! items.get(0).equals(db_username.toUpperCase()) )
		VTLError . RunTimeError ( "I have found more than 1 owner of VTL system tables (mdt_objects)" ) ;

	owner_of_system_tables = items.get ( 0 ) ;

	mdt_object_id 			= owner_of_system_tables + ".mdt_object_id" ;
	mdt_change_id			= owner_of_system_tables + ".mdt_change_id" ;
	mdt_session_id			= owner_of_system_tables + ".mdt_session_id" ;
	mdt_createindex_id		= owner_of_system_tables + ".mdt_createindex_id" ;
	mdt_recyclebin_id 		= owner_of_system_tables + ".mdt_recyclebin_id" ;
	mdt_blob_id				= owner_of_system_tables + ".mdt_blob_id" ;
	mdt_objects      		= owner_of_system_tables + ".mdt_objects" ;
	mdt_objects_comments    = owner_of_system_tables + ".mdt_objects_comments" ;
	mdt_dimensions   		= owner_of_system_tables + ".mdt_dimensions" ;
	mdt_positions   		= owner_of_system_tables + ".mdt_positions" ;
	mdt_equations   		= owner_of_system_tables + ".mdt_equations" ;
	mdt_equations_items   	= owner_of_system_tables + ".mdt_equations_items" ;
	mdt_equations_tree   	= owner_of_system_tables + ".mdt_equations_tree" ;
	mdt_sources   	        = owner_of_system_tables + ".mdt_sources" ;
	mdt_audits   	        = owner_of_system_tables + ".mdt_audits" ;
	mdt_profiles   	        = owner_of_system_tables + ".mdt_profiles" ;
	mdt_modifications	    = owner_of_system_tables + ".mdt_modifications" ;
	mdt_dependencies		= owner_of_system_tables + ".mdt_dependencies" ;
	mdt_syntax_trees		= owner_of_system_tables + ".mdt_syntax_trees" ;
	mdt_metabase_version	= owner_of_system_tables + ".mdt_metabase_version" ;
	mdt_validation_rules 	= owner_of_system_tables + ".mdt_validation_rules" ;
	mdt_validation_conditions 	= owner_of_system_tables + ".mdt_validation_conditions" ;
	mdt_privileges			= owner_of_system_tables + ".mdt_privileges" ;
	mdt_merge_flags 		= owner_of_system_tables + ".mdt_merge_flags"	;			// PL/SQL function
	mdt_user_objects 		= owner_of_system_tables + ".mdt_user_objects" ;
	mdt_user_objects_comments 	= owner_of_system_tables + ".mdt_user_objects_comments" ;
	mdt_user_positions   	= owner_of_system_tables + ".mdt_user_positions" ;
	mdt_user_dimensions   	= owner_of_system_tables + ".mdt_user_dimensions" ;
	mdt_user_equations   	= owner_of_system_tables + ".mdt_user_equations" ;
	mdt_user_equations_items= owner_of_system_tables + ".mdt_user_equations_items" ;
	mdt_user_equations_tree = owner_of_system_tables + ".mdt_user_equations_tree" ;
	mdt_user_sources  		= owner_of_system_tables + ".mdt_user_sources" ;
	mdt_user_audits  		= owner_of_system_tables + ".mdt_user_audits" ;
	mdt_user_profiles  		= owner_of_system_tables + ".mdt_user_profiles" ;
	mdt_user_modifications	= owner_of_system_tables + ".mdt_user_modifications" ;
	mdt_user_dependencies	= owner_of_system_tables + ".mdt_user_dependencies" ;
	mdt_user_renamed_objects= owner_of_system_tables + ".mdt_user_renamed_objects" ;
	mdt_user_dropped_objects= owner_of_system_tables + ".mdt_user_dropped_objects" ;
	mdt_user_syntax_trees	= owner_of_system_tables + ".mdt_user_syntax_trees" ;
	mdt_user_sessions		= owner_of_system_tables + ".mdt_user_sessions" ;
	mdt_user_validation_rules 	= owner_of_system_tables + ".mdt_user_validation_rules" ;
	mdt_user_validation_conditions	= owner_of_system_tables + ".mdt_user_validation_conditions" ;
	mdt_user_privileges		= owner_of_system_tables + ".mdt_user_privileges" ;
  
	mdt_user_audits_ins  	= owner_of_system_tables + ".mdt_user_audits_ins" ;
	mdt_user_modifications_ins	= owner_of_system_tables + ".mdt_user_modifications_ins" ;
	mdt_user_positions_ins	= owner_of_system_tables + ".mdt_user_positions_ins" ;
	mdt_user_objects_ins	= owner_of_system_tables + ".mdt_user_objects_ins" ;

	mdt_blob_type			= owner_of_system_tables + ".mdt_blob" ;
  
	mdt_history_id			= owner_of_system_tables + ".mdt_history_id";
	mdt_history				= owner_of_system_tables + ".mdt_history";
	mdt_user_history		= owner_of_system_tables + ".mdt_user_history";
	mdt_update_id			= owner_of_system_tables + ".mdt_update_id";
}

/*
  db_username = "refuser" ; // args [ 1 ] ;
  password = "refuser99" ; // args [ 2 ] ;
  connect_string = "thin:@oraldg1.cc.cec.eu.int:1537:APPESTP" ;  
  NB: DbConnection == null means that this is the first connection

	Solution for bug 1461 (found on the internet):
Properties properties = new Properties();
properties.put("user", username);
properties.put("password", password);
properties.put("oracle.jdbc.RetainV9LongBindBehavior", "true");        
Connection con = DriverManager.getConnection(url, properties);
*/
public static void dbConnect ( String url, String username, String password, boolean read_only ) throws SQLException, VTLError
{
	Connection 	newConnection ;
	
	try {	
		if ( DbConnection == null )
			DriverManager.registerDriver( new oracle.jdbc.driver.OracleDriver() );
		// Sys.println ( "Creating new connection ************") ;

		if ( Connected ) 
			disconnectDb ( ) ;
		try {
			String pureUrl = url.substring(url.indexOf("@")+1);
			url = "jdbc:oracle:thin:@" + pureUrl.trim() ; 
			username = username.trim().toUpperCase () ;
			password = password.trim() ; 
		}
		catch (Exception e) {
			VTLError.RunTimeError( e . toString () ) ;
		}

		newConnection = DriverManager.getConnection( url, username, password) ;	
		
	    DbConnection = newConnection ;
	    DbConnection.setAutoCommit ( false ) ;
	    sql_statement = DbConnection.createStatement () ;
	    Connected = true ;
	}
	catch ( SQLException e ) {
		VTLError . SqlError ( e . toString () ) ;
		return ;
	}
	
	db_url = url ;
	db_username = username ;
	db_password = password ;
	db_readonly = read_only ;
  
	if ( read_only )
		DbConnection.setReadOnly ( true ) ;
}

public static void readVTLMetabase ( ) throws VTLError
{
	String		tmp ;

	getOwnerOfSystemTables ( ) ;
  
	VTLObject.versionMetabase () ;			// VTLObject.checkMetabaseVersionSupported ( ) ;
											// sql "select * from REFSYS.UNUSED_MDT_METABASE_VERSION"
	UserProfile.getUserProfile ( ) ;
	// db_session_id = Integer . parseInt ( sql_get_value ("SELECT SYS_CONTEXT('USERENV','sessionid') FROM DUAL") ) ;
    
	if ( ( tmp = Db.sqlGetValue ("SELECT " + Db.mdt_session_id + ".nextval FROM DUAL") ) == null )
		VTLError.InternalError ( "mdt_session_id" ) ;
	Db.db_session_id = Integer . parseInt ( tmp ) ;
	sqlExec ( "INSERT INTO " + Db.mdt_user_sessions 
			  + "(session_id,user_name,os_userid,time_logon,version,ecas_username)" 
			  + "SELECT " + Db.db_session_id + ",USER,SYS_CONTEXT('USERENV','OS_USER'),SYSDATE"
			  + ",'',USER FROM DUAL" ) ;
	sqlCommit () ;
  
	/* 
	VTLObject	obj ;

	if ( ( ( obj = VTLObject.get_object_desc( "auto_open_session", false ) ) != null ) && obj.object_type == VTLObject.O_OPERATOR ) {
		try {
			Command.eval("auto_open_session ( );", false) ;		  
		}
		catch ( VTLError e ) {
			Sys.println("Connect to db: Error running function auto_open_session" ) ;	  
		}
	}*/
}

/*
 * Disconnect database (close connection).
 */
static void disconnectDb ( ) throws VTLError
{
	try {
		if ( ! Connected )
		  VTLError.RunTimeError( "Connection is closed - cannot close it again") ;
		// Sys.println ( "Closing connection ...") ;
		sqlExec ( "UPDATE " + Db.mdt_user_sessions + " SET time_logoff=SYSDATE WHERE session_id=" + Db.db_session_id ) ;
		History.saveHistory () ;
		VTLObject.closePreparedStatements() ;
		Dataset . removeAllTableDesc ( );
		Db.sqlCommit() ;
		DbConnection . close ( ) ;
		Connected = false ;
	}
	catch ( Exception e ) {
		VTLError . RunTimeError ( e . toString () ) ;
	}
}

/*
 * Check that connection has not been created as read-only.
 */
public static void checkConnectionNotReadOnly () throws VTLError
{
	if ( Db.db_readonly )
		VTLError.RunTimeError ( "Connection to database is read-only" ) ;	  
}

/*
 * Create Statement.
 */
static Statement createStatement () throws SQLException
{
	return ( Db.DbConnection.createStatement() ) ;
}

/*
 * Close Statement: must be put in a finally block (stmt be initialised to null). 
 * JDBC closes also the associated ResultSet (if any)
 */
static void closeStatement ( Statement stmt ) throws VTLError
{
	try {
		if ( stmt != null )
			stmt.close() ;		
	}
	catch ( SQLException e ) {
		VTLError.RunTimeError( e.toString() ) ;
	}
}

/*
 * Returns session id.
 */
static int sessionId () throws VTLError
{
	return ( db_session_id ) ;
}

/*
 * Commit current transaction.
 */
static void sqlCommit () throws VTLError
{
	try {
		DbConnection.commit () ;
	}
	catch ( SQLException e ) {
		VTLError.SqlError  ( e.toString () ) ;
	}
}

/*
 * Rollback current transaction.
 */
static void sqlRollback () throws VTLError
{
 	try {
 		if ( DbConnection != null )
 			DbConnection.rollback () ;
 	}
 	catch ( SQLException e ) {
 		VTLError.SqlError  ( e.toString () ) ;
 	}
}

/*
 * Lock Oracle table in exclusive mode.
 * What if 
 */
static void sqlLockTable ( String table_name ) throws VTLError
{
	try {
		// correction for open cursors
		Statement st = Db.createStatement();
		st.executeUpdate("lock table " + table_name + " in exclusive mode nowait");
		st.close();
		st=null;
		//sql_statement . executeUpdate ( "lock table " + table_name + " in exclusive mode nowait" ) ;
	  }
	catch ( SQLException e ) {
		if ( e.getErrorCode() == 54 ) // 4063 )		// ORA-00054
			VTLError.SqlError  ( "Object " + table_name 
					+ " has been locked by another user and is not available for update (" + e.getErrorCode() + ")" ) ;
		else 
			VTLError.SqlError  ( "Error locking " + table_name + ": " + e.getErrorCode() + "," + e.toString() ) ;
		
		// ORA-04063: view "REFIN.EBD_ALL" has errors
		/*		if ( e.getErrorCode() == 54 )
			AppError.SqlError  ( "Object: " + table_name + " has been locked by another user and is not available for update" ) ;
		Sys.printErrorMessage("SQL Error", message) ;
	*/
	}

	/*
	try {
		sql_exec ( "lock table " + table_name + " in exclusive mode nowait" ) ;
		Session.debug( "__lock table: " + table_name ) ;
	  }
	  catch ( AppError e ) {
		  if ( e.)
	    AppError.SqlError  ( "Object: " + table_name + " has been locked by another user and is not available for update" ) ;
	  }
*/
}

/*
 * Quote string: replace each ' with ''.
 */
static String sqlQuoteString ( String str )
{
	return ( str == null ? "''" : "'" + str.replaceAll ( "'", "''" ) + "'" ) ;
}

/*
 * Prepare statement
 */
final static PreparedStatement prepareStatement ( String sqlStatement ) throws SQLException
{
	return ( DbConnection.prepareStatement( sqlStatement ) ) ;
}


/*
 * Returns ( first) value returned by query.
 * Returns null if no rows are returned from db.
*/
static String sqlGetValue ( String sqlQuery ) throws VTLError
{
	return ( sqlGetValue ( sqlQuery, false ) ) ;
}

/*
 * Returns ( first) value returned by query.
 * Returns null if no rows are returned from db.
 * Generate an error if more than 1 column/row is returned
 */
static String sqlGetValue ( String sqlQuery, boolean checkNumberRowsColumns ) throws VTLError
{
	String	tmp = null ;
	ResultSet 	rs ;
	
	try {
		// change
		Statement stmt = DbConnection.createStatement() ;
		rs = stmt.executeQuery ( sqlQuery ) ;
		//
		// rs = sql_statement.executeQuery ( sql_query ) ;
		// rs = DbConnection . createStatement ().executeQuery ( sql_query ) ; 
		// var dim_sub string = sql_get ( "SELECT * from dual WHERE 1=2" )  ; ERROR 17401 Protocol violation
		rs.setFetchSize ( 1 ) ;
		
		if ( rs.next () ) 
		    tmp = rs.getString ( 1 ) ; 
		if ( checkNumberRowsColumns && rs.getMetaData().getColumnCount() != 1 )
			VTLError.RunTimeError ( "sql_get: query returns more than 1 column" ) ;
		if ( checkNumberRowsColumns && rs.next() )
			VTLError.RunTimeError ( "sql_get: query returns more than 1 row" ) ;
		
		rs.close () ;
		// change
		stmt.close () ;
	}
	catch ( SQLException e ) {
		VTLError . SqlError  ( e.toString () + "\nSQL query:\n" + sqlQuery ) ;
	}
	
	return ( tmp ) ;
}

/*
   Fill array with values returned by query.
*/
static ListString sqlFillArray ( String sql_query ) throws VTLError
{
  ListString 	items = new ListString ( 32 ) ;
  ResultSet 	rs ;

  try {
	  // corrections for open cursors
	  Statement st = Db.createStatement();
	  rs = st.executeQuery(sql_query);
	  
	  //rs = sql_statement . executeQuery ( sql_query ) ;
	  rs . setFetchSize ( 64 ) ;
	
	  while ( rs . next () ) 
		  items . add ( rs . getString ( 1 ) ) ;
	
	  rs . close () ;
	  st.close();
	  rs = null;
	  st=null;
  }
  catch ( SQLException e ) {
     VTLError . SqlError  ( e . toString () + "\nSQL query:\n" + sql_query ) ;
  }
  return ( items ) ;
}

/*
 * Execute SQL statement. 
 * Return number of rows affected (inserted/updated/deleted).
 */
static int sqlExec ( String sql_cmd ) throws VTLError 
{
	try {
		Statement st = Db.createStatement();
		int result = st.executeUpdate(sql_cmd);
		st.close();
		st=null;
		return result;
	}
	catch ( SQLException e ) {	
		String	str ;
		str = e.toString () ;
	  
		// Sys.println ( "SQL ERROR: " + str ) ;
		if ( str.indexOf( "ORA-") > 0 )
			str = str.substring( str.indexOf( "ORA-") ) ;
	  
		VTLError.SqlError  ( str + "\nSQL statement:\n" + sql_cmd ) ; // + " (" +e.getErrorCode() +")") ;
		return ( 0 ) ;
	}
/*
   // Oracle error code for "inserted value too large"
   if ( SQLCA.SQLDBCode = 1401 )		
      AppError . SqlError ("Dimension value is too large") ;
  // BUG: check for specific error code for lock
*/
}

/*
 * Execute SQL select statement. 
 * Print result on standard output / output file.
 */
public static void sqlUnload ( String sql_query, String fileName ) throws VTLError 
{
	int					n_columns ;
	ResultSet 			rs = null;
	ResultSetMetaData 	md ;
	Statement			stmt = null ;
	
	Sys.setFileName ( fileName ) ;
	Sys.output_open () ;

  	try {
  		rs = ( stmt = DbConnection.createStatement () ).executeQuery ( sql_query ) ;	
  		rs.setFetchSize ( 1024 ) ;	// 2048 ) ;
  		md = rs.getMetaData() ;
  		n_columns = md.getColumnCount() ;
  		
		for ( int idx = 1; idx <= n_columns; idx ++ ) {
			if ( idx > 1 )
				Sys.print( "\t" ) ; 
			Sys.print( md.getColumnName ( idx ).toLowerCase ( ) ) ; 
		}
		Sys.println() ;
  		
  		while ( rs . next () ) {
  			String	tmp ;
  			for ( int idx = 1; idx <= n_columns; idx ++ ) {
  				if ( idx > 1 )
  					Sys.print ( "\t" ) ; 
  				if ( ( tmp = rs.getString ( idx ) ) != null )
  					Sys.print ( tmp ) ;   	  					  				
  			}
  			Sys.println ( ) ; 
  		}

  		// rs . close () ;									
		Sys.output_close () ;
  	}
  	catch ( SQLException e ) {
  		VTLError . SqlError  ( e . toString () + "\nSQL query:\n" + sql_query ) ;
  	}
  	finally  {
  		Db.closeStatement ( stmt ) ;
  	}
}

/*
 * Execute SQL select statement. Return vector of rows.
 */
static Vector < Vector < String > > sqlGetRows ( String sql_query, int num_columns ) throws VTLError 
{
  int			idx ;
  ResultSet 	rs ;
  Vector <String>				row ;
  Vector < Vector < String > >	rows ;

  	rows = new Vector < Vector < String > > ( ) ;
  
  	try {
  		// corrections for open cursors
  	    Statement st = Db.createStatement();
  	    rs = st.executeQuery(sql_query);
  		//rs = sql_statement . executeQuery ( sql_query ) ;

  		while ( rs . next () )  {
  			row = new Vector <String> ( num_columns ) ;
  			for ( idx = 1; idx <= num_columns; idx ++ )
  				row.add ( rs . getString ( idx ) );
  			rows.add( row ) ;
  		}
  		rs . close () ;
  		st.close();
  		rs=null;
  		st=null;
  	}
  	catch ( SQLException e ) {
  		VTLError . SqlError  ( e . toString () + "\nSQL query:\n" + sql_query ) ;
  	}
   
  	return ( rows ) ;
}

/*
 * Delete object from system table. 
 */
public static void dbDelete ( String system_table, int object_id ) throws VTLError
{
  String sql_cmd ;

  sql_cmd = "DELETE FROM " + system_table + " WHERE object_id=" + object_id ;

  Db . sqlExec ( sql_cmd ) ;
}

/*
 * Returns true if SQL table exists in current schema.
 */
static boolean sqlUserTableExists ( String objectName ) throws VTLError
{
	String	sqlQuery ;

	sqlQuery = "SELECT '0' FROM user_tables WHERE table_name='" + objectName.toUpperCase () + "'" ;

	return ( sqlGetValue ( sqlQuery ) != null ) ;
}

/* 
 * Get SQL columns of table mdt_objects_comments.
 */
static Query getDescObjectsComments ( String dblink ) throws VTLError
{
	int					idx, n_columns, colType ;
	String				colName, vtlType = null ;
	Query				tab = null ;
	ResultSet 			rs ;
	ResultSetMetaData 	md ;

	tab = new Dataset ( "mdt_objects_comments", VTLObject.O_DATASET ) ;

	try {
		// corrections for open cursors
		Statement st = Db.createStatement();
		rs = st.executeQuery( "SELECT * FROM " + Db . mdt_objects_comments + ( dblink == null ? "" : dblink ) + " WHERE 1=0") ;
		md = rs . getMetaData() ;
	    rs.close() ;
	    n_columns = md . getColumnCount() ;
	     
        for ( idx = 1; idx <= n_columns; idx ++ ) {
        	 colName = md . getColumnName ( idx ) .toLowerCase () ;
        	 if ( colName . compareTo ( "user_name" ) != 0 && colName . compareTo ( "object_id" ) != 0 ) {
        		 colType = md . getColumnType ( idx ) ;
        		 switch ( colType ) {
	      		 	case Types . CHAR :
	     		 	case Types . VARCHAR :
	    		 		vtlType = "string" ;
	     		 		break ;
	    		 	case Types . DECIMAL :
	    		 	case Types . DOUBLE :
	    		 	case Types . NUMERIC :
	    		 		vtlType = "number" ;
	    		 		break ;
	    		 	case Types . DATE :
	    		 	case Types . TIMESTAMP :
	    		 		vtlType = "date" ;
	     		 		break ;
	    		 	case java.sql.Types.STRUCT :	
	    		 		vtlType = "blob" ;
	    		 		break ;
	    		 	case java.sql.Types.CLOB :	
	    		 		vtlType = "clob" ;
	    		 		break ;
	     		 	default : 
	     		 		VTLError . InternalError ( "Type not found: " + colType ) ;
        		 }
        		 if ( vtlType == null )
        			 VTLError . InternalError ( "Type null: " + colType ) ;
        		 tab.addMeasure(colName, vtlType, null, 0, md.isNullable( idx ) == ResultSetMetaData.columnNullable );
        	 }
	     }
         st.close();
         rs = null;
         md = null;
         st = null;
	}
   catch ( SQLException e ) {
	     VTLError . SqlError  ( e . toString () ) ;
   }
   
   return ( tab ) ;
}

/*
   Inserts the source text of an object into mdt_sources.
   The string is split after the last newline in each piece (to be able to to find a string in all sources).
*/
static void insertSource ( int object_id, String object_definition ) throws VTLError
{
  int				buffer_index, i_start, i_end, str_len, max_end ;
  String			buffer_text, str, insert_part ;
  PreparedStatement pstmt ;
  
  str = object_definition ; 
  
  buffer_index = 0 ;
  i_start = 0 ;
  str_len = str . length () ;

  insert_part = "INSERT INTO " + Db . mdt_user_sources + "(object_id,buffer_index,buffer_text)VALUES(" + object_id + ",?,?)" ;
  
  try {
	pstmt = prepareStatement ( insert_part ) ;
  
  	while ( i_start < str_len ) {
		// find last newline in each piece (sql column buffer_text: varchar(4000))
		max_end = i_start + 3999 ;
		if ( max_end >= str_len )
		   i_end = str_len ;
		else {
		   i_end = str . lastIndexOf ( '\n', max_end ) ;
	
		   if ( i_end <= i_start )
			   i_end = max_end ;
		   i_end ++ ;
	}

	buffer_text = str . substring ( i_start, i_end ) ;		// "'" + str . substring ( i_start, i_end ) + "'" ;
	buffer_index ++ ;
	pstmt . setInt ( 1, buffer_index ) ;
	pstmt . setString ( 2, buffer_text ) ;
	pstmt . executeUpdate () ;

	i_start = i_end ;
  }
  pstmt.close() ;
  } catch ( SQLException e ) {
	  VTLError . SqlError( e . toString ()  ) ;
  }
}

static int	uniqueIndexTemporaryName = 1 ;

/*
 * make temporary table name
 */
static String temporaryTableName ( String table_prefix )
{
  String	str = ( table_prefix.length () >= 10 ? table_prefix.substring ( 0, 10) : table_prefix ) ;

  return ( "tmp$" + db_session_id + str + uniqueIndexTemporaryName++ ) ;
}

/*
 * not used anymore
static boolean isTemporaryTable ( String table_name )
{
	return ( table_name.startsWith( "tmp$" )) ;
}
*/

/*
 * Returns true if SQL table has primary key.
 */
static boolean tableHasPrimaryKey ( String dsName ) throws VTLError
{
	int	idx ;
	if ( ( idx = dsName.indexOf( Parser.ownershipSymbol ) ) > 0 )
		dsName = dsName.substring (idx + 1 ) ;
	return ( sqlGetValue ( "SELECT constraint_name FROM sys.user_constraints WHERE constraint_type='P' AND table_name='" 
				+ dsName.toUpperCase () + "'" ) != null ) ;
}

/*
 * 
 */
public static void colDesc ( Dataset ds, ListString columns, boolean file_header_is_dim [], int columnDataTypeLength  [], 
		boolean columnCanBeNull  [] ) throws VTLError, SQLException
{
	for ( int idx = 0; idx < columns.size(); idx ++ ) {
		DatasetComponent c ;
		if ( ( c = ds.getDimension( columns.get(idx) ) ) != null ) {
			file_header_is_dim [ idx ] = true ;
		}
		else if ( ( c = ds.getMeasureAttribute( columns.get(idx) ) ) != null ) {
			file_header_is_dim [ idx ] = false ;			
		}
		else
			VTLError.TypeError( "Load, component " + columns.get(idx) + " not found in dataset: " + ds.dsName );
		String dataType = c.compType ;
		if ( ! Check.isPredefinedType(dataType))
			dataType = Dataset.getValuedomainBaseType(dataType) ;
		switch ( dataType ) {
			case "integer" : columnDataTypeLength [idx] = 0 ; break ;
			case "number" : columnDataTypeLength [idx] = -1 ; break ;
			case "date" : columnDataTypeLength [idx] = -2 ; break ;
			case "time_period" : columnDataTypeLength [idx] = -3 ; break ;
			case "string" : if ( Check.isPredefinedType(c.compType) )
								columnDataTypeLength [idx] = c.compWidth ;
							else
								columnDataTypeLength [idx] = Dataset.getValuedomainBaseWidth( c.compType ) ; 
							break ;
			default : VTLError.InternalError( "case not implemented");
		}
		columnCanBeNull [ idx ] = c.canBeNull ;
	}
	
    for ( DatasetComponent dim : ds.dims ) {
    	if ( columns.indexOf ( dim.compName ) < 0 ) 
    		VTLError.RunTimeError( "Load file, dimension " + dim.compName + " not found in file" ) ;
    }
    for ( DatasetComponent c : ds.getMeasures() ) {
    	if ( columns.indexOf ( c.compName ) < 0 && c.canBeNull == false ) 
    		VTLError.RunTimeError( "Load file, not null measure " + c.compName + " not found in file" ) ;
    }
    for ( DatasetComponent c : ds.getAttributes() ) {
    	if ( columns.indexOf ( c.compName ) < 0 && c.canBeNull == false ) 
    		VTLError.RunTimeError( "Load file, not null attribute " + c.compName + " not found in file" ) ;
    }
}

/*
 * Check key uniqueness
 */
static boolean checkUniqueKey ( String sqltableName, Dataset tab, StringBuffer dups ) throws VTLError
{
	ListString	lsDups, lsDupsSameValues ;
	String		num ;
	int			deleted_rows ;

	lsDups = Db.sqlFillArray( "SELECT " + tab.stringAllComponents("||'\t'||") 
			+ " FROM " + sqltableName 
			+ " WHERE (" + tab.stringDimensions(',') 
			+ ") IN ( SELECT " + tab.stringDimensions(',') + " FROM (SELECT " + tab.stringDimensions(',') 
			+ ",row_number() OVER (PARTITION BY " 
			+ tab.stringDimensions(',') + " ORDER BY " + tab.stringMeasuresAttributes( ',' )
			+ ") AS num FROM " + sqltableName + ") WHERE num > 1 )" ) ;    			

    if ( lsDups.size() == 0 )
		return ( true ) ;
	
	num = Db.sqlGetValue( "SELECT COUNT(*) FROM ( SELECT rank() OVER (PARTITION BY " 
					+ tab.stringDimensions(',') + " ORDER BY " + tab.stringMeasuresAttributes( ',' )
					+ ") AS num FROM " + sqltableName + ") WHERE num > 1 " ) ;
	
	if ( num.compareTo ( "0" ) == 0 ) {
		// duplicated keys have the same values 
		
		String tmpS = "SELECT " + tab.stringAllComponents("||'\t'||") 
				+ " FROM " + sqltableName 
				+ " WHERE rowid IN ( SELECT rowid FROM (SELECT rowid,row_number() OVER (PARTITION BY " 
				+ tab.stringDimensions(',') + " ORDER BY " + tab.stringMeasuresAttributes( ',' )
				+ ") AS num FROM " + sqltableName + ") WHERE num > 1 )";
		
		lsDupsSameValues = Db.sqlFillArray( tmpS ) ;
		
		//Sys.print("Selected values: " + tmpS + "\n");
		
		dups.append( "File loaded with warnings (duplicated keys with the same values of the properties) - deleted " 
				+ lsDupsSameValues.size() + " rows:\n" ) ;
		dups.append( tab.stringAllComponents("\t") ).append( "\n" ) ;
		for ( String s : lsDupsSameValues )
			dups.append( s ).append( "\n" ) ; 			
		deleted_rows = Db.sqlExec( "DELETE FROM " + sqltableName 
				+ " WHERE rowid IN ( SELECT rowid FROM (SELECT rowid,row_number() OVER (PARTITION BY " 
				+ tab.stringDimensions(',') + " ORDER BY " + tab.stringMeasuresAttributes( ',' )
				+ ") AS num FROM " + sqltableName + ") WHERE num > 1 )" ) ;
		if ( deleted_rows != lsDupsSameValues.size () )
			VTLError.InternalError( "Different n. of rows: " + deleted_rows + "," + lsDupsSameValues.size () ) ;
		return ( true ) ;
	}
	else {
		dups.append ( "Found " + lsDups.size() + " duplicated keys with different values of the properties:\n" ) ;
		dups.append( tab.stringAllComponents("\t") ).append( "\n" ) ;
		for ( String s : lsDups )
			dups.append( s ).append( "\n" ) ; 		
		return ( false ) ;
	}
}

static String prepareMerge ( String tableName, Dataset tab, String tmpLoadTable, ListString file_header, boolean file_header_is_dim[] )
{
	StringBuffer 	sql_query = new StringBuffer ( 100 ) ;
	int				idx, num_header_items = file_header.size() ;
	boolean			skip_first ;
	// merge statement
	sql_query.append( "MERGE INTO " ).append( tableName ).append( " a$ USING (SELECT " )
			.append( tab.stringAllComponents(','))
			.append( " FROM " ).append( tmpLoadTable ).append( ") b$ ON (" ) ;
	
	for ( idx = 0, skip_first = false; idx < num_header_items; idx ++ ) {
		if ( file_header_is_dim [ idx ] == true ) {
	    	if ( skip_first )
	    		sql_query.append( " AND " ) ;
	    	else
	    		skip_first = true ;
			sql_query.append( "a$." ).append( file_header.get(idx) ).append("=b$.").append( file_header.get(idx) ) ;            		
		}
	}
	sql_query.append( ") WHEN MATCHED THEN UPDATE SET " ) ;
	for ( idx = 0, skip_first = false; idx < num_header_items; idx ++ ) {
		if ( file_header_is_dim [ idx ] == false ) {
	    	if ( skip_first )
	    		sql_query.append( "," ) ;
	    	else
	    		skip_first = true ;            		
			sql_query.append( file_header.get(idx) ).append("=b$.").append( file_header.get(idx) ) ;
		}
	}
	sql_query.append( " WHEN NOT MATCHED THEN INSERT(" ).append ( file_header.toString( ',' ) ) .append( ")VALUES(" ) ;
	for ( idx = 0; idx < num_header_items; idx ++ ) {
		if ( idx > 0 )
			sql_query .append( ',' ) ;
		sql_query.append( "b$." ).append( file_header.get(idx) ) ;
	}
	sql_query . append( ')' ) ;						// System.out.println ( sql_query ) ; 
	return( sql_query.toString() ) ;
}

/*
 * Manage autoextend at the end of data loading
 */
static void autoExtend ( ListString file_header, Dataset tab, ListString codeLists[], 
					boolean file_header_is_dim [ ], boolean mergeData ) throws VTLError
{
	for ( int idx = 0; idx < file_header.size(); idx ++ ) {
		if ( file_header_is_dim [ idx ] ) {
	   		ListString 	new_dim_values = codeLists[idx] ;
    		DatasetComponent dim = tab.getDimension( file_header.get( idx ) ) ;
    		if ( tab.objectType == VTLObject.O_VALUEDOMAIN ) {				
            	if ( mergeData )												// merge: add new codes 
            		tab.modify_positions ( 0, "add", new_dim_values.minus( dim.dim_values ), -1 ) ;
            	else
            		tab.modify_positions ( 0, "all", new_dim_values, 0 ) ;  	// replace: replace all codes
    		}
    		else {										// it is a data object: extend dimension time
        		if ( Check.isValueDomainType( dim.compType ) ) {		// if possible, sort by valuedomain order 
        			ListString ls2 = Dataset.getValuedomainCodeList( dim.compType ) ;
        			if ( ls2 == null )
        				VTLError.InternalError( "Load: " + dim.compName + " of type " + dim.compType );
        			new_dim_values = new_dim_values.sort_by_list( ls2 ) ;
        		}
        		new_dim_values = dim.dim_values.merge( new_dim_values ).sort_asc() ;			// sort all codes
        		tab.modify_positions ( tab.getDimensionIndex(dim.compName), "all", new_dim_values, 0 ) ;							// replace: replace all codes
    		}	
		}    			
	}
}

static void addMessage ( StringBuffer errors, String fields[], boolean fileHeaderIsDim [ ], String errorcode, String errorlevel ) 
{
	if ( errors.length() > 0 )
		errors.append( " UNION " ) ;
	errors.append( "SELECT " ) ;
	for ( int idx = 0; idx < fields.length; idx ++ ) {
		if ( fileHeaderIsDim [ idx ] )
			errors.append( "'" ).append( fields[idx] ).append( "'" ).append( ',') ;
	}
	errors.append( "'" ).append( errorcode ).append( "'" ) ;
	errors.append( ",'" ).append( errorlevel ).append( "'" ) ;
	errors.append( " FROM DUAL " ) ;
}

/*
 * Load data file into table.
 * update aa_mc with empty ;
 * sql "select * from aa_mc" ;
 * load "d:\aa_mc.dat" into aa_mc merge ;
 * BUG: with option merge when no properties are specified in the header line (ORA-00927: missing equal sign) 
 * it is rather an error that must be signalled in advance
 * Max. n. of errors: 50
 * auto_extend can extend a valuedomain (positions of the dimension) or the time positions (for a table/template)
 * added duplicates to log file
 * Sample command: load "H:\MyDocuments\aact_ali01.dat" into aact_ali01_copy add ;
 	performance improvement using executeBatch + temporary table
 	check uniqueness if table has no primary key
 	skip rows with all properties = null
 	
 	load "P:\VIP_Validation\VTL\NA\Create objects\NAMAIN_T0101 bad.dat" into na_main2
 */
public static void loadDataFile ( String fileName, String tableName, String optionData, boolean autoExtend, 
			String separator, boolean compile_only ) throws VTLError
{
	LineNumberReader	infile = null ;
	String				fields[] ;
	int					countNotNulls[] ;
	StringBuffer		sqlInsert ;
	String				period_min = "", period_max = "" ;
	Dataset				tab ;
	String				error_header, str, tmpLoadTable = null ;
	PreparedStatement	sqlStatement ;
	int					cells_deleted = 0, num_rows, cells_inserted = 0, num_header_items, 
						dim_index_time_period = -1, num_errors = 0 ;
	boolean				allPropertiesNull, mergeData, fileHeaderIsDim [], columnCanBeNull [] ;
	ListString			file_header ;
	StringBuffer		errorMessage = new StringBuffer ( 100 ) ;
	StringBuffer		dups = new StringBuffer ( 100 ) ;
	int					columnDataTypeLength [] ;
	int					rowsAllPropertiesNull = 0 ;
	DatasetComponent	timeDim ;

	Db.checkConnectionNotReadOnly () ;					
	
	tab = Dataset.getDatasetDesc(tableName) ;		
	
	// options: default is replace
	if ( optionData == null )
		optionData = "replace" ;

	switch ( optionData ) {
		case "replace" : mergeData = false ; break ;
		case "merge" : mergeData = true ; break ;
		default : mergeData = true ; VTLError.RunTimeError ( "Load: invalid option " + optionData ) ;	
	}

	if ( separator.length() > 1 || "\t ;,".indexOf( separator ) < 0 )
		VTLError.RunTimeError ( "Load: separator must be one of: tab, space, semicolon, comma" ) ;

	if ( compile_only )
		return ;
	
	error_header = "Loading file " + fileName ;

	try {
		infile = new LineNumberReader ( new BufferedReader ( new InputStreamReader ( new FileInputStream ( fileName ), "ISO-8859-1" ) ) ) ;
        if ( ( str = infile.readLine ( ) ) == null )
        	VTLError.RunTimeError ( fileName + ": file is empty" ) ;
        fields = str.toLowerCase().split( separator ) ;					// convert to lowercase
        num_header_items = fields.length ;
    	file_header = new ListString( num_header_items );
    	file_header.addAll ( Arrays.asList(fields ) ) ;
    	countNotNulls = new int[ num_header_items ] ;
    	Arrays.fill( countNotNulls, 0 ) ; 
    	fileHeaderIsDim = new boolean [ num_header_items ] ;
    	columnDataTypeLength = new int [ num_header_items ] ;
    	columnCanBeNull = new boolean [ num_header_items ] ;
        colDesc ( tab, file_header, fileHeaderIsDim, columnDataTypeLength, columnCanBeNull ) ;
        
		if ( ( timeDim = tab.getTimePeriodDimension() ) == null ) {			
			dim_index_time_period = -1 ;
		}
		else {
			dim_index_time_period = file_header.indexOf( timeDim.compName ) ;	// index of time period
	        period_min = "9" ;
	        period_max = "0" ;    	
		}
        
        sqlInsert = new StringBuffer () ;
        
    	// insert statement
        tmpLoadTable = Db.temporaryTableName( "ERR$_" + tableName) ;
        sqlInsert.append( "INSERT INTO " + tmpLoadTable + '(' + file_header.toString( ',' ) + ")VALUES(" ) ;
        for ( int idx = 0; idx < num_header_items; idx ++ ) {
        	if ( idx > 0 )
        		sqlInsert .append( ',' ) ;
        	if ( columnDataTypeLength [ idx ] == -2 )
        		sqlInsert.append( "to_date(?,'YYYYMMDDHH24MISS')" ) ;
        	else 
        		sqlInsert.append( '?' ) ;
        }
        sqlInsert . append( ')' ) ;						// System.out.println ( sql_query ) ;
        
        // start database operations -implicit start transaction
        Db.sqlExec( "CREATE TABLE " + tmpLoadTable 
        			+ " PCTFREE 0 NOLOGGING AS SELECT * FROM " + tableName + " WHERE 1=0") ;
        
        Db.sqlLockTable(tableName);
        
        if ( ! mergeData )
        	cells_deleted = Db.sqlExec( "DELETE FROM " + tableName ) ;
        
        sqlStatement = Db.prepareStatement(sqlInsert.toString()) ;
                
        // new
        ListString codeLists [] = new  ListString [num_header_items] ;
        for ( int idx = 0; idx < num_header_items; idx ++ )
        	codeLists [idx] = new ListString();
        
        while ( ( str = infile.readLine ( ) ) != null ) {
            try { // Sys.println ( str ) ;
	        	// if ( infile.getLineNumber() % 1000 == 0 ) Sys.displayStatusMessage( "Reading line: " + infile.getLineNumber() ) ;			
	        	if ( str.length() == 0 )
	        		VTLError.RunTimeError( "Empty line" ) ;
	        	fields = str.split( separator ) ; 
	        	if ( fields.length > num_header_items )
    				// Db.addMessage(errors, fields, fileHeaderIsDim, "Too many fields", "Error" );
	        		VTLError.RunTimeError( "Too many fields" ) ;
	        	if ( fields.length == 0 )
	        		VTLError.RunTimeError( "Empty line" ) ;
	        
	        	for ( int idx = 0; idx < fields.length; idx ++ ) {
	        		String tmp = fields[idx].trim() ;	
	        		
	        		if ( tmp != null && tmp.length() > 0 )					// count not null values for all properties (dimensions cannot be null)
	        			countNotNulls [ idx ] ++ ;
		        	if ( idx == dim_index_time_period ) {					// compute time period min. and max.
		        		if ( tmp . compareTo ( period_min ) < 0 )
		        			period_min = tmp ;
		        		if ( tmp . compareTo ( period_max ) > 0 )
		        			period_max = tmp ;
		        	}
	        		// new
		        	if ( autoExtend ) {
		        		if ( fileHeaderIsDim [ idx ] ) { 		// {idx == dim_index_auto_extend ) {
			        		ListString ls = codeLists[idx] ;
			        		if ( ls.contains( tmp ) ) {
				        		if ( tab.objectType == VTLObject.O_VALUEDOMAIN )
				        			VTLError.RunTimeError( error_header + " - duplicated position: " + tmp + " at line number " + infile.getLineNumber()) ;	        					        			
			        		}
			        		else
			        			ls.add( tmp ) ;		        			
		        		}
		        	}
	        	}
	        	
	        	for ( int idx =  fields.length ; idx < num_header_items; idx ++ )
	        		sqlStatement.setString ( idx + 1, null ) ;
	        	
	        	allPropertiesNull = true ;
	        	for ( int idx = 0; idx < fields.length; idx ++ ) {
	        		String tmp ;
	        		if ( ( tmp = fields [ idx ] ) != null && tmp.length() > 0 ) {
		        		if ( columnDataTypeLength [ idx ] > 0 ) {
		        			if ( tmp.length() > columnDataTypeLength [ idx ] )
		        				// idx: index in the row
		        				// Db.addMessage(errors, fields, fileHeaderIsDim, "Value too large: " + tmp + " for " + file_header.get(idx), "Error" );
		        				VTLError.RunTimeError( "Load, Value too large: " + tmp + " for " + file_header.get(idx) ) ;
		        		}
	        			else if ( columnDataTypeLength [ idx ] == -1 ) {
	        				if ( ! Check.isNumber( tmp ) ) 
		        				// Db.addMessage(errors, fields, fileHeaderIsDim, "Load, Not a number: " + tmp + " for " + file_header.get(idx), "Error" );
	        					VTLError.RunTimeError( "Load, Not a number: " + tmp + " for " + file_header.get(idx) ) ;
	        			}
	        			else if ( columnDataTypeLength [ idx ] == 0 ) {
	        				if ( ! Check.isInteger( tmp ) )
		        				// Db.addMessage(errors, fields, fileHeaderIsDim, "Load, Not an integer: " + tmp + " for " + file_header.get(idx), "Error" );
	        					VTLError.RunTimeError( "Load, Not an integer: " + tmp + " for " + file_header.get(idx) ) ;
	        			}
	        			else if ( columnDataTypeLength [ idx ] == -2 ) {			// date - delete T because it does not work
	        				if ( ( fields [ idx ] = Check.toDate( tmp ) ) == null )
	        					VTLError.RunTimeError( "Load, bad date value: " + tmp + " for " + file_header.get(idx) );
	        			}
	        			else if ( columnDataTypeLength [ idx ] == -3 ) {			// time_period
	        				if ( ( fields [ idx ] = Check.toTimePeriod( tmp ) ) == null )
	        					VTLError.RunTimeError( "Load, bad time_period value: " + tmp + " for " + file_header.get(idx) );
	        			}
		        		
		        		if ( fileHeaderIsDim [ idx ] == false )
		        			allPropertiesNull = false ;
		        		sqlStatement.setString ( idx + 1, fields [ idx ] ) ;		// value of this file has been possibly changed
	        		}
	        		else {
	        			if ( columnCanBeNull [ idx ] == false )
	        				VTLError.RunTimeError( "Value of not null measure/attribute is null: " + fields [ idx ] ) ;
		        		sqlStatement.setString ( idx + 1, null ) ;	
	        		}
	        	}
	        	
	        	if ( allPropertiesNull ) 
	        		rowsAllPropertiesNull ++ ;			// skip rows with all properties have a null value
	        	else {		
		        	sqlStatement.addBatch() ;
		        	if ( infile.getLineNumber() % 200 == 0 )	
		        		sqlStatement.executeBatch() ;	
	        	}
            }
            catch ( VTLError e ) {
            	if ( num_errors ++ > 100 ) {
                	errorMessage.append( "Too Many errors (100) - exit\n") ;
                	break ;            		
            	}
        		errorMessage.append( e.getErrorMessage() ).append( " at line ").append( infile.getLineNumber() ).append( "\n") ;
            }
        }
        
        num_rows = infile.getLineNumber() - 1 ; 			// merge: cells deleted = cells inserted
           
        // close file and statement
    	infile.close() ;
    	infile = null ;
        sqlStatement.executeBatch() ;	
        sqlStatement.close();
        sqlStatement = null ;

        // Db.sql_unload( "SELECT * FROM " + tmpLoadTable, true, false) ;
        // Session.sqlDebugging = true ;
        Sys.displayStatusMessage("Checking key uniqueness");
        if ( ! checkUniqueKey ( tmpLoadTable, tab, dups ) )
        	num_errors ++ ;
            
    	if ( autoExtend && num_errors == 0 )			// auto extend: modify list of positions
    		autoExtend( file_header, tab, codeLists, fileHeaderIsDim, mergeData ) ;
        	
		// verify data after that possibly new codes have been added to table definition
		Vector<Object[]> 	data = new Vector<Object[]> () ;
		int					dim_index ;
		Sys.displayStatusMessage("Checking values of dimensions");
		for ( dim_index = 0; dim_index < tab.getDims().size (); dim_index ++ )
			tab.verifyDimensionValues ( dim_index, tmpLoadTable, data ) ;
		Sys.displayStatusMessage("Checking values of properties");
		for ( DatasetComponent pro : tab.getMeasures() )
			tab.verifyPropertyValues (pro.compName, pro.compType, tmpLoadTable, data ) ;

        if ( data.size() > 0 ) {
        	num_errors ++ ;
        	errorMessage.append( "Positions not valid:\n") ;
        	for ( Object[] o : data ) {
        		for ( Object i : o )
        			errorMessage.append( i ).append( " " ) ;
        		errorMessage.append ( "\n" ) ;
        	}
        }

        if ( num_errors == 0 ) {
            if ( mergeData ) {
            	Sys.displayStatusMessage("Merging data ...");
            	cells_deleted = cells_inserted = Db.sqlExec( prepareMerge ( tableName, tab, tmpLoadTable, file_header, fileHeaderIsDim ) ) ;               	
            }
            else {
            	Sys.displayStatusMessage("Inserting data ...");
                try {
                	cells_inserted = DbConnection.createStatement ().executeUpdate ( "INSERT INTO " + tableName + " SELECT * FROM " + tmpLoadTable ) ;
                }
                catch ( SQLException e ) {
                	if ( e.getErrorCode() == 1 )
                		VTLError.RunTimeError( "load: key uniqueness violated" ) ;
                }
            }            	
        }
        Sys.displayStatusMessage("");
        
    	StringBuffer loadStats = new StringBuffer (100) ;
    	loadStats.append( "File: " + fileName 
    			+ "\nRows: " + num_rows 
    			+ "\nTime min: " + (period_min.length() == 1 ? "" : period_min ) 
    			+ "\nTime max: " + (period_max.length() == 1 ? "" : period_max ) 
    			+ "\nMode: " + optionData + "\n" ) ;
    	if ( rowsAllPropertiesNull > 0 )
    		loadStats.append( "Rows skipped because all properties have a null value: " + rowsAllPropertiesNull + "\n" ) ;
    	    	
    	for ( int idx = 0; idx < file_header.size(); idx ++ )
    		if ( fileHeaderIsDim [ idx ] == false )
    			loadStats.append( file_header.get( idx ) + ": " + countNotNulls [ idx ] + " not null values\n" ) ;

    	if ( num_errors > 0 ) {
    		loadStats.append( "\nERRORS: " + num_errors + "\n" ) ;
        	loadStats.append(errorMessage) ;
        	loadStats.append(dups) ;
        	Db.sqlRollback() ;
        	if ( tmpLoadTable != null )
        		Db.sqlExec( "DROP TABLE " + tmpLoadTable ) ;
        	tmpLoadTable = null ;
        	Audit.start ( ) ;	// normally in Command.eval			
            Audit.set("load", 0, 0, tableName, tab.objectId, null, null, "ERRORS", loadStats.toString() ) ;
        	Audit.finish ( "load " + fileName + " " + optionData + ( autoExtend ? " autoextend" :  "" ) ) ;	
        	VTLError.RunTimeError( errorMessage.append(dups).toString() ) ;
    	}
    	else {
    		if ( dups.length() > 0 )
    			loadStats.append ( "\n" ).append( dups ) ;
        	Audit.set("load", cells_deleted, cells_inserted, tableName, tab.objectId, period_min, period_max, 
        			(dups.length() > 0 ? " File loaded with warnings" : "File successfully loaded" ), loadStats.toString() ) ;            
        	if ( autoExtend ) {
            	Dataset.removeTableDesc ( tab.objectId ) ;
            	VTLObject.setObjectModified ( tab.objectId ) ;
        	}
        	else 
                Db.sqlCommit() ;
        	if ( tmpLoadTable != null )
        		Db.sqlExec( "DROP TABLE " + tmpLoadTable + " PURGE" ) ;	
            tmpLoadTable = null ;
    	}
	}
	catch ( Exception e ) {
		Db.sqlRollback() ;
    	if ( tmpLoadTable != null )
    		Db.sqlExec( "DROP TABLE " + tmpLoadTable  + " PURGE" ) ;	
    	if ( infile != null )
    		try {
    			infile.close() ;
    		}
    	catch ( Exception e1) {
    		VTLError.RunTimeError( e1.toString() + "\n" + e.toString() ) ;    		
    	}
		VTLError.RunTimeError( e.toString() ) ;
	}
}

/* 
 * Statement: update target_expression with expression [ options ].
	na_main2 [ sub ref_area = "DK", test123= "ACT" ] <- na_main [ sub ref_area = "DK" ] * 1000
	na_main2 [ sub ref_area = "DK", test123= "ACT" ][filter obs_value > 100 ] <- na_main [ sub ref_area = "DK" ] * 1000
*/
static void dbUpdate ( Query qLeft, ListString lsNames, ListString lsValues, boolean leftFilter, Query qExpr ) throws VTLError
{
	Dataset		ds ;
	String		sqlTableName ;
	String		sqlInsert = "", sqlSelect = "", target_sql_where, tempTable = null ;
	String		createStatement = null, deleteStatement = null, insertStatement = null ;
	String		str_pivot_columns = null, str_pivot_expr = null, tmp ;
	String		temp_sql_insert, temp_sql_select ;
	String		period_min, period_max ;
	boolean		useTempTable = false, expr_is_empty = false ;
	int			idx, cells_inserted = 0, cells_deleted = 0 ;

	ds = Dataset.getDatasetDesc( qLeft.referencedDatasets.firstElement()) ;
	
	ds.checkObjectForUpdate ( ) ;

	sqlTableName = ds.sqlTableName ;
	
	target_sql_where = ( qLeft.sqlWhere == null ? "" : " WHERE " + qLeft.sqlWhere ) ;

	// compare the dimensions of the two queries
    qLeft.checkIdenticalDimensions( qExpr, "<-" ) ;
    
    // check for common measures and attributes
    tmp = qLeft.dsUpdateMeasuresAttributes ( qExpr ) ;
    idx = tmp.indexOf ( " " ) ;
    str_pivot_columns = tmp.substring ( 0, idx ) ;
    str_pivot_expr = tmp.substring ( idx + 1 ) ;
       
    // condition: intersection of current filter, values of query dimensions and tab dimensions.
    qLeft.filterqueryForUpdate ( ds.objectId, qExpr ) ;

  	//compute period_min, period_max for auditing
  	period_max = period_min = null ;
  	DatasetComponent timeDim = qLeft.getTimePeriodDimension() ;
  	if ( timeDim != null ) {
  		idx = lsNames.indexOf( timeDim.compName ) ;
		if ( idx >= 0 )
			period_max = period_min = lsValues.get(idx) ;
		else {
			if ( qExpr.getTimePeriodDimension( ) != null ) {
				ListString timeValues = qExpr.getDimension( timeDim.compName ).dim_values ;
				if ( timeValues.size() > 0 ) {
					period_max = timeValues.findMax();
					period_min = timeValues.findMin();
				}
			}
		}
  	}

  	if (expr_is_empty ) {
		// just delete the data points - contains possible condition on the subscript
		if ( qLeft.sqlWhere == null ) {
	        Db.sqlExec ( "TRUNCATE TABLE " + sqlTableName + " REUSE STORAGE" ) ;
	        Audit.set("update", -1, 0, ds.dsName, ds.objectId, null, null) ; 		// -1 means truncate
	    }
		else {
			cells_deleted = Db.sqlExec ( "delete from " + qLeft.sqlFrom + target_sql_where ) ;
	        Audit.set( "update", -1, 0, ds.dsName, ds.objectId, period_min, period_max ) ; 		// -1 means truncate
		}			
        Db.sqlCommit () ;
        return ;
	} 
	
	// use temporary table if target table appears in the source expression 
	// BUG: we don't know the dependencies of a view in the source expression or target_has_condition
	// BUG: how can check for synonyms
	useTempTable = true ;

	if ( useTempTable == false ) {				 // 		don't use temporary table
	   	// 		free dimensions
	   if ( qExpr.dims.size() > 0 ) {
           sqlInsert = qExpr.stringDimensions ( ) + "," ;
           sqlSelect = qExpr.dimensionsSqlColumns ( ) + "," ;    		   
	   }

        //		bound dimensions (not free)
		for ( idx = 0; idx < lsNames.size(); idx ++ ) {
			if ( lsValues.get(idx) != null ) {
				sqlInsert = sqlInsert + lsNames.get(idx) + "," ;
				sqlSelect = sqlSelect + "'" + lsValues.get(idx) + "'," ;				
			}	
		}
		
		//		insert statement
        sqlInsert = sqlInsert + str_pivot_columns ;
        sqlSelect = sqlSelect + str_pivot_expr ;
        insertStatement = "INSERT INTO " + sqlTableName + "(" + sqlInsert + ") SELECT " 
        					+ sqlSelect + qExpr.build_sql_from() + ( qExpr.sqlWhere == null ? "" : " WHERE " + qExpr.sqlWhere ) ;
        
        //		delete statement
        deleteStatement = "DELETE FROM " + qLeft.sqlFrom + target_sql_where ;
	}
	else {
	    // 		use temporary table
	    idx = ds.dsName.indexOf ( Parser.ownershipSymbol ) ;		
	    tempTable = Db.temporaryTableName ( ds.dsName.substring ( idx + 1 ) ) ;
		
		temp_sql_insert = ( qExpr.dims.size () == 0 ? "" : qExpr.stringDimensions () + "," + str_pivot_columns ) ;
		temp_sql_select = ( qExpr.dims.size () == 0 ? "" : qExpr.dimensionsSqlColumns () + "," + str_pivot_expr ) ;

    	deleteStatement = "DELETE FROM " + qLeft.sqlFrom + target_sql_where ;

    	String	wherePart ;
    	if ( leftFilter ) {
    		wherePart = ( qExpr.sqlWhere == null ? " WHERE " : " WHERE " + qExpr.sqlWhere + " AND (" )
    				+ qExpr.stringDimensions() + ") IN ( SELECT " + qExpr.stringDimensions() 
    				+ " FROM " + qLeft.sqlFrom + target_sql_where + ")" ;
    	}
    	else {
    		wherePart = qExpr.sqlWhere == null ? "" : " WHERE " + qExpr.sqlWhere ;
    	}
	    // create statement			// PCTFREE 0 is not compatible with GLOBAL TEMPORARY
	    createStatement = "CREATE TABLE " + tempTable
	    		+ "(" + temp_sql_insert + ") PCTFREE 0 NOLOGGING AS SELECT " 
	    		+ temp_sql_select + qExpr.build_sql_from() + wherePart ;

	    // prepare insert and select list for insert statement, no alias used for temp table			
	    if ( qExpr.dims.size () > 0 ) {
	       sqlInsert = qExpr . stringDimensions () + "," ;
	       sqlSelect = sqlInsert ;
	    }
	    // dimensions specified in the subscript
   		for ( idx = 0; idx < lsNames.size(); idx ++ ) {
			if ( lsValues.get(idx) != null ) {
				sqlInsert = sqlInsert + lsNames.get(idx) + "," ;
				sqlSelect = sqlSelect + "'" + lsValues.get(idx) + "'," ;				
    			}	
    		}
 
		    sqlInsert = sqlInsert + str_pivot_columns ;
		    sqlSelect = sqlSelect + str_pivot_columns ;
	    
		    // insert statement
	    insertStatement = "INSERT INTO " + sqlTableName + "(" + sqlInsert + ") SELECT " + sqlSelect + " FROM " + tempTable ;
	}

	//execute DB operations
	try {
		if ( useTempTable )
			Db.sqlExec ( createStatement ) ;	// create a temporary table for keeping data before delete statement
	
		Db.sqlLockTable ( sqlTableName ) ;
		if ( deleteStatement != null )
			cells_deleted = Db.sqlExec ( deleteStatement ) ;
		if ( insertStatement != null )
			cells_inserted = Db.sqlExec ( insertStatement ) ;
	
		Audit.set("update", cells_deleted, cells_inserted, ds.dsName, ds.objectId, period_min, period_max) ;
		Db . sqlCommit () ;	  
	}
	catch ( VTLError e ) {
		Db.sqlRollback() ;
		throw e ;
	}

	if ( useTempTable )
		Db.sqlExec ( "DROP TABLE " + tempTable + " PURGE" ) ;	// BUG: need to be dropped in case of error
}

/*
 * Create a new name for a bitmap index.
 */
static String newIndexName ( ) throws VTLError
{
	String	index_id = Db.sqlGetValue ("SELECT " + Db.mdt_createindex_id + ".NEXTVAL FROM DUAL") ;
	
	return ( "BI$" + index_id ) ;
}

/*
 * Grant select privilege to users and roles.
 */
static void sqlGrantSelect ( String object_name, String users, String roles ) throws VTLError
{
	if ( users != null )
		Db . sqlExec ( "GRANT SELECT ON " + object_name + " TO " + users + " WITH GRANT OPTION" ) ;
	if ( roles != null )
		Db . sqlExec ( "GRANT SELECT ON " + object_name + " TO " + roles ) ;
}

/*
 * Build syntax for merge statement.
 * Not yet used.
 */
static public String sqlMerge ( String tableName, ListString dimensions, String setClause )  
{
	StringBuffer	sqlUpdate = new StringBuffer ( 100 );
	
	sqlUpdate.append( "MERGE INTO " ).append( tableName ).append( " a$ USING (SELECT " ) ;
	
	for ( String s : dimensions ) 
		sqlUpdate.append( "? AS " ).append( s ).append( ',' ) ;
	sqlUpdate.setLength( sqlUpdate.length() - 1 ) ;
	sqlUpdate.append( " FROM dual ) b$ ON (" ) ;
	for ( String s : dimensions ) 
		sqlUpdate.append( "a$." ).append( s ).append("=b$.").append( s ).append( " AND " ) ;
	sqlUpdate.setLength( sqlUpdate.length() - 5 ) ;
	sqlUpdate.append( ") WHEN MATCHED THEN UPDATE SET " ).append( setClause ) ;
	sqlUpdate.append( " WHEN NOT MATCHED THEN INSERT(" ).append ( dimensions.toString( ',' ) ) .append( ")VALUES(" ) ;
	for ( String s : dimensions )
    	sqlUpdate.append( "b$." ).append( s ).append( ',' ) ;
	sqlUpdate.setLength( sqlUpdate.length() - 1 ) ;
	sqlUpdate.append( ')' ) ;						
	// System.out.println ( sqlUpdate ) ;    
	return ( sqlUpdate.toString() ) ;
}

// END of class
}
