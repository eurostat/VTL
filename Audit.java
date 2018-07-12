

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Vector;

/*
 * Audit class.
 * Oracle tables:
	ALL_DEF_AUDIT_OPTS		Auditing options for newly created objects
	USER_AUDIT_OBJECT		Audit trail records for statements concerning objects, specifically: 
	USER_AUDIT_SESSION		All audit trail records concerning CONNECT and DISCONNECT
	USER_AUDIT_STATEMENT	Audit trail records concerning  grant, revoke, audit, noaudit and alter system
	USER_AUDIT_TRAIL		Audit trail entries relevant to the user
	USER_OBJ_AUDIT_OPTS		Auditing options for user's own tables and views
	AUDIT_ACTIONS			Description table for audit trail action type codes. Maps action type numbers to action type names
 */

/*
  Concatenate cmd to history buffer.
	cmd 				VTL statement
	deleted 			n. of cells deleted
	inserted			n. of cells inserted
	dataset				dataset updated
	object_id			object_id of updated object
	
	g_history_audit_level is defined in mdt_profiles. Default: 1.
*/

public class Audit {

static long auditStartTime ;			// used to mark the starting time of a command
static int	auditCmdId = 0 ;			// used to identify an audit record together with the sessionId

static class AuditRecord {
	String		user_name ;
	int			object_id ;
	int			ses_id ;
	String		cmd_text ;
	int			n_ops ;
	int			cells_deleted ;
	int			cells_inserted ;
	int			elapsed_time ;
	String		period_min ;
	String		period_max ;
	String		audit_comment ;
	String		audit_blob ;
}

static	Vector <AuditRecord> auditRecords = new Vector <AuditRecord> () ;

/*
 * Start audit (reset variables). Called by eval.
 */
static void start ( ) throws VTLError
{
	auditRecords.clear();
	auditStartTime = System . currentTimeMillis () ;
}

/*
 * Set audit info
 */
static void set ( String my_cmd_text, int my_cells_deleted, int my_cells_inserted, String dsName, 
		int my_object_id, String my_period_min, String my_period_max )	throws VTLError
{
	set ( my_cmd_text, my_cells_deleted, my_cells_inserted, dsName, my_object_id, my_period_min, my_period_max, null, null ) ;
}

/*
 * Add audit data to temporary Vector.
 * os username: sql " SELECT SYS_CONTEXT ('USERENV', 'OS_USER') FROM dual"
 */
static void set ( String my_cmd_text, int my_cells_deleted, int my_cells_inserted, String dsName, 
				int my_object_id, String my_period_min, String my_period_max, String audit_comment, String audit_blob )	throws VTLError
{
	int			idx, time_diff, audit_index = -1 ;
	long			current_time ;
	AuditRecord	auditRecord ;
  
  
  	if ( my_object_id == 0 || dsName.indexOf ( "@" ) > 0 )		// it is a variable or remote table
  		return ;
  	current_time = System . currentTimeMillis () ;
  	time_diff = (int) ( current_time - auditStartTime ) / 1000 ;
  	auditStartTime = current_time ;

	for ( idx = 0; idx < auditRecords.size(); idx ++)
		if ( auditRecords.get(idx).object_id == my_object_id ) {
			audit_index = idx ;
			break ;
		}
	
	if ( audit_index < 0 ) {
		auditRecord = new AuditRecord ( ) ;
		auditRecord.user_name = Db.db_username ;
		auditRecord.object_id = my_object_id ;
		auditRecord.ses_id = Db.db_session_id ;
		auditRecord.cmd_text = ( my_cmd_text.length() <= 1000 ? my_cmd_text : my_cmd_text.substring(0, 1000) ) ;
		auditRecord.n_ops = 1 ;
		auditRecord.cells_deleted = my_cells_deleted ;
		auditRecord.cells_inserted = my_cells_inserted ;
		auditRecord.elapsed_time = time_diff ;
		auditRecord.period_min = my_period_min ;
		auditRecord.period_max = my_period_max ;		
		auditRecord.audit_comment = ( audit_comment == null || audit_comment.length() <= 1000 ? audit_comment : audit_comment.substring(0, 1000) ) ;
		auditRecord.audit_blob = audit_blob ;
	  
		auditRecords.add(auditRecord) ;
		// Sys.println( "Length: " + auditRecord.cmd_text.length()  ) ;
	} 
	else {
		auditRecord = auditRecords .get(audit_index) ;
		if ( my_period_min != null ) {
			if ( auditRecord.period_min != null && auditRecord.period_min.compareTo ( my_period_min) > 0 )
				auditRecord.period_min = my_period_min ;
		}
		if ( my_period_max != null ) {
			if ( auditRecord.period_max != null && auditRecord.period_max.compareTo ( my_period_max) < 0 )
				auditRecord.period_max = my_period_max ;
		}
		auditRecord.cells_deleted += my_cells_deleted ;
		auditRecord.cells_inserted += my_cells_inserted ;
		auditRecord.elapsed_time += time_diff ;
		auditRecord.n_ops ++ ;
	}
}

static final String auditsColumns = 
"timestamp,user_name,object_id,ses_id,cmd_id,upd_id,cmd_text,n_ops,cells_deleted,cells_inserted,elapsed_time,period_min,period_max,audit_comment,audit_clob" ;

/*
 * Insert audit data into audit system table mdt_audits.
	Blob:
	CREATE OR REPLACE TYPE mdt_blob OID 'B84B19DACE959EB3E040A79E60C81682' 
	AS OBJECT ( blob_file blob, blob_filename VARCHAR ( 100 ) ) " ;
	load "H:\MyDocuments\aact_ali01_test_2.dat" into aact_ali01_test_2 add ;
 */
static void finish ( String top_level_cmd ) throws VTLError
{
	int					idx ;
	AuditRecord			auditRecord ;
	PreparedStatement	sqlStatement ;
	String				insertCommand ;
  
	if ( auditRecords.size() == 0 )
		return ;
	
	try {
	  	for ( idx =0;idx<auditRecords.size(); idx++ ) {
	  		auditRecord = auditRecords.get( idx )  ;
	  		Audit.auditCmdId ++ ;
	  		// nullblob = ( auditRecord.audit_blob == null ) ;
	  		insertCommand = "INSERT INTO " + Db.mdt_user_audits_ins + "(" + auditsColumns + ")"
	  				+ "VALUES(SYSDATE,?,?,?,?,?,?,?,?,?,?,?,?,?,?)" ;	  			
			sqlStatement = Db.prepareStatement( insertCommand ) ;
			sqlStatement.setString 	( 1, auditRecord.user_name ) ;
			sqlStatement.setInt 	( 2, auditRecord.object_id );
			sqlStatement.setInt 	( 3, auditRecord.ses_id ) ;
			sqlStatement.setInt 	( 4, auditCmdId ) ;
			sqlStatement.setInt 	( 5, 1 ) ;
			sqlStatement.setString 	( 6, ( top_level_cmd.length() <= 1000 ? top_level_cmd : top_level_cmd.substring(0, 1000) ) ) ;
			sqlStatement.setInt 	( 7, auditRecord.n_ops ) ;
			sqlStatement.setInt 	( 8, auditRecord.cells_deleted ) ;
			sqlStatement.setInt 	( 9, auditRecord.cells_inserted ) ;
			sqlStatement.setInt 	( 10, auditRecord.elapsed_time ) ;
			sqlStatement.setString 	( 11, auditRecord.period_min ) ;
			sqlStatement.setString 	( 12, auditRecord.period_max ) ; 
			sqlStatement.setString 	( 13, auditRecord.audit_comment ) ;
			sqlStatement.setString 	( 14, auditRecord.audit_blob ) ;
			sqlStatement.executeUpdate() ; 
		  	sqlStatement.close();
	  	}
		Db.sqlCommit() ;
	}
	catch ( Exception e ) {
		Db.sqlRollback() ;
		VTLError.SqlError ( "Audit - " + e.toString() ) ;
	}  
}


/*
 * Downloads a blob stored in the audit system table mdt_audits.
 * Usage: 
 * 	sessionId, cmdId are the key columns in the audit table
 * 	filePath is null
 */

static String downloadAuditBlob ( String sessionId, String cmdId ) throws VTLError {
	String text = null;
	StringBuffer	sql_query = new StringBuffer () ;
	Statement		stmt = null ;
	ResultSet 		rs = null ;
	
	try {
		sql_query.append( "SELECT audit_clob" 
				+ " FROM (" + Db.mdt_audits + ")a1 WHERE a1.audit_clob IS NOT NULL" 
				+ " AND ses_id='" + sessionId + "' AND cmd_id='" + cmdId + "'" ) ;
		rs = ( stmt = Db.createStatement () ).executeQuery ( sql_query.toString() ) ;
		if ( ! rs . next () ) 
			VTLError.InternalError( "Cannot find audit record: " + sessionId + "," + cmdId ) ;
		text = rs . getString ( 1 ) ;
			// rs.close() ;										
	}catch ( SQLException e ) {
		VTLError.SqlError  ( e.toString () ) ;
	} 
	finally {
		Db.closeStatement (stmt) ;												
	}
	return text;
}

/*
 * Delete table from audit records.
 */
static void deleteTable ( int object_id ) throws VTLError
{
	int					idx ;
	AuditRecord			auditRecord ;
	
  	for ( idx =0;idx<auditRecords.size(); idx++ ) {

  		auditRecord = auditRecords.get( idx )  ;
  		if ( auditRecord.object_id == object_id )
  			auditRecords.remove ( idx ) ;
  	}
}

//END class
}
