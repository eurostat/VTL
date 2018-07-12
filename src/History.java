

import java.sql.PreparedStatement;
import java.sql.Types;
import java.util.Vector;

/*
 * Log info: history of commands, errors, etc.
 */
public class History {
	
	static class LocalHistory {
		String	statement ;
		int		elapsedTime ;
	}
	static	Vector <LocalHistory> historyOfCommands = new Vector <LocalHistory> () ;
	
	static final Vector <LocalHistory> getHistoryOfCommands () { return historyOfCommands ; } 
	
	/*
	 * Add command text and elapsed time to history.
	 */
	static void addHistory ( String cmd, int elapsed_time )
	{
		LocalHistory h = new LocalHistory () ;
		h.statement = cmd ;
		h.elapsedTime = elapsed_time ;
		historyOfCommands.add ( h ) ;       
	}
	
	//	error logging
	public static void log_error(String statement, String customMessage ) throws VTLError{
		writeLogToDB( null, -1, customMessage, "E");
	}
	
	public static void log_info(String statement, int elapsed_time) throws VTLError{
		writeLogToDB( statement, elapsed_time, null, "H");
	}
	
	private static void writeLogToDB( String statement, int elapsed_time, String customMessage, String log_type) throws VTLError{
		PreparedStatement pstmt = null;

		try{
			String insert_history = "INSERT INTO " + Db.mdt_user_history +
					"(history_id,session_id,statement,elapsed_time,custom_message,log_type)" +
					"VALUES("+ Db.mdt_history_id + ".nextval,?,?,?,?,?)" ;

			if ( statement != null && statement.length() > 4000 )
				statement = statement.substring( 0, 4000 ) ;
			if ( customMessage != null && customMessage.length() > 4000 )
				customMessage = customMessage.substring( 0, 4000 ) ;
			
			pstmt = Db.prepareStatement( insert_history );
		  	pstmt.setInt ( 1, Db.db_session_id ) ;
		  	pstmt.setString ( 2, statement ) ;
		  	if ( elapsed_time < 0 )
		  		pstmt.setNull( 3, Types.NUMERIC ) ;
		  	else
		  		pstmt.setInt ( 3, elapsed_time ) ;
		  	pstmt.setString ( 4, customMessage ) ;
		  	pstmt.setString ( 5, log_type ) ;
		  	
		  	pstmt.executeUpdate () ;
		  	
		  	pstmt.close();
		  	pstmt = null;
			
		}catch (Exception e) {
			VTLError.RunTimeError( e.toString() ) ;
		}finally{
			try{
				if(pstmt!=null){
					pstmt.close();
					pstmt = null;
				}
			} catch(Exception ex) { 
				VTLError.RunTimeError( ex.toString() ) ;
			}
		}
	}
	
	/*
	 * Save all history of commands to DB.
	 */
	public static void saveHistory( ) throws VTLError {
		for ( LocalHistory h : historyOfCommands )
			History.log_info( h.statement, h.elapsedTime ) ;
		Db.sqlCommit() ;
		historyOfCommands.clear() ;
	}

}
