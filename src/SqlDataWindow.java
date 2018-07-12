

import java.io.IOException;
import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Vector;

/*
 * Open cursor, execute query, retrieve data. Create independence between client and storage.
 * NB: could be implemented using integer (index in sqlDataWindows) instead of descriptor.
 */
public class SqlDataWindow {
	
	private 	ResultSet	rs ;
	private		int			rowsRetrieved ;
	private		int 		sqlTypes[] ;
	private		String		headers[] ;
	private		String		sqlQuery ;				// since v. 142
	private		Statement	stmt ;					// since v. 142
	private		Object 		parameters [] ;			// since v. 144
	private		ListString	uniqueColumns ;			// v. 164: unique columns used to open a blob/clob

	public final int getRowsRetrieved() {
		return rowsRetrieved;
	}

	public final int[] getSqlTypes() {
		return sqlTypes;
	}

	public final String[] getHeaders() {
		return headers;
	}

	public final ListString getUniqueColumns() {
		return uniqueColumns;
	}

	static		Vector <SqlDataWindow> sqlDataWindows = new Vector <SqlDataWindow> ( 10 ) ;
	
	SqlDataWindow ( ResultSet rs, String sqlQuery, Statement stmt, Object parameters [], ListString uniqueColumns ) throws SQLException
	{
		int					n_columns, sql_type ;
		ResultSetMetaData 	md ;
		// sql " select data_scale from user_tab_columns where table_name='GEO' order by column_id" ;
		this.rs = rs ;
		md = this.rs.getMetaData() ;
  		n_columns = md.getColumnCount() ;
  		this.sqlTypes = new int [ n_columns ] ;		
  		this.headers = new String [ n_columns ] ;		
  		for ( int idx = 1 ; idx <= n_columns; idx ++ ) {
  			sql_type = md.getColumnType(idx ) ;
  			// System.out.println ( "SQL : " + md.getColumnName( idx ) + " type: " + sql_type + " scale: " + md.getScale( idx ) + " precision: " + md.getPrecision( idx )) ;
  			if ( sql_type == java.sql.Types.NUMERIC && md.getScale( idx ) == 0 && md.getPrecision( idx ) == 38 )
  				this.sqlTypes [ idx - 1 ] = java.sql.Types.INTEGER ;	// otherwise type is NUMERIC 
  			else
  	  			this.sqlTypes [ idx - 1 ] = sql_type ;				// correction v. 142: -1
  				
  			this.headers [ idx - 1 ] = md.getColumnName( idx ) ;				// correction v. 142: -1
  		}
  		this.sqlQuery = sqlQuery ;					// since v. 142
  		this.stmt = stmt ;							// since v. 142
  		this.parameters = parameters ;				// to cope with query parameters "?" in retrieve blob/clob
		this.rowsRetrieved = 0 ;
  		this.rs.setFetchSize ( 1024 ) ;
  		this.uniqueColumns = uniqueColumns ;				// v. 164
  		
  		sqlDataWindows.add( this ) ;
	}
	
	/*
	 * Create new SQL data window, with no parameters.
	 */
	static SqlDataWindow open ( String sqlQuery ) throws VTLError {
		Statement			stmt ;
		ResultSet			rs ;
		SqlDataWindow 		dw = null ;
		
		try {
			stmt = Db.DbConnection.createStatement() ;
			rs = stmt.executeQuery ( sqlQuery ) ;
		    dw = new SqlDataWindow ( rs, sqlQuery, stmt, null, null ) ;			// v. 164	    
		} catch ( SQLException e ) {
		     VTLError.SqlError  ( "Error opening query:\r\n" + sqlQuery + "\r\n" + e.getErrorCode() + " " + e.toString () ) ; // v. 144
		}
		return ( dw ) ;
	}
	
	/*
	 * Create new SQL data window, with parameters.
	 * Example: 
	 * sql "select * from aact_ali01 where geo=?" using "IT" ;
	 	Object	parameters [ ] = new Object [2]; parameters[0] = "IT"; parameters[1] = new Integer ( 2000 ); 
  		dw = SqlDataWindow.open ( "SELECT * FROM aact_ali01 WHERE geo=? AND time=?", parameters ) ;
	 */
	static SqlDataWindow open ( String sqlQuery, Object parameters [], ListString uniqueColumns ) throws VTLError {
		PreparedStatement	stmt ;
		ResultSet			rs ;
		SqlDataWindow 		dw = null ;
		
		try {
			stmt = Db.DbConnection.prepareStatement( sqlQuery ) ;
		    for ( int idx = 0 ; idx < parameters.length; idx ++ )
		    	stmt.setObject( idx + 1, parameters[idx] ) ;
		    rs = stmt.executeQuery ( ) ;
		    dw = new SqlDataWindow ( rs, sqlQuery, stmt, parameters, uniqueColumns ) ;   			// v. 164
		} catch ( SQLException e ) {
			String	msg ;															// v. 144
			switch ( e.getErrorCode() ) {
				case 17003 : msg = "too many actual parameters provided" ; break ;
				case 17041 : msg = "too few actual parameters provided" ; break;
				default : msg = e.toString () ;
			}
			VTLError.SqlError  ( "Error opening query: " + sqlQuery + "\r\n" + msg ) ;	// v. 144
		}
		return ( dw ) ;
	}
	
	static SqlDataWindow open ( String sqlQuery, Object parameters [] ) throws VTLError {
		return ( open ( sqlQuery, parameters, null ) ) ;
	}
	
	/*
	 * Retrieve 1 row as an array of objects
	 * CLOB/BLOB: returns null or the string to be displayed in the cell (link)
	 */
	public Object[] retrieveRow ( ) throws VTLError {
		int					n_columns ;
		Object				o ;
		ResultSet			rs ;
		Object 				row [] = null ;
		
		try { 
			if ( this.rs.next() == false )
				return ( null ) ;
			
			n_columns = this.sqlTypes.length ;
			row = new Object[ n_columns ] ;
			rs = this.rs ;
			for ( int idx = 0; idx < n_columns; idx ++ ) {
  				switch ( this.sqlTypes [ idx ] ) {  
  					case java.sql.Types.CHAR :
  					case java.sql.Types.VARCHAR :
						o = rs.getString( idx + 1 ) ;  							
						break ;						
					// case java.sql.Types.INTEGER : unfortunately Oracle stores an INTEGER column as a NUMBER
					case java.sql.Types.NUMERIC :
						o = rs.getBigDecimal( idx + 1 ) ;
						break;
					case java.sql.Types.DATE :
						o = rs.getTimestamp( idx + 1 ) ;  							
						break ;
					case java.sql.Types.STRUCT :
						o = ( rs.getObject( idx + 1 ) != null ? "Open report" : null ) ; // just to distinguish null/not null in the client
						break ;
					case java.sql.Types.CLOB :		// change in v. 142: getClob instead of getString
						o = ( rs.getClob ( idx + 1 ) != null ? "Open report" : null ) ;// just to distinguish null/not null in the client
	  					break ;
					default :
	    				o = rs . getString ( idx + 1 ) ;  	  					
  				}
  				row [ idx ] = o ;					// +1 in SQL
			}
			this.rowsRetrieved ++ ;
		}
		catch ( SQLException e ) {
		     VTLError.SqlError  ( e.toString () ) ;
		}
		return ( row ) ;
	}

	/*
	 * Retrieve num_rows rows
	 */
	public Vector<Object[]> retrieveRows ( int num_rows ) throws VTLError {
		Vector<Object[]>	rows = new Vector<Object[]> ( num_rows) ;
		Object[]			row ;
		
		while ( ( row = this.retrieveRow() ) != null && num_rows > 0 ) {
			rows.add ( row) ;
			num_rows -- ;			
		}

		return ( rows ) ;
	}
	
	/*
	 * Close object
	 */
	public void close ( ) throws VTLError {
		try {
			sqlDataWindows.remove( this ) ;
			this.rs.close () ;
			this.stmt.close();						// since v. 142
		}
		catch ( SQLException e ) {
		     VTLError.SqlError  ( e.toString () ) ;
		}
	}
	
	/*
	 * Since v. 142
	 * Retrieve clob as a String object based on the values of the columns that uniquely identify a row (the dimensions)
	 */
	public String retrieveClob ( ListString uniqueDims, ListString uniqueValues, String clobColumn ) throws VTLError
	{
		StringBuffer		sql_query = new StringBuffer ( 100 ) ;
		String				strClob = null, val ;
		PreparedStatement 	stmt = null;						// v. 144 to cope with parameters "?"
		ResultSet 			rs = null;
		
		try {
			sql_query.append( "SELECT " + clobColumn + " FROM (" + this.sqlQuery + ") WHERE " + clobColumn + " IS NOT NULL" ) ;

			for ( int idx = 0; idx < uniqueDims.size(); idx ++ ) {
				if ( ( val = uniqueValues.get(idx) ) != null)
					sql_query.append( " AND " ).append( uniqueDims.get(idx) ).append( "='").append( val ) .append ("'" ) ;
			}
			
			// Session.debug ( sql_query.toString() ) ;
			stmt = Db.DbConnection.prepareStatement( sql_query.toString() );
			if ( this.parameters != null ) {										// v. 144 to cope with query parameters "?"
			    for ( int idx = 0 ; idx < this.parameters.length; idx ++ )
			    	stmt.setObject( idx + 1, this.parameters[idx] ) ;				
			}
			rs = stmt.executeQuery ( ) ;
			if ( rs . next () ) {
			    strClob = rs . getString ( 1 ) ;
			}
		}
		catch ( SQLException e ) {
			VTLError.SqlError  ( "retrieve clob - exception: " + e.toString () ) ;
		} 
		finally {
			try{
				if ( stmt != null)
					stmt.close();
				if (rs!=null)
					rs.close();
			}
			catch ( Exception ex ){
				VTLError.SqlError  ( "retrieve clob - close statement: " + ex.toString () ) ;
			}
		}
		
		return ( strClob ) ;
	}
	
	/*
	 * Since v. 142
	 * Retrieve blob based on the values of the columns that uniquely identify a row (the dimensions) and save it to a
	 * temporary file.
	 * Return value: absolute path of the file that contains the blob
	 * The file can be opened calling: Basic.shell( "CMD /C " + fileAbsolutePath , false ) ;
	 */
	public String retrieveBlobToFile ( ListString uniqueDims, ListString uniqueValues, String blobColumn ) throws VTLError
	{
		String				storedFileName, fileAbsolutePath = null, val ;
		ResultSet 			rs = null;
		InputStream 		bodyOut ;
		StringBuffer		sql_query = new StringBuffer () ;
		PreparedStatement 	stmt = null;						// v. 144 to cope with query parameters "?"
		
		try {
			// syntax to select a component of a column whose type is user-defined: alias.column_name.component_name
			sql_query.append( "SELECT " + "a1." + blobColumn + ".blob_filename,a1." + blobColumn + ".blob_file" + " FROM (" 
					+ this.sqlQuery + ")a1 " + " WHERE a1." + blobColumn + " IS NOT NULL" ) ;

			for ( int idx = 0; idx < uniqueDims.size(); idx ++ ) {
				if ( ( val = uniqueValues.get(idx) ) != null) {					// v. 165
					sql_query.append( " AND " ).append( uniqueDims.get(idx) ).append( "='").append( val ) .append ("'" ) ;			// since v. 102: table alias removed: "a1." + 
				}
			}

			stmt = Db.DbConnection.prepareStatement( sql_query.toString() );
			if ( this.parameters != null ) {										// v. 144 to cope with query parameters "?"
			    for ( int idx = 0 ; idx < this.parameters.length; idx ++ )
			    	stmt.setObject( idx + 1, this.parameters[idx] ) ;				
			}
			rs = stmt.executeQuery ( ) ;
			if ( rs . next () ) {
			    storedFileName = rs . getString ( 1 ) ;
			    bodyOut = rs.getBinaryStream( 2 );
			    if ( storedFileName == null )
			    	VTLError.InternalError( "blob_download: stored filename is null" ) ;						// since v. 107
			    if ( bodyOut == null )
			    	VTLError.InternalError( "blob_download: blob is null" ) ;									// since v. 107
			    fileAbsolutePath = Sys.saveOutputStream( null, storedFileName, bodyOut ) ;				// true: v. 149
			    bodyOut.close();
			}
			rs.close();
			stmt.close();
			stmt=null;
			rs=null;
		}
		catch ( SQLException e ) {
			VTLError.SqlError  ( "blob_download: " + e.toString () ) ;
		} catch (IOException e) {
			VTLError.SqlError  ( "blob_download: " + e.toString () ) ;
		} finally {
			try{
				if(stmt!=null)
					stmt.close();
				if(rs!=null)
					rs.close() ;
			}
			catch(Exception ex){
				VTLError.SqlError  ( "blob_download: " + ex.toString () ) ;
			}
		}
		
		return ( fileAbsolutePath ) ;
	}
}
