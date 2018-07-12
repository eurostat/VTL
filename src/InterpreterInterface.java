
/*
 * This class contain the interpreter methods called by the user interface
 * TBD: some methods still to be moved here
 */

public class InterpreterInterface {
	
/*
 * Execute the list of statements contained in the string cmd
 */
static final String eval ( String cmd ) throws VTLError
{
	return ( Command.eval ( cmd, false ) ) ;
}

/*
 * Export all definitons and data
 */
static final void exportAll ( String fileName, boolean exportData ) throws VTLError 
	{ VTLObject.exportAll ( fileName, exportData ) ; } ;

/*
 * Choose the possible labels (measures of valuedomains)
 */
static final ListString executeSelectLabels ( ) { 
	try {
		return ( Db.sqlFillArray( 
				"SELECT DISTINCT dim_name FROM " + Db.mdt_dimensions + " WHERE dim_type= 'X' AND object_id IN ("
				+ "SELECT object_id FROM " + Db.mdt_objects + " WHERE object_type='D')"
				) ) ;			
	}
	catch ( VTLError e ) {
		return ( new ListString() ) ;
	}
} ;

}
