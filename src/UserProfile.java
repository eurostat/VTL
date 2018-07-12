

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

public class UserProfile {

public static String	defaultSelectPrivRole 	= null ;
public static String	defaultSelectPrivUser 	= null ;
public static String	searchUser 				= null ;
public static boolean	tablesHavePrimaryKey 	= true ;
public static boolean	tablesHaveBitmapIndexes = false ;
public static String 	dateFormat 				= "dd.MM.yyyy" ;

static	String variables [] = {
		"default_select_priv_role", "default_select_priv_user", "search_user", "tables_have_primary_key",
		"tables_have_bitmap_indexes", "date_format"
} ;

/*
 * Default values
 */
static	String defaultValues [] = { null,null,null,"yes","no","dd.MM.yyyy" } ;

// variables whose value is yes/no
static	boolean changeYesNoValues [] = { false,false,false,true,true,false } ;

// current values as strings
static	String currentValues [ ] ;

/*
 * Index of variable in the list of variables
 */
public static int getVariableIndex ( String variable )
{
	return ( Sys.getPosition( variables, variable ) ) ;
}

public static String getSearchUser ( )
{
	return ( searchUser ) ;
}	

/*
 * Set value of user user profile variable (argument: variable name).
 * Verify if value is correct
 * Update user_profile system table
 */
public static void setValue ( String variable, String value ) throws VTLError
{
	String	oldValue, sqlUpdate ;
	int		n_rows ;
	int		variable_index ;
	
	if ( ( variable_index = getVariableIndex ( variable ) ) < 0 )
		VTLError.RunTimeError( "Bad variable name: " + variable ) ;
	
	oldValue = currentValues [ variable_index ] ;
	
	if ( ( ( ( value == null || value.length() == 0 ) && oldValue == null )
			|| ( value != null && oldValue != null && value.compareTo( oldValue ) == 0 ) ) )
		return ;
	
	setInternalValue ( variable_index, value ) ;
	currentValues [ variable_index ] = value ;
	
	sqlUpdate = "UPDATE " + Db.mdt_user_profiles + " SET " + variables [ variable_index ] + ( value == null ? "=null" : "='" + value + "'" ) ;
	n_rows = Db.sqlExec( sqlUpdate ) ;
	if ( n_rows == 0 ) {
		Db.sqlExec( "INSERT INTO " + Db.mdt_user_profiles + "(user_name)VALUES('" + Db.db_username + "')" ) ;
		Db.sqlExec( sqlUpdate ) ;
	}
	Db.sqlCommit ( ) ;
}

/*
 * Set value of user user profile variable.
 * Verify if value is correct
 * Update user_profile system table (if update_system_table = true)
 */
static void setInternalValue ( int variable_index, String value ) throws VTLError
{		
	switch ( variable_index ) {
		case 0 :
			defaultSelectPrivRole = value ;
			break ;
		case 1 :
			if ( ( defaultSelectPrivUser = value ) != null && defaultSelectPrivUser.compareToIgnoreCase ( Db.db_username ) == 0 )
				defaultSelectPrivUser = null ;
			break ;
		case 2 :
			if ( ( searchUser = value ) != null )
				searchUser = searchUser.toLowerCase() ;
			break ;
		case 3 :
			if ( value != null && value.compareToIgnoreCase("yes") != 0 && value.compareToIgnoreCase("no") != 0)
				VTLError.RunTimeError( "Bad value (yes/no)") ;
			tablesHavePrimaryKey = ( value != null && ( value.compareToIgnoreCase("yes") == 0 || value.compareToIgnoreCase("y") == 0) ) ;
			break ;
		case 4 :
			if ( value != null && value.compareToIgnoreCase("yes") != 0 && value.compareToIgnoreCase("no") != 0)
				VTLError.RunTimeError( "Bad value (yes/no)") ;
			tablesHaveBitmapIndexes = ( value != null && ( value.compareToIgnoreCase("yes") == 0 || value.compareToIgnoreCase("y") == 0) ) ;
			break ;
		case 5 :
			if ( value == null )
				VTLError.RunTimeError( "date_format: value cannot be null" ) ;
			dateFormat = value.toLowerCase () ;
			break ;												// decimalPlaces
	}
}

/*
 * Get profile of current user from database.
 */
public static void getUserProfile ( ) throws VTLError
{
	int 		idx, arrayLength ;
	String 		sql_query, sql_select, value ;
	Statement	stmt = null ;
	ResultSet 	rs = null ;
	
	arrayLength =  variables.length;
	
	currentValues = new String [ arrayLength ] ;
	Arrays.fill ( currentValues, null) ;
	
	sql_select = "SELECT default_select_priv_role,default_select_priv_user,"
		+ "search_user,tables_have_primary_key,tables_have_bitmap_indexes,date_format" ;
	
	sql_query = sql_select + " FROM " + Db . mdt_user_profiles ;
	
	try {
		rs = ( stmt = Db.createStatement() ).executeQuery ( sql_query ) ;		
	    if ( rs . next () ) {
	    	for ( idx = 0; idx < arrayLength; idx ++ ) {
	    		value = rs.getString ( idx + 1 ) ;
				if ( value != null && changeYesNoValues[ idx ] )
					currentValues [ idx ] = ( value.compareToIgnoreCase("yes") == 0 || value.compareToIgnoreCase("y") == 0 ? "yes" : "no" ) ;
				else
					currentValues [ idx ] = value ;			
	    	}
		}
	    rs . close () ;		// close to reuse stmt
	
		sql_query = sql_select + " FROM " + Db . mdt_profiles + " WHERE user_name='" + Db.owner_of_system_tables + "'" ;
		rs = stmt.executeQuery ( sql_query ) ;
		if ( rs . next () ) {
			for ( idx = 0; idx < arrayLength; idx ++ ) {
				if ( ( value = rs.getString ( idx + 1 ) ) != null ) {
					if ( changeYesNoValues[ idx ] )
						defaultValues [ idx ] = ( value.compareToIgnoreCase("yes") == 0 || value.compareToIgnoreCase("y") == 0 ? "yes" : "no" ) ;
					else
						defaultValues [ idx ] = value ;				
				}
			}
		}
	}
	catch ( SQLException e ) {	
		VTLError.SqlError ( e.toString () ) ;
	} 
	finally {
		Db.closeStatement (stmt) ;
	}
	
    for ( idx = 0; idx < arrayLength; idx ++ ) {
    	if ( currentValues [idx ] == null )
    		currentValues [idx ] = defaultValues [ idx ] ;
    	setInternalValue ( idx, currentValues [idx ] ) ;    	
    }
}

public static String getDefault_select_priv_role() {
	return defaultSelectPrivRole;
}

public static String getDefault_select_priv_user() {
	return defaultSelectPrivUser;
}

public static boolean isTables_have_bitmap_indexes() {
	return tablesHaveBitmapIndexes;
}

public static boolean isTables_have_primary_key() {
	return tablesHavePrimaryKey;
}

}
