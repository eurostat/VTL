

/*
 * VTL interpreter main class.
 */

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;

public class VTLMain {

static boolean vtl20only = false ;

/*
 * Command-line interpreter. Usage: 
 * 		VTL.jar database_url username password [ -r | -x command_file ]
 * 
 * Sample database url: 
 * 		jdbc:oracle:thin:@s-alouette:1521:eurobasd 
 */
public static void main ( String [] args )
{
	BufferedReader 	stdin ; 
	String 			str ;
	StringBuffer	cmd = new StringBuffer ( 1000 ) ;
	boolean 		read_only = false, create_metabase = false ;
	String			command_file_name = null, url,username, password ;
  	
	if ( args.length < 3 || args.length > 5 ) {
		Sys.printStdOut ( "\nUsage: " ) ;
		Sys.printStdOut ( "vtl.jar databaseUrl username password { -createmetabase | -readonly | -vtl2.0 | -x commandFile }" ) ; 
		return ;
	}
	
	url = args[0] ;
	username = args[1] ;
	password = args[2] ;
	
	switch ( args.length ) {
		case 4 :
			switch ( args[3] ) {
				case "-readonly" : read_only = true ; break ;
				case "-createmetabase" : create_metabase = true ; break ;
				case "-vtl2.0" : vtl20only = true ; break ;
			}
			break ;
		case 5 : 
	  		if ( args [3].equals ( "-x" ) )
	  			command_file_name = args [ 4 ] ;
	  		else {	
	  			Sys.printStdOut( "bad option" ) ;
	  			return;	  
	  		}
	}
	
	try {
		Db.dbConnect ( url, username, password, read_only ) ;		  
	}
	catch ( Exception e ) {
	    Sys.printErrorMessage ( e.getMessage () ) ;
	    System.exit ( 1 ) ;
	}
	
	if ( create_metabase ) {
		try {
			Metabase.createMetabase( );
		}
		catch ( VTLError e ) {
		    Sys.printErrorMessage ( e.getErrorType(), e.getErrorMessage (), "", 0 ) ;
		    System.exit ( 1 ) ;
		}
	}
	
	try {
		Db.readVTLMetabase ( ) ;
	}
	catch ( VTLError e ) {
	    Sys.printErrorMessage ( e.getErrorType(), e.getErrorMessage (), "", 0 ) ;
	    System.exit ( 1 ) ;
	}

	if ( command_file_name != null ) {
		Sys.printStdOut ( "Opening file " + command_file_name ) ;
		try {
			stdin = new BufferedReader ( new InputStreamReader ( new FileInputStream ( command_file_name ) ) ) ;
			while ( ( str = stdin.readLine ( ) ) != null ) {
				  cmd.append( str ).append( "\n" ) ;
			}
		}
		catch ( Exception e ) {
	    	Sys.printErrorMessage ( e.getMessage () ) ;
	    	System.exit ( 1 ) ;			
		}
		try {
		    Command.eval ( cmd.toString(), false ) ;
		    Db.disconnectDb() ;
		    System.exit ( 0 ) ;
		}
		catch ( VTLError e ) {
		    Sys.printErrorMessage ( e.getErrorType(), e.getErrorMessage (), e.getErrorFunctionName(),  
		    							e.getErrorLineNumber() ) ;
		    System.exit ( 1 ) ;
		}
	}
	
	String db = args[0] ;
	int idx = Math.max( db.lastIndexOf ( ':' ) , db.lastIndexOf ( '/' ) ) ;
	if ( idx > 0 )
		db = db.substring( idx + 1 ) ;
	db = db.toLowerCase() ;
	UIConsole.setConsole("VTL Sandbox version 2.0 - " + args[1] + "@" + db ) ;
}

}

/* new
stdin = new BufferedReader ( new InputStreamReader ( System.in ) ) ;
Sys.print( "Cannot find system tables. Do you want to create them?" ) ;
try { 
	str = stdin.readLine ( ) ;
}
catch ( Exception e1 ) {
	Sys.printErrorMessage ( e1.getMessage () ) ;
	str = null ;
	System.exit ( 1 ) ;			    	
}
if ( str == null || ! str.equals ( "yes" ) )
	System.exit ( 1 ) ;
// end */