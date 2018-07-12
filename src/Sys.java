
/*
   Basic operations: manipulation of arrays and system calls.
*/

import java.io.* ;

public class Sys {

static public void shell ( String command_line, boolean print_output ) throws VTLError
{
  int		exit_value ;
  Process	proc ;
  String	str ;

  try { 
    proc = Runtime . getRuntime () . exec ( command_line ) ; 

	if ( print_output ) {
	// get its output (your input) stream
	BufferedReader proc_in = new BufferedReader ( new InputStreamReader ( proc . getInputStream() ) ) ;

	try {
		while (( str = proc_in.readLine()) != null) {
		  Sys.printStdOut( str );
		}
	} catch (IOException e) {
		throw new Exception ( "Error reading output of external command" ) ;
	}
	}

	exit_value = proc . waitFor () ;

	if ( exit_value != 0 )
	   throw new Exception ( "Exit value: " + exit_value ) ;
  }
  catch ( Exception e ) {
        VTLError . RunTimeError ( e . toString () ) ;
  }
 
}

static public PrintWriter create_pipe ( String command_line ) throws VTLError
{
  Process	proc ;

  try { 
    proc = Runtime . getRuntime () . exec ( command_line ) ; 

    return ( new PrintWriter ( proc . getOutputStream() ) ) ;

  }
  catch ( Exception e ) {
        VTLError . RunTimeError ( e . toString () ) ;
  }
  
  return ( null ) ;
}


static public boolean file_delete ( String path ) throws VTLError
{
  File 	my_file = new File ( path ) ;

  return ( my_file.delete ( ) ) ;
}

static public boolean file_exists ( String path ) throws VTLError
{
  File 			my_file = new File ( path ) ;

  return ( my_file.exists ( ) ) ;
}

/*
 * Find filename in a file path (remove the path to the last "/")
 */
static String findFileName ( String filename ) throws VTLError
{
	int		idx ;
	
	if ( ( idx = filename.lastIndexOf( '/' ) ) < 0 )
		idx = filename.lastIndexOf( '\\' ) ;
	
	return ( idx > 0 ? filename.substring( idx + 1 ) : filename ) ;
}

/*
 * Find the path in a file path (up to the last "/")
 */
static String findFilePath ( String filename ) throws VTLError
{
	int		idx ;
	
	if ( ( idx = filename.lastIndexOf( '/' ) ) < 0 )
		idx = filename.lastIndexOf( '\\' ) ;
	
	return ( idx > 0 ? filename.substring( 0, idx + 1 ) : "" ) ;
}

/*
 * Find filename in a file path (remove the path to the last "/")
 */
static String findFileSuffix( String filename ) throws VTLError
{
	int		idx ;
	
	idx = filename.lastIndexOf( '.' ) ;
	
	return ( idx > 0 ? filename.substring( idx + 1 ) : null ) ;
}

/*
 * Write blob from input stream to file.
 * filePath == null			create new temporary file with the same suffix of storedFileName
 * filePath is a directory	create new file fileName under directory filePath
 * filePath is a file		create new file filePath
 */
public static String saveOutputStream( String filePath, String fileName, InputStream body ) throws VTLError
{
    int 	c;
    File	myFile ;
    
    try {
	    if ( filePath == null || filePath.length() == 0 )
	    	myFile = File.createTempFile ( "tmp" , "." + Sys.findFileSuffix( fileName ) ) ;
	    else {
	    	if ( new File ( filePath ).isDirectory() )
	    		myFile = new File ( filePath, fileName ) ; // File.createTempFile ( "tmp" , "." + suffix, new File ( directory ) ) ;
	    	else
	    		myFile = new File ( filePath ) ;
	    }
										
	    myFile.deleteOnExit();		// automatically delete the temporary file when the VM exits

    	// BufferedWriter f = new BufferedWriter( new FileWriter( filePath ) );
    	BufferedOutputStream f = new BufferedOutputStream ( new FileOutputStream ( myFile ) ) ;
    	while ((c=body.read())>-1) {
    		f.write(c);
    	}
    	f.close();
    	} 
    catch (Exception e) {
    	VTLError.RunTimeError( e.getMessage() );
    	myFile = null ;
    }
    return ( myFile.getAbsolutePath() ) ;
}

// Output file

static String		outputDevice = "" ;
static boolean		output_device_is_new ;
static PrintWriter 	OutputFile ;

static void setFileName ( String fileName ) 
{
	outputDevice = fileName.trim ( ) ;
	output_device_is_new = true ;
}

public static void output_open ( ) throws VTLError
{
	boolean	append_mode ;
	
	if ( outputDevice . length () > 0 ) {
		if ( output_device_is_new ) {
			append_mode = false ;
			output_device_is_new = false ;
		} 
		else {
	        append_mode = true ;
	    }
	
		try { 
			OutputFile = new PrintWriter ( new BufferedWriter ( 
			  new OutputStreamWriter ( new FileOutputStream ( outputDevice, append_mode ) , "ISO-8859-1" ) ) ) ;
		}
		catch ( IOException e ) { 
			VTLError . RunTimeError  ( e . toString () ) ; 
		}
	}
	else
		OutputFile = new PrintWriter ( System.out, true ) ;  // should it be a BufferedWriter ?
}

public static void output_close ( ) throws VTLError
{ 
	if ( outputDevice.length () > 0 ) {
		if ( OutputFile.checkError() )
			VTLError.RunTimeError ( "ERROR: error writing file" ) ;
		OutputFile.close () ;
	}
}

public final static void print ( String str )
{
	OutputFile.print ( str ) ;
}

public final static void print ( StringBuffer str )
{
	OutputFile.print ( str ) ;
}

public final static void println ( String str )
{
	OutputFile.println ( str ) ;
}

public final static void println ( )
{
	OutputFile.println ( ) ;
}

public final static void write ( String str )
{
	OutputFile.write ( str ) ;
}

public final static void write ( String str, int off, int len )
{
	OutputFile.write ( str, off, len ) ;
}

static int getPosition(String keys [], String k)
{
	int arraydim = keys.length ;

	for ( int idx = 0; idx < arraydim ; idx ++ )
		if ( k.equals ( keys [ idx ] ) )
			return ( idx ) ;
  
	return ( -1 ) ;
}

static public void printStdOut ( String str ) {
	System.out.println( str );
}

static public void printErrorMessage ( String messageType, String message, String functionName, int inputLineNumber ) {
	if ( functionName == null || functionName.equals( "Top level" ) )
		System.out.println( messageType + " at line " + inputLineNumber + ": " + message );
	else
		System.out.println( messageType + " in function " + functionName + " at line " + inputLineNumber + ": " + message );
}

static public void printErrorMessage ( String message ) {
	System.out.println( message );
}

/*
 * Print message to the status line (bottom line in MDI frame)
 */
static public void displayStatusMessage ( String message ) {
	// if ( Session.sqlDebugging ) System.out.println( "Status line. " + message );
	// GUI: print message in the status line
}

}
