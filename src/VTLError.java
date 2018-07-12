
/*
   Generic class for exceptions. Types of exceptions managed by VTL:

     syntax error		
     type error		(compiler error, static )
     run time error   
     SQL error (error in the SQL syntax built by the interpreter)
     internal error (caused by malfunction of the VTL interpreter)

   System exceptions (e.g. NullPointerException ) are managed by Java.
*/

public class VTLError extends Exception 
{
	static final long	serialVersionUID = 1;		// required by Java serializable class
  
	private String 	errorType ;
	private String 	errorMessage ;
	private String 	functionName  ;
	private short 	lineNumber ;
	private int 	tokenStart ;				// in case of syntax error, where the token starts
	private int 	tokenEnd ;					// in case of syntax error, where the token ends
	private int		sqlErrorCode ;

	public String getErrorType() {
	  return errorType ;
	}
	public String getErrorMessage() {
		return errorMessage ;
	}
	public String getErrorFunctionName () {
		return functionName ;
	}
	public short getErrorLineNumber () {
		return lineNumber ;
	}
	public int getErrorTokenStart () {
		return tokenStart ;
	}
	public int getErrorTokenEnd () {
		return ( tokenStart == tokenEnd ? tokenEnd + 1 : tokenEnd ) ;
	}
	public int getSqlErrorCode () {
		return sqlErrorCode ;
	}

  /*
   * VTL Error
   */
  public VTLError (  String errType, String msg, int argSqlErrorCode ) {
	    super ( msg ) ;
		this.errorType = errType ;
		this.errorMessage = msg ;
		this.functionName = Env.runtimeErrorFunctionName ( ) ;
		this.lineNumber = ( this.errorType == "Syntax error" ? Lex.getInputLineNumber() : Env.runtimeErrorLineNumber() ) ;
		this.tokenStart = ( this.errorType == "Syntax error" ? Lex.getInputTokenStart() : -1 ) ;
		this.tokenEnd = ( this.errorType == "Syntax error" ? Lex.getInputTokenEnd() : -1 ) ;
		this.sqlErrorCode = argSqlErrorCode ;
	  }

  public static void SyntaxError ( String msg ) throws VTLError {
    throw new VTLError ( "Syntax error" , msg, 0 ) ;
  }

  public static void TypeError ( String msg ) throws VTLError {
	  throw new VTLError ( "Type error" , msg, 0 ) ;
  }

  public static void TypeError ( String operator, String msg ) throws VTLError {
	  throw new VTLError ( "Type error" , operator + ": " + msg, 0 ) ;
  }

  public static void RunTimeError ( String msg ) throws VTLError {
	  throw new VTLError ( "Runtime error" , msg, 0 ) ;
  }

  public static void SqlError ( String msg ) throws VTLError {
	  throw new VTLError ( "SQL error" , msg, 0 ) ;
  }

  public static void InternalError ( String msg ) throws VTLError {
	  throw new VTLError ( "Internal error" , msg, 0 ) ;
  }

  public static void UserDefinedError ( String msg ) throws VTLError {
	  throw new VTLError ( "User-defined error" , msg, 0 ) ;
  }
}

