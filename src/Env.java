
/*
   Environment: management of local variables.
   A variable can be:
     scalar variable				( value stored in the local environment )
     set scalar variable			( value stored in the local environment )
     multidimensional variable  	( values stored in a SQL table )
*/

import java.util.Vector;

class FunctionCall {
	String			function_name ;
	Vector <Env>	environment ;
	short			line_number;
	Query			return_query ;					// used to compute the return value of a procedure (if any)
	FunctionCall ( String my_function_name ) {
		function_name = my_function_name ;
		environment = new Vector <Env>() ;
		return_query = null ;		
	}
}

class Env {

String		variable_name ;
Object		variable_type ;				// String or Dataset
Object		variable_value ;

Env ( String my_variable_name, Object my_variable_type,Object my_variable_value) {
	variable_name = my_variable_name ;
	variable_type = my_variable_type ;
	variable_value = my_variable_value ;
}

static Vector <FunctionCall> functionCalls = new Vector <FunctionCall> () ;

/*
 * Create a new stack and return the previous (can be restored after execution).
 */
static final void newStackFunctionCalls ( ) throws VTLError
{
	for ( int idx = 0; idx < functionCalls.size(); idx ++ ) 
		Env.removefunctionCall( ) ;	
	addfunctionCall( "Top level" ) ;
}

/*
 * Return line number of function currently running.
 */
static short runtimeErrorLineNumber ( )
{	
	return ( functionCalls.size( ) > 0 ? functionCalls.lastElement().line_number : Lex.getInputLineNumber() ) ;
}

/*
 * Return name of function currently running.
 */
static String runtimeErrorFunctionName ( )
{
	return ( functionCalls.size( ) > 0 ? functionCalls.lastElement( ).function_name : null ) ;
}

/*
 * Check existence of stack of function calls.
 */
static final void checkStack ( ) throws VTLError
{
	if ( functionCalls.size() == 0 )
		VTLError.InternalError( "Environment" ) ;
}

/*
 * Print stack of function calls.
 */
static String printStack ( short line_number ) throws VTLError
{	
	checkStack ( ) ;
	StringBuffer bufferStackFunctionCalls = new StringBuffer ( ) ;
	
	functionCalls.lastElement().line_number = line_number ;
	
	for ( FunctionCall	f : functionCalls ) {
		if ( bufferStackFunctionCalls.length() > 0 )
			bufferStackFunctionCalls.append( ", " ) ;
		bufferStackFunctionCalls.append ( f.function_name ).append( ": " ).append( f.line_number ) ;
	}
	return ( bufferStackFunctionCalls.toString() ) ;
}

/*
 * Return environment of current function in execution.
 */
static Vector <Env> currentEnvironment ( ) throws VTLError
{
	checkStack ( ) ;
	return ( functionCalls.lastElement ( ).environment ) ;
}

/*
 * Return environment of current function in execution.
 */
static void setLineNumberCurrentEnv ( short lineNumber ) throws VTLError
{
	checkStack ( ) ;
	functionCalls.lastElement ( ).line_number = lineNumber ;
}

/*
 * Return top-level environment (variables assigned).
 */
static final Vector <Env> topLevelEnvironment ( ) throws VTLError
{
	checkStack ( ) ;
	return ( functionCalls.firstElement ( ).environment ) ;
}

/*
 * Add function call to the stack.
 */
static void addfunctionCall ( String function_name )
{
	functionCalls.add ( new FunctionCall ( function_name ) ) ; 
}

/*
 * Add function call to the stack.
 */
static void removefunctionCall ( ) throws VTLError
{
	checkStack ( ) ;
	
	Env.dropTemporaryTables ( functionCalls.lastElement().environment ) ;
	
	functionCalls.remove( functionCalls.size() - 1 ) ;
}

/*
 * Add new variable.
 */
static int addVar ( String variable_name, Object data_type, Object var_value ) throws VTLError
{
	Vector <Env> 	environment = Env.currentEnvironment ( ) ;
	
	if ( getVarIndex ( variable_name ) >= 0 ) 
		VTLError.TypeError ( "Duplicate variable: " + variable_name ) ;
	
	environment.add ( new Env (variable_name, data_type, var_value) ) ;
	
	return ( environment.size() - 1 ) ;
}

/*
 * Remove variable.
 */
static void removeVar ( int var_index ) throws VTLError
{
	Vector <Env> 	environment = Env.currentEnvironment ( ) ;
	if ( var_index < 0 )
		VTLError.InternalError("removeVar") ;
	environment.remove( var_index ) ;
}

/*
 * Get index of variable.
 * // x := "DK" ;  na_main [ filter ref_area = x ]
 */
static int getVarIndex ( String variable_name ) throws VTLError
{
	Vector <Env> 	environment ;

	environment = Env.currentEnvironment ( ) ;
	
	for ( int var_index = environment.size() - 1 ; var_index >= 0 ; var_index -- )
		if ( environment.get ( var_index ).variable_name.equals ( variable_name ) )
			return ( var_index ) ;

	return ( -1 ) ;
}

/*
 * Get index of variable.
 * // x := "DK" ;  na_main [ filter ref_area = x ]
 */
static int getVarIndexTopLevel ( String variable_name ) throws VTLError
{
	Vector <Env> 	environment = Env.topLevelEnvironment() ;

	for ( int var_index = environment.size() - 1 ; var_index >= 0 ; var_index -- )
		if ( environment.get ( var_index ).variable_name.equals ( variable_name ) )
			return ( var_index ) ;
	
	return ( -1 ) ;
}

/*
 * Get type of variable whose index = var_index.
 */
static Object getVarTypeTopLevel ( int var_index ) throws VTLError
{
	Vector <Env> 	environment = Env.topLevelEnvironment ( ) ;
	
	if ( var_index < 0 || var_index >= environment.size() )
		VTLError.InternalError ( "get_env_type top level" ) ;

	return ( environment.get ( var_index ).variable_type ) ;
}
/*
 * Drop temporary tables existing in this environment.
 */
static void dropTemporaryTables ( Vector <Env> 	environment ) throws VTLError
{
	for ( Env e : environment ) {
		if ( e.variable_value != null && Check.isQueryType( e.variable_type ) ) {
			if ( ((String) e.variable_value).startsWith("tmp$") ) {
				// Sys.println ( "Drop temporary table: " + e.variable_value ) ;
				Db.sqlExec ( "DROP TABLE " + e.variable_value + " PURGE" ) ;
				e.variable_value = null ;
			}
		}
	}
}

/*
 * Print environment.
 */
static String printEnvironment ( ) throws VTLError
{	
	StringBuffer	str = new StringBuffer();
	
	str.append ("Variables: ") ;
	for ( Env e : Env.currentEnvironment ( ) )
		str.append( e.variable_name ).append( " " ) ;

	return ( str.toString() ) ;
}

/*
 * Get type of variable whose index = var_index.
 */
static Object getVarType ( int var_index ) throws VTLError
{
	Vector <Env> 	environment = Env.currentEnvironment ( ) ;
	
	if ( var_index < 0 || var_index >= environment.size() )
		VTLError . InternalError ( "get_env_type" ) ;

	return ( environment.get ( var_index ).variable_type ) ;
}

/*
 * Get list value of variable whose index = var_index.
 */
static ListString getSetScalarValue ( int var_index ) throws VTLError
{
	Vector <Env> 	environment = Env.currentEnvironment ( ) ;
	Env				my_env ;

	if ( var_index < 0 || var_index >= environment.size()  )
		VTLError . InternalError ( "get_env_value" ) ;

	my_env = environment . get ( var_index ) ;
	  
	if ( ! Check . isSetScalarType ( my_env . variable_type ) )
		VTLError . TypeError ( "Variable " + my_env . variable_name + " is not a set scalar" ) ;
  
	return ( ( ListString ) my_env.variable_value ) ; 
}

/*
 * Get Query value of variable whose index = var_index.
 */
static Query getQueryValue ( int varIndex ) throws VTLError
{
	Vector <Env> 	environment = Env.currentEnvironment ( ) ;
	Env				my_env ;

	if ( varIndex < 0 || varIndex >= environment.size() )
		VTLError.InternalError ( "getQueryValue" ) ;

	my_env = environment.get ( varIndex ) ;

	if ( Check.isSetScalarType ( my_env.variable_type ) )
		VTLError.TypeError ( "Not a scalar value" ) ;

	return ( ( Query ) my_env.variable_type ) ; 
}

/*
 * Get scalar value of variable whose index = var_index.
 */
static String getScalarValue ( int var_index ) throws VTLError
{
	Vector <Env> 	environment = Env.currentEnvironment ( ) ;
	Env				my_env ;

	if ( var_index < 0 || var_index >= environment.size() )
		VTLError . InternalError ( "get_env_value" ) ;

	my_env = environment.get ( var_index ) ;

	if ( Check.isSetScalarType ( my_env.variable_type ) )
		VTLError.TypeError ( "Not a scalar value" ) ;

	return ( ( String ) my_env . variable_value ) ; 
}

/*
 * Set env value of variable_name.
static void setDataType ( int var_index, Object data_type ) throws VTLError
{
	Vector <Env> 	environment = Env.currentEnvironment ( ) ;
	Env	my_env ;
	
	if ( var_index < 0 || var_index >= environment.size() )
	  VTLError . InternalError ( "setValue" ) ;
	
	my_env = environment . get ( var_index ) ;
	my_env .variable_type = data_type ;
}
*/
/*
 * Set env value of variable_name.
 */
static void setValue ( int var_index, Object var_value ) throws VTLError
{
	Vector <Env> 	environment = Env.currentEnvironment ( ) ;
	Env	my_env ;
	
	if ( var_index < 0 || var_index >= environment.size() )
		VTLError . InternalError ( "setValue" ) ;
	
	my_env = environment.get ( var_index ) ;
	my_env.variable_value = var_value ;
}

/*
 * Compute return value of function.
 */
static void computeReturnValue ( Query q ) throws VTLError
{
	Env.checkStack ( ) ;
	Env.functionCalls.lastElement ( ).return_query = q ;
}

/*
 * Get return value of function.
 */
static Query getReturnValue ( ) throws VTLError
{
	Env.checkStack ( ) ;
	return ( Env.functionCalls.lastElement ( ).return_query ) ;
}

}

