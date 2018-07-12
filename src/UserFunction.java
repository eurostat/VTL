
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Vector;

/*
 * User defined operator.
 */

class UserFunctionParm 
{
	String	name ;
	Node	parmType ;
	String	defaultValue ;	
}

class UserFunction {
	String						function_name ;		// function name
	int   						object_id ;			// object id
	Vector<UserFunctionParm>	parameters ;		// Vector of parameters
	Node						return_type ;		// return type
	Node						hd_body ;			// body of function in syntax tree
	char						objectType ;		// operator or function

	// boolean	is_valid ;			// true if this object is valid
	/*
	 * Get description of function parameters.
	 */
	final Object getReturnType () { 
		return ( this.return_type ) ;
	}

	void addParameter ( String parmName, Node pParmType, String parmDefValue ) {
		UserFunctionParm parm = new UserFunctionParm ( ) ;
		parm.name = parmName ;
		parm.parmType = pParmType ;
		parm.defaultValue = parmDefValue ;
		this.parameters.add( parm ) ;
	}

static int	node_index ;

/*
 * Store tree into system table.
 */  
public static void saveMdtTree ( Node hd, PreparedStatement pstmt ) throws VTLError, SQLException
{
    int       	num;
    Node		p ;
    String    	node_value_1, node_value_2 ;

    p = hd .child ;

    num = ( p == null ? 0 : p . nodeListLength ( ) ) ;

    if ( hd . val != null ) {
        if ( ( node_value_1 = hd . val ) . length () > 4000 ) {
        	node_value_2 = node_value_1 . substring ( 4000 ) ;
        	node_value_1 = node_value_1 . substring ( 0, 4000 ) ;
        	if ( node_value_2 . length () > 4000 )
        		VTLError . RunTimeError ( "string too long ( > 8000 characters ): " + ( 4000 +  node_value_2 . length () ) ) ;
        }
        else
        	node_value_2 = null ;
    }
    else 
    	node_value_2 = node_value_1 = null;
    
    pstmt.setInt ( 1, node_index ++ ) ;
    pstmt.setInt ( 2, num ) ;
    pstmt.setShort ( 3, hd.name ) ;
    pstmt.setShort ( 4, hd.info ) ;		// if ( hd.info == 0 ) pstmt.setNull( 4, Types.INTEGER ) ; else
    pstmt.setString ( 5, node_value_1 ) ;      
    pstmt.setString ( 6, node_value_2 ) ;      
    
    pstmt.executeUpdate () ;

    while ( p != null ) {
    	saveMdtTree ( p, pstmt ) ;
    	p = p . next ;
    }
}

/*
 * Store abstract syntax tree into system table.
*/
public static void saveSyntaxTree ( int object_id, Node hd ) throws VTLError
  {
    String	sqlStatement ;
    
    try {
    	sqlStatement = "INSERT INTO " + Db . mdt_user_syntax_trees 
    		+ "(object_id,node_index,node_num_children,node_name,node_info,node_value_1,node_value_2)" 
    		+ "VALUES(" + object_id + ",?,?,?,?,?,?)" ;

    	PreparedStatement pstmt = Db . prepareStatement ( sqlStatement ) ;
    	node_index = 1 ;
    	saveMdtTree ( hd, pstmt ) ;
    	pstmt.close ( ) ;
    } 
    catch ( SQLException e ) {
        VTLError.SqlError  ( e.toString () ) ;
    }
}

/*
 * Read an abstract syntax tree from system table.
*/
static Node getTree ( ResultSet rs ) throws VTLError, SQLException
{
  int		node_num_children ;
  String	node_value_2 ;
  Node		hd = null, p ;

  if ( ! rs . next () ) {
	  if ( rs.getRow() == 0 )
		  return ( null ) ;
	  else
		  VTLError . RunTimeError ( "User function - bad syntax tree" ) ;
  }

  // "SELECT node_num_children,node_name,node_info,node_value_1,node_value_2,node_value_3 FROM "
  
  node_num_children = rs . getInt ( 1 ) ;
  
  hd = Parser.newnode ( rs . getShort ( 2 ) ) ;
  hd.info = rs.getShort( 3 ) ;

  if ( ( node_value_2 = rs . getString ( 5 ) ) != null )
	  hd.val = rs . getString ( 4 ) + node_value_2 ;  
  else
	  hd.val = rs . getString ( 4 ) ;
	  
  if ( node_num_children > 0 ) {
	  hd .child = p = getTree ( rs ) ;
	  node_num_children -- ;
	  while ( node_num_children > 0 ) {
		  p . next = getTree ( rs ) ;
		  p = p . next ;
		  node_num_children -- ;
	  }
  }
	  
  return (hd);
}

/*
 * Get syntax tree from local database.
 */
static final Node getSyntaxTree ( int object_id ) throws VTLError
{
	return ( getSyntaxTree ( Db.mdt_syntax_trees, object_id ) ) ;
}

/*
 * Get syntax tree from database.
 */
public static Node getSyntaxTree ( String sys_mdt_syntax_trees, int objectId ) throws VTLError
{
	String			sqlStatement ;
	Statement		stmt = null ;
	ResultSet 		rs = null ;
	Node			hd ;
	
	sqlStatement = "SELECT node_num_children,node_name,node_info,node_value_1,node_value_2 FROM " 
	  + sys_mdt_syntax_trees + " WHERE object_id=" + objectId + " ORDER BY node_index " ;

	try {
		//Sys.println ( "Get syntax tree: start - " + object_id ) ;
	    rs = ( stmt = Db.createStatement() ).executeQuery ( sqlStatement ) ;
	    stmt.setFetchSize(128);
	    if ( rs == null )
		    VTLError.InternalError( "Result set is null:" + objectId ) ;
	    hd = getTree ( rs ) ;
	    // if ( hd == null )	Sys.println( "Warning: syntax tree is null - object_id: " + object_id ) ;
	    // else hd . printMdtSyntaxTree ( ) ;					// debug
	    // rs . close () ;
		//Sys.println ( "Get syntax tree: end - " + object_id ) ;
	}
	catch ( SQLException e ) {
	    VTLError . SqlError  ( e . toString () + "\nSQL query:\n" + sqlStatement ) ;
	    hd = null ;
	}  
	finally {
		Db.closeStatement (stmt) ;
	}
	return ( hd ) ;
}

/*
	define operator  operator_name ( { parameter { , parameter }* } )
		{ returns outputType }
	is operatorBody
	end define operator
	parameter::= parameterName parameterType { default parameterDefaultValue }

	function_args	return_type	function_body
	the function body (text) is attached to a temporary node (this node must not be saved in the syntax tree)
  	define operator f ( x integer ) returns integer 
  	is x + 1
  	end define operator

	Get user function from database.
 */
public static UserFunction getUserDefinedOperator ( String function_name ) throws VTLError
{
	UserFunction	fun = new UserFunction () ;
	VTLObject		obj ;
	Node			hd = null, pParms ;

	obj = VTLObject.getObjectDescSpecific( function_name, VTLObject.O_OPERATOR ) ;

	fun.function_name = function_name ;
	fun.object_id = obj.objectId ;
  
	hd = getSyntaxTree ( Db.mdt_syntax_trees, fun.object_id ) ;
  
	if ( hd == null || hd.child == null || hd.countChildren() != 3 )
		VTLError.RunTimeError ("User defined operator: " + function_name + " badly formed (1)") ;
    
	fun.parameters = new Vector<UserFunctionParm> ();
	  
	pParms = hd.child ;		// list of argument name, type and default value
	
	for ( Node p = pParms.child ; p != null ; p = p.next )
		fun.addParameter ( p.child.val, p.child_2(), p.child_3().val ) ;

	fun.return_type = hd.child_2() ;
	fun.hd_body = hd.child_3() ;
	fun.objectType = ( fun.hd_body.child.name == Command.Commands.N_STATEMENT_LIST ? VTLObject.O_FUNCTION 
									: VTLObject.O_OPERATOR ) ;
	return ( fun ) ;
}

boolean isOperator ()
{
	return ( this.objectType == VTLObject.O_OPERATOR ) ;
}

/*
 * Return the body (text).
 */
String getDefinition ( ) throws VTLError { 
	VTLObject obj = VTLObject.getObjectDesc( this.function_name ) ;

	if ( obj == null )
		VTLError.InternalError ( "User defined operator " + function_name + " does not exist" ) ;

	if ( obj.objectType != VTLObject.O_OPERATOR )
		VTLError.InternalError ( "Object " + function_name + " is not a user-defined operator" ) ;

	return ( obj.get_source ( ) ) ;	 
}

/*
 * Save function.
	define operator testFuction2 ( x string not null ) is
	x + 1
	end define operator
 */
static void saveUserDefinedOperator ( String object_name, char objectType, boolean create_or_replace, String body, Node hd ) throws VTLError
{
	int			object_id = 0 ;
	boolean		is_new_object ;

	if ( create_or_replace )
		object_id = VTLObject . createReplaceObjectId ( object_name, VTLObject.O_OPERATOR ) ;
	is_new_object = ( object_id == 0 ) ;
	if ( is_new_object )
		object_id = VTLObject . createObjectId ( object_name, VTLObject.O_OPERATOR ) ;
		
	Db.insertSource ( object_id, body ) ;
		
	UserFunction.saveSyntaxTree ( object_id, hd ) ;
		  
	if ( ! is_new_object )
		VTLObject.setObjectModified ( object_id ) ;
}

/*
 * Copy user function.
 */
public static void copySyntaxTree ( VTLObject obj, String dblink, int new_object_id ) throws VTLError
{
	String		mdt_sys ;
	 
	mdt_sys = ( dblink != null ? obj.remoteOwner + ".mdt_syntax_trees" + dblink : Db.mdt_syntax_trees ) ;
	Db.sqlExec( "INSERT INTO " + Db.mdt_user_syntax_trees 
		+ "(object_id,node_index,node_num_children,node_name,node_info,node_value_1,node_value_2)" 
		+ " SELECT " + new_object_id + ",node_index,node_num_children,node_name,node_info,node_value_1,node_value_2"	
		+ " FROM " + mdt_sys + " WHERE object_id=" + obj.objectId ) ;
}

/*
 * Copy user function.
 */
public static void copyUserFunction ( VTLObject obj, String dblink, int new_object_id ) throws VTLError
{
	String		mdt_sys ;
	
	mdt_sys = ( dblink != null ? obj.remoteOwner + ".mdt_sources" + dblink : Db.mdt_sources ) ;
	Db.sqlExec( "INSERT INTO " + Db.mdt_user_sources 
	     + "(object_id, buffer_index, buffer_text)"
	     + " SELECT " + new_object_id + ", buffer_index, buffer_text"	
	     + " FROM " + mdt_sys + " WHERE object_id=" + obj.objectId ) ;
	 
	mdt_sys = ( dblink != null ? obj.remoteOwner + ".mdt_dependencies" + dblink : Db.mdt_dependencies ) ;
	Db.sqlExec( "INSERT INTO " + Db.mdt_user_dependencies 
	     + "(object_id,ref_object_owner,ref_object_name,ref_type)"
	     + " SELECT " + new_object_id + ",ref_object_owner,ref_object_name,ref_type"	
	     + " FROM " + mdt_sys + " WHERE object_id=" + obj.objectId ) ;
	 
	mdt_sys = ( dblink != null ? obj.remoteOwner + ".mdt_syntax_trees" + dblink : Db.mdt_syntax_trees ) ;
	Db.sqlExec( "INSERT INTO " + Db.mdt_user_syntax_trees 
		+ "(object_id,node_index,node_num_children,node_name,node_info,node_value_1,node_value_2)" 
		+ " SELECT " + new_object_id + ",node_index,node_num_children,node_name,node_info,node_value_1,node_value_2"	
		+ " FROM " + mdt_sys + " WHERE object_id=" + obj.objectId ) ;
}

}
