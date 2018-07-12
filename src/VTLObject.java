
/*
 * Operations on generic object descriptor.
 */

import java.sql.* ;
import java.util.* ;

class VTLObject {

int   		objectId ;
char  		objectType ;
String		synonymFor ;
int   		realObjectId ;
String		realObjectName ;
String		dbLink ;
String		remoteOwner ;
int			changeId ;
String		dropOriginalName ;

static PreparedStatement 	pstmtgetObjectDesc = null ;
static String		 	 	mdtSysGetObjectDesc = "" ;

/*
 * Object type
 */
static final char
	O_VALUEDOMAIN 	= 'D',
	O_DATASET 		= 'T' ,
	O_OPERATOR 		= 'F',
	O_DATAPOINT_RULESET = 'N' ,
	O_HIERARCHICAL_RULESET = 'R' ,
	O_VIEW 		= 'V' ,
	O_FUNCTION	= 'P' ,
	O_SYNONYM 	= 'Y' ;

/*
 * Used by get_object_desc.
 */
public static VTLObject get_object_desc_2 ( String mdt_sys, String user_name, String object_name ) throws VTLError
{
	String			sql_query ;
	VTLObject		obj ;
	ResultSet 		rs ;
	  
	// Sys.println( mdt_sys + " " + user_name+ " " + object_name ) ;
	  
	try {
		if ( mdt_sys.compareTo( mdtSysGetObjectDesc ) != 0 ) {
			sql_query = "SELECT object_id,object_type,synonym_for,change_id,drop_original_name FROM "
				+ mdt_sys + " WHERE user_name=? AND object_name=?" ;	
			if ( pstmtgetObjectDesc !=  null )
				pstmtgetObjectDesc.close() ;
			pstmtgetObjectDesc = Db.prepareStatement( sql_query ) ;
			mdtSysGetObjectDesc = mdt_sys ;
		}

		pstmtgetObjectDesc.setString ( 1, user_name ) ;
		pstmtgetObjectDesc.setString ( 2, object_name ) ;
		rs = pstmtgetObjectDesc.executeQuery ( ) ;

		if ( rs . next () ) {			// found
			obj = new VTLObject () ;
			obj.objectId = rs . getInt ( 1 ) ;
			obj.realObjectId = obj .objectId ;
			obj.objectType = rs . getString ( 2 ) . charAt ( 0 ) ;
			obj.synonymFor = rs . getString ( 3 ) ;
			obj.changeId = rs . getInt ( 4 ) ;
			if ( rs.wasNull())
				obj.changeId = 0 ;
			obj.dropOriginalName = rs . getString ( 5 ) ;
		}
		else
			  obj = null ;

		rs .close () ;
		// st . close () ;
	}
	catch ( SQLException e ) {
		VTLError . SqlError  ( e . toString () ) ;
		obj = null ;
	}

	return ( obj ) ;
}

/*
  Returns the object descriptor of object.
  If object_name is not in the form ( user_name . object_name ) then search object_name under 
  the search user defined in the user profile.
  The object name is one of:
	object_name
	user_name . object_name
	user_name . object_name @ db_link
  A distinct version should be used for real_object_id.
*/

static VTLObject getObjectDesc ( String objectName ) throws VTLError
{
	return ( getObjectDesc ( objectName, true ) ) ;
} 

/*
 * Get object descriptor and check it is of a specific type of object 
 */
static VTLObject getObjectDescSpecific ( String objectName, char objectType ) throws VTLError
{
	VTLObject	obj = VTLObject.getObjectDesc( objectName, true ) ;
	if ( obj == null )
		VTLError.TypeError ( "Object " + objectName + " does not exist" ) ;
	if ( obj.objectType != objectType )
		VTLError.TypeError ( "Object " + objectName + " is not a: " + stringObjectType ( objectType ) ) ;
	return ( obj ) ;
} 

/*
 * Find object descriptor. If follow_links is true then follow the synonym link
 */
public static VTLObject getObjectDesc ( String real_object_name, boolean follow_links ) throws VTLError
{
  String		user_name, object_name, mdt_sys, db_link, remote_mdt_owner ;
  int 	        idx_point, idx_at ;
  VTLObject		obj ;

  object_name = real_object_name ;

  if ( (idx_at = object_name.indexOf( '@' ) ) >= 0 ) {
	  db_link = object_name.substring( idx_at ) ;
	  object_name = object_name.substring( 0, idx_at ) ;
	  remote_mdt_owner = Db.findOwnerOfSystemTables( db_link ) ;
	  mdt_sys = remote_mdt_owner + ".mdt_objects" + db_link ;
  }
  else {
	  mdt_sys = Db.mdt_objects ;
	  remote_mdt_owner = null ;
	  db_link = null ;
  }
  
  idx_point = object_name . indexOf ( Parser.ownershipSymbol ) ;
  if ( idx_point > 0 ) {
	  user_name = object_name . substring ( 0 , idx_point ) . toUpperCase () ;
	  object_name = object_name . substring ( idx_point + 1 ) ;
  }
  else
     user_name = Db . db_username . toUpperCase () ;

  obj = get_object_desc_2 ( mdt_sys, user_name, object_name  ) ;

  // find in schema search_user
  if ( obj == null && idx_point < 0 && UserProfile .searchUser != null && follow_links ) {
	  user_name = UserProfile .searchUser.toUpperCase() ;
	  obj = get_object_desc_2 ( mdt_sys, user_name, object_name ) ;
	  if ( obj != null ) {
		  real_object_name = UserProfile .searchUser + "." + object_name ; // VTL 1 #
	  }
  }
 
  // is it a synonym?
  while ( obj != null && obj.synonymFor != null && follow_links ) {
	  object_name = obj.synonymFor ;
	  if ( object_name.indexOf('@') >= 0 )
		  return ( getObjectDesc ( object_name, follow_links ) ) ;
	  
	  idx_point = object_name . indexOf ( Parser.ownershipSymbol ) ;
	  if ( idx_point > 0 ) {
		  user_name = object_name . substring ( 0 , idx_point ) . toUpperCase () ;
		  object_name = object_name . substring ( idx_point + 1 ) ;
	  }
	  // else user_name is the last user_name used
	     
	  obj = get_object_desc_2 ( mdt_sys, user_name, object_name ) ;
	  if ( obj == null )
		  VTLError.RunTimeError( "Broken link for synonym " + object_name ) ;
  } 
  
  if ( obj != null ) {
	  obj.realObjectName = real_object_name ;
	  obj.remoteOwner = remote_mdt_owner ;
	  obj.dbLink = db_link ;
  }
  
  return ( obj ) ;
}

/*
 * Close Oracle statements that have been prepared.
 */
static void closePreparedStatements () throws SQLException
{
	if ( VTLObject.pstmtObjectModif != null ) {
		  VTLObject.pstmtObjectModif.close() ;
		  VTLObject.pstmtObjectModif = null ;
	}		

	if ( pstmtgetObjectDesc != null ) {
		pstmtgetObjectDesc.close() ;
		pstmtgetObjectDesc = null ;
	}
	
	mdtSysGetObjectDesc = "" ;
}

static PreparedStatement pstmtObjectModif = null ;

/*
 * Return true if object has been modified after last access or if object does not exist (anymore).
 */
public static boolean was_modified_after ( int object_id, int change_id ) throws VTLError 
{
  int			db_change_id ;
  ResultSet 	rs ;
  boolean		result ;
  
  /* sql_query = "SELECT TO_CHAR(NVL(last_modified,created),'YYYY.MM.DD HH24:MI:SS') FROM " 
	  + Db . mdt_objects + " WHERE object_id=" + object_id ; */
/*
 * PreparedStatement pstmt = null ;
  
  try {
    num_values = dim_values . size () ;
    
    pstmt = Db . prepareStatement ( "INSERT INTO " + Db . mdt_user_positions
                                       + "(object_id,dim_index,pos_index,pos_code)VALUES("
                                       + this.object_id + "," + ( dim_index + 1 ) + ",?,?)" ) ;
    for ( idx = 0; idx < num_values; idx ++ ) {
    	// Sys.println ( pos_index+ "*" + dim_values . get (idx )  ) ;
        pstmt . setInt ( 1, pos_index ++ ) ;
 */
  

  //sql_query = "SELECT change_id FROM " + Db . mdt_objects + " WHERE object_id=" + object_id ;
  
  try {
	  if ( pstmtObjectModif == null )
		  pstmtObjectModif = Db . prepareStatement ( "SELECT change_id FROM " + Db . mdt_objects + " WHERE object_id=?" ) ;
	  pstmtObjectModif . setInt ( 1, object_id ) ;
	  rs = pstmtObjectModif.executeQuery() ;		// Db . sql_statement . executeQuery ( sql_query ) ;
	
	  /* if ( rs . next () ) {
		  if ( ( db_modif_time = rs . getString ( 1 ) ) == null )
			  result = true ;
		  else 
			  result = ( last_modification_time.compareTo(db_modif_time) < 0 ) ;
	  }
	  */
	  if ( rs . next () ) {
		  db_change_id = rs . getInt ( 1 ) ;
		  if ( rs.wasNull() && change_id == 0 )			
			  result = false ;
		  else 
			  result = ( change_id < db_change_id ) ;
	  }
	  else
		  result = true ;
	  rs .close () ;
  }
  catch ( SQLException e ) {
	  VTLError . SqlError  ( e . toString () ) ;
	  result = true ;
  }

  return ( result ) ;
}

/*
 * Used by get_object_desc.
 */
public static Vector <VTLObject> getAllObjectDescs ( String mdt_sys, String where_condition ) throws VTLError
{
	String					sql_query ;
	Vector <VTLObject>		myObjects = new Vector <VTLObject> () ;
	VTLObject				obj ;
	ResultSet 				rs ;
	Statement				st ;
	  
	// Sys.println( mdt_sys + " " + user_name+ " " + object_name ) ;
	  
	try {
		sql_query = "SELECT object_id,object_type,synonym_for,change_id,drop_original_name,object_name"
				+ " FROM " + mdt_sys + ( where_condition == null ? "" : " " + where_condition ) ;	
		
	    st = Db .DbConnection.createStatement() ;
	    rs = st.executeQuery ( sql_query ) ;

		while ( rs . next () ) {			// found
			obj = new VTLObject () ;
			obj.objectId = rs . getInt ( 1 ) ;
			obj.realObjectId = obj .objectId ;
			obj.objectType = rs . getString ( 2 ) . charAt ( 0 ) ;
			obj.synonymFor = rs . getString ( 3 ) ;
			obj.changeId = rs . getInt ( 4 ) ;
			if ( rs.wasNull())
				obj.changeId = 0 ;
			obj.dropOriginalName = rs . getString ( 5 ) ;
			obj.realObjectName = rs . getString ( 6 ) ;
			myObjects.add( obj ) ;
		}

		rs .close () ;
		st . close () ;
	}
	catch ( SQLException e ) {
		VTLError . SqlError  ( e . toString () ) ;
	}

	return ( myObjects ) ;
}

/*
   Create new object. Generate new object_id. Insert into mdt_objects. 
*/
static int new_object ( String object_name, String object_comment, char object_type, String synonym_for ) throws VTLError
{
	String object_id ;

	object_id = Db . sqlGetValue ("SELECT " + Db . mdt_object_id + ".nextval FROM DUAL") ;
	if ( object_id == null )
		VTLError . InternalError ( "new_object_id" ) ;

	Db . sqlExec ( "INSERT INTO " + Db . mdt_user_objects +
			"(user_name,object_id,object_name,object_type,created,synonym_for)VALUES('" + Db . db_username + "'," 
			+ object_id + ",'" + object_name + "','" + object_type + "',SYSDATE,'" + synonym_for + "')" ) ;

	Db . sqlExec ( "INSERT INTO " + Db . mdt_user_objects_comments 
			+ "(object_id, object_comment)VALUES(" + object_id + ",'" + object_comment + "')" ) ;
	// + object_comment . replaceAll ( "'", "''") + "')" ) ;
	
	return ( Integer.parseInt ( object_id ) ) ;
}

/*
   Delete object descriptor.
static void delete_object_desc ( String object_name )
{
  int		idx ;
  MdtObject	obj ;

  for ( idx = 0 ; idx < object_descriptors . size () ; idx ++ )
      {
	obj = object_descriptors . get ( idx ) ;
	if ( object_name . equals ( obj . real_object_name ) )
	   {
             object_descriptors . remove ( idx ) ;
	     break ;
	   }
      }
}
*/

/*
 * Create synonym.
 */
static void create_synonym ( String synonym_name, String object_name, boolean compile_only ) throws VTLError
{
	VTLObject	obj ;
	String	synonym_comment ;

	synonym_name = Check . checkObjectOwner ( synonym_name ) ;

	obj = VTLObject . getObjectDesc ( object_name, false ) ;
	if ( obj == null )
		VTLError.TypeError ( "Object " + object_name + " does not exist") ;

	if ( obj.objectType == VTLObject.O_VALUEDOMAIN )
		VTLError.TypeError( "Cannot creat synonym for a value domain");

	Check.checkNewObjectName ( synonym_name ) ;

	if ( compile_only )
		return ;
  
	if ( object_name . indexOf ( "@" ) < 0 )
		synonym_comment = Db . sqlGetValue ( "SELECT object_comment FROM " 
		                               		+ Db . mdt_objects_comments + " WHERE object_id=" + obj .objectId ) ;
	else
		synonym_comment = "(Synonym to a remote object)" ;	// cannot access remote object

	new_object ( synonym_name, synonym_comment, obj.objectType, object_name ) ;

	Db.sqlCommit () ;

	if ( obj.isDataObject ( ) )
		Db.sqlExec ( "CREATE SYNONYM " + synonym_name + " FOR " + object_name ) ;
}

/*
 * Change the object's description.
 */
static void setObjectDescription ( String object_name, Query q ) throws VTLError
{
	String				sql_query, property_name, tmp;
	StringBuffer		prop_list ;
	VTLObject			obj ;
	Query				qComments ;
	DatasetComponent	prop_objects_comments ;

	obj = VTLObject.getObjectDesc ( object_name, false ) ;
	if ( obj == null )
		VTLError.TypeError ( "Object " + object_name + " does not exist") ;

	qComments = Db.getDescObjectsComments ( null ) ;
	
	object_name = Check.checkObjectOwner ( object_name ) ;

	prop_list = new StringBuffer () ;
	
	for ( DatasetComponent q_prop : qComments.measures ) {
		prop_objects_comments = q.getMeasureAttribute (  q_prop.compName ) ;
		if ( prop_objects_comments == null ) {
			if ( q .measures . size () == 1 && q_prop.compName.equals( "string_var" ) )
				property_name = "object_comment" ;	// corresponds to the following syntax: comment on t is "comm" ;
			else {
				VTLError . RunTimeError ( "Undefined property for mdt comments: " + q_prop .compName ) ;			
				property_name = null ;
			}
		}
		else {
			property_name = prop_objects_comments .compName ;
			if ( prop_objects_comments.canBeNull == false
					&& (q_prop.sql_expr == null || q_prop.sql_expr.length() == 0 || q_prop.sql_expr.compareTo( "''" ) == 0 ) )
				VTLError.RunTimeError( "The value of property " + property_name + " cannot be null" ) ;
			if ( prop_objects_comments.compType.equals ( "date" ) ) {
				tmp = UserProfile.dateFormat.replace("HH", "HH24").replace("mm", "MI") ;
				q_prop.sql_expr = "to_date(" + q_prop.sql_expr + ",'" + tmp + "')" ;				
			}
		}
		
		if ( prop_list . length() > 0 )
			prop_list . append ( ',' ) ;
		prop_list . append ( property_name ) ;
	}
	
	sql_query = q.build_sql_query ( false ) ;
	
	// execute DB operations
	// single command - no need to rollback
	Db . sqlExec ( "UPDATE " + Db . mdt_user_objects_comments 
				 	+ " SET (" + prop_list . toString() + ")" + " = (" + sql_query + ")" 
					+ " WHERE object_id=" + obj.realObjectId ) ;		    

	Db . sqlCommit () ;
}

/*
 * Return the object id of an existing object or 0 for a new object.
 */
static int createReplaceObjectId ( String object_name, char object_type ) throws VTLError
{
  VTLObject	obj ;
  int		object_id ;

  if ( ( obj = getObjectDesc( object_name, false ) ) == null ) 
	  object_id = 0 ;
  else {
	  if ( object_type != obj.objectType )
		  VTLError.RunTimeError( "create or replace: object type of existing object " 
				  + object_name + " is not correct (" + obj.objectType + ")" ) ;
	  
	  object_id = obj.objectId ;
	  switch ( object_type ) {
	      case VTLObject.O_OPERATOR :
	    	  Db.dbDelete ( Db.mdt_user_dependencies, object_id ) ;
	    	  Db.dbDelete ( Db.mdt_user_sources, object_id ) ;
	    	  Db.dbDelete ( Db.mdt_user_syntax_trees, object_id ) ;
	    	  break ;
	      case VTLObject.O_HIERARCHICAL_RULESET :
	    	  Db.dbDelete ( Db.mdt_user_equations_items, object_id ) ;
	    	  Db.dbDelete ( Db.mdt_user_equations_tree, object_id ) ;
	    	  Db.dbDelete ( Db.mdt_user_equations, object_id ) ;
	    	  Db.dbDelete ( Db.mdt_user_dimensions, object_id ) ;
	    	  Db.dbDelete ( Db.mdt_user_syntax_trees, object_id ) ;
	    	  break ;
	      case VTLObject.O_DATAPOINT_RULESET :
		      Db.dbDelete ( Db.mdt_user_validation_conditions, object_id ) ;
		      Db.dbDelete ( Db.mdt_user_validation_rules, object_id ) ;
		      Db.dbDelete ( Db.mdt_user_syntax_trees, object_id ) ;
		      break ;
	      case VTLObject.O_VALUEDOMAIN :	// valuedomain subset
	      case VTLObject.O_VIEW :
	    	  Db.dbDelete ( Db.mdt_user_positions, object_id ) ;
	    	  Db.dbDelete ( Db.mdt_user_dimensions, object_id ) ;
	    	  Db.dbDelete ( Db.mdt_user_sources, object_id ) ;
	    	  Db.dbDelete ( Db.mdt_user_audits, object_id ) ;
	    	  Db.dbDelete ( Db.mdt_user_dependencies, object_id ) ;
	    	  Db.dbDelete ( Db.mdt_user_syntax_trees, object_id ) ;
	    	  break ;
	  }
  }   
  
  return ( object_id ) ;
}

/*
 * Create the object_id if of a new object.
 * Check that new name is correct and not used by an existing object.
 */
static int createObjectId ( String object_name, char object_type ) throws VTLError
{
	Check.checkNewObjectName (object_name) ;
	
	return ( new_object ( object_name, "(New)", object_type, "" ) );
}


/*
 * Set date of modification and change_id for given object_id.
 */
public static void setObjectModified ( int object_id ) throws VTLError
{
	Db .sqlExec ( "UPDATE " + Db . mdt_user_objects_ins +  " SET last_modified=SYSDATE,change_id=" 
				+ Db.mdt_change_id + ".nextval WHERE object_id=" + object_id ) ;	
}

/*
 * Get textual definition of object.
 */
String get_source ( ) throws VTLError
{
	String	mdt_sys, sql_query ;
	StringBuffer	object_def = new StringBuffer ( 1000 ) ;
	ListString	my_vector ;
	
	if ( this.dbLink != null )
	  mdt_sys = this.remoteOwner + ".mdt_sources" + this.dbLink ;
	else
	  mdt_sys = Db . mdt_sources ;
	
	sql_query = "SELECT buffer_text FROM " + mdt_sys + " WHERE object_id=" + this.objectId + " ORDER BY buffer_index" ;
	
	my_vector = Db.sqlFillArray ( sql_query ) ;
	
	if ( my_vector . size () == 0 )
	  VTLError . RunTimeError ( "Empty definition" ) ;
	
	for ( String s : my_vector )
	   object_def.append ( s ) ;
	
	// return ( object_def . toString () ) ;
	return ( object_def.toString ().replaceAll ( "\r", "" ) ) ;	
}

/*
 * Get textual definition of object.
 */
static String getSource ( int object_id ) throws VTLError
{
	String			sql_query ;
	StringBuffer	object_def = new StringBuffer ( 1000 ) ;
	ListString		ls ;
	
	sql_query = "SELECT buffer_text FROM " + Db.mdt_sources + " WHERE object_id=" + object_id + " ORDER BY buffer_index" ;
	
	ls = Db.sqlFillArray ( sql_query ) ;
	
	if ( ls.size () == 0 )
	  VTLError . RunTimeError ( "Empty definition" ) ;
	
	for ( String s : ls )
	   object_def.append ( s ) ;
	
	return ( object_def.toString ().replaceAll ( "\r", "" ) ) ;		
}

/*
 * Copy object.
 * if copy_data == false then the object is create but data are not copied (for data objects)
 * TBD: catch error and drop table
 * TBD: verify table ?
 * valuedomain subset
*/
public static void copyObject ( String object_from, String object_to, boolean copy_data, boolean compile_only ) throws VTLError
{
	String		mdt_sys, dblink ;
	VTLObject	obj, newobj ;
	int			new_object_id ;
	char		object_type ;
	Node		hd ;
	
	object_to = Check.checkObjectOwner ( object_to ) ;

	Check.checkNewObjectName ( object_to ) ;

	if ( ( obj =  getObjectDesc( object_from, false ) ) == null )
		VTLError.RunTimeError ( "Object " + object_from + " does not exist" ) ;
	
	if ( compile_only )
		return ;
	
	object_type = obj.objectType ;
	dblink = obj.dbLink ;
	
	if ( obj.synonymFor != null ) {
		new_object_id =  new_object ( object_to, "(New)", object_type, obj.synonymFor ) ;
		Db . sqlCommit () ;
		if ( obj.isDataObject ( ) )
			Db . sqlExec ( "CREATE SYNONYM " + object_to + " FOR " + obj.synonymFor ) ;
	}
	else {
		if ( object_type == VTLObject.O_VIEW ) {
			mdt_sys = ( dblink != null ? obj.remoteOwner + ".mdt_syntax_trees" + dblink : Db.mdt_syntax_trees ) ;
			hd = UserFunction.getSyntaxTree ( mdt_sys, obj.objectId ) ;
			if ( hd == null )
				VTLError.InternalError( "Copy view - bad object") ;
			hd.name = Command.Commands.N_CREATE_VIEW ;
			hd.val = object_to ;
			
		    if ( hd.child.next.next.next != null )
		    	hd.child.next.next.next.val = obj.get_source() + ";" ;
		    else
		    	hd.child.next.next.next = Parser.newnode ( Nodes.N_STRING, obj.get_source() + ";" ) ;
			Command.evalDefineView ( hd, false ) ;
			if ( ( newobj = getObjectDesc ( object_to ) ) == null )
				VTLError.InternalError( "Copy view - cannot get object desc") ;
			new_object_id = newobj.objectId ;
		}
		else {
			if ( copy_data == false && object_type == 'S' )
				object_type = VTLObject.O_DATASET ;

			new_object_id =  new_object ( object_to, "(New)", object_type, "" ) ;

			switch ( object_type ) {
				case VTLObject.O_VALUEDOMAIN :
				case 'S' : 
				case VTLObject.O_DATASET :
					Dataset.copyDataObject ( obj, object_from, object_to, copy_data, dblink, new_object_id ) ;
					break ;
				case VTLObject.O_OPERATOR :
					UserFunction.copyUserFunction ( obj, dblink, new_object_id ) ;
					break ;
				case VTLObject.O_HIERARCHICAL_RULESET :
					HierarchicalRuleset.copyRuleset( obj, dblink, new_object_id ) ;
					break ;
				case VTLObject.O_DATAPOINT_RULESET :
					DatapointRuleset.copyRuleset ( obj, dblink, new_object_id ) ;
					break ;
			}					
		}
	}

	VTLObject.copyObjectProperties ( obj, dblink, new_object_id ) ;
 
	Db.sqlCommit ( ) ;
	
}

/*
 * Copy object properties.
 */
public static void copyObjectProperties ( VTLObject obj, String dblink, int new_object_id ) throws VTLError
{
	Query		q ;
	String		prop_list, mdt_sys ;

	if ( dblink != null ) {
		prop_list = "object_comment" ;
	}
	else {
		q = Db.getDescObjectsComments ( null ) ;
		prop_list = q.stringMeasures ( ',' ) ;
	}
	
	mdt_sys = ( obj.remoteOwner == null ? Db.mdt_objects_comments : obj.remoteOwner + "." + "mdt_objects_comments" ) ;
	
	Db.sqlExec ( "UPDATE " + Db.mdt_user_objects_comments 
				+ " SET (" + prop_list + ")" + "=(SELECT " + prop_list + " FROM " + mdt_sys
													+ ( dblink == null ? "" : dblink )
													+ " WHERE object_id=" + obj.objectId + ")"
				+ " WHERE object_id=" + new_object_id ) ;
}

/*
 * Returns true if MDT object exists. If owner is not specified then object is searched in current schema.
 */
public static boolean object_exists ( String object_name ) throws VTLError
{
  return (  getObjectDesc( object_name, false ) != null ) ;	
}

/*
 * Returns object_type as the string specified in the syntax.
 */
static String stringObjectType ( char object_type ) throws VTLError
{
	String	typ ;
	
	switch ( object_type ) {
		case VTLObject.O_VALUEDOMAIN : typ = "valuedomain" ; break ;
		case VTLObject.O_OPERATOR : typ = "operator" ; break ;
		case VTLObject.O_DATAPOINT_RULESET : typ = "datapoint ruleset" ; break ;
		case VTLObject.O_HIERARCHICAL_RULESET : typ = "hierarchical ruleset" ; break ;
		case VTLObject.O_DATASET : typ = "dataset" ; break ;
		case VTLObject.O_VIEW : typ = "view" ; break ;
		default: 
			VTLError.InternalError( "stringObjectType: bad object type") ;
			typ = null ;
	}
	return ( typ ) ;
}

/*
 * Returns full object name (owner.object) if object exists and is of expected object type. 
 * Otherwise throw exception.
 */
static String fullObjectName ( String object_name, char object_type ) throws VTLError
{
	VTLObject	obj ;
	String		tmp ;

	obj = getObjectDesc( object_name ) ;
	
	if ( obj == null )
		VTLError.TypeError ( "Cannot find " + stringObjectType (object_type) + " named " + object_name ) ;
	
	if ( obj.objectType != object_type ) {
		VTLError.TypeError ( "Object: " + object_name + " is not a " + stringObjectType (object_type) ) ;
	}
	if ( ( tmp = obj.realObjectName).indexOf( Parser.ownershipSymbol ) < 0 ) {
		tmp = Db.db_username.toLowerCase () + Parser.ownershipSymbol + tmp ;
	}
	return ( tmp ) ;
}

/*
 * get data type
 * Look for valuedomain of dimension.
 */
static String getFullDataType ( String dim_type ) throws VTLError
{
	if ( ! Check.isPredefinedType(dim_type) ) {
		if ( dim_type.indexOf( Parser.ownershipSymbol ) < 0 )
			dim_type = VTLObject.fullObjectName(dim_type, VTLObject.O_VALUEDOMAIN) ;
	}
	return ( dim_type ) ;
}

/*
 * Is this a data object?
*/
boolean isDataObject () {
	// improvement: return ( "DSTV".indexOf ( obj.object_type ) >= 0 ) ;
	switch ( this.objectType ) {
		case VTLObject.O_VALUEDOMAIN :
		case VTLObject.O_DATASET :
		case VTLObject.O_VIEW :	
			return ( true ) ;
	}
	return ( false ) ;
}

/*
 * Drop object.
 * REFERENCES table(column) ON DELETE CASCADE is defined for:
 * mdt_dependecies
 * mdt_user_renamed_objects
 * mdt_user_modifications
 * mdt_syntax_trees
*/
public static void objectTrueDrop ( String object_name ) throws VTLError
{
	int			object_id ;
	boolean		is_synonym, isValuedomainSubset = false ;
	char			object_type ;
	VTLObject 	obj ;

	if ( ( obj =  getObjectDesc ( object_name, false ) ) == null )
		VTLError . TypeError (" Drop: " + object_name + " does not exist" ) ; 

	object_id = obj.realObjectId ;
	object_type = obj.objectType ;
	is_synonym = ( obj.synonymFor != null ) ;
  
	if ( object_type == VTLObject.O_VALUEDOMAIN ) {
		try {
			isValuedomainSubset = Dataset.getValueDomain( object_name  ).isValuedomainSubset() ;		  
		}
		catch ( Exception e ) {
			// valuedomain with 0 dimensions
			isValuedomainSubset = false ;
		}
	}
	// execute DB operations
	try {

	if ( is_synonym == false ) {
		switch ( object_type ) {
	      	case VTLObject.O_VALUEDOMAIN :
			case VTLObject.O_DATASET :
			case VTLObject.O_VIEW :
			     Db . dbDelete ( Db . mdt_user_positions, object_id ) ;
			     Db . dbDelete ( Db . mdt_user_dimensions, object_id ) ;
			     Db . dbDelete ( Db . mdt_user_audits, object_id ) ;	
			     if ( object_type == VTLObject.O_VIEW ) {
			          Db . dbDelete ( Db . mdt_user_dependencies, object_id ) ;
			          Db . dbDelete ( Db . mdt_user_sources, object_id ) ;
			     }
			     break ;
	
			case VTLObject.O_OPERATOR :
				 Db . dbDelete ( Db . mdt_user_syntax_trees, object_id ) ;
			     Db . dbDelete ( Db . mdt_user_dependencies, object_id ) ;
			     Db . dbDelete ( Db . mdt_user_sources, object_id ) ;
			     break ;
			case VTLObject.O_HIERARCHICAL_RULESET :
				 Db . dbDelete ( Db . mdt_user_syntax_trees, object_id ) ;
				 break ;
			case VTLObject.O_DATAPOINT_RULESET :
				 Db . dbDelete ( Db . mdt_user_syntax_trees, object_id ) ;
			     break ;
	  	}
	}
	Db.dbDelete ( Db . mdt_user_objects_comments, object_id ) ;
	Db.dbDelete ( Db . mdt_user_objects, object_id ) ;

	Db.sqlExec( "INSERT INTO " + Db.mdt_user_dropped_objects + "(user_name,object_name,object_type,timestamp)"
		  		+ "VALUES(USER,'" + object_name + "','" + object_type + "',SYSDATE)" ) ;
	Db.sqlCommit () ;
	}
	catch ( Exception e ) {
		Db.sqlRollback() ;
		VTLError.RunTimeError( e.toString() ) ;
	}

	if ( is_synonym ) {
		if ( obj.isDataObject() )
			Db.sqlExec("DROP SYNONYM " + object_name) ;
	}
	else {
		switch ( object_type ) {
		   	case VTLObject.O_VALUEDOMAIN : 
		   	case VTLObject.O_DATASET :
			   if ( object_type == VTLObject.O_VALUEDOMAIN ) {
				   if ( isValuedomainSubset )
					   Db.sqlExec ("DROP VIEW " + object_name ) ;
				   else
					   Db.sqlExec ("DROP TABLE " + object_name + " PURGE" ) ;
			   }
			   else
				   Db.sqlExec ("DROP TABLE " + object_name + " PURGE" ) ;
			   break;
		   	case VTLObject.O_VIEW :
		   		Db.sqlExec ("DROP VIEW " + object_name) ;
		   		break;
      	}
	}

	if ( obj.isDataObject () ) {
		Audit.deleteTable ( object_id ) ;
		Db.sqlCommit () ;	  
  	}
}

/*
 * Drop object - purge or move the object to the recyclebin.
 * TBD: drop original_name
 */
public static void objectDrop ( char object_type, String object_name, boolean purge, boolean compile_only ) throws VTLError
{
	VTLObject 	obj ;
	
	object_name = Check . checkObjectOwner ( object_name ) ;

	if ( compile_only )
		return ;

	if ( ( obj =  getObjectDesc ( object_name, false ) ) == null )
		VTLError . TypeError (" Drop: " + object_name + " does not exist" ) ; 
	
	if ( object_type == VTLObject.O_SYNONYM ) {
		if ( obj.synonymFor == null )
			VTLError . TypeError ( "Drop: " + object_name + " is not a synonym") ;
		object_type = obj.objectType ;
	}
	else {
		if ( obj.synonymFor != null )
			VTLError . TypeError ( "Drop: " + object_name + " is a synonym, please use: drop synonym") ;
	      
	    if ( obj.objectType != object_type )
	    	VTLError . TypeError ("Drop: " + object_name + " is not a " + object_type );
	}

	if ( object_type == VTLObject.O_VALUEDOMAIN ) 
		Dataset.checkDropValuedomain ( object_name ) ;		// dim_name = object_name
	
	if ( obj.isDataObject () ) 
		Dataset.removeTableDesc ( obj.objectId ) ;
	
	if ( obj.dropOriginalName == null && purge == false ) {
		  String recycled_name ;
		  recycled_name = "recycled_object_" + Db.sqlGetValue ("SELECT " + Db.mdt_recyclebin_id + ".NEXTVAL FROM DUAL") ;
		  Db.sqlExec ( "UPDATE " + Db.mdt_user_objects + " SET drop_original_name=object_name,object_name='" 
				  				+ recycled_name + "',drop_time=SYSDATE"
				  				+ " WHERE object_id=" + obj.realObjectId )  ;
		  Db.sqlCommit () ;
		  if ( obj.isDataObject () ) 
			  Db.sqlExec( "RENAME " + obj.realObjectName + " TO " + recycled_name ) ;
	}
	else {
		objectTrueDrop ( object_name ) ;
	}
}

/*
 * Purge recyclebin.
 * TBD: drop original_name
 */
public static void purgeRecycleBin ( ) throws VTLError
{
	ListString	dropped_objects ;

	dropped_objects = Db.sqlFillArray( "SELECT object_name FROM " + Db.mdt_user_objects 
												+ " WHERE drop_original_name IS NOT NULL AND drop_time IS NOT NULL" ) ; 
	
	for ( String object_name : dropped_objects ) {
		objectTrueDrop ( object_name ) ;
	}
}

/*
 * Restore object dropped with a drop statement.
 * TBD: restore original_name
 */
public static void objectRestore ( String object_name, String rename_to ) throws VTLError
{
	VTLObject 	obj ;
	String 		new_name  ;
 	
	if ( ( obj = getObjectDesc ( object_name, false ) ) == null )
		VTLError . TypeError (" Drop: " + object_name + " does not exist" ) ; 

	if ( obj.dropOriginalName == null ) 										// removed since v. 96
		VTLError.RunTimeError( "Cannot restore " + object_name ) ;

	new_name = ( rename_to == null ? obj.dropOriginalName : rename_to ) ;
	
	if ( VTLObject.getObjectDesc( new_name, false ) != null )
		VTLError . RunTimeError ( "Flashback: name " + new_name + " is already used by an existing object" ) ;

	Db.sqlExec ( "UPDATE " + Db.mdt_user_objects + " SET drop_original_name=null,object_name='" 
		  				+ new_name + "',drop_time=null"
		  				+ " WHERE object_id=" + obj.realObjectId )  ;
	Db.sqlCommit() ;
	
	if ( obj.isDataObject () )
		Db.sqlExec( "RENAME " + object_name + " TO " + new_name ) ;
}

/*
 * Rename object.
 */
public static void objectRename ( String object_from, String object_to, boolean compile_only ) throws VTLError
{
  VTLObject	obj ;
  String	dim_from, dim_to ;

  if ( compile_only )
	  return ;

  object_from = Check . checkObjectOwner ( object_from ) ;
  object_to = Check . checkObjectOwner ( object_to ) ;
  Check . checkNewObjectName ( object_to ) ;
  
  if ( ( obj =  getObjectDesc( object_from, false ) ) == null )
     VTLError . RunTimeError ( "Object " + object_from + " does not exist" ) ;

  if ( obj.objectType == VTLObject.O_VALUEDOMAIN ) 
	  Dataset.checkDropValuedomain ( object_from ) ;		// dim_name = object_name

  try {
	  if ( obj.isDataObject () ) {
		  Dataset.removeTableDesc ( obj.objectId ) ;	  
		  Db.sqlExec ( "RENAME " + object_from + " TO " + object_to ) ;

		  if ( obj.synonymFor == null && obj .objectType == VTLObject.O_VALUEDOMAIN ) {
			  dim_from = object_from ;
			  dim_to = object_to ;
			  Dataset 	tab = Dataset.getDatasetDesc( object_from );
			  if ( tab.isValuedomainSubset() )
				  VTLError.InternalError( "rename valuedomain subset: not yet implemented" );
			  else
				  Db.sqlExec ( "ALTER TABLE " + object_to + " RENAME COLUMN " + dim_from + " TO " + dim_to ) ;
			  Db.sqlExec ( "UPDATE " + Db.mdt_user_dimensions + " SET dim_name='" + dim_to  
		  			+ "' WHERE object_id=" + obj .objectId + " AND dim_index=1" ) ;        	
		  }
	  }


	  Db.sqlExec ( "UPDATE " + Db.mdt_user_objects + " SET object_name='" + object_to + "'"
	  			+ " WHERE object_id=" + obj.realObjectId )  ;

	  Db.sqlExec ( "INSERT INTO " + Db.mdt_user_renamed_objects + "(object_id,timestamp,ses_id,previous_object_name)"
			  + " VALUES(" + obj.realObjectId + ",SYSDATE," + Db.db_session_id + ",'" + object_from + "')" ) ;

	  VTLObject . setObjectModified ( obj.realObjectId ) ;

	  Db . sqlCommit ( ) ;	  
  }
  catch ( Exception e ) {
		Db.sqlRollback() ;
		VTLError.RunTimeError( e.toString() ) ;
  }
}

/* 
 * Build syntax for command: description of object.
 */
static String getObjectComment ( String objectName, int objectId ) throws VTLError
{
	String			colname, col_type ;
	Query			q ;
	StringBuffer	sql_query = new StringBuffer () ;
	
	q = Db.getDescObjectsComments ( null ) ;
	
	sql_query.append( "SELECT " ) ;
	
	int idx = 0 ;
	for ( DatasetComponent prop : q.measures ) {
		colname = prop.compName ;
		col_type = prop.compType ;
		
		if ( idx > 0 )
			sql_query.append ( "|| CASE WHEN " + colname + " IS NULL THEN '' ELSE " + "', " 
													+ colname + ":='||" ) ;
		else
			sql_query.append ( "'" + colname + ":='||" ) ;
		
		if ( col_type.equals("string") )
			sql_query.append( "'\"'||" + colname + "||'\"'" );
		else if ( col_type.equals("number") )
			sql_query.append(colname) ;
		else if ( col_type.equals("date") ) 
			sql_query.append( "'\"'||" + "TO_CHAR(" + colname + ",'DD.MM.YYYY')" + "||'\"'" ) ;
		else
			VTLError .RunTimeError("getObjectComment: case not found");
		if ( idx > 0 )
			sql_query.append( " END" ) ;		
		idx ++ ;
	}
	
	sql_query.append ( " FROM " + Db.mdt_objects_comments + " WHERE object_id=" + objectId ) ;
	
	return ( "description of " + objectName + " is [ " + Db.sqlGetValue ( sql_query.toString() ) + " ] ;\n\n" ) ;
}

/* 
 * Build VTL syntax corresponding to object definition.
 */
public static String objectDefinition ( String object_name, char object_type, boolean include_comment ) throws VTLError
{
	StringBuffer		syntax ;
	Dataset				ds ;
	int					object_id ;
	VTLObject			obj ;
	
	if ( ( obj =  getObjectDesc ( object_name, false ) ) == null )
	    VTLError.TypeError ("Object definition: " + object_name + " does not exist" ) ; 

	if ( object_type != obj.objectType )
		VTLError.InternalError( "Object defintion - type is wrong" ) ;
	 
	if ( obj.synonymFor != null )
		return ( "create synonym " + object_name + " for " + obj.synonymFor + " ;\n" ) ;

	syntax = new StringBuffer ( 1000 ) ;

	switch ( object_type ) {
		case VTLObject.O_VALUEDOMAIN:
		case VTLObject.O_DATASET :
		case VTLObject.O_VIEW :
			// always use define as default
			syntax.append( object_type == VTLObject.O_VIEW ? "define " : "define " ).append( stringObjectType ( object_type ) )
								.append( " " ).append( object_name ) ;
			ds = Dataset .getDatasetDesc(object_name) ;
			syntax.append( ds.getDefinitionDataObject ( object_type ) ) ;
			object_id = ds.objectId ;
			break ;
		case VTLObject.O_OPERATOR :
			UserFunction fun = UserFunction.getUserDefinedOperator (object_name);
			syntax.append(fun.getDefinition() ) ;
			object_id = fun.object_id ;
			break ;
		case VTLObject.O_HIERARCHICAL_RULESET :
			HierarchicalRuleset	ir = HierarchicalRuleset.getRuleset ( object_name ) ;
			syntax.append( ir.getDefinition () ) ;
			object_id = ir.objectId ;
			break ;
		case VTLObject.O_DATAPOINT_RULESET :
			DatapointRuleset	vr ;
			vr = DatapointRuleset.getRuleset( object_name ) ;
			syntax.append( vr.getDefinition () ) ;
			object_id = vr.objectId ;
			break ;
		default:
			object_id = -1 ;
	}
	
	if ( include_comment )
		syntax.append( " ; \n" ).append( getObjectComment (object_name, object_id) ) ;
	
	return ( syntax . toString () ) ;	
}

/*
 * Called from the GUI.
 */
public static void exportAll ( String fileName, boolean exportData ) throws VTLError
{
	ListString ls = Db.sqlFillArray( "SELECT object_name FROM " + Db.mdt_objects + 
			" WHERE drop_original_name IS NULL ORDER BY object_type, object_name" ) ;
	StringBuffer syntax = new StringBuffer ( 1000 ) ;
	
	String fPath = Sys.findFilePath ( fileName ) ;

	for ( String oName : ls ) {
		char oType = getObjectDesc ( oName ).objectType ;
		syntax.append( objectDefinition ( oName, oType, true ) ) ;	
		if ( exportData && oType == VTLObject.O_DATASET || oType == VTLObject.O_VALUEDOMAIN ) {
			Dataset ds = Dataset.getDatasetDesc(oName) ;
			if ( ! ds.isValuedomainSubset() )
				Db.sqlUnload( "SELECT " + ds.stringAllComponents() + " FROM " + oName + 
					( ds.dims.size() == 0 ? "" : " ORDER BY " + ds.stringDimensions() ) , fPath + oName + ".dat" );			
		}
	}
	Sys.setFileName ( fileName + "vtl" ) ;
	Sys.output_open () ;
	Sys.println( syntax.toString() ) ;
	Sys.output_close () ;	
}

/*
 * Save referenced objects.
 */
static void saveReferencedObjects ( int objectId, Vector <String> referencedObjects ) throws VTLError
{
	for ( String referencedObject : referencedObjects  )
		VTLObject.saveReferencedObject ( objectId, referencedObject, VTLObject.O_DATASET ) ;
}

/*
 * Save referenced object.
 */
static void saveReferencedObject ( int object_id, String referenced_object, char object_type ) throws VTLError
{
	String	insert_dim, ref_object_owner, ref_object_name ;
	int		idx_point ;

	insert_dim = "INSERT INTO " + Db . mdt_user_dependencies 
  		+ "(object_id,ref_object_owner,ref_object_name,ref_type)VALUES(" + object_id + ",'" ;

	if ( ( idx_point = referenced_object . indexOf ( '.' ) ) >= 0 ) {
		ref_object_owner = referenced_object . substring ( 0, idx_point ) ;
		ref_object_name = referenced_object . substring ( idx_point + 1 ) ;
	}
	else {
		ref_object_owner = Db . db_username . toLowerCase () ;
		ref_object_name = referenced_object ;
	}
	Db . sqlExec ( insert_dim + ref_object_owner + "','" + ref_object_name + "','" + object_type + "')" ) ;
}

/*
 * Get views defined on a data object (a table or a view).
 * extended to function object.
 */
static Vector <VTLObject> getDependentObjects ( String object_name ) throws VTLError
{
	String				sql_query, ref_object_owner, ref_object_name ;
	int					idx_point ;
	VTLObject			obj ;
	ListString			ls ;
	Vector <VTLObject> 	objs ;
	
	if ( (obj = getObjectDesc ( object_name ) ) == null )
		VTLError.RunTimeError( "object not found: " + object_name ) ;

	if ( ( idx_point = object_name . indexOf ( '.' ) ) >= 0 ) {
		ref_object_owner = object_name . substring ( 0, idx_point ) ;
		ref_object_name = object_name . substring ( idx_point + 1 ) ;
	}
	else {
		ref_object_owner = Db . db_username . toLowerCase () ;
		ref_object_name = object_name ;
	}
	
	switch ( obj.objectType ) {
		case VTLObject.O_DATASET : case VTLObject.O_VIEW :																
			sql_query = "SELECT LOWER(user_name) || '.' || object_name FROM " + Db.mdt_objects 										
				+ " WHERE object_id IN (SELECT object_id FROM "+ Db.mdt_dependencies 
		  			+ " WHERE ref_object_owner='" + ref_object_owner +"' AND ref_object_name='" + ref_object_name + "')" ;
		
			ls = Db.sqlFillArray(sql_query) ;
			break ;
		case VTLObject.O_VALUEDOMAIN :
			// ls = getDerivedDictionaries ( object_name ) ;												
			sql_query = "SELECT LOWER(user_name) || '.' || object_name FROM " + Db.mdt_objects 	
				+ " WHERE object_id IN (SELECT d.object_id FROM " + Db.mdt_dimensions 
					+ " d WHERE d.dim_type='D' AND d.column_type='" + object_name + "')" ;
			ls = Db.sqlFillArray(sql_query) ;
			break ;
		case VTLObject.O_OPERATOR :
			sql_query = "SELECT user_name || '.' || object_name FROM " + Db.mdt_objects + " WHERE object_id IN ( SELECT object_id FROM "+ Db.mdt_syntax_trees 
				+ " WHERE node_name =" + Nodes.N_USER_OPERATOR_CALL + " AND (node_value_1='" + ref_object_owner + '.' + ref_object_name 
						+ "' OR (node_value_1='" + ref_object_name + "'AND user_name='" + ref_object_owner.toUpperCase () + "')))"	;
			// Sys.println( "***** " + sql_query + " *****");		
			ls = Db.sqlFillArray(sql_query) ;
			break ;
		default :
			ls = null ;
	}
	
	objs = new Vector <VTLObject> () ;
	for ( String str : ls ) {
		objs.add( VTLObject.getObjectDesc( str )) ;
	}
	return ( objs ) ;
}

/*
 * Get data objects used in a view.
 * Changed in v.79: return Vector MdtObjects.
 * extended to function object. 
 * For function:
 * 	sql " select * from refsys.mdt_user_syntax_trees where node_name = 36 order by object_id, node_index " ;
 * N_IDENTIFIER should not be used for a position in a subscript

 */
static Vector <VTLObject> getUsedObjects ( String object_name ) throws VTLError
{
	String		sql_query ;
	VTLObject	obj ;
	Dataset		tab ;
	ListString	ls ;
	Vector <VTLObject> 	objs ;
	
	obj = getObjectDesc ( object_name ) ;
	
	switch ( obj.objectType ) {
		case VTLObject.O_DATASET : case VTLObject.O_VIEW :	
			sql_query = "SELECT ref_object_owner || '.' || ref_object_name FROM " + Db.mdt_dependencies + " WHERE object_id=" + obj.objectId  ;		
			ls = Db.sqlFillArray(sql_query) ;
			tab = Dataset.getDatasetDesc( object_name ) ;
			for ( DatasetComponent tabDim: tab.getDims() )
				if ( Check.isValueDomainType( tabDim.compType ) )
					ls.add ( tabDim.compType ) ;
			break ;
			
		case VTLObject.O_VALUEDOMAIN :
			ls = new ListString() ;
			if ( (tab = Dataset.getValueDomain( object_name  ) ).isValuedomainSubset() ) {
				ls.add( tab.getDims().get(0).compType ) ;
			}
			break ;
		case VTLObject.O_OPERATOR :
			sql_query = "SELECT DISTINCT node_value_1 FROM " + Db.mdt_syntax_trees 
			+ " WHERE node_name IN(" + Nodes.N_USER_OPERATOR_CALL + "," + Nodes.N_IDENTIFIER + ") AND node_value_1 IS NOT NULL AND object_id=" + obj.objectId  ;		
			ls = Db.sqlFillArray(sql_query) ;
			break ;
		default :
			ls = null ;
	}

	objs = new Vector <VTLObject> () ;
	for ( String str : ls ) {
		if ( ( obj = VTLObject.getObjectDesc( str ) ) != null )
			objs.add( obj ) ;
	}
	return ( objs ) ;
}

// Version of MDT metabase (system tables)

static float	version_metabase = -1 ;

/*
 * NB: version number should be reset when connecting to another database
 * The version has been removed from meta base
 */
public static float versionMetabase () throws VTLError 
{
	if ( version_metabase < 0 ) {
		version_metabase = 19 ;
		/* if ( ( tmp = Db.sql_get_value( "SELECT metabase_version FROM " + Db.mdt_metabase_version ) ) == null )
			AppError.RunTimeError( "Metabase version is null") ;
		version_metabase = Float.parseFloat( tmp ) ; */
	}
	
	return ( version_metabase ) ;
}

}
