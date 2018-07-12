

/*
   Multidimensional dataset.
*/


/*
 * changes:
sql "ALTER TABLE MDT_VALIDATION_RULES MODIFY DATA_OBJECT_NAME VARCHAR2(1000 BYTE)" 
 */
import java.sql.* ;
import java.util.* ;

class Dataset extends Query {
	int   			objectId ;
	char  			objectType ;			// dataset, valuedomain, view
	String			dsName ;
	String			sqlTableName ;
	int				changeId ;
 
final Vector <DatasetComponent> getDims() {
	return dims;
}

final void setDimensions(Vector <DatasetComponent> myDims) {
	dims = myDims;
}

final Vector <DatasetComponent> getMeasures() {
	return measures;
}

public final void setMeasures(Vector <DatasetComponent> myMeasures) {
	measures = myMeasures;
}

final Vector <DatasetComponent> getAttributes() {
	return attributes;
}

final void setAttributes(Vector <DatasetComponent> myAttributes) {
	attributes = myAttributes;
}

Dataset ( String myDataset, char myObjectType )
{
	super () ;
	dsName = myDataset ;
	sqlTableName = myDataset ;
	objectType = myObjectType ;
}

Dataset ( Dataset ds )
{
	super () ;
	for ( DatasetComponent dim : ds.getDims() )
		dims.add( new  DatasetComponent ( dim ) ) ;
	for ( DatasetComponent c : ds.getMeasures() )
		measures.add( new  DatasetComponent ( c ) ) ;
	for ( DatasetComponent c : ds.getAttributes() )
		measures.add( new  DatasetComponent ( c ) ) ;

	dsName = ds.dsName ;
	sqlTableName = ds.dsName ;
	objectType = ds.objectType ;	
}

static final int max_descriptors = 20 ;
static Vector  <Dataset> table_descriptors = new Vector <Dataset> () ;

/*
 * Create a virtual table mdt_user_objects that contains all objects owned by the current user
 * exclude objects that are in the recycle bin
 * table_name can be "vtl_all_objects" or "vtl_user_objects"
 */
static Query f_db_vtl_objects ( String table_name ) throws VTLError
{
	Query 			q ;
	DatasetComponent  pro ;
	String			sql_query, sql_type, sql_select, sys_owner, sys_mdt_objects, sys_mdt_comments, sys_mdt_audits, sys_db_link ;
	String 			property_name[]  = { "owner", "object_type", "created" , "last_modified", "last_updated", "synonym_for" } ;
	String			property_type[] = { "string", "string", "date", "date", "date", "string" } ;

	if ( table_name.startsWith( "vtl_all_objects@") ) {								// v. 147: query mdt_objects of remote database
		sys_db_link = table_name.substring( table_name.indexOf( '@' ) ) ;
		sys_owner = Db.findOwnerOfSystemTables( sys_db_link ) ;
		sys_mdt_objects = sys_owner + '.' + "mdt_objects" ;
		sys_mdt_comments = sys_owner + '.' + "mdt_user_objects_comments" + sys_db_link ;
		sys_mdt_audits = sys_owner + '.' + "mdt_audits" + sys_db_link ;
	}
	else {
		sys_db_link = "" ;
		sys_owner = Db.owner_of_system_tables.toUpperCase( ) ;
		if ( table_name.compareTo( "vtl_all_objects") == 0 ) {
			sys_mdt_objects = Db.mdt_objects ;					// sys_owner + '.' + table_name ;
			sys_mdt_comments = Db.mdt_objects_comments ;			// sys_owner + '.' + "mdt_user_objects_comments" ;
			sys_mdt_audits = Db.mdt_audits ;						// sys_owner + '.' + "mdt_audits" ;		  
		}
		else {
		  sys_mdt_objects = Db.mdt_user_objects ;					// sys_owner + '.' + table_name ;
		  sys_mdt_comments = Db.mdt_user_objects_comments ;			// sys_owner + '.' + "mdt_user_objects_comments" ;
		  sys_mdt_audits = Db.mdt_user_audits ;						// sys_owner + '.' + "mdt_audits" ;		 		  
		}
	}
    
	q = new Query ( ) ;	
	q.addDimension("object_name", "string", new ListString ( 0 ) );

	for ( int idx = 0; idx < property_name . length ; idx ++ )
	  	q.measures.add ( new DatasetComponent (property_name [ idx ],property_type [ idx ],property_name [ idx ]) ) ;
  
	sql_query = "SELECT column_name, data_type, data_length FROM all_tab_columns" + sys_db_link
			+ " WHERE owner='" + sys_owner + "' AND table_name='MDT_OBJECTS_COMMENTS'"
			+ " AND column_name NOT IN ('OWNER','OBJECT_ID')"
			+ " ORDER BY column_id" ; 

	try {
		sql_select = "" ;
		Statement st = Db . DbConnection . createStatement () ;
		ResultSet rs = st . executeQuery ( sql_query ) ;
		while ( rs.next () ) {  
			pro = new DatasetComponent () ;
			pro .compName = rs . getString ( 1 ) . toLowerCase () ;
			sql_type = rs . getString ( 2 ) . toLowerCase () ;
			if ( sql_type.equals ( "number" ) || sql_type.equals ( "integer" ) || sql_type.equals ( "date" ) )
				pro .compType = sql_type ;
			else
				pro .compType = "string" ;
			pro .compWidth = rs.getInt ( 3 ) ;
			q.measures.add ( pro ) ;
			sql_select = sql_select + "," + pro .compName ;
		}
		rs . close () ;
		st . close () ;
		q.sqlFrom = "(SELECT o.user_name as owner,o.object_name,o.object_type,o.created,o.last_modified,max(a.timestamp) as last_updated,o.synonym_for"
						+ sql_select
						+ " FROM " + sys_mdt_objects + " o LEFT JOIN " + sys_mdt_comments + " c USING (object_id) " 			// corrected v. 111
						+ " LEFT JOIN " + sys_mdt_audits + " a USING (object_id)"
						+ " WHERE o.drop_time IS NULL"
						+ " GROUP BY o.user_name,o.object_name,o.object_type,o.created,o.last_modified,o.synonym_for" + sql_select + ")" ;
	} 
	catch ( SQLException e ) {
		VTLError . SqlError  ( e . toString () ) ;
	}	

	return ( q ) ;
}

/*
 * Create a virtual table mdt_user_objects that contains all objects owned by the current user
 */
static Query predefinedVTLdatasets ( String table_name ) throws VTLError
{
	Query q ;
	String	sys_owner, sys_mdt_objects, sys_mdt_dimensions, sys_mdt_positions, sys_db_link ;

	if ( table_name.startsWith( "vtl_all_datasets@") ) {								// v. 147: query mdt_objects of remote database
		  sys_db_link = table_name.substring( table_name.indexOf( '@' ) ) ;
		  sys_owner = Db.findOwnerOfSystemTables( sys_db_link ) ;
		  sys_mdt_objects = sys_owner + '.' + "mdt_objects" + sys_db_link;
		  sys_mdt_dimensions = sys_owner + '.' + "mdt_dimensions" + sys_db_link ;
		  sys_mdt_positions = sys_owner + '.' + "mdt_positions" + sys_db_link ;
	}
	else {
		sys_db_link = "" ;
		sys_owner = Db.owner_of_system_tables.toUpperCase( ) ;
		if ( table_name.compareTo( "vtl_all_datasets") == 0 ) {
		  sys_mdt_objects = Db.mdt_objects ;					
		  sys_mdt_dimensions = Db.mdt_dimensions ;		
		  sys_mdt_positions = Db.mdt_positions ;						  
		}
		else {
		  sys_mdt_objects = Db.mdt_user_objects ;				
		  sys_mdt_dimensions = Db.mdt_user_dimensions ;			
		  sys_mdt_positions = Db.mdt_user_positions ;		 		  
		}
	}
    
	q = new Query ( ) ;							// "mdt_objects" or "mdt_user_objects"

	q.addDimension("owner", "string", new ListString ( 0 ) );
	q.addDimension("object_name", "string", new ListString ( 0 ) );
	q.addDimension("dimension", "string", new ListString ( 0 ) );
	q.addMeasure( "position", "string", "position");

	q.sqlFrom =
		"(SELECT lower(o.user_name) AS owner,o.object_name AS object_name, d.dim_name AS dimension,p.pos_code AS position, null AS dummy"
			+ " FROM " + sys_mdt_objects + " o LEFT JOIN " + sys_mdt_dimensions + " d USING (object_id) " 
			+ " LEFT JOIN " + sys_mdt_positions + " p USING (object_id,dim_index)"
			+ "WHERE o.object_type='T' AND d.dim_type='D')" ;

  return ( q ) ;
}

static Dataset find_table_desc ( String table_name ) throws VTLError
{
	for ( Dataset tab : table_descriptors ) {
		  if ( table_name . compareTo ( tab.dsName ) == 0 ) {
			  if ( VTLObject.was_modified_after (tab.objectId, tab.changeId ) ) // last_modification_time ) )
				  table_descriptors . remove ( tab ) ; // { table_descriptors . remove ( idx ) ; Sys.println( "remove: " + table_name ) ; } //
			  else
				  return( tab ) ;
			  break ;
		  }
	}
	return ( null ) ;
}

static Query vtl_meta_dataset ( String dsName ) throws VTLError
{
  	if ( dsName.equals ( "vtl_all_datasets" ) || dsName.equals ( "vtl_user_datasets" )  )
  		return ( predefinedVTLdatasets ( dsName ) ) ;	
  	
  	else
  		return ( f_db_vtl_objects ( dsName ) ) ;
/*  		if ( dsName.compareTo ( "vtl_all_objects" ) == 0 || dsName.compareTo ( "vtl_user_objects" ) == 0
  			|| dsName.startsWith( "vtl_all_objects@") )				// v. 147
*/  
}

/*
 * VTL: change 'R' for attribute component (was type of dynamic view).
 * 
   Get table descriptor of valuedomain, dataset or view. 
   dsName can be prefixed by owner and "\"
*/
static Dataset getDatasetDesc ( String dsName ) throws VTLError
{
	int					dim_index, my_width ;
	String				dim_name, my_null, my_type, mdt_sys, tabname, sql_query ;
	char 				dim_type ;
	VTLObject			obj ;
	Dataset				ds ;

	// mdt_objects
	if ( dsName == null )
		VTLError.InternalError ( "getDatasetDesc: dataset name is null" ) ; 
  	
  	// look in current descriptors
  	if ( ( ds = find_table_desc ( dsName ) ) != null )
  		return( ds ) ;

  	if ( dsName.indexOf ( Parser.ownershipSymbol ) < 0 && UserProfile.searchUser != null )		
  		if ( ( ds = find_table_desc ( UserProfile.searchUser + Parser.ownershipSymbol + dsName ) ) != null )
  			return( ds ) ;	  

  	// get object_id
  	if ( ( obj = VTLObject.getObjectDesc ( dsName ) ) == null ) 
  		VTLError.RunTimeError ( "Object " + dsName + " does not exist" ) ;			// since v. 97, previously: TypeError
  
  	// look for a synonym
  	if ( obj.synonymFor == null )
  		tabname = obj.realObjectName ;	// table_name ;
  	else {
	     if ( obj.dbLink == null )
	    	 tabname = obj.synonymFor ;
	     else
	    	 tabname = obj.synonymFor + obj.dbLink ;
	     // look in current descriptors
	     if ( ( ds = find_table_desc ( tabname ) ) != null )
	    	 return( ds ) ;
  	}
  	
	if ( tabname.indexOf ( Parser.ownershipSymbol ) < 0 && tabname.indexOf ( '@' ) < 0) {	
  		tabname = Db.db_username.toLowerCase () + Parser.ownershipSymbol + dsName ;
  	    if ( ( ds = find_table_desc ( tabname ) ) != null ) {
  	    	ds.sqlTableName = tabname.replace ( Parser.ownershipSymbol, ".") ;
  	    	return ( ds ) ;
  	    }
	}
	
	// initialize components

	ds = new Dataset ( tabname, obj.objectType ) ;
 	ds.sqlTableName = tabname.replace ( Parser.ownershipSymbol, ".") ;
	ds.objectId = obj.objectId ;
	ds.changeId = obj.changeId ;
   
	//  check object type
	switch ( obj.objectType ) {
		case VTLObject.O_VALUEDOMAIN :
		case 'T' :
		case VTLObject.O_VIEW :
		  break ;
		default :
		  VTLError.TypeError ( "Object " + dsName + " is not a valuedomain, dataset or view" ) ;
	}

	// read dimensions
	if ( obj.dbLink != null ) {
		mdt_sys = obj.remoteOwner + ".mdt_dimensions" + obj.dbLink ;
		ds.dsName = dsName ;
	}
	else
		mdt_sys = Db . mdt_dimensions ;

	sql_query = "SELECT dim_name,dim_type,dim_null,dim_width,column_type"
		  		+ " FROM " + mdt_sys + " WHERE object_id=" + obj.objectId + " ORDER BY dim_index" ;

	try {
     Statement st = Db . DbConnection . createStatement () ;
     ResultSet rs = st . executeQuery ( sql_query ) ;
     while ( rs . next () ) {
         dim_name = rs . getString ( 1 ) ;      
         dim_type = rs . getString ( 2 ) . charAt ( 0 ) ;
         my_null = rs . getString ( 3 ) ;      
         my_width = rs . getInt ( 4 ) ;
         my_type = rs . getString ( 5 ) ;      
         if ( my_type == null )
        	 my_type = dim_name ;

         switch ( dim_type ) {
		     case 'D' : // dimension
				ds.addDimension(dim_name, my_type, my_width, null, null);
				break ;
	
		     case 'X' : // measure
				ds.addMeasure(dim_name, my_type, null, my_width, (my_null.charAt(0) == 'Y') );
				break ;

		     case 'R' : // attribute
				ds.addAttribute(dim_name, my_type, null, my_width, (my_null.charAt(0) == 'Y') );
				break ;
		
		     case 'G' : // viral attribute
				ds.addAttribute(dim_name, my_type, null, my_width, (my_null.charAt(0) == 'Y'), true );
				break ;
		
		     /* case 'N' : // was: note dictionary */
			 /* case 'R' : // was: dynamic view */
			 /* case 'G' : // was: group of dimensions */
			 /* case 'C' : // was (not used in VTL): component with constant value */
         }
     }
     rs.close () ;
     st.close () ;
	}
	catch ( SQLException e ) {
		VTLError . SqlError  ( e . toString () ) ;
	}

	// read code lists (positions)
	if ( obj.dbLink != null )
		mdt_sys = obj.remoteOwner + ".mdt_positions" + obj.dbLink ;
	else
		mdt_sys = Db . mdt_positions ;
	for ( dim_index = 0 ; dim_index < ds.getDims().size () ; dim_index ++ ) {
		sql_query = "SELECT pos_code FROM " + mdt_sys 
					+ " WHERE object_id=" + obj.objectId + " AND dim_index=" + ( dim_index + 1 ) 
					+ " ORDER BY pos_index" ;
		ds.getDims().get ( dim_index ).dim_values = Db.sqlFillArray ( sql_query ) ;
	} 
	 	
	if ( obj.objectType == VTLObject.O_VIEW ) {
 		for ( dim_index = 0; dim_index < ds.getDims().size (); dim_index ++ ) {
 			DatasetComponent	tab_dim ;
 			tab_dim = ds.getDims().get ( dim_index ) ;
 			if ( tab_dim.dim_values.size() == 0 )
 				tab_dim.dim_values = Db.sqlFillArray ( "SELECT DISTINCT " + tab_dim.compName + " FROM " + ds.sqlTableName + " ORDER BY 1" ) ;
 		}
 	}

	add_table_desc ( ds ) ;

 	// MDT table - convert 1 property to measure and remaining properties to attributes
 	// TBD need to change the test is mdt object? maybe leave X for mdt and use other values for vtl
 	/*
 	if ( ds.getAttributes().size() == 0 ) {		// && ( ds.dim_group == null || ds.dim_group.size() == 0 ) ) {
 		boolean first = true ;
 		for ( DatasetComponent m1 : ds.getMeasures() ) {
 			if ( first )
 				first = false ;
 			else
 				ds.getAttributes().add( m1 ) ;
 		}
 		for ( DatasetComponent a1 : ds.getAttributes() )
 			ds.getMeasures().remove(ds.getMeasureAttribute( a1.col_name )) ;
 		
 		for ( DatasetComponent d : ds.dims ) {
 			if ( d.dim_name.equals ( "time" ) && d.dim_type.equals ( "time" ) )
 				d.dim_type = "time_period" ;
 		}
 	}
 	*/
 		
 	return ( ds ) ;
}

/*
 * Add tab descriptor to descriptors kept in memory (not for a remote object)
 */
static void add_table_desc ( Dataset ds )
{ 	
	if ( ds.dsName.indexOf( '@' ) < 0 ) {
		if ( table_descriptors . size () > max_descriptors )
			table_descriptors . remove ( 0 ) ;
		table_descriptors . add ( ds ) ;
	}
}

/*
 * Remove tab descriptor with given object_id
 */
public static void removeTableDesc ( int object_id ) throws VTLError
{

	int		idx ;
	Dataset	ds ;

	for ( idx = 0 ; idx < table_descriptors . size () ; idx ++ ) {
		ds = table_descriptors . get ( idx ) ;
		if ( object_id == ds .objectId ) {
			table_descriptors . remove ( idx ) ;
			return ;
		}
	}
}

public static void removeAllTableDesc ( ) throws VTLError
{
	table_descriptors.removeAllElements() ;
}

// value domains

/*
 * raise exception if valuedomain does not exist
static final void checkValueDomain ( String dataType ) throws VTLError
{
	getValueDomain ( dataType ) ;	
}
*/

/*
 * Return type (predefined or valuedomain) of variable (any component used in an existing dataset)
 * Raise exception if variable or valuedomain do not exist
 * NB: the type of a variable should be the same in all datasets
 */
static String getVariableDataType ( String variableName, boolean raiseError ) throws VTLError
{
	String	dataType = Db.sqlGetValue( "SELECT column_type FROM " + Db.mdt_dimensions + " WHERE dim_name='" + variableName 
					+ "' AND object_id IN (SELECT object_id FROM " + Db.mdt_objects + " WHERE object_type IN ('T','V') )") ;
	if ( dataType == null ) {
		if ( raiseError )
			VTLError.TypeError( "Variable not found: " + variableName );
		return ( null ) ;
	}
	if ( ! Check.isPredefinedType( dataType ) )
		getValueDomain ( dataType ) ;				// raise error is valuedomain does not exist
	return ( dataType ) ;	
}

static String getVariableDataType ( String variableName ) throws VTLError
{
	return ( getVariableDataType ( variableName, true ) ) ;
}

/*
 * Get valuedomain (user-defined data type).
 * If valuedomain is a subset of another valuedomain and has no defined list of positions
 * then return the valuedomain from which it is derived.
 */
static Dataset getValueDomain ( String data_type ) throws VTLError
{
	Dataset		ds ;
	
	ds = Dataset.getDatasetDesc ( data_type ) ;
	
	if ( ds.objectType != VTLObject.O_VALUEDOMAIN )
		VTLError.TypeError ( data_type + " is not a valuedomain" ) ;

	if ( ds.getDims().size() == 0 )
		VTLError.InternalError ( "Valuedomain with 0 dimensions: " + data_type ) ; 
	
	return ( ds ) ;
}

/*
 * Is this a valuedomain subset?
 */
boolean isValuedomainSubset ( )
{	
	return ( this.objectType == VTLObject.O_VALUEDOMAIN && ! Check.isPredefinedType( this.getDims().get(0).compType ) ) ; 
}

/*
 * Get base type of valuedomain 
 * For a valuedomain subset return the type of the base valuedomain
 */
static String getValuedomainBaseType ( String dataType ) throws VTLError
{
	Dataset ds = Dataset.getValueDomain( dataType ) ;

	if ( ds.isValuedomainSubset() )
		ds = getValueDomain ( ds.dims.firstElement().compType ) ;

	return ( ds.dims.firstElement().compType ) ;	
}

/*
 * Get width of base type of valuedomain 
 * For a valuedomain subset return the type of the base valuedomain
 */
static int getValuedomainBaseWidth ( String dataType ) throws VTLError
{
	Dataset ds = Dataset.getValueDomain( dataType ) ;

	if ( ds.isValuedomainSubset() )
		ds = getValueDomain ( ds.dims.firstElement().compType ) ;

	return ( ds.dims.firstElement().compWidth ) ;	
}

/*
 * Get valuedomain of user-defined datatype.
 * If valuedomain is subset of another valuedomain and has no defined list of positions
 * then return the valuedomain of which it is subset.
 */
static ListString getValuedomainCodeList ( String data_type ) throws VTLError
{
	Dataset	ds = Dataset.getDatasetDesc ( data_type ) ;

	if ( ds.isValuedomainSubset() && ds.dims.firstElement().dim_values.size() == 0 )
		ds = getValueDomain ( ds.dims.firstElement().compType ) ;

	return ( ds.dims.firstElement().dim_values ) ;		// the original code list is empty
}

/*
 * Returns the width of the dimension, used to create a db column. Correction for derived dictionaries:
 * 	create table t ( obs_value number, key ( partner ) ) ;
 */
int getBaseDimensionWidth ( ) throws VTLError
{ 
	Dataset 			base_dict ;
	DatasetComponent	dim ;
	
	dim = this.getDims().get(0) ;

	if ( this.isValuedomainSubset() ) {
		base_dict = getValueDomain ( dim.compType ) ;
		return ( base_dict.getDims().get(0).compWidth ) ;		// the original code list is empty
	}

	return ( dim.compWidth ) ;	
}

/*
 * For a valuedomain subset, it returns the object_id of the base valuedomain
 */
public int getBaseObjectId ( ) throws VTLError
{
	Dataset 			base_dict ;
	DatasetComponent	dim ;
	
	dim = this.getDims().get(0) ;

	if ( this.isValuedomainSubset() ) {
		base_dict = getValueDomain ( dim.compType ) ;
		return ( base_dict.objectId ) ;		// the original code list is empty
	}

	return ( this.objectId ) ;
	
}

/*
 * Create a primary key containing all dimensions.
 */
public void createPrimaryKey ( ) throws VTLError
{
	if ( this.getDims().size() > 0 )
		Db.sqlExec ( "ALTER TABLE " + this.dsName + " ADD PRIMARY KEY(" + this.stringDimensions() + ")" ) ;
}

/*
 * Create a bitmap index for each dimension.
 */
public void createBitmapIndexes ( ) throws VTLError
{
	int		dim_index ;
	String	dim_name ;
	
	for ( dim_index = 0; dim_index < this . getDims() . size (); dim_index ++ ) {
		dim_name = getDims().get(dim_index) . compName ;
		/*
		Db.sql_exec ( "CREATE BITMAP INDEX " + this.table_name + "$" + dim_index
					+ " ON " + this.table_name + "(" + dim_name + ")PCTFREE 4 COMPUTE STATISTICS" ) ;
		*/
		
		Db.sqlExec ( "CREATE BITMAP INDEX " + Db.newIndexName() 
				+ " ON " + this.dsName + "(" + dim_name + ")PCTFREE 4 COMPUTE STATISTICS" ) ;			
	}
}

/*
 * Create indexes. Bitmap indexes are created when:
 * 	object is not a valuedomain
 * 	the option tables_have_bitmap_indexes is true and, if tables_have_primary_key is true then object must not have 1 dimension
 * 	
 * Oracle exception ORA-01408: 
 * 	A CREATE INDEX statement specified a column that is already indexed. A single column may be indexed only once. 
 * 	Additional indexes may be created on the column if it is used as a portion 
 * 	of a concatenated index, that is, if the index consists of multiple columns
 */
public void createIndexes ( ) throws VTLError
{
	if ( objectType == VTLObject.O_VALUEDOMAIN || UserProfile.tablesHavePrimaryKey )
		this.createPrimaryKey ( ) ;
	if ( objectType != VTLObject.O_VALUEDOMAIN && UserProfile.tablesHaveBitmapIndexes 
			&& ! ( this.getDims().size() == 1 && UserProfile.tablesHavePrimaryKey ) )
		this.createBitmapIndexes ( ) ;
}

/*
 * Alter property type "string" in this data object.
 * See comment above.
 */
void alterMeasureAttributeTypeString ( String property_name, int width ) throws VTLError
{
	DatasetComponent	pro ;
	
	if ( ( pro = this.getMeasureAttribute( property_name )) == null )
		VTLError.RunTimeError ( property_name + " is not a property of " + this.dsName ) ;

	if ( pro.compType.compareTo( "string" ) != 0 )
		VTLError.RunTimeError ( property_name + " is not string" ) ;

	if ( width < 1 )
		VTLError.TypeError("width must be positive");
	
	Db.sqlExec ( "UPDATE " + Db.mdt_user_dimensions + " SET dim_width=" + width 
			+ " WHERE object_id=" + this.objectId + " AND dim_name='" + property_name + "'" ) ;
	
	Db.sqlCommit() ;
	
	Db.sqlExec ( "ALTER TABLE " + this.dsName + " MODIFY " + property_name + " VARCHAR(" + width + ")" ) ;
}

/*
 * Build SQL column data type from VTL data type.
 */
static String sqlColumnType ( String vtlDataType, int colWidth ) throws VTLError
{
	String		sqlType ;

	switch ( vtlDataType ) {
		case "integer" : 
		case "number" : 
		case "date" :
			sqlType = vtlDataType ;
			break ;
		case "string" :
			sqlType = "varchar(" + colWidth + ")" ;
			break ;
		case "boolean" :
			sqlType = "varchar(5)" ;
			break ;
		case "scalar" :
		case "time" :
		case "null" :
			VTLError.TypeError( "Cannot create dataset with a component whose type is " + vtlDataType );
			sqlType = null ;
		default :
			if ( Check.isPeriodDataType ( vtlDataType ) )
				sqlType = "varchar(10)" ;
			else {
				Dataset dict = getValueDomain ( vtlDataType ) ;
				sqlType = sqlColumnType ( dict.dims.firstElement().compType, dict.getBaseDimensionWidth () ) ;
				// sqlType = "varchar(" + dict.getBaseDimensionWidth () + ")" ;
			}	
	}

	return ( sqlType ) ;
}

/*
 * Check uniqueness of component names
 */
void checkComponentUniqueness () throws VTLError
{
	for ( DatasetComponent pro : this.getMeasures() ) {
		if ( this.getDimension( pro.compName ) != null )
			VTLError.TypeError( "Measure: " + pro.compName + " is already used as a dimension");
	}
	
	for ( DatasetComponent a : this.getAttributes() ) {
		if ( this.getDimension( a.compName ) != null )
			VTLError.TypeError( "Attribute: " + a.compName + " is already used as a dimension");
		if ( this.getMeasure( a.compName ) != null )
			VTLError.TypeError( "Attribute: " + a.compName + " is already used as a measure");
	}
}

/*
 * Create data object.
 */
void createSqlTable ( boolean create_indexes ) throws VTLError
{
	StringBuffer	column_list = new StringBuffer () ;
	String			sql_statement ;
	  
	// dimensions
	for ( DatasetComponent dim : this.getDims() ) {
		if ( column_list.length() > 0 )
			column_list.append( ',' ) ;
		column_list.append( dim.compName + " " + sqlColumnType ( dim.compType, dim.compWidth ) + " NOT NULL" ) ;
	}

	// measures
	for ( DatasetComponent pro : this.getMeasures() ) {
		if ( column_list.length() > 0 )
			column_list.append( ',' ) ;
		column_list.append( pro.compName + " " + sqlColumnType ( pro.compType, pro.compWidth ) ) ;
		if ( pro.canBeNull == false )							// v. 150, changed the order (first default, then not null)
			column_list.append( " NOT NULL" ) ;
	}

	// attributes
	for ( DatasetComponent pro : this.getAttributes() ) {
		if ( column_list.length() > 0 )
			column_list.append( ',' ) ;
		column_list.append( pro.compName + " " + sqlColumnType ( pro.compType, pro.compWidth ) ) ;
		if ( pro.canBeNull == false )							// v. 150, changed the order (first default, then not null)
			column_list.append( " NOT NULL" ) ;
	}
  
	// SQL primary key
	if ( create_indexes && ( this.objectType == VTLObject.O_VALUEDOMAIN || ( UserProfile.tablesHavePrimaryKey && this.getDims().size () > 0 ) ) )
		column_list.append( ",PRIMARY KEY (" + this.stringDimensions() + ")" ) ;	  

	sql_statement = "CREATE TABLE " + this.dsName + "(" + column_list + ")" ;

	// session . debug ( sql_statement ) ;
  
	// create table
	Db.sqlExec ( sql_statement ) ;
	  
	// create bitmap indexes
	if ( create_indexes && this.objectType != VTLObject.O_VALUEDOMAIN && UserProfile.tablesHaveBitmapIndexes
			&& ! ( this.getDims().size() == 1 && UserProfile.tablesHavePrimaryKey ) )
		this.createBitmapIndexes ( );
	  
	// grant select privilege
	Privilege.grant_select ( this.dsName ) ;
}

/*
 * Build SQL query to retrieve positions of given dim index.
 * dim_index is incremented by 1 because in dim_index in mdt_positions starts with 1.
 */
static String query_positions ( int object_id, int dim_index )
{
  return ( "SELECT pos_code FROM " + Db . mdt_positions + " WHERE object_id=" + object_id + " AND dim_index=" + ( dim_index + 1 ) ) ;
}

/*
 * Delete object from mdt_user_positions for given dim_index. 
 * dim_index is incremented by 1 because in dim_index in mdt_positions starts with 1.
 */
public void delete_positions ( int dim_index ) throws VTLError
{
  Db . sqlExec ( "DELETE FROM " + Db . mdt_user_positions
                   + " WHERE object_id=" + this . objectId + " AND dim_index=" + ( dim_index + 1 ) ) ;
}

/*
 * Save positions of dimension with given index of tab starting from given pos_index.
 * dim_index is incremented by 1 because in dim_index in mdt_positions starts with 1.
 */
public void insert_positions ( int dim_index, ListString dim_values, int pos_index_start ) throws VTLError
{
  int				pos_index = pos_index_start + 1 ;
  PreparedStatement pstmt ;
  
  try {    
	pstmt = Db . prepareStatement ( "INSERT INTO " + Db . mdt_user_positions
            + "(object_id,dim_index,pos_index,pos_code)VALUES("
            + this.objectId + "," + ( dim_index + 1 ) + ",?,?)" ) ;
    
    for ( String str : dim_values ) {
        pstmt . setInt ( 1, pos_index ++ ) ;
        pstmt . setString ( 2, str ) ;
        pstmt . executeUpdate () ;
        // Sys . println ( dim_index + ":" + pos_index + ":" + dim_values.get (idx ) ) ;
        }
    pstmt . close ( ) ;
  } 
  catch ( SQLException e ) {
        VTLError . SqlError  ( e . toString () ) ;
  }
}

/*
 * Save positions of dimension with given index of tab starting from given pos_index.
 * dim_index is incremented by 1 because in dim_index in mdt_positions starts with 1.
 */
public void insert_positions ( ) throws VTLError
{
	int		dim_index ;
	for ( dim_index = 0; dim_index < this.getDims().size(); dim_index ++ )
		this . insert_positions ( dim_index, this.getDims().get ( dim_index ).dim_values, 0 ) ;
}

/*
 * Insert positions of dimension with given index at the end of existing list of positions.
 */
public void add_positions ( int dim_index, ListString dim_values, int current_list_size ) throws VTLError
{
	int					pos_index = current_list_size + 1 ;
	PreparedStatement 	pstmt ;
  
	dim_index ++ ;		// incremented by 1 because in dim_index in mdt_positions starts with 1
  
	try {
		pstmt = Db . prepareStatement ( "INSERT INTO " + Db . mdt_user_positions_ins
                                       + "(object_id,dim_index,pos_index,pos_code)VALUES("
                                       + this.objectId + "," + dim_index + ",?,?)" ) ;
		for ( String str : dim_values ) {
			pstmt . setInt ( 1, pos_index ++ ) ;
			pstmt . setString ( 2, str ) ;      
			pstmt . executeUpdate () ;
			// Sys . println ( dim_index + ":" + pos_index + ":" + (String) dim_values . get (idx ) ) ;
        }
		pstmt . close ( ) ;
	} 
	catch ( SQLException e ) {
        VTLError . SqlError  ( e . toString () ) ;
	}
}

/*
 * Insert into mdt_positions for all dimensions.
 */
void save_positions ( ) throws VTLError
{
	int		dim_index ;

	for ( dim_index = 0; dim_index < getDims() . size (); dim_index ++ )
		if ( this.get_dimension( dim_index ).dim_values != null )
			this.insert_positions ( dim_index, this.get_dimension( dim_index ).dim_values, 0 ) ;

	for ( dim_index = 0; dim_index < getDims() . size (); dim_index ++ )		
		if ( this.get_dimension( dim_index ).dim_values != null )
			this.insert_modifications ( this.get_dimension( dim_index ).compName, 		// since v. 132
									this.get_dimension( dim_index ).dim_values, null ) ;
}

/*
 * Save positions of dimension with given index of tab.
 * op_type: drop, add, all.
 * Sample commands:
	alter table aact_ali01 modify geo add ( "US" ) ;
	alter table aact_ali01 modify geo drop ( "US" ) ;
	alter table aact_ali01 modify geo add ( "US" ) ;
	alter table aact_ali01 modify geo replace ( "US" ) with ( "CA" ) ;
	alter table aact_ali01 modify geo replace ( "US" ) with ( "CA" ) ;
	alter table aact_ali01 modify geo replace ( "CA" ) with ( "CdddA" ) ;
	public: for input module
 */
void modify_positions ( int dim_index, String op_type, ListString dim_values, int add_index ) throws VTLError
{
	String				dim_name ;
	DatasetComponent 	dim ;
	ListString			existing_values, new_list, dim_values_added, dim_values_deleted, ls_errors ;

	dim = this.get_dimension ( dim_index ) ;
	dim_name = dim .compName ;
	existing_values = dim.dim_values ;

  if ( op_type . equals ( "all" ) ) {
	  dim_values_deleted = existing_values.minus ( dim_values ) ;
	  dim_values_added = dim_values.minus ( existing_values ) ;
	  this . delete_positions ( dim_index ) ;
      this . insert_positions ( dim_index, dim_values, 0 ) ;
      new_list = dim_values ;
  } 
  else if ( op_type . equals ( "drop" ) ) {
	  	ls_errors = dim_values.minus( existing_values ) ;
		if ( ls_errors.size() > 0 )
			VTLError . RunTimeError ("Dimension values " + ls_errors.toString( ',' ) + " do not exist for " + dim_name ) ;
     dim_values_deleted = dim_values ;
     dim_values_added = null ;
     new_list = existing_values . minus ( dim_values ) ;
     // deletes from mdt_positions before checking if they are in use
     if ( this.objectType == VTLObject.O_VALUEDOMAIN && dim_values_deleted != null && dim_values_deleted.size() > 0 )	
    	 checkValuedomainDropCodes ( dim_name, dim_values_deleted ) ;
     this . delete_positions ( dim_index ) ;
     this . insert_positions ( dim_index, new_list, 0 ) ;
  } 
  else if ( op_type . equals ( "add" ) ) {
	  ls_errors = existing_values.join( dim_values ) ;
	  if ( ls_errors.size() > 0 )
		  VTLError . RunTimeError ("Dimension values " + ls_errors.toString( ',' ) + " already exist for " + dim_name ) ;
	  if ( add_index >= 0 ) {
		  Db . sqlExec ( "UPDATE " + Db.mdt_user_positions_ins + " SET pos_index = pos_index + " + dim_values.size()
					+ " WHERE object_id=" + this.objectId + " AND dim_index=" + ( dim_index + 1 ) 
					+ " AND pos_index>=" + ( add_index + 1 ) ) ;
		  new_list = (ListString) existing_values.clone() ;
		  new_list.addAll( add_index, dim_values ) ;
	  }
	  else {
		  add_index = existing_values . size () ;
		  new_list = (ListString) existing_values.clone() ;
		  new_list.addAll( dim_values ) ;
	  }
	  dim_values_deleted = null ;
	  dim_values_added = dim_values ;
	  this.add_positions(dim_index, dim_values, add_index ) ;
  }
  else {
	  VTLError.InternalError( "Save positions - case not found" ) ;
	  return ;
  }
  dim . dim_values = new_list ;		// v. 137: to fix issue with load autoextend
  // for a valuedomain, check whether the codes to be dropped are used by existing objects
  if ( this.objectType == VTLObject.O_VALUEDOMAIN && dim_values_deleted != null && dim_values_deleted.size() > 0 )	
	  checkValuedomainDropCodes ( dim_name, dim_values_deleted ) ;

  // insert modifications
  this.insert_modifications ( dim_name, dim_values_added, dim_values_deleted ) ;

  // delete existing observations whose positions have been dropped
  if ( this.isValuedomainSubset() == false && this.objectType != VTLObject.O_VIEW 
		  && dim_values_deleted != null && dim_values_deleted.size() > 0 ) {
	  Db.sqlExec ( "DELETE FROM " + this.sqlTableName + " WHERE " + dim_name
					+ " NOT IN (" + query_positions (objectId, dim_index) + ")" ) ;
  }
}

/*
 * Verify if valuedomain can be dropped.
 */
static void checkDropValuedomain ( String dim_name ) throws VTLError
{
	String		sql_query, tmp ;

	//	verify that this valuedomain is not used by derived dictionaries, tables, templates, views, etc.
	sql_query = "SELECT COUNT(*) FROM " + Db.mdt_dimensions 
				+ " WHERE column_type='" + Db.db_username.toLowerCase() + "." + dim_name + "'" ;
	if ( ( tmp = Db.sqlGetValue( sql_query ) ) . compareTo ( "0" ) != 0 )
		VTLError.RunTimeError( "Drop valuedomain - cannot drop valuedomain used in " + tmp + " existing objects" ) ;
}

/*
 * Check if codes can be dropped from valuedomain.
 */
static void checkValuedomainDropCodes ( String dim_name, ListString dim_values ) throws VTLError
{
	int			idx ;
	String		sql_query ;
	ListString	synonyms, codes_in_use ;
	
	//	 retrieve synonyms
	sql_query = "SELECT object_name FROM " + Db.mdt_user_objects + " WHERE synonym_for='" + dim_name + "'" ;
						// + " WHERE synonym_for ='" + Lower (g_connection.username) + "." + dim_name

	synonyms = Db.sqlFillArray ( sql_query ) ;

	//	 add dim_name 
	synonyms.add( dim_name ) ;

	for ( idx = 0; idx < synonyms.size(); idx ++ )
		synonyms .set ( idx , Db.db_username.toLowerCase()+ "." + synonyms.get ( idx ) ) ;

	
	sql_query = "SELECT distinct(pos_code) FROM " + Db.mdt_positions 
					+ " WHERE (object_id,dim_index) IN (SELECT object_id,dim_index FROM " + Db.mdt_dimensions
													+ " WHERE " + synonyms.sqlSyntaxInList ( "column_type", "IN" ) 
													+ ")AND " + dim_values.sqlSyntaxInList ( "pos_code", "IN" ) ;

	codes_in_use = Db.sqlFillArray ( sql_query ) ;
	if ( codes_in_use.size() > 0 )
		VTLError.RunTimeError( "Alter valuedomain - codes to be dropped (" + codes_in_use.toString( ',' )
						+ ") are used in existing data objects" ) ;
	
	/* if ( ( tmp = Db.sql_get_value( sql_query ) ).compareTo ( "0" ) != 0 ) {
		AppError.RunTimeError( "Alter valuedomain - some codes to be dropped are used in " + tmp + " existing data objects" ) ;
	}*/
}

/*
 * Replace a list of codes with a new one
 */
static void sqlReplacePositions ( String sqlTableName, String dim_name, ListString ls_old, ListString ls_new ) throws VTLError
{
	int					idx ;
	PreparedStatement 	pstmt ;

	try {
		pstmt = Db.prepareStatement( "UPDATE " + sqlTableName + " SET " + dim_name + "=? WHERE " + dim_name + "=?" ) ;
		for ( idx = 0;  idx < ls_old . size (); idx ++ ) {
			pstmt . setString ( 1, ls_new . get ( idx ) ) ;
			pstmt . setString ( 2, ls_old . get ( idx ) ) ;
			pstmt . executeUpdate () ;
		}
		pstmt .close();
	}
	catch ( SQLException e ) {
		VTLError.SqlError(e.toString()) ;
	}
}

/*
 * Replace positions of dimension.
 */
void replace_positions ( int dim_index, ListString ls_old, ListString ls_new ) throws VTLError
{
	int					idx, idx_current ;
	String				tmp, dim_name ;
	ListString			dim_values ;
	PreparedStatement 	pstmt = null;

	dim_name = this .getDims() . get ( dim_index ) . compName ;
	dim_values = this .getDims() . get ( dim_index ) . dim_values ;

	if ( ls_old.size() != ls_new.size() ) 
		VTLError.TypeError( "Alter - replace: the two lists of positions have different number of elements" ) ;

	for ( idx = 0;  idx < ls_old . size (); idx ++ ) {
		tmp = ls_old.get ( idx ) ;
		if ( ( idx_current = dim_values . indexOf ( tmp ) ) < 0 )
			VTLError.TypeError( "alter replace: " + tmp + " is not a position of dimension " + dim_name 
					+ " in " + this.dsName ) ;
		if ( ls_new.indexOf ( tmp ) >= 0 )
			VTLError.TypeError( "alter replace: " + tmp + " is contained in both lists" ) ;		
		dim_values.set( idx_current, ls_new.get( idx ) ) ;										// v. 133
	}

	// if valuedomain, check whether the codes to be dropped are used by existing objects
	if ( this.objectType == VTLObject.O_VALUEDOMAIN )
		checkValuedomainDropCodes ( dim_name, ls_old ) ;	// this.check_codes_to_be_dropped ( dim_name, dim_index ) ;

	// update positions
	try {
	pstmt = Db.prepareStatement( "UPDATE " + Db . mdt_user_positions + " SET pos_code=?" 
								+ " WHERE object_id=" + this.objectId 
								+ " AND dim_index=" + ( dim_index + 1 )			// corrected in v. 40
								+ " AND pos_code=?" ) ;		  
	for ( idx = 0;  idx < ls_old . size (); idx ++ ) {
		pstmt . setString ( 1, ls_new . get ( idx ) ) ;
		pstmt . setString ( 2, ls_old . get ( idx ) ) ;
		pstmt . executeUpdate () ;
	}
	pstmt .close();
	}
	catch ( SQLException e ) {
		VTLError.SqlError(e.toString()) ;
	}
		
	// insert modifications
	this.insert_modifications ( dim_name, ls_new, ls_old ) ;
	// Presentation.alterPresentationPositions(this.object_id, dim_name, ls_new, ls_old, dim_values) ;	// v. 133

	// update data
	if ( this.objectType != VTLObject.O_VIEW ) {
		sqlReplacePositions ( this.dsName, dim_name, ls_old, ls_new ) ;
	}
}

/*
 * Save dimensions_properties of table descriptor.
 * Insert into mdt_dimensions.
 */
void saveDatasetComponents ( ) throws VTLError
{
	int					serial_index = 0 ;
	String				insert_dim ;
	PreparedStatement 	pstmt ;
	
	try {
	insert_dim = "INSERT INTO " + Db.mdt_user_dimensions
		+ "(object_id,dim_type,dim_name,dim_index,dim_const,dim_null,dim_width,column_type)"
		+ "VALUES(" + this.objectId + ",?,?,?,?,?,?,?)" ;

	pstmt = Db.DbConnection.prepareStatement( insert_dim ) ;
	
	//			insert dimensions
	pstmt.setString ( 1, "D" ) ;	
	for ( DatasetComponent dim : this.getDims() ) {
		serial_index ++ ;
		pstmt.setString ( 2, dim.compName ) ;
		pstmt.setInt ( 3, serial_index ) ;
		pstmt.setString ( 4, null ) ;
		pstmt.setString ( 5, null ) ;
		if ( dim.compWidth == 0 ) 
			pstmt.setNull ( 6, Types.NUMERIC ) ;
		else
			pstmt.setInt ( 6, dim.compWidth ) ;
		pstmt.setString ( 7, dim.compType ) ;
		pstmt.executeUpdate () ;
	}
		
	//			insert measures
	pstmt.setString ( 1, "X" ) ;
	for ( DatasetComponent pro : this.getMeasures() ) {
		serial_index ++ ;
		pstmt.setString ( 2, pro.compName ) ;
		pstmt.setInt ( 3, serial_index ) ;
		pstmt.setString ( 4, null ) ;
		pstmt.setString ( 5, pro.canBeNull ? "Y" : "N" ) ;
	
		if ( pro.compWidth == 0 ) 
			pstmt.setNull ( 6, Types.NUMERIC ) ;
		else
			pstmt.setInt ( 6, pro.compWidth ) ;
		
		pstmt.setString ( 7, pro.compType ) ;
		pstmt.executeUpdate () ;
	}
	//			insert attributes
	for ( DatasetComponent pro : this.getAttributes() ) {
		pstmt.setString ( 1, ( pro.isViralAttribute ? "G" :"R" ) ) ; // G=viral attribute, R=non-viral attribute
		serial_index ++ ;
		pstmt.setString ( 2, pro .compName ) ;
		pstmt.setInt ( 3, serial_index ) ;
		pstmt.setString ( 4, null ) ;
		pstmt.setString ( 5, 	pro .canBeNull ? "Y" : "N" ) ;
	
		if ( pro .compWidth == 0 ) 
			pstmt.setNull ( 6, Types.NUMERIC ) ;
		else
			pstmt . setInt ( 6, pro.compWidth ) ;
		
		pstmt . setString ( 7, pro.compType ) ;
		pstmt.executeUpdate () ;
	}
		
	pstmt.close ( ) ;
	} catch ( SQLException e ) {
		VTLError . SqlError  ( e . toString () ) ;
	}
	
	/* NOT in VTL insert is dynamic view
	if ( this.is_dynamic_view ) {
		serial_index ++ ;
		pstmt . setString ( 1, "R" ) ;
	  	pstmt . setString ( 2, "_dynamic" ) ;
	//			insert dim groups
	pstmt . setString ( 1, "G" ) ;
	for ( String gdim : this.dim_group ) {
		serial_index ++ ;
		pstmt . setString ( 2, gdim ) ;
		pstmt . setInt ( 3, serial_index ) ;
		pstmt . setString ( 4, null ) ;
		pstmt . setString ( 5, null ) ;
		pstmt . setString ( 6, null ) ;
		pstmt . setString ( 7, null ) ;
		pstmt . executeUpdate () ;
	}*/
}

/*
 * Save table descriptor. Commit/rollback must be done by the calling function.
 * Insert into mdt_dimensions and mdt_positions
 */
void saveDatasetdesc ( ) throws VTLError
{
	// insert dimensions and properties
	this.saveDatasetComponents () ;
	this.save_positions ( ) ;
}

/*
 * Create SQL view.
 */
static void create_sql_view ( Query q, String objectName ) throws VTLError
{
	String		sql_create_view ;

	sql_create_view = "CREATE OR REPLACE VIEW " + objectName 
  		+ "(" + q.stringAllComponents ( ',' ) + ") AS " + q.build_sql_query ( false ) ;
  
	if ( q.sqlFrom.startsWith("(") || q.referencedDatasets.size () > 1) // see checkObjectForUpdate
		sql_create_view = sql_create_view + " WITH READ ONLY"  ;
	else
		sql_create_view = sql_create_view + " WITH CHECK OPTION" ;

	// create view
	Db.sqlExec ( sql_create_view ) ;
  
  	// grant select privileges
	Privilege.grant_select ( objectName ) ;
}



/*
 * Get list of dimensions and properties comma-separated.
 */
public String stringAllComponents ( )
{
	StringBuffer	buff = new StringBuffer () ;

	buff.append ( this.stringDimensions ( ) ) ;
	
	if ( buff.length() > 0 )
		buff.append ( ',' ) ;
	buff.append( this.stringMeasures(',')) ;
	
	if ( this.getAttributes().size() > 0 )
		buff.append( ',').append( this.stringAttributes( ',' ) ) ;

	return ( buff.toString () ) ;
}

/*
 * Returns string of dimensions.
 */
public String stringDimensions ( char sep )
{
	StringBuffer	str = new StringBuffer ( 50 ) ;
	  
	for ( DatasetComponent dim : this.getDims() ) {
		if ( str.length () > 0 )
			str.append( sep ) ;
		str.append( dim.compName ) ;		  
	}

  return ( str . toString () ) ;
}

/*
 * Verify that query q has at least the dimensions and properties of this.
 */
public void checkValidationRuleQuery ( Query q ) throws VTLError
{
	int		idx ;
	String	dim_name, property_name ;
	
	for ( idx = 0; idx < this . getDims() . size (); idx ++ ) {
		dim_name = this . getDims() . get ( idx ) . compName ;
		if ( q.getDimension ( dim_name) == null )
			VTLError .RunTimeError( "Datapoint ruleset: dimension " + dim_name + " of " + this.dsName + " not found in query") ;
		}

	for ( idx = 0; idx < this . getMeasures() . size (); idx ++ ) {
		property_name = this . getMeasures() . get ( idx ) . compName ;
		  if ( q.getMeasure ( property_name ) == null )
			VTLError .RunTimeError( "Datapoint ruleset: property " + property_name + " of " + this.dsName + " not found in query") ;
		}
}

/*
 * Verify that table tab has at least the dimensions and properties of this.
 */
public void check_function_argument ( Dataset tab, String parm_name ) throws VTLError
{
	int		idx ;
	String	dim_name, property_name ;
	
	for ( idx = 0; idx < this . getDims() . size (); idx ++ ) {
		dim_name = this . getDims() . get ( idx ) . compName ;
		if ( tab . getDimension ( dim_name) == null )
			VTLError .TypeError( "Argument " + parm_name + " does not have dimension " + dim_name ) ;
		}

	for ( idx = 0; idx < this . getMeasures() . size (); idx ++ ) {
		property_name = this . getMeasures() . get ( idx ) . compName ;
		  if ( tab . getMeasureAttribute ( property_name ) == null )
			  VTLError .TypeError( "Argument " + parm_name + " does not have property " + property_name ) ;
		}
}

/*
 * Verify that two expressions have the same dimensions.
 * q can have less properties.
*/
public void check_assign_query2tab ( Query q, boolean exact_match_properties ) throws VTLError
{
	int					dim_index, idx ;
	String				str, wrong_dim_name = null ;
	DatasetComponent 	pro, pro2 ;
	
	if ( "empty" . equals ( q .measures . get ( 0 ).sql_expr ) )
		return ;
	
	// check that the two queries have exactly the same dimensions
	if ( exact_match_properties )
		this.checkIdenticalDimensions ( q, "assignment" );	
	
	// check measures
	
	//expr. if constant, then check if all properties, except the first, are not mandatory
	if ( q.measures.get ( 0 ).compType.length () == 0 ) {
		  for ( dim_index = 1; dim_index < this . getMeasures() . size (); dim_index ++ ) {
			  pro = this . getMeasures() . get (dim_index ) ;
			  if ( pro .canBeNull == false )
				  wrong_dim_name = pro .compName ;
		  }
	}
	else
		  for ( dim_index = 0; dim_index < this . getMeasures() . size (); dim_index ++ ) {
			    pro = this . getMeasures() . get (dim_index ) ;
			    idx = q . getMeasureAttributeIndex ( pro .compName ) ;
			    if ( idx < 0 ) {
			    	if ( exact_match_properties || pro .canBeNull == false )
			    		wrong_dim_name = pro .compName ;
			    }
			    else {
				    pro2 = q . getMeasure ( pro .compName ) ;
			    	if ( pro .compType . compareTo ( pro2.compType) != 0 ) {
			    		if ( pro .compType . compareTo ( "string" ) == 0 ) {
			    			// nothing
			    		}
			    		else if ( pro2 .compType . compareTo ( "string" ) == 0 ) {
			    			str = q .measures . get ( idx ).sql_expr ;
			    			str = str . substring ( 1, str . length () - 1 ) ;
			    			Check . checkLegalValue ( pro .compType, str ) ;
			    		}
			    		else if ( q .measures . get ( idx ).sql_expr.compareTo( "null" ) == 0 ) 
			    			;
			    		else
			    			VTLError.TypeError ("Different types: " + pro .compType + "," + pro2.compType ) ;
			    	}
			    }
		  }
	
	if ( wrong_dim_name != null )
		VTLError.TypeError ("Dimension: " + wrong_dim_name ) ;
	
	// check that the two queries have exactly the same properties
	if ( exact_match_properties )
		this.checkIdenticalMeasures( q, "assignment" );
}

/*
 * Returns string of properties.
 */
public String stringMeasures ( char charSep )
{
	StringBuffer	str = new StringBuffer ( 100 ) ;
	for ( DatasetComponent c : this.getMeasures() ) {
		if ( str.length() > 0 )
			str.append( charSep ) ;
		str.append( c.compName ) ;
	}	
	return ( str . toString () ) ;
}

/*
 * Returns string of properties.
*/
public String stringAttributes ( char charSep )
{
	StringBuffer	str = new StringBuffer ( 100 ) ;
	for ( DatasetComponent c : this.getAttributes() ) {
		if ( str.length() > 0 )
			str.append( charSep ) ;
		str.append( c.compName ) ;
	}	
	return ( str . toString () ) ;
}
/*
 * Returns true if table is empty.
*/
public boolean is_empty_data ( ) throws VTLError
{
	return ( Db.sqlGetValue( "SELECT count(*) FROM " + this . dsName ).compareTo( "0" ) == 0 ) ;
}

/*
 * Codes inserted into mdt_user_modifications
 * Operation	Code
	Position added			A
	Position dropped		D
	Positions re-ordered	O
	Dimension renamed		R
	Dimension dropped		P
	Dimension added			E
	Dimension moved (dimensions re-ordered)	M
*/

/*
* Alter command: rename dimension/property. 
*/
public void alterRenameComponent ( String dim_from, String dim_to ) throws VTLError
{
	int			dim_index_from, pro_index_from  ;
	String		data_type ;
	Dataset		tab_dict ;
	boolean		has_primary_key ;
	String		sql_query ;
	
	dim_index_from = this.getDimensionIndex ( dim_from ) ;
	pro_index_from = this.getMeasureAttributeIndex ( dim_from ) ;
	if ( dim_index_from < 0 && pro_index_from < 0 )
		VTLError . TypeError ( "alter rename: " + dim_from + " is not a dimension or property of " + this . dsName ) ;

	if ( this.getDimensionIndex ( dim_to ) >= 0 || this.getMeasureAttributeIndex(dim_to) >= 0 )
		VTLError . TypeError ( "alter rename: " + dim_to + " already defined in " + this . dsName ) ;

	if ( dim_index_from >= 0 ) {
		// rename dimension
		/*
		 * 	if("string".equals(this.dims.get(dim_index_from).dim_type.toLowerCase()))
			data_type = "string";
		else
			data_type = MdtObject . get_dim_data_type ( dim_to )  ;
		 */
		try{
			data_type = VTLObject.getFullDataType ( dim_to )  ;
		}catch (Exception e) {
			data_type = null;
		}
		
		if(data_type!=null){
			tab_dict = getValueDomain ( data_type ) ;
			// dim_index + 1 in db
			sql_query = "SELECT COUNT(*) FROM " + Db.mdt_positions
				+ " WHERE object_id=" + this . objectId + " AND dim_index=" + ( dim_index_from + 1 )
				+ " AND pos_code NOT IN (SELECT pos_code FROM " + Db.mdt_positions 
										+ " WHERE object_id =" + tab_dict.objectId + " AND dim_index=1)" ;
			if ( Db.sqlGetValue ( sql_query ) .compareTo("0") != 0 )
				VTLError . RunTimeError ( "alter rename: cannot convert positions of " + dim_from + " to " + dim_to ) ;		
			//	 Part 1. modify table descriptor
		}
		
		this.get_dimension ( dim_index_from ).compName = dim_to ;
		//	 Part 2. alter table and indexes
		has_primary_key = Db.tableHasPrimaryKey ( this . dsName ) ;
		if ( has_primary_key )
			Db.sqlExec ( "ALTER TABLE " + dsName + " DROP PRIMARY KEY" ) ;
		Db.sqlExec ( "ALTER TABLE " + this . dsName + " RENAME COLUMN " + dim_from + " TO " + dim_to ) ;
		if ( has_primary_key )
			this.createPrimaryKey ( ) ;
		//	 Part 3. save table descriptor	// 	 change implemented directly
		if(data_type!=null)
			Db.sqlExec ( "UPDATE " + Db.mdt_user_dimensions + " SET dim_name='" + dim_to + "', column_type='" + data_type 
				+ "' WHERE object_id=" + this.objectId + " AND dim_name='" + dim_from + "'" ) ;
		else
			Db.sqlExec ( "UPDATE " + Db.mdt_user_dimensions + " SET dim_name='" + dim_to + "', column_type='string', dim_width=1000" 
					+ " WHERE object_id=" + this.objectId + " AND dim_name='" + dim_from + "'" ) ;
		
		
		
	    //	 Part 4. update modifications
		this.update_mdt_user_modifications ( dim_from, 'R', dim_to ) ;
	}
	else {
		//	 Part 1. modify table descriptor
		this.getMeasure ( pro_index_from ).compName = dim_to ;
		//	 Part 2. alter table and indexes
		Db.sqlExec ( "ALTER TABLE " + this . dsName + " RENAME COLUMN " + dim_from + " TO " + dim_to ) ;
		//	 Part 3. save table descriptor	// 	 change implemented directly
		Db.sqlExec ( "UPDATE " + Db.mdt_user_dimensions + " SET dim_name='" + dim_to 
				+ "' WHERE object_id=" + this.objectId + " AND dim_name='" + dim_from + "'" ) ;		
	    //	 Part 4. update modifications
		this.update_mdt_user_modifications ( dim_from, 'S', dim_to ) ;
	}
}

/*
 * Alter command: drop dimension.
	alter na_main2 drop test1
*/
public void alterDropComponent ( String compName ) throws VTLError
{
	if ( this.getDimension ( compName ) != null )
		this.alterDropDimension( compName );
	else if ( this.getMeasure ( compName ) != null || this.getAttribute ( compName ) != null )
		this.alterDropMeasureAttribute( compName );
	else
		VTLError.TypeError( "Component not found: " + compName );
}

/*
 * Alter command: drop dimension.
 */
public void alterDropDimension ( String dim_name ) throws VTLError
{
	int			num_dim_values, dim_index ;
	DatasetComponent	tab_dim ;
	boolean		has_primary_key ;
	
	tab_dim = this . getDimension ( dim_name ) ;

	if ( tab_dim == null )
		VTLError.TypeError ( "alter drop dimension: " + dim_name + " is not a dimension of " + this . dsName ) ;

	num_dim_values = tab_dim . dim_values .size();
	
	if ( num_dim_values != 1 && ! this . is_empty_data () )
		VTLError.TypeError ("alter drop dimension: " + dim_name + " has more than 1 position and dataset is not empty" ) ;
		
	dim_index = this .getDimensionIndex(dim_name) ;
	
	//	 Part 1. modify table descriptor
	this.getDims().remove(dim_index) ;
	
	//	 Part 2. alter table and indexes

	has_primary_key = Db.tableHasPrimaryKey ( this . dsName ) ;
	
	if ( has_primary_key )
		Db.sqlExec ( "ALTER TABLE " + dsName + " DROP PRIMARY KEY" ) ;
	// else: Oracle automatically drops the indexes defined on the column (but not the primary key because that is a constraint)

	Db.sqlExec ( "ALTER TABLE " + this.dsName + " DROP COLUMN " + dim_name ) ;
	
	if ( has_primary_key )
		Db.sqlExec ( "ALTER TABLE " + dsName + " ADD PRIMARY KEY (" + this.stringDimensions ( ) + ")" ) ; 
		
	//	 Part 3. save table descriptor
    Db . dbDelete ( Db.mdt_user_positions, this.objectId ) ;
    Db . dbDelete ( Db.mdt_user_dimensions, this.objectId ) ;
    this.saveDatasetComponents () ;
    this.insert_positions ( ) ;

    //	 Part 4. update modifications
    this.update_mdt_user_modifications ( dim_name, 'P', "" ) ;
}

/*
 * Alter command: drop measure or attribute.
 */
public void alterDropMeasureAttribute ( String prop_name ) throws VTLError
{
	int			prop_index, dim_index ;
	
	if ( ( prop_index = this.getMeasureAttributeIndex ( prop_name ) ) < 0 )
		VTLError.InternalError( "alter drop: " + prop_name + " is not a measure or attribute of " + this.dsName ) ;
	
	Db.sqlExec( "ALTER TABLE " + this.sqlTableName + " DROP COLUMN " + prop_name ) ;

	dim_index = prop_index + this.getDims().size() + 1 ; 
	
	Db.sqlExec ( "DELETE FROM " + Db.mdt_user_dimensions + " WHERE object_id=" + this.objectId 
						+ " AND dim_index=" + dim_index ) ;
	
	Db.sqlExec ( "UPDATE " + Db.mdt_user_dimensions + " SET dim_index=dim_index-1 WHERE object_id=" + this.objectId 
						+ " AND dim_index>" + dim_index ) ;
	
	//	 Part 4. update modifications
	this.update_mdt_user_modifications ( prop_name, 'X', "" ) ;
}

static	int	modificationsCmdId = 1 ;

/*
 * Insert new/deleted codes in user_modifications of dimension.
 */
public void insert_modifications ( String dim_name, ListString dim_values_added, ListString dim_values_deleted ) throws VTLError
{
  int	idx, num_values ;
  PreparedStatement pstmt ;
  
  try {
	  // values added

	  pstmt = Db.DbConnection.prepareStatement( "INSERT INTO " + Db . mdt_user_modifications_ins
                + "(object_id,timestamp,ses_id,cmd_id,dim_name,op_type,pos_code)VALUES("
                + this.objectId + ",SYSDATE," + Db.db_session_id + "," + modificationsCmdId + ",?,?,?)" ) ;		
	  
	  pstmt . setString ( 1, dim_name ) ;
	  if ( dim_values_added != null ) {
		pstmt . setString ( 2, "A" ) ;
		num_values = dim_values_added . size () ;
	    for ( idx = 0; idx < num_values; idx ++ ) {
	        pstmt . setString ( 3, dim_values_added . get (idx ) ) ;
	        pstmt . executeUpdate () ;
	    }
	  }
    
	  // values deleted
	  if ( dim_values_deleted != null ) {
		pstmt . setString ( 2, "D" ) ;
		num_values = dim_values_deleted . size () ;
		for ( idx = 0; idx < num_values; idx ++ ) {
	        pstmt . setString ( 3, dim_values_deleted.get (idx ) ) ;
	        pstmt . executeUpdate () ;
	    }
	  }
	  pstmt . close ( ) ;
  } 
  catch ( SQLException e ) {
        VTLError . SqlError  ( e . toString () ) ;
  }
  modificationsCmdId ++ ;
}

static final String mdtModificationsColumns = "object_id, timestamp,ses_id,cmd_id,dim_name,op_type,pos_code" ;
/*
 * Update system table mdt_user_modifications after Alter table statement.
 * op_type:
	A	add position of dimension
	D	drop position of dimension
	E	add dimension
	Y	add property
	P	drop dimension
	X	drop property
	M	move dimension
	R	rename dimension
	S	rename property
	
	NB: A and D are set by the insert_modifications() method
 */
void update_mdt_user_modifications ( String dim_prop_name, char op_type, String dim_value ) throws VTLError
{
	String insert_modif ;
	
	insert_modif = "INSERT INTO " + Db . mdt_user_modifications + "(" + mdtModificationsColumns + ")VALUES(" 
		+ this.objectId + ",SYSDATE," + Db.db_session_id + "," + modificationsCmdId + ",'" + dim_prop_name 
			+ "','" + op_type + "','" + dim_value + "')" ;

	Db.sqlExec ( insert_modif ) ;
	
	modificationsCmdId ++ ;
}

/*
 * Alter command: add dimension.
 */
void alterModifyDimension ( DatasetComponent c ) throws VTLError
{
	DatasetComponent	dim ;
	
	if ( ( dim = this.getDimension( c.compName) ) != null ) {
		int dimIndex = this.getDimensionIndex(dim.compName) ;
		this.modify_positions ( dimIndex, "all", c.dim_values, -1 ) ;			
	}
}

/*
 * Alter command: modify measure or attribute.
 */
void alterModifyMeasureAttribute ( String compRole, DatasetComponent c2 ) throws VTLError
{	
	DatasetComponent c ;
	if ( ( c = this.getMeasureAttribute( c2.compName ) ) == null )
		VTLError.TypeError( "Component not found: " + c2.compName );
	
	// null or not null
	if ( c.canBeNull != c2.canBeNull ) {
		String null_type = ( c2.canBeNull ? " null" : " not null" );
		Db.sqlExec ( "ALTER TABLE " + this.dsName + " modify " + c.compName + null_type ) ;
		Db.sqlExec ( "UPDATE " + Db.mdt_user_dimensions
						+ " SET dim_null='" + ( c2.canBeNull ? "Y" : "N" ) + "'"
						+ " WHERE object_id=" + this.objectId + " AND dim_name='" + c.compName + "'" ) ;	
	}

	// width
	if ( c.compType.equals( "string" ) && c2.compType.equals( "string" ) && c.compWidth != c2.compWidth )
		this.alterMeasureAttributeTypeString ( c.compName, c2.compWidth ) ;

	// different type
	if ( ! c.compType.equals( c2.compType ) )
		VTLError.InternalError( "alter change type, not yet implemented");
}

/*
 * Alter command: add dimension.
	alter na_main2 add identifier test2 sto { "ACT" }
 */
void alterAddDimension ( DatasetComponent c ) throws VTLError
{
	DatasetComponent	dim ;
	String 				sql_data_type, dimValue, dimName = c.compName ;
	boolean				has_primary_key ;
	
	dim = this.getDimension ( dimName ) ;
	
	if ( dim != null )
	   VTLError.TypeError ( "alter add dimension: " + dimName + " already defined in " + this.dsName ) ;

	if ( c.dim_values.size() != 1 )
		VTLError.TypeError( "add identifier: expected exactly 1 value");
	dimValue = c.dim_values.firstElement() ;
	Check.checkLegalValue( c.compType, dimValue ) ;
	
	sql_data_type = Dataset.sqlColumnType ( c.compType, c.compWidth ) ;

	//	 Part 1. modify table descriptor
	this.dims.add(c) ;
	
	//	 Part 2. alter table and indexes
	has_primary_key = Db.tableHasPrimaryKey ( this.sqlTableName ) ;
	if ( has_primary_key )
		Db.sqlExec ( "ALTER TABLE " + this.sqlTableName + " DROP PRIMARY KEY" ) ;

	Db.sqlExec ( "ALTER TABLE " + this.sqlTableName + " ADD " + dimName + " " + sql_data_type ) ;

	if ( dimValue != null ) 
		Db.sqlExec ( "UPDATE " + this.sqlTableName + " SET " + dimName + "='" + dimValue + "'" ) ;

	Db.sqlExec ( "ALTER TABLE " + this.sqlTableName + " MODIFY " + dimName + " NOT NULL" ) ;
	
	if ( has_primary_key )
		Db.sqlExec ( "ALTER TABLE " + this.sqlTableName + " ADD PRIMARY KEY (" + this.stringDimensions( ) + ")" ) ;
	else 
		Db.sqlExec ( "CREATE BITMAP INDEX " + Db.newIndexName() 
						+ " ON " + this.sqlTableName + "(" + dimName + ")PCTFREE 4 COMPUTE STATISTICS" ) ;

	//	 Part 3. save table descriptor
    Db.dbDelete ( Db.mdt_user_positions, this.objectId ) ;
    Db.dbDelete ( Db.mdt_user_dimensions, this.objectId ) ;
    this.saveDatasetComponents () ;
    this.insert_positions ( ) ;
	
	//	Part 4. update modifications
    this.update_mdt_user_modifications ( dimName, 'E', dimValue ) ; 
}

/*
 * Alter command: add measure or attribute.
	alter na_main2 add measure test1 number
*/
public void alterAddMeasureAttribute ( String compRole, DatasetComponent c ) throws VTLError
{
	String	sql_data_type ;
	
	if ( this.getMeasureAttribute( c.compName ) != null )
		VTLError.TypeError ( "alter add measure or attribute: " + c.compName + " already defined in " + this.dsName ) ;

	//	 Part 1. modify table descriptor
	if ( compRole.equals( "measure" ))
		this.getMeasures().add ( c ) ;
	else
		this.getAttributes().add ( c ) ;
		
	//	 Part 2. alter table and indexes
	sql_data_type = Dataset.sqlColumnType ( c.compType, c.compWidth ) ;
	Db.sqlExec ( "ALTER TABLE " + this.sqlTableName + " ADD " + c.compName + " " + sql_data_type 	
						+ ( c.canBeNull == false ? " NOT NULL" : "" )  ) ;	
	
	//	 Part 3. save table descriptor
    Db.dbDelete ( Db.mdt_user_positions, this.objectId ) ;
    Db.dbDelete ( Db.mdt_user_dimensions, this.objectId ) ;
    this.saveDatasetComponents () ;
    this.insert_positions ( ) ;
	
	//	 Part 4. update modifications
    this.update_mdt_user_modifications ( c.compName, 'Y', "" ) ;
}

/*
 * Alter command: move dimension [ before dimension_before ].
*/
public void alterMoveDimension ( String dim_name, String dimension_before ) throws VTLError
{
	int				dim_index, dim_index_before = -1 ;
	DatasetComponent		tab_dim ;
	
	if ( ( dim_index = this . getDimensionIndex ( dim_name ) ) < 0 )
		VTLError . TypeError ( "alter move dimension: " + dim_name + " is not a dimension of " + this.dsName ) ;

	if ( dimension_before != null ) {
		if ( ( dim_index_before = this . getDimensionIndex ( dimension_before ) ) < 0 )
			VTLError . TypeError ( "alter move dimension: " + dim_name + " is not a dimension of " + this.dsName ) ;
	}

	if ( dim_index == dim_index_before || dim_index == dim_index_before - 1 )			// no change
		return ;
	
	tab_dim = this.get_dimension(dim_index) ;
	
	//	 Part 1. modify table descriptor
	
	if ( dim_index_before < 0 ) {
		this.getDims().add(tab_dim) ;
		this.getDims().remove(dim_index) ;
	}
	else {
		if ( dim_index > dim_index_before )
			dim_index ++ ;
		this.getDims().insertElementAt(tab_dim, dim_index_before) ;	
		this.getDims().remove(dim_index) ;
	}
	
	//	 Part 2. alter table and indexes
	// no changes to Oracle table and indexes
	
	//	 Part 3. save table descriptor
    Db . dbDelete ( Db.mdt_user_positions, this.objectId ) ;
    Db . dbDelete ( Db.mdt_user_dimensions, this.objectId ) ;
    this.saveDatasetComponents () ;
    this.insert_positions ( ) ;
	
	//	 Part 4. update modifications
    this.update_mdt_user_modifications ( dim_name, 'M', "" ) ;
}

/*
 * Create table containing empty series with respect to table definition (lists of positions).
 */
void setStorageOptions ( String sql_storage_options ) throws VTLError
{
	Db.sqlExec( "ALTER TABLE " + this.sqlTableName + " " + sql_storage_options ) ;
}

/*
 * Convert to query. Add alias.
	aact_ali01#obs_value + aact_ali01#obs_decimals as obs_value
 */
public Query convert2query ( ) throws VTLError
{
	Query		q = new Query () ;
	String		t_alias, compType ;
	
	t_alias = Query.newAlias () ;
	
	// return ( this ) ;

	for ( DatasetComponent tab_dim : this.getDims() ) {
		q.addDimension(tab_dim.compName, tab_dim.compType, tab_dim.compWidth, t_alias + "." + tab_dim.compName, tab_dim.dim_values);
	}
	for ( DatasetComponent c : this.getMeasures() ) {
		compType = ( c.compType.equals( "boolean" ) 
				? "(" + t_alias + "." + c.compName + "='true')" : t_alias + "." + c.compName ) ;
		q.addMeasure(c.compName, c.compType, compType, c.compWidth, c.canBeNull );
	}
	for ( DatasetComponent c : this.getAttributes() ) {
		compType = ( c.compType.equals( "boolean" ) 
				? "(" + t_alias + "." + c.compName + "='true')" : t_alias + "." + c.compName ) ;
		q.addAttribute(c.compName, c.compType, compType, c.compWidth, c.canBeNull, c.isViralAttribute );
	}
	q.sqlFrom = this.sqlTableName + " " + t_alias ;			// this.table_name
	q.referencedDatasets.add( this.dsName ) ;
	return ( q ) ;
}

/*
 * Verify key uniqueness.
*/
public void checkUniqueKey ( ) throws VTLError
{
	int				idx ;
	String			tmp ;
	StringBuffer	sql_query, dim_list ;
	 
	sql_query = new StringBuffer () ;
	dim_list = new StringBuffer () ;
	
	for ( idx = 0; idx < this.getDims().size(); idx ++ ) {
		if ( idx > 0 )
			dim_list.append(',') ;
		dim_list . append( this.getDims().get(idx).compName ) ;
    }

	sql_query .append( "SELECT COUNT(*) ") . append( " FROM " ). append(this.dsName )
		.append(" GROUP BY " ).append( dim_list ).append( " HAVING COUNT(*) > 1" ) ;

	tmp = Db . sqlGetValue ( sql_query .toString() ) ;
	//Db.dbSql(sql_query.toString(), "rr", false) ;
	//Sys.println(sql_query .toString() + " : " + tmp);
	if ( tmp != null )
		VTLError.SqlError ( "Error found in " + this.dsName + ": dimension values are not unique" ) ;			// since v. 131
}

/*
 * Perform cross checking with the valuedomain
 */
public void verifyDimensionValues ( int dim_index, String sqlTableName, Vector<Object[]> data ) throws VTLError
{
	String		dim_name ;
	ListString	wrong_values = new ListString () ;
	DatasetComponent	tab_dim	;			// , tab_dict_dim ;
	Object[]	data_row ;
	ListString 	stored_values  ;
	ListString 	dic_values ;
	ListString	tab_values ;
	
	tab_dim = this.get_dimension ( dim_index ) ;
	dim_name = tab_dim .compName ;
	tab_values = tab_dim.dim_values ;
	stored_values = Db.sqlFillArray ( "SELECT DISTINCT " + dim_name + " FROM " + sqlTableName + " ORDER BY 1" ) ;

	if ( Check.isPredefinedType ( tab_dim.compType ) ) {			// string, integer or time
		if ( tab_values.size() > 0 ) {
			if ( Check.isPeriodDataType( tab_dim.compType ) ) {
				for ( String tmp : tab_values ) {
					if ( ! Check.isTimePeriod ( tmp ) )
						wrong_values.add ( tmp ) ;
				}				
			}
			// codes stored in db table and not in table def.			
			wrong_values = wrong_values.merge( stored_values.minus( tab_values ) ) ;		
		}
		else {
			if ( Check.isPeriodDataType( tab_dim.compType ) ) {
				for ( String tmp : stored_values ) {
					if ( ! Check.isTimePeriod ( tmp ) )
						wrong_values.add ( tmp ) ;
				}
			}
	    }
	}
	else {
		dic_values = Dataset.getValuedomainCodeList( tab_dim.compType ) ;
		if ( tab_values.size() > 0 ) {			
			if ( dic_values.size() > 0 )
				wrong_values = tab_values.minus( dic_values ) ;			// codes in table def. and not in valuedomain
			wrong_values = wrong_values.merge( stored_values.minus( tab_values ) ) ;		// codes stored in db table and not in table def.			
		}
		else {
			wrong_values = stored_values.minus( dic_values ) ;		// codes stored in db table and not in table def.			
		}
	}

	if ( wrong_values . size () > 0 ) {
	    data_row = new Object[2] ;
	    data_row[0] = dim_name ;
	    data_row[1] = wrong_values . toString ( ' ' ) ;
	    data.add (data_row) ;
	    // Session . println ( dim_name + "\t" + Basic . list_to_string ( wrong_values, " " ) ) ;
	}
}

/*
 * Verify values of property.
 */
public void verifyPropertyValues ( String pro_name, String pro_type, String sqlTableName, Vector<Object[]> data ) throws VTLError
{
	String		sql_select ;
	Dataset		tab_dict ;
	DatasetComponent	tab_dict_dim ;	
	ListString	wrong_values  ;
	Object[]	data_row ;	
	
	if ( ! Check.isPredefinedType( pro_type ) ) {
		tab_dict =  getValueDomain ( pro_type ) ;
		tab_dict_dim = tab_dict . getDims() . get ( 0 ) ;
		if ( tab_dict_dim . dim_values . size () > 0 )	{		// else: nothing
			// Sys . displayStatusMessage("Verifying values of property " + pro_name );
		  	sql_select = "SELECT DISTINCT " + pro_name + " FROM " + sqlTableName	// v. 144 prev: this.sql_table_name 
							+ " WHERE " + pro_name + " NOT IN (SELECT pos_code FROM " + Db . mdt_positions 
												+ " WHERE object_id=" + tab_dict .objectId + " AND dim_index=1)"
							+ " ORDER BY 1" ;

		    if ( ( wrong_values = Db.sqlFillArray ( sql_select ) ).size () > 0 ) {
			     data_row = new String[2] ;
			     data_row[0] = pro_name ;
			     data_row[1] = wrong_values.toString ( ' ' ) ;
			     data.add(data_row) ;
			     //Session . println ( pro_name + "\t" + Basic . list_to_string ( wrong_values, " " ) ) ;
			}
		}
	}
}

/*
 * Copy data object (S,T,D, not a view).
 */
static void copyDataObject ( VTLObject obj, String object_from, String object_to, boolean copy_data, String dblink, 
		int new_object_id ) throws VTLError
{
	Dataset		tab_to, tab_from ;
	String		mdt_sys ;
	char		object_type ;
	String 		mdt_dim_columns ;
	
	mdt_dim_columns = "dim_name,dim_index,dim_type,dim_null,dim_width,dim_const,column_type" ;

	object_type = obj.objectType ;

	tab_from = Dataset.getDatasetDesc(object_from) ; 

	tab_to = new Dataset ( tab_from ) ;
	
	tab_to.dsName = tab_to.sqlTableName = object_to ;
	tab_to.objectId = new_object_id ;
	if ( object_type == VTLObject.O_VALUEDOMAIN ) {
		if ( tab_from.isValuedomainSubset() )									// since v. 132
			; 
		tab_to.getDims().get(0).compName = object_to ;		// valuedomain: change name of first (and only) dimension
	}
	else {
		if ( dblink != null ) {					// remote object
			int	idx ;
			String	data_type ;
			for ( DatasetComponent dim : tab_to.getDims() ) {
				data_type = dim.compType ;
				if ( VTLObject.getObjectDesc( data_type ) == null ) {
					if ( ( idx = data_type.indexOf( '.' ) ) > 0 )
						data_type = data_type.substring( idx + 1 ) ;
					dim.compType = VTLObject.getFullDataType( data_type ) ;
				}
			}
			for ( DatasetComponent prop : tab_to.getMeasures() ) {
				data_type = prop.compType ;
				if ( VTLObject.getObjectDesc( data_type ) == null ) {
					if ( ( idx = data_type.indexOf( '.' ) ) > 0 )
						data_type = data_type.substring( idx + 1 ) ;
					prop.compType = VTLObject.getFullDataType( data_type ) ;
				}
			}
			for ( DatasetComponent prop : tab_to.getAttributes() ) {
				data_type = prop.compType ;
				if ( VTLObject.getObjectDesc( data_type ) == null ) {
					if ( ( idx = data_type.indexOf( '.' ) ) > 0 )
						data_type = data_type.substring( idx + 1 ) ;
					prop.compType = VTLObject.getFullDataType( data_type ) ;
				}
			}
		}
	}
	
	tab_to.createSqlTable ( false ) ;
	
	if ( copy_data && ! tab_from.isValuedomainSubset() ) {			// v. 139
		Db.sqlExec ( "INSERT INTO " + object_to + "(" + tab_to.stringAllComponents() + ")"
						+ "SELECT " + tab_from.stringAllComponents() + " FROM " + object_from ) ;
		Db.sqlCommit();
	}
	
	if ( ! tab_from.isValuedomainSubset() )			// v. 139
		tab_to.createIndexes ( ) ;			
	
	if ( dblink != null ) {
		tab_to.saveDatasetdesc() ;
	}
	else {
		mdt_sys = ( dblink != null ? obj.remoteOwner + ".mdt_dimensions" + dblink : Db.mdt_dimensions ) ;
		Db.sqlExec( "INSERT INTO " + Db.mdt_user_dimensions 
	      + "(object_id," + mdt_dim_columns + ")SELECT " + new_object_id + "," + mdt_dim_columns
	      + " FROM " + mdt_sys + " WHERE object_id=" + obj.objectId) ;
		if ( object_type == VTLObject.O_VALUEDOMAIN )
			Db.sqlExec( "UPDATE " + Db.mdt_user_dimensions 
				+ " SET dim_name='" + object_to + "' WHERE dim_index=1 AND object_id=" + new_object_id ) ;
		mdt_sys = ( dblink != null ? obj.remoteOwner + ".mdt_positions" + dblink : Db.mdt_positions ) ;
		Db.sqlExec( "INSERT INTO " + Db.mdt_user_positions + "(object_id,dim_index,pos_index,pos_code)"
			+ "SELECT " + new_object_id + ",dim_index,pos_index,pos_code"
			+ " FROM " + mdt_sys + " WHERE object_id=" + obj.objectId) ;		
	}
}

/* 
 * get definition of dataset, valuedomain or view.
 */
String getDefinitionDataObject ( char object_type ) throws VTLError
{	
	if ( object_type == VTLObject.O_VIEW )
		return ( " is " + VTLObject.getSource ( this.objectId ) ) ;

	StringBuffer syntax = new StringBuffer ( ) ;

	if ( object_type == VTLObject.O_VALUEDOMAIN && this.isValuedomainSubset() ) {
		syntax.append( " subset of " ).append( this.dims.firstElement().compName ).append( ' ' ) ;
		Node hd = UserFunction.getSyntaxTree( objectId ) ;
		Node pConstraint = hd.child.child ;
		if ( pConstraint.name != Nodes.N_SET_SCALAR )
			syntax.append( "[ " ) ;
		pConstraint.unparseOp(syntax) ;
		if ( pConstraint.name != Nodes.N_SET_SCALAR )
			syntax.append( " ]" ) ;
		return ( syntax.toString() ) ;
	}
	
	syntax.append( " is \n  " ) ;
	
	// dimensions
	int idx = 0 ;
	for ( DatasetComponent dim : this.getDims() ) {
		if ( idx++ > 0 )
			syntax.append ( " ;\n  " ) ;
		syntax.append ( "identifier " ).append(dim.compName ).append ( " " ).append ( dim.compType );
		if ( dim.compWidth > 0 )
			syntax.append ( " (" ) . append ( dim.compWidth ) . append ( ")" );
		if ( dim.dim_values.size () > 0 )
			syntax.append ( " {" ) . append ( dim.dim_values.toString () ). append ( "}" );  
	}
	
	for ( DatasetComponent m : this.getMeasures() ) {
		if ( idx > 0 )
			syntax.append ( " ;\n  " ) ;
		syntax.append ( "measure " ).append ( m.compName ).append ( " " ).append( m.compType );
		if ( m.compWidth > 0 )
			syntax.append ( " (" ).append ( m .compWidth ).append ( ")" );
		if ( m.canBeNull == false )
			syntax.append ( " not null" );
	}

	for ( DatasetComponent a : this.getAttributes() ) {
		if ( idx > 0 )
			syntax.append ( " ;\n  " ) ;
		syntax.append ( a.isViralAttribute() ? "viral attribute " : "attribute " )
							.append ( a.compName ).append ( " " ).append( a.compType );
		if ( a.compWidth > 0 )
			syntax.append ( " (" ).append ( a.compWidth ).append ( ")" );
		if ( a.canBeNull == false )
			syntax.append ( " not null" );
	}

	syntax.append ( "\nend " + VTLObject.stringObjectType ( object_type ) ) ;

	return ( syntax.toString () ) ;
}


}
