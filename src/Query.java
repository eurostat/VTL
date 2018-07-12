

/*
 * Query descriptor. A descriptor is built by evaluating expressions and is used to produce a SQL query.
 */

import java.util.Vector;

public class Query
{
	Vector <DatasetComponent>	dims ;
	Vector <DatasetComponent>	measures ;
	Vector <DatasetComponent>  	attributes ;
	String 						sqlFrom ;
	String 						sqlWhere ;
	boolean						doFilter ;
	ListString		 			referencedDatasets ;

	static int	indexTableAlias = 0 ;							// used to create unique table alias	
/*
 * Constructor.
 */
Query ()
{
	dims			= new Vector <DatasetComponent> () ;
	measures 		= new Vector <DatasetComponent> () ;
	attributes 		= new Vector <DatasetComponent> () ;
	referencedDatasets = new ListString () ;
	doFilter		= false;
}

void setDoFilter () {
	this.doFilter = true ;
}

static final String newAlias ()
{
	return ( "a" + indexTableAlias ++ ) ;
}

/*
 * Reset alias at each execution
 */
static final void resetAlias ()
{
	indexTableAlias = 0 ;
}



public DatasetComponent getDimension ( String dim_name )
{
	for ( DatasetComponent dim : dims )
		if ( dim_name.equals ( dim.compName ) )
			return ( dim ) ;

	return ( null ) ;
}

public int getDimensionIndex ( String dim_name )
{
	for ( int idx = 0 ; idx < dims.size () ; idx ++ ) {
		if ( dim_name.equals ( dims.get( idx).compName ) )
			return ( idx ) ;
	}
  
	return ( -1 ) ;
}

/*
 * get dimension with given type, raise error if 0 or more than 1 found
 */
public DatasetComponent getDimensionWithGivenType ( String dimType ) throws VTLError
{
	DatasetComponent	dimFound = null ;
	
	for ( DatasetComponent dim : dims )
		if ( dimType.equals ( dim.compType ) ) {
			if ( dimFound != null )
				VTLError.TypeError( "Found more than 1 identifier component with type " + dimType );
			dimFound = dim ;
		}

	if ( dimFound == null )
		VTLError.TypeError( "Cannot find identifier component with type " + dimType );
	return ( dimFound ) ;
}


/*
 * Get index of measure or attribute: first measures and then attributes
 */
public int getMeasureAttributeIndex ( String column_name )
{
	int idx;
	for ( idx = 0 ; idx < measures.size () ; idx ++ )
		if ( column_name . equals ( ( measures.get ( idx ) ).compName))
			return ( idx ) ;
	for ( idx = 0 ; idx < attributes.size () ; idx ++ )
		if ( column_name . equals ( ( attributes.get ( idx ) ).compName))
			return ( idx + measures.size() ) ;
	
	return ( -1 ) ;
}

/*
 * Return string containing all measures and attributes separated by sep
 */
String stringMeasuresAttributes ( char sep )
{
	StringBuffer	str = new StringBuffer ( ) ;
	for ( DatasetComponent pro : this.measures ) {
		if ( str.length() > 0 )
			str.append( sep ) ;
		str.append( pro.compName ) ;
	}
	for ( DatasetComponent pro : this.attributes )
		str.append( sep ).append( pro.compName ) ;
	
	return ( str.toString() ) ;
}

/*
 * Return string containing all measures and attributes separated by sep
 */
String stringMeasuresAttributesSql ( char sep )
{
	StringBuffer	str = new StringBuffer ( ) ;
	for ( DatasetComponent pro : this.measures ) {
		if ( str.length() > 0 )
			str.append( sep ) ;
		str.append( pro.sql_expr ) ;
	}
	for ( DatasetComponent pro : this.attributes )
		str.append( sep ).append( pro.sql_expr ) ;
	
	return ( str.toString() ) ;
}

/*
 * get measure or attribute
 */
public DatasetComponent getMeasureAttribute ( String column_name )
{
	for ( DatasetComponent m : this.measures )
		if ( m.compName.equals( column_name ) )
			return ( m ) ;
	
	for ( DatasetComponent m : this.attributes )
		if ( m.compName.equals( column_name ) )
			return ( m ) ;

	return ( null ) ;
}

/*
 * get measure or attribute
 */
public String getRole ( String column_name )
{
	for ( DatasetComponent m : this.measures )
		if ( m.compName.equals( column_name ) )
			return ( "measure" ) ;
	
	for ( DatasetComponent m : this.attributes )
		if ( m.compName.equals( column_name ) )
			return ( "attribute" ) ;

	for ( DatasetComponent m : this.dims )
		if ( m.compName.equals( column_name ) )
			return ( "identifier" ) ;

	return ( null ) ;
}

public DatasetComponent get_dimension ( int dim_index )
{
	return ( dims.get ( dim_index ) ) ;
}

/*
 * Propagate attributes for binary operators.
	print  'refin.aact_ali02' + 'refin.aact_ali01' 
	print  'refin.aact_ali02' [ geo = "FR" ] + 'refin.aact_ali01' [ geo = "IT" ] 
 */
void  propagateAttributesBinary ( Query q2 )
{
	Query				q1 = this;
	DatasetComponent 	m2 ;
	ListString			ls = new ListString ( ) ;
	
	for ( DatasetComponent m1 : q1.attributes ) {
		if ( ( m2 = q2.getAttribute(m1.compName) ) != null && ( m1.compType.equals( m2.compType)) ) {
			if ( m1.isViralAttribute() ) {	// test viral attributes
				m1.sql_expr = Db.mdt_merge_flags + "(" + m1.sql_expr + "," + m2.sql_expr + ")" ;
			}
			else
				ls.add ( m1.compName ) ;
		}
		else
			ls.add ( m1.compName ) ;
	}
	for ( String s : ls )
		this.attributes.remove( this.getAttribute( s ) ) ;
	// this.attributes.removeAllElements();
}

/*
 * Propagate attributes for unary operators.
print  - 'refin.aact_ali02' 
 */
void  propagateAttributesUnary ( )
{
	Query				q1 = this;
	ListString			ls = new ListString ( ) ;
	
	for ( DatasetComponent m1 : q1.attributes ) {
		if ( m1.isViralAttribute() ) {	// test viral attributes
			// keep it
		}
		else
			ls.add ( m1.compName ) ;
	}
	for ( String s : ls )
		this.attributes.remove( this.getAttribute( s ) ) ;
	// this.attributes.removeAllElements();
}

/*
 * Propagate attributes for unary operators.
print  sum ( 'refin.aact_ali02' ) group by geo
 */
void  propagateAttributesAggregateFunction ( )
{
	ListString			ls = new ListString ( ) ;
	
	for ( DatasetComponent m : this.attributes ) {
		if ( m.isViralAttribute() ) {	// test viral attributes
			m.sql_expr = "substr (" + Db.mdt_merge_flags + "('0',LISTAGG(" + m.sql_expr 
					+ ") WITHIN GROUP (ORDER BY " + m.sql_expr + ")),2)" ;
		}
		else
			ls.add ( m.compName ) ;
	}
	for ( String s : ls )
		this.attributes.remove( this.getAttribute( s ) ) ;
	// this.attributes.removeAllElements();
}

public final void  removeAttributes( )
{
	this.attributes.removeAllElements();
}

public final void  removeDimensions( )
{
	this.dims.removeAllElements();
}

public final void  removeMeasures( )
{
	this.measures.removeAllElements();
}

/*
 * Return string of dimension names without a dimension.
 */
public String stringDimensionsWithout ( String excludeDimName )
{
	StringBuffer	str = new StringBuffer ( ) ;
  
	for ( DatasetComponent dim : this.dims ) {
		if ( dim.compName.compareTo( excludeDimName ) != 0 ) {
			if ( str.length () > 0 )
				str.append( ',' ) ;
			str.append( dim.compName ) ;		  
		}
	}
  
	return( str.toString() ) ;
}

/*
 * Return true if compName is a component of dataset, false otherwise.
 */
public boolean hasComponent ( String compName )
{
	return ( this.getDimension( compName ) != null || this.getMeasure( compName ) != null 
			|| this.getAttribute( compName ) != null ) ;
}

/*
 * Check that q has all the components of this.
 */
public void checkHasAllComponents ( Query q ) throws VTLError
{
	for ( DatasetComponent c : this.dims )
		if ( q.getDimension( c.compName ) == null )
			VTLError.TypeError( "Identifier component not found in dataset: " + c.compName );
	for ( DatasetComponent c : this.dims )
		if ( q.getMeasure( c.compName ) == null )
			VTLError.TypeError( "Measure not found in dataset: " + c.compName );
	for ( DatasetComponent c : this.dims )
		if ( q.getAttribute( c.compName ) == null )
			VTLError.TypeError( "Attribute not found in dataset: " + c.compName );
}

/*
 * Return string of dimension names.
 */
public String stringDimensions ( char sep )
{
	StringBuffer	str = new StringBuffer ( 50 ) ;
  
	for ( DatasetComponent dim : this.dims ) {
		if ( str.length () > 0 )
			str.append( sep ) ;
		str.append( dim.compName ) ;		  
	}

  return ( str . toString () ) ;
}

public final String stringDimensions ( )
{
	return ( this.stringDimensions(',') ) ;
}

String stringAllComponents ( char sep )
{
	return ( ( this.dims.size() > 0 ? this.stringDimensions( sep ) + sep : "" ) 
			+ this.stringMeasures( sep ) 
			+ ( this.attributes.size() > 0 ? sep + this.stringAttributes( sep ) : "" ) ) ;
}

String stringAllComponents ( String sep )
{
	return ( this.stringAllComponents( ',' ).replace(",", sep) ) ;
}

public String dimensionsSqlColumns ( )
{
	StringBuffer str = new StringBuffer () ;
	for ( DatasetComponent dim : this.dims ) {
		if ( str.length() > 0 )
			str.append( ',' ) ;
		str.append( dim.sql_expr ) ;	
	}
	return ( str.toString() ) ;
}

public String dimensionsOrderBy ( )
{
	StringBuffer str = new StringBuffer () ;
	
	for ( int idx = 1; idx <= this.dims.size() ; idx ++ ) {
		if ( str.length() > 0 )
			str.append( ',' ) ;
		str.append( idx ) ;	
	}
	return ( str.toString() ) ;
}

/*
 * Exchange dimensions having indexes index_1 and index_2.
 */
public void dim_exchange ( int dim_index_1, int dim_index_2 ) throws VTLError
{
  int 		num_dims ;
  DatasetComponent	dim ;

  num_dims = dims.size () ;

  if ( dim_index_1 >= num_dims || dim_index_2 >= num_dims )
     VTLError . TypeError ( "dim_exchange: bad dimension indexes " + dim_index_1 + "," + dim_index_2 ) ;

  dim = dims.get ( dim_index_1 ) ;
  dims.set ( dim_index_1, dims.get ( dim_index_2 ) ) ;
  dims.set ( dim_index_2, dim ) ;
}

/*
 * Move dimension dim_name to last position.
 */
public void move_dimension_last ( String dim_name ) throws VTLError
{
  DatasetComponent	dim ;

  if ( ( dim = this.getDimension ( dim_name )) == null )
     VTLError . TypeError ( dim_name + " is not a dimension of expression" ) ;

  dims.remove ( dim  ) ;
  dims.add ( dim ) ;
}

public void addDimension ( String dim_name, String dim_type, int dim_width,
				String sql_expr, ListString dim_values )
{
	this.dims.add( new DatasetComponent (dim_name, dim_type, dim_width, sql_expr, dim_values) ) ;
}

public final void addDimension ( String dim_name, String dim_type, ListString dim_values )
{
	addDimension(dim_name, dim_type, 0, dim_name, dim_values );
}

public final void addDimension ( String dim_name, String dim_type, String sql_expr, ListString dim_values )
{
	addDimension(dim_name, dim_type, 0, sql_expr, dim_values );
}

ListString listDimensions ( )
{
	ListString	ls = new ListString() ;
  
	for ( DatasetComponent d : this.dims )
		ls.add( d.compName ) ;

	return ( ls ) ;
}

ListString listMeasures ( )
{
	ListString	ls = new ListString() ;
  
	for ( DatasetComponent d : this.measures )
		ls.add( d.compName ) ;

	return ( ls ) ;
}

ListString listAllComponents ( )
{
	ListString	ls = this.listDimensions() ;
  
	for ( DatasetComponent c : this.measures )
		ls.add( c.compName ) ;

	for ( DatasetComponent c : this.attributes )
		ls.add( c.compName ) ;

	return ( ls ) ;
}

/*
 * add cast operator to null expressions

public String prepareForCreateTable ( )
{
	for ( DatasetComponent c : this.measures ) {
		if ( c.sql_expr.equals( "NULL" ) || c.sql_expr.equals( "''" ))
			c.sql_expr = "CAST(" + c.sql_expr + as varchar2(10) ) ;
	}
}
*/
/*
 cannot specify column length in create table as select
String insertColumns2 ( ) throws AppError
{
	StringBuffer str = new StringBuffer ( 200 ) ;
	
	for ( DatasetComponent dim : this.dims ) {
		if ( str.length() > 0 )
			str.append( "," );
		str.append( dim.dim_name + " " + Dataset.sqlColumnType ( dim.dim_type, ( dim.dim_width == 0 ? 1000 : dim.dim_width  ) ) ) ;
	}
	
	for ( DatasetComponent c : this.measures ) {
		if ( str.length() > 0 )
			str.append( "," );
		str.append( c.col_name + " " + Dataset.sqlColumnType ( c.col_type, ( c.col_width == 0 ? 1000 : c.col_width  ) ) ) ;
	}

	for ( DatasetComponent c : this.attributes ) {
		if ( str.length() > 0 )
			str.append( "," );
		str.append( c.col_name + " " + Dataset.sqlColumnType ( c.col_type, ( c.col_width == 0 ? 1000 : c.col_width  ) ) ) ;
	}
	return ( str.toString() ) ;
}*/

public DatasetComponent getMeasure ( String pro_name )
{
	for ( DatasetComponent pro : measures ) {
		if ( pro_name.equals ( pro.compName ) )
			return ( pro ) ;
	}

	return ( null ) ;
}

public DatasetComponent getAttribute ( String name )
{
	for ( DatasetComponent m : attributes ) {
		if ( name.equals ( m.compName ) )
			return ( m ) ;
	}

	return ( null ) ;
}

/*
 * Get first property.
 */
public DatasetComponent getFirstMeasure ( ) throws VTLError
{
  if ( measures . size () == 0 )
	  VTLError.InternalError( "No measures found") ;

  return ( this.measures.get(0)) ;
}

/*
 * Get sql expression of first property.
 */
public String getFirstMeasureSqlExpr ( ) throws VTLError
{
  if ( measures . size () == 0 )
	  VTLError.InternalError( "No measures found") ;

  return ( this.measures.get(0).sql_expr) ;
}

public DatasetComponent getMeasure ( int idx )
{
  return ( measures.get ( idx ) ) ;
}

public void addToEnvironment ( ) throws VTLError
{
	Env.addVar ( "$this", this, null ) ;					// for analytic functions
	
	for ( DatasetComponent c : this.measures )
		Env.addVar ( c.compName, c.compType, c.sql_expr ) ; 

	for ( DatasetComponent c : this.attributes )
		Env.addVar ( c.compName, c.compType, c.sql_expr ) ; 

	for ( DatasetComponent dim : this.dims )
		Env.addVar ( dim.compName, dim.compType, dim.sql_expr ) ;
}

public void addMeasure ( String my_col_name, String my_col_type, String my_sql_expr, 
		int my_col_width, boolean my_col_null )
{
	measures.add ( new DatasetComponent ( my_col_name, my_col_type, my_sql_expr, my_col_width, my_col_null ) ) ;
} 

public void addAttribute ( String my_col_name, String my_col_type, String my_sql_expr, boolean my_col_null )
{
	DatasetComponent c = new DatasetComponent ( my_col_name, my_col_type, my_sql_expr, 0, my_col_null ) ;
	attributes.add ( c ) ;
}

public void addViralAttribute ( String my_col_name, String my_col_type, String my_sql_expr, boolean my_col_null )
{
	DatasetComponent c = new DatasetComponent ( my_col_name, my_col_type, my_sql_expr, 0, my_col_null ) ;
	c.setViralAttribute();
	attributes.add ( c ) ;
}

public void addAttribute ( String my_col_name, String my_col_type, String my_sql_expr, 
		int my_col_width, boolean my_col_null )
{
	DatasetComponent c = new DatasetComponent ( my_col_name, my_col_type, my_sql_expr, my_col_width, my_col_null ) ;
	attributes.add ( c ) ;
}

public void addAttribute ( String my_col_name, String my_col_type, String my_sql_expr, 
		int my_col_width, boolean my_col_null, boolean isViral )
{
	DatasetComponent c = new DatasetComponent ( my_col_name, my_col_type, my_sql_expr, my_col_width, my_col_null ) ;
	if ( isViral )
		c.setViralAttribute();
	attributes.add ( c ) ;
}

/*
 * insert new measure with default values
 */
public void insertMeasure ( String my_col_name, String my_col_type, String my_sql_expr )
{
	measures.insertElementAt( new DatasetComponent ( my_col_name, my_col_type, my_sql_expr ), 0 );
}

/*
 * add measure with default values
 */
public void addMeasure ( String my_col_name, String my_col_type, String my_sql_expr )
{
	measures.add ( new DatasetComponent ( my_col_name, my_col_type, my_sql_expr ) );
}

/*
 * add measure with default values
 */
public void addMeasure ( String my_col_name, String my_col_type, String my_sql_expr, boolean nullable )
{
	measures.add ( new DatasetComponent ( my_col_name, my_col_type, my_sql_expr, 0, nullable ) );
}

/*
 * add attribute with default values
 */
public void addAttribute ( String my_col_name, String my_col_type, String my_sql_expr )
{
	attributes.add ( new DatasetComponent ( my_col_name, my_col_type, my_sql_expr ) );
}

/*
 * add viral attribute with default values
 */
public void addViralAttribute ( String my_col_name, String my_col_type, String my_sql_expr )
{
	DatasetComponent comp = new DatasetComponent ( my_col_name, my_col_type, my_sql_expr ) ;
	comp.setViralAttribute();
	attributes.add ( comp );
}

/*
 * Returns string of attributes.
 */
public String stringAttributes ( char sep )
{
	StringBuffer	str = new StringBuffer ( ) ;
	for ( DatasetComponent c : this.attributes ) {
		if ( str.length() > 0 )
			str.append( sep ) ;
		str.append( c.compName ) ;
	}
	return ( str.toString () ) ;
}

/*
 * Returns string of measures.
 */
public String stringMeasures ( char sep )
{
	StringBuffer	str = new StringBuffer ( ) ;
	for ( DatasetComponent c : this.measures ) {
		if ( str.length() > 0 )
			str.append( sep ) ;
		str.append( c.compName ) ;
	}
	return ( str.toString () ) ;
}

/*
 * Returns string of properties.
 */
public final String stringMeasures (  )
{
	return ( this.stringMeasures ( ',' ) ) ;
}

/*
 * Return list of dimensions common to both operands.
 */
public ListString listCommonDimensions ( Query q2 )
{
  ListString	ls = new ListString() ;
  
  for ( DatasetComponent d : dims )
	  if ( q2.getDimension(d.compName) != null )
		  ls.add( d.compName ) ;

  return ( ls ) ;
}

/*
 * Add condition to the query "where" clause.
 */
public void addWherePart ( String condition )
{
	this.sqlWhere = ( this.sqlWhere == null ? condition : this.sqlWhere + " AND " + condition ) ;
}

/*
 * Add dsName table_alias to the query "from" clause.
 */
public void addFromPart ( String dsName, String table_alias )
{
  if ( sqlFrom == null )
     sqlFrom = " FROM " + dsName + " " + table_alias ;
  else
     sqlFrom = sqlFrom + " CROSS JOIN " + dsName + " " + table_alias ;
}

/*
 * Build from part.
 */
String build_sql_from ( ) throws VTLError
{
  	return ( sqlFrom != null ? " FROM " + sqlFrom : " FROM dual" ) ;
}

/*
 * Add dataset names in q2 to the objects referenced by this query.
 * The object name is stored as full object name (including the owner)
 */
void addReferencedDatasets ( Query q2 )
{
	for ( String dsName : q2.referencedDatasets ) {
		if ( dsName.indexOf ( Parser.ownershipSymbol ) < 0 )
			dsName = Db.db_username.toLowerCase () + Parser.ownershipSymbol + dsName ;
		
		if ( ! this.referencedDatasets.contains ( dsName ) )
			this.referencedDatasets.add ( dsName ) ; ;
	}
}

void checkIdenticalAllComponents ( Query q2, String opMessage ) throws VTLError
{
	this.checkIdenticalDimensions ( q2, opMessage ) ;
	this.checkIdenticalMeasures ( q2, opMessage ) ;
	this.checkIdenticalAttributes ( q2, opMessage ) ;	
}

/*
 * ds [ pivot identifier , measure ]
 
	na_main [ pivot ref_sector, obs_value ] + na_main [ pivot ref_sector, obs_value ]	
	na_main [ pivot ref_sector, obs_value ] [unpivot ref_sector, obs_value ]
 */
void pivot ( DatasetComponent pivotDim, DatasetComponent pivotMeasure ) throws VTLError
{
	StringBuffer	sqlPivot = new StringBuffer () ;
	String			table_alias = Query.newAlias () ;
	ListString		dimValues ;
	
	// set dim values of pivot dim
	this.setValues(pivotDim);
	dimValues = pivotDim.dim_values ;
	if ( dimValues.size() == 0 )		// empty dataset
		VTLError.RunTimeError( "pivot: empty dataset");

	// attributes
	this.removeAttributes();		// they are not maintained in the result

	// sql_from
	for ( String str : dimValues ) {
		if ( sqlPivot.length() == 0 ) 
			sqlPivot.append( "SELECT * FROM (" + this.build_sql_query(true) + ") "
					+ "PIVOT (min(" + pivotMeasure.compName + ") FOR " + pivotDim.compName + " IN (");
		else
			sqlPivot.append( "," ) ;
	
		sqlPivot.append( '\'' ).append( str ).append( "' AS \"" ).append( str ).append ( '"' ) ;
	}
	sqlPivot.append( "))" ) ;
	this.sqlFrom = '(' + sqlPivot.toString() + ')' + ' ' + table_alias ;

	// sql_where
	this.sqlWhere = null ;

	// dimensions
	this.dims.remove ( pivotDim ) ;
	for ( DatasetComponent dim : this.dims )
		dim.sql_expr = table_alias + "." + dim.compName ;
	
	// measures
	this.removeMeasures();
	for ( String newm : pivotDim.dim_values )
		this.addMeasure( newm, pivotMeasure.compType, table_alias + '.' + '"' + newm + '"' );
	// this.addMeasure( '"' + str + '"', pivotMeasure.col_type, table_alias + '.' + '"' + str + '"' );	
}

/*
 * ds [ unpivot identifier , measure ]
 
	na_main [ pivot ref_sector, obs_value ] [unpivot ref_sector, obs_value ]
 */
void unpivot ( String pivotDim, String pivotMeasure ) throws VTLError
{
	Query			q1 = this ;
	ListString 		dimValues = new ListString() ;
	String 			typeResult = null ;
	StringBuffer	sqlUnpivot = new StringBuffer () ;
	String			alias = Query.newAlias () ; 
	
	// attributes
	this.removeAttributes();		// they are not maintained in the result

	for ( DatasetComponent m : q1.measures ) {
		dimValues.add( m.compName ) ;
		typeResult = ( typeResult == null ? m.compType : Query.deriveType ( typeResult, m.compType ) ) ; 
	}

	// sql_from
	for ( String str : dimValues ) {
		if ( sqlUnpivot.length() == 0 ) 
			sqlUnpivot.append ( "SELECT * FROM ( " + q1.build_sql_query(true) + ") UNPIVOT (" 
					+ pivotMeasure + " FOR " + pivotDim + " IN(" ) ;
		else
			sqlUnpivot.append( "," ) ;
	
		sqlUnpivot.append ( '"' + str.toUpperCase() + '"' ) ;
	}
	sqlUnpivot.append( "))" ) ;
	q1.sqlFrom = "(" + sqlUnpivot.toString() + ")" + alias ;

	// dimensions
	q1.addDimension( pivotDim, "string", 0, pivotDim, dimValues );		// type of dimension?
	for ( DatasetComponent dim : q1.dims )
		dim.sql_expr = alias + "." + dim.compName ;
	
	// measures
	q1.measures.removeAllElements (  ) ;
	q1.addMeasure( pivotMeasure, typeResult, alias + '.' + pivotMeasure );
}

/*
 * unpivot query, or fold
 */
String unpivotQuery ( String pivotMeasure, String pivotQuery, String pivotDim, ListString allDimValues ) throws VTLError
{
	StringBuffer	sqlQuery = new StringBuffer () ;

	for ( String str : allDimValues ) {
		if ( sqlQuery.length() == 0 ) 
			sqlQuery.append ( "SELECT * FROM ( " + pivotQuery + ") UNPIVOT (" + pivotMeasure + 
					  " FOR " + pivotDim + " IN(" ) ;
		else
			sqlQuery.append( "," ) ;
	
		sqlQuery.append ( str ) ;
	}
	sqlQuery.append( "))" ) ;
		
	return ( sqlQuery.toString() ) ;
}

/*
 * pivot query, or pivot
 * Quote column names.
 */
String pivotQuery ( String measure, String dim_name, ListString allDimValues ) throws VTLError
{
	StringBuffer	sqlQuery = new StringBuffer () ;
	
	for ( String str : allDimValues ) {
		if ( sqlQuery.length() == 0 ) 
			sqlQuery.append( "SELECT * FROM (" + this.build_sql_query(true) + ") "
					+ "PIVOT (min(" + measure + ") FOR " + dim_name + " IN (");
		else
			sqlQuery.append( "," ) ;
		sqlQuery.append( '\'' ).append( str ).append( "'AS\"" ).append( str ).append ( '"' ) ;
	}
	sqlQuery.append( "))" ) ;
	
	return ( sqlQuery.toString() ) ;
}

final static String nullExpr = "CAST(NULL AS VARCHAR(1))" ;	
// to avoid error in create table (assignment statement) 
// ORA-01723: zero-length columns are not allowed

/*
	check ( op { errorcode errorcode } { errorlevel errorlevel } 
		{ imbalance imbalance } { output } )
	output ::= all | invalid
	
	check ( na_main#obs_value > 1000 errorcode "Bad value" errorlevel "Warning" )
	check ( na_main > 1000 errorcode "Bad value" errorlevel "Warning" )
	check ( na_main > 1000 errorcode "Bad value" errorlevel "Warning" invalid )
	check ( na_main > 1000 invalid )
	check ( na_main > 1000 imbalance na_main#obs_value invalid )
	TBD:
	imbalance
	type of errorcode, errolevel
 */
Query checkSingle ( String errorcode, String errorlevel, Query qimbalance, String optionOutput ) throws VTLError
{	
	Query q1 = this.copy();
	
	// measures
	q1.checkBooleanMonoMeasure ( "check(condition)" ) ;
	
	// imbalance
	if ( qimbalance != null ) {
		qimbalance.checkMonoMeasure ( "check(imbalance)", "number" ) ;
		q1.operatorBinaryJoin ( qimbalance ) ;
		q1.addMeasure( "imbalance", "number", qimbalance.getFirstMeasure().sql_expr );
	}
	
	q1.getFirstMeasure().compName = "bool_var" ; // force bool_var

	// all or invalid
	if ( optionOutput.equals( "invalid" ) ) {
		q1.sqlWhere = "NOT (" + q1.sqlWhere + ")" ;
		q1.setDoFilter();
		q1.addMeasure( "errorcode", "string", ( errorcode == null ? nullExpr : Db.sqlQuoteString(  errorcode ) ) ) ;
		q1.addMeasure( "errorlevel", "string", ( errorlevel == null ? nullExpr : Db.sqlQuoteString( errorlevel ) ) ) ;
	} 
	else {
		if ( errorcode != null )
			errorcode = "CASE WHEN NOT (" + q1.sqlWhere + ") THEN " + Db.sqlQuoteString(  errorcode ) + " END" ;
		if ( errorlevel != null )
			errorlevel = "CASE WHEN NOT (" + q1.sqlWhere + ") THEN " + Db.sqlQuoteString(  errorlevel ) + " END" ;
		q1.addMeasure( "errorcode", "string", ( errorcode == null ? nullExpr : errorcode ) ) ;
		q1.addMeasure( "errorlevel", "string", ( errorlevel == null ? nullExpr : errorlevel ) ) ;
	}

	//attributes
	q1.removeAttributes();
	
	return ( q1 );
}

/*
 * Select boolean component. Return true, false or null.
 */
public final String selectBooleanComp ( String compName, String compExpr )
{
	return ( "CASE WHEN " + compExpr + " THEN 'true' "
				+ "WHEN NOT (" + compExpr + ") THEN 'false' END AS \"" + compName.toUpperCase() + '"' ) ;
	/* alternative:
	return ( "CASE WHEN " + compExpr + " THEN 'true' "
		+ "WHEN (" + compExpr + ") IS NULL THEN NULL ELSE 'false' END AS \"" + compName + '"' ) ; 
	*/
}

/*
 * Build standard SQL query from query descriptor.
 * The column aliases are used by the output datawindow as column headings.
 */
String build_sql_query ( boolean apply_current_filters ) throws VTLError
{
	return ( build_sql_query (apply_current_filters, false ) ) ;
}

/*
 * Build standard SQL query from query descriptor.
 * The column aliases are used by the output datawindow as column headings.
 */
String build_sql_query ( boolean apply_current_filters, boolean withClauses ) throws VTLError
{
	StringBuffer	sqlQuery = new StringBuffer ( ) ;
	boolean			firstComp = true;

	if ( this.measures.size () == 0 )
		VTLError.InternalError( "Query with no measures" );
	
	if ( withClauses ) {
		for ( Env e : Env.currentEnvironment() ) {
			sqlQuery.append( sqlQuery.length() == 0 ? "WITH " : ", " )
					.append( e.variable_name ).append( " AS (" ).append((String)e.variable_value).append( ") " ) ;
		}		
	}
	
	for ( DatasetComponent dim : this.dims ) {
		sqlQuery.append( firstComp ? "SELECT " : "," ).append( dim.sql_expr )
			.append( " AS " ).append(dim.compName) ; //.append( " AS \"" ).append( dim.dim_name.toUpperCase() ).append( '"' ) ;
		firstComp = false ;
	}

	// clause SELECT: measures 
	for ( DatasetComponent c : this.measures ) {
		sqlQuery.append( firstComp ? "SELECT " : "," ) ;
		/*if ( c.compType.equals( "boolean" ) )
			sqlQuery.append ( selectBooleanComp ( c.compName, c.sql_expr ) ) ;
		else*/
		sqlQuery.append( c.sql_expr ).append( " AS \"" ).append( c.compName.toUpperCase() ).append( '"' ) ;
		firstComp = false ;
	}

	// clause SELECT: attributes 
	for ( DatasetComponent c : this.attributes ) {
		sqlQuery.append( ',' ) ;
		if ( c.compType.equals( "boolean" ) )
			sqlQuery.append ( selectBooleanComp ( c.compName, c.sql_expr ) ) ;
		else
			sqlQuery.append( c.sql_expr ).append( " AS \"" ).append( c.compName.toUpperCase() ).append( '"' ) ;
	}
 
	// clause FROM
	sqlQuery.append( this.build_sql_from ( ) ) ;
  
	// clause WHERE
	if ( apply_current_filters )
		this.filterQuery ( false ) ;
	
	if ( doFilter ) {
		if ( sqlWhere == null )
			VTLError.InternalError( "build query: condition is null");
		sqlQuery.append( " WHERE " ).append( sqlWhere ) ;
	}

	return ( sqlQuery.toString() ) ;
}

/*
 * Build SQL query that selects only someDims dimensions, in the order of someDims.
 * No measures nor attributes.
 */
public String sqlQuerySomeDims ( ListString someDims ) throws VTLError
{
	StringBuffer		sqlQuery = new StringBuffer ( ) ;

	// clause SELECT: dimensions
	sqlQuery.append( "SELECT " ) ;
  
	for ( String dimName : someDims ) {
		DatasetComponent d ;
		if ( ( d = this.getDimension(dimName) ) != null ) {
			if ( sqlQuery.length() > 7 )	// "SELECT "
				sqlQuery.append( ',' ) ;
			sqlQuery.append( d.sql_expr ) ;			
		}
	}
	
	// clause FROM
	sqlQuery.append( this.build_sql_from ( ) ) ;
  
	// clause WHERE
	if ( sqlWhere != null )
		sqlQuery.append( " WHERE " ).append( sqlWhere ) ;

	return ( sqlQuery.toString() ) ;
}

/*
 * Check that variable exists in query
 */
void checkVariables ( ListString variables ) throws VTLError
{
	for ( String v : variables ) {
		if ( this.getDimension( v ) == null && this.getMeasure( v ) == null
				&& this.getAttribute( v ) == null )
			VTLError.TypeError( "Variable " + v + " not found in query") ;
		}
}

/*
 * Check that variable exists in query
 */
void checkVariable ( String varName ) throws VTLError
{
	if ( this.getDimension( varName ) == null && this.getMeasure( varName ) == null	&& this.getAttribute( varName ) == null )
		VTLError.TypeError( "Variable " + varName + " not found in operand") ;
}

/*
 * Check that variable exists in query has the specified type
 */
void checkComponentType ( String varName, String vdName ) throws VTLError
{
	DatasetComponent	c ;
	if ( ( c = this.getDimension( varName ) ) != null || ( c = this.getMeasure( varName ) ) != null
		|| ( c = this.getAttribute( varName ) ) != null ) {
		if ( ! c.compType.equals( vdName ) )
			VTLError.TypeError( "Variable " + varName + " has not the expected type " + vdName ) ;
	}
	VTLError.TypeError( "Variable " + varName + " not found in operand") ;
}

/*
 * check type and value w.r.t. existing variable with this name
 * if a variable exists in any dataset (in the db) with that name and type t:
 *   if t is predefined then check that type of the expression is compatible with t
 *   if t is a defined type then allow only a single value (not a combined expression) and check that value is compatible with t
 *   the resulting component has type t 
 	na_main [ calc identifier x := 1 ]
 	myview [ calc identifier ref_area := "DK" ]
 	myview [ calc identifier ref_area :=1 ]			// wrong
 */
void checkCalc ( String compName, String compType, String compValue, boolean isConstant ) throws VTLError
{
	String	existingDataType = Dataset.getVariableDataType(compName, false) ;
	if ( existingDataType != null ) {
		if ( ! checkTypeOperand ( existingDataType, compType ) ) //( existingDataType.equals( compType) || compType.equals("null")) )
			VTLError.TypeError( "Calc: the type of component " + compName 
									+ " (" + compType + ") is not compatible with the type of existing component (" + existingDataType + ")" );
		if ( ! Check.isPredefinedType( existingDataType ) ) {
			if ( ! isConstant )
				VTLError.TypeError( "calc: expressions not allowed - only constant value for an existing variable of valuedomain type" ) ;
			String	valNotQuoted ;
			valNotQuoted = ( compValue.charAt(0) == '\'' ? compValue.substring( 1, compValue.length() - 1 ) : compValue ) ;
			Check.checkLegalValue( existingDataType, valNotQuoted ) ;
		}
		compType = existingDataType ;
	}
}
/*
	op [ calc { calcRole } calcComp := calcExpr { , { calcRole } calcComp := calcExpr }* ]
	calcRole ::= identifier | measure | attribute | viral attribute

	na_main [ calc comp := 3 ]
	na_main [ calc identifier comp := 3 ]
	na_main [ calc measure comp := 3 ]
	na_main [ calc attribute comp := 3 ]
	na_main [ calc viral attribute comp := 3 ]

	wrong (cannot redefine identifier):  na_main [ calc identifier ref_area:="IT" ] 
	TBD: check that only components are used in the expression
	called in clauses calc and aggr
 */
void calcAddRedefineComponent ( String compRole, String compName, String compType, String compValue, boolean isConstant ) throws VTLError
{
	String	existingRole = this.getRole(compName) ;

	if ( compRole == null )
		compRole = ( existingRole == null ? "measure" : existingRole ) ;
	
	if ( existingRole != null ) {		// existing component
		switch ( compRole ) {
			case "viral attribute" :
			case "measure" :
			case "attribute" :
				DatasetComponent c = this.getMeasureAttribute(compName) ;
				// TBD check type w.r.t. type of component
				c.compType = compType ;
				c.sql_expr = compValue ;
				break ;
			case "identifier" :
				VTLError.TypeError( "calc: dimensions cannot be redefined: " + compName );			
		}
	}
	else {
		checkCalc ( compName, compType, compValue, isConstant ) ;
		switch ( compRole ) {
			case "measure" : 
				this.addMeasure( compName, compType, compValue );				
			break ;
			case "attribute" : 
				this.addAttribute( compName, compType, compValue );							
			break ;
			case "viral attribute" : 
				this.addViralAttribute( compName, compType, compValue );							
			break ;
			case "identifier" :
				ListString dim_values = new ListString ( 1 ) ;
				if ( isConstant ) {
					String	valNotQuoted = ( compValue.charAt(0) == '\'' ? compValue.substring( 1, compValue.length() - 1 ) : compValue ) ;
					dim_values.add(valNotQuoted) ;			
				}
				this.addDimension ( compName, compType, 0, compValue, dim_values ) ; 
				break ;
			default :
				VTLError.InternalError ( "Bad component role: " + compRole );
		}		
	}
}

/*
 * Drop a dimension/measure/attribute.
	'refin.aact_ali01' [ sub geo = "IT" ]
	A := na_main[activity=A];
	B := na_main[activity=BTE];
	A+B
*/ 
public void subscript ( ListString lsNames, ListString lsValues ) throws VTLError
{
	DatasetComponent	dim ;
	int					idx = 0 ;
	ListString			lsRemoved = new ListString ( );

	for ( String dimName : lsNames ) {
		String	dimValue = lsValues.get( idx ) ;
		if ( ( dim = this.getDimension ( dimName ) ) == null )
			VTLError.TypeError ( dimName + " is not a dimension of the dataset" ) ;  // TBD: print object name
		
		if ( dim.dim_values.size( )== 0 )
			Check.checkLegalValue(dim.compType, dimValue) ;
		else if ( ! dim.dim_values.contains ( dimValue ) )
			VTLError.TypeError ( dimValue + " is not a position of dimension " + dimName + " in the dataset" ) ;

		if ( lsRemoved.indexOf( dimName ) >= 0)
			VTLError.TypeError ( dimName + " dimension appears twice in the subscript" ) ;
		lsRemoved.add( dimName ) ;		  
		this.addWherePart( dim.sql_expr + "=" + Db.sqlQuoteString ( dimValue ) );	
		idx ++ ;
	}
	for ( String str : lsRemoved )
		this.dims.remove( this.getDimension(str) ) ;
	this.setDoFilter() ;
}

/*
 * Drop a dimension/measure/attribute.
	print 'refin.aact_ali01' [ drop obs_value ]
*/ 
public void dropComponents ( ListString dropComponents ) throws VTLError
{
  	for ( String compName : dropComponents ) 
  		dropComponent ( compName ) ;
  	
	if ( this.measures.size () == 0 )
		VTLError.TypeError( "Clause operators: the resulting dataset has no measures" ) ;
}

/*
 * Drop a dimension/measure/attribute.
	print 'refin.aact_ali01' [ drop obs_value ]
*/ 
public void dropComponent (String compName ) throws VTLError
{
	DatasetComponent	c ;

	if ( this.getDimension ( compName ) != null ) 
		VTLError.TypeError ( "drop/keep: cannot drop a dimension" ) ;
	if ( ( c = this.getMeasure( compName ) ) != null )
		this.measures.remove ( c ) ;
	else if ( ( c = this.getAttribute( compName ) ) != null )
		this.attributes.remove ( c ) ;
	else
		VTLError.TypeError ( "drop: component " + compName + " not found in expression" ) ;
}

/*
 * keep components of a dataset
	na_main [ keep [obs_value,dd]]
*/ 
public void keepComponents ( ListString keepComponents ) throws VTLError
{
	ListString dropComponents = new ListString () ;
	
  	for ( String dim_name : keepComponents ) {
  		if ( this.getDimension ( dim_name ) != null ) 
			VTLError.TypeError ( "keep: cannot keep a dimension" ) ;
  		
  		if ( this.getMeasure( dim_name ) == null && this.getAttribute( dim_name ) == null )
  			VTLError . TypeError ( "keep: component " + dim_name + " not found in expression" ) ;  
  	}
  	for ( DatasetComponent pro : this.measures ) {
		if ( keepComponents.indexOf( pro.compName ) < 0 )
			dropComponents.add( pro.compName ) ;
	}
  	for ( DatasetComponent pro : this.attributes ) {
		if ( keepComponents.indexOf( pro.compName ) < 0 )
			dropComponents.add( pro.compName ) ;
	}
	this.dropComponents ( dropComponents ) ;
}

/*
 * Rename components of a dataset to another name, with the same type.
	na_main [ rename ref_area to counterpart_area, counterpart_area to ref_area]
*/ 
public void renameComponents ( ListString listFrom, ListString listTo ) throws VTLError
{
	String				nameFrom, nameTo ;
	DatasetComponent	cFrom, cTo ;

  	for ( int idx = 0 ; idx < listFrom.size() ; idx ++ ) {
  		nameFrom = listFrom.get( idx ) ;
  		nameTo = listTo.get( idx ) ;
  		  		
  		if ( ( cFrom = this.getDimension ( nameFrom ) ) != null ) {
  			// from: dimension
  			cTo = this.getDimension ( nameTo ) ;
  			if ( cTo == null && this.getMeasureAttribute( nameTo ) != null )
  				VTLError.TypeError( "rename: " + nameFrom + " and " + nameTo + " must have the same role" ) ;
  		}
  		else if ( ( cFrom = this.getMeasure ( nameFrom ) ) != null ) {
  			// from: measure
  			cTo = this.getMeasure( nameTo ) ;
  			if ( cTo == null && ( this.getDimension( nameTo ) != null || this.getAttribute( nameTo ) != null ) )
  				VTLError.TypeError( "rename: " + nameFrom + " and " + nameTo + " must have the same role" ) ;
  		}
  		else if ( ( cFrom = this.getAttribute ( nameFrom ) ) != null ) {
  			// from: attribute
  			cTo = this.getAttribute( nameTo ) ;			// rename to another measure of ds
  			if ( cTo == null && ( this.getDimension( nameTo ) != null || this.getMeasure( nameTo ) != null ) )
  				VTLError.TypeError( "rename: " + nameFrom + " and " + nameTo + " must have the same role" ) ;
  		}
  		else {
  			cFrom = cTo = null ;
			VTLError.TypeError ( "rename: component " + nameFrom + " not found in dataset" ) ;
  		}

  		if ( cTo != null ) {
  			if (  cFrom.compType != cTo.compType )
  				VTLError.TypeError( "rename: " + nameFrom + " and " + nameTo + " must have the same type or valuedomain");
  		}
  		else {
  			String	existingDataType = Dataset.getVariableDataType( nameTo, false) ;
  			if ( existingDataType != null && ! existingDataType.equals( cFrom.compType ) )
  				VTLError.TypeError( "rename: type of " + nameFrom + " is different from type of existing component " + existingDataType );
  		}
  	}
  	
  	// change component name
  	for ( int idx = 0 ; idx < listFrom.size() ; idx ++ ) {
  		nameFrom = listFrom.get( idx ) ;
  		nameTo = listTo.get( idx ) ;
  		if ( ( cFrom = this.getDimension ( nameFrom ) ) != null )
  			cFrom.compName = nameTo ;
  		else if ( ( cFrom = this.getMeasureAttribute ( nameFrom ) ) != null )
  			cFrom.compName = nameTo ; 
  	}
  	
  	this.checkUniqueComponents ( "rename: duplicated component" ) ;
}

/*
 * Check type of operand wrt type expected.
 * Type expected is a predefined type.
 */
static boolean checkTypeOperand ( String typeExpected, String typeActual ) throws VTLError
{
	if ( typeExpected.equals( typeActual ) )
		return ( true ) ;

	if ( typeActual.equals( "null" ) )
		return ( true ) ;

	switch ( typeExpected ) {
		case "scalar" : return ( true ) ;
		case "number" : if ( typeActual.equals( "integer" ) ) return ( true ) ;
		case "string" : switch ( typeActual ) {
							case "number": case "integer" : case "boolean" : 
							case "time_period" : case "duration" : case "date" : case "time" :
								return ( true ) ;
						}
						break ;
	}
	
	if ( Check.isPredefinedType( typeActual ) )
		return ( false );
	
	return ( checkTypeOperand ( typeExpected, Dataset.getValuedomainBaseType ( typeActual ) ) ) ;
}

/*
 * Check type of operand wrt type expected.
 * Type expected is a predefined type.
 */
static void checkTypeOperand ( String op, String typeExpected, String typeActual ) throws VTLError
{
	if ( ! checkTypeOperand ( typeExpected, typeActual ) )
		VTLError.TypeError ( "Operator " + op + ", type expected: " + typeExpected + ", type found: " + typeActual ) ;	
}

/*
 * Derive type of result of binary operand.
 */
static String deriveType ( String typeOperand1, String typeOperand2 ) throws VTLError
{
	if ( typeOperand1.equals( typeOperand2 ) )
		return ( typeOperand1 ) ;
	
	if ( ( typeOperand1.equals( "number" ) && typeOperand2.equals( "integer" ) ) 
		|| ( typeOperand1.equals( "integer" ) && typeOperand2.equals( "number" ) ) ) 
		return ( "number" ) ;

	if ( typeOperand1.equals( "string" ) || typeOperand2.equals( "string" ) ) 
		return ( "string" ) ;
	
	if ( typeOperand1.equals( "null" ) ) 
		return ( typeOperand2 ) ;
	
	if ( typeOperand2.equals( "null" ) )
		return ( typeOperand1 ) ;
	
	VTLError.TypeError( "Incompatible types: " + typeOperand1 + ", " + typeOperand2 );
	// TBD: user defined types
	return (null );
}

/*
	if ( typeResult.compareTo( "scalar" ) == 0 ) {
		if ( typeOperand1.compareTo( "scalar" ) == 0 )
			return ( typeOperand2 ) ;
		else {
			AppError.InternalError( "any: " + typeOperand1 );
			return ( null ) ;
		}
	}
	else 
		return ( typeResult ) ;
*/

/*
 * Check: the dimensions in onDims must be in all datasets to be joined
 */
void checkJoinOnDims ( ListString onDims ) throws VTLError
{
	if ( onDims != null ) {
		for ( String dname : onDims )
			if ( this.getDimension( dname ) == null )
				VTLError.TypeError( "Dimension: " + dname + " is not in the dataset" ) ;		
	}
}

/*
 * Prepare first query in flatComponents
 */
void prepareFirstQuery ( Query q2, String alias, String typeOfJoin, ListString onDims ) throws VTLError
{
	Query 	q = this ;
	String 	dname ;
		
	for ( DatasetComponent d2 : q2.dims ) {
		dname = d2.compName ;
		if ( typeOfJoin.equals("cross_join") || ( onDims != null && onDims.indexOf( dname ) < 0 ) )
			dname = alias + '#' + dname ;
	
		q.addDimension ( dname, d2.compType, 0, d2.sql_expr, d2.dim_values );			
	}
	for ( DatasetComponent c2 : q2.measures ) 
		q.addMeasure ( alias + "#" + c2.compName, c2.compType, c2.sql_expr );
	for ( DatasetComponent c2 : q2.attributes ) 
		q.addAttribute ( alias + "#" + c2.compName, c2.compType, c2.sql_expr );
	q.sqlFrom = q2.sqlFrom ;
	q.sqlWhere = q2.sqlWhere ;
}

/*
 * Check that the component names are unique in the dataset
 */
void checkUniqueComponents ( String errorMessage ) throws VTLError
{
	ListString	allComponents = new ListString () ;
	
	for ( DatasetComponent c1 : this.measures )
		allComponents.addUnique ( c1.compName, errorMessage ) ;
	for ( DatasetComponent c1 : this.attributes )
		allComponents.addUnique ( c1.compName, errorMessage ) ;
	for ( DatasetComponent d1 : this.dims )
		allComponents.addUnique ( d1.compName, errorMessage ) ;		
}

/*
 * remove ds# from the name of all components
 * check validity of the resulting dataset
 */
void joinReturnCheckQuery ( String typeOfJoin, ListString onCond ) throws VTLError
{
	for ( DatasetComponent c1 : this.measures ) 
		c1.compName = c1.compName.substring ( c1.compName.indexOf( "#" ) + 1 );	// indexOf returns -1
	for ( DatasetComponent c1 : this.attributes ) 
		c1.compName = c1.compName.substring ( c1.compName.indexOf( "#" ) + 1 );
	for ( DatasetComponent d1 : this.dims )
		d1.compName = d1.compName.substring ( d1.compName.indexOf( "#" ) + 1 );
	
	checkUniqueComponents ( "join: duplicated component" ) ;
	// not needed to check dimensions when typeOfJoin.equals( "cross_join" ) || onCond != null 
}

/*
ListString ls = new ListString () ;
for ( DatasetComponent m : q.measures ) {
	if ( m.col_name.indexOf('#') > 0 )
		ls.add(m.col_name) ;
}
for ( String str : ls )
	q.measures.remove(q.getMeasure(str)) ;
if ( q.measures.size() == 0 )
	AppError.TypeError( "Join expression: the resulting dataset has no measures");
return ( q.flatQuery() ) ;*/

/*
 * Check whether the operand is a mono-measure dataset (or a scalar)
 */
final boolean isMonoMeasure ( ) {
	return ( this.measures.size() == 1 ) ;
}

/*
 * Check whether the operand has a time dimension
 */
DatasetComponent getMandatoryTimeDimension ( String op ) throws VTLError 
{
	DatasetComponent timeDim ;
	
	if ( ( timeDim = this.getTimePeriodDimension() ) == null )
		VTLError.TypeError ( op + ": dataset has no dimension whose type is time_period" ) ;
	return ( timeDim ) ;
}

/*
 * Check whether the operand is a mono-measure dataset (or a scalar)
 */
final void checkMonoMeasure ( String op ) throws VTLError 
{
	if ( this.isMultiMeasure() )
		VTLError.TypeError( op, "can be applied only to mono-measure operand");
}

/*
 * Check whether the operand is a mono-measure dataset (or a scalar)
 */
void checkMonoMeasure ( String op, String expectedType ) throws VTLError 
{
	if ( this.isMultiMeasure() )
		VTLError.TypeError( op, " operand must be mono-measure");
	
	DatasetComponent 	m = this.measures.firstElement() ;
	
	if ( ! checkTypeOperand ( expectedType, m.compType ) )
		errorTypeOperand ( op, m.compName, expectedType , m.compType ) ;
}

/*
 * Check whether the operand is a mono-measure dataset (or a scalar)
 */
final void checkBooleanMonoMeasure ( String op ) throws VTLError 
{
	checkMonoMeasure ( op, "boolean" ) ;
}

/*
 * Return expressions of components in compNames (analytic functions)
 */
String compExpressions ( String analyticFunction, ListString compNames, ListString AscDesc) throws VTLError
{
	StringBuffer		res = new StringBuffer () ;
	DatasetComponent	c ;
	int					idx = 0 ;
	
	for ( String s : compNames ) {
		if ( res.length() > 0 )
			res.append( ',' ) ;
		if ( ( c = this.getDimension( s ) ) != null || ( c = this.getMeasureAttribute( s ) ) != null )
			res.append( c.sql_expr ) ;
		else
			VTLError.TypeError( analyticFunction + ": unknown component " + s );	
		if ( AscDesc != null )
			res.append( ' ' ).append( AscDesc.get( idx++ )) ;
	}
	return ( res.toString() ) ;
}

/*
 * Analytic function
	avg ( aact_ali01 [geo=IT] ) over ( partition by itm_newa order by time )
{ 
  x := avg ( aact_ali01 [geo="IT"] ) over ( partition by itm_newa order by time rows between 1 preceding and 1 following) ;
  y := aact_ali01 ; 
  inner_join ( x alias d1, y alias d2 ) [ rename d1#obs_value to v1, d2#obs_value to v2 ; keep v1, v2 ]
}
	{ 	"first_value",		"A",	"scalar",		"*", 	"scalar"											} ,
	{ 	"last_value",		"A",	"scalar",		"*", 	"scalar"											} ,
	{ 	"lag",				"A",	"scalar",		"**", 	"scalar", 		"integer",	"scalar"					} ,
	{ 	"lead",				"A",	"scalar",		"**", 	"scalar", 		"integer",	"scalar"					} ,
	{ 	"ratio_to_report",	"A",	"number",	"*", 	"number"											} ,												} ,
	{ 	"rank",				"A",	"integer",	"", 													} ,
					
	WindowPossible = { "first_value", "last_value" }
	NoDatasetVersion = { "ntile", "percent_rank", "rank" }
					ORDER BY	Window		Component or dataset version
	lag				mandatory	NO			both
	lead			mandatory	NO			both
	first_value		optional	optional	both
	last_value		optional	optional	both
	ntile 			mandatory	NO			NO
	percent_rank	mandatory	NO			NO
	rank			mandatory	NO			NO
	ratio_to_report	NO			NO			both
	
	na_main [ calc v1 := rank ( over ( order by time ) ) ]
	na_main [ calc x := sum ( obs_value over ( order by time ) ) ]
	sum ( na_main over ( partition by ref_area order by time ) )
	sum ( na_main over ( partition by ref_area order by time desc ) )
 */
Query analyticFunction ( String analyticFunction, boolean isComponentVersion, Vector<Query> arguments, 
		ListString ls_partition, ListString ls_orderby, ListString lsAscDesc, String windowClause ) throws VTLError
{
	Query 			q = this.copy();
	StringBuffer	overClause = new StringBuffer ().append( " OVER (") ;
	
	// no partition by - implicit, if at least a dimension is specified in order by
	if ( ls_partition.size() == 0 && ls_orderby.size() > 0 ) {	
		for ( DatasetComponent dim : q.dims ) {
			if ( ls_orderby.indexOf( dim.compName ) < 0 )
				ls_partition.add( dim.compName ) ;
		}
		if ( ls_partition.size() == q.dims.size() )		// no dimensions in order by
			ls_partition.setSize(0);
	}
	
	if (ls_partition.size() > 0 )
		overClause.append ( " PARTITION BY " ).append( compExpressions ( analyticFunction, ls_partition, null ) ) ;
	if (ls_orderby.size() > 0 )
		overClause.append ( " ORDER BY " ).append( compExpressions ( analyticFunction, ls_orderby, lsAscDesc ) ) ;
	if ( windowClause.length() > 0 )
		overClause.append ( windowClause ) ;
	overClause.append ( ")" ) ;
	
	if ( analyticFunction.equals ( "rank") ) {
		if ( ! isComponentVersion )						// dataset version, not component version
			VTLError.TypeError( analyticFunction + " can be used only in component version");
		q.removeMeasures();			
		q.addMeasure( "num_var", "number", analyticFunction + "() " + overClause.toString() ) ;
		return ( q ) ;		
	}
	
	// na_main[ calc x := lag ( obs_value , 1 ) over ( order by time ) ]
	q = q.operatorFunction ( analyticFunction, arguments, ! isComponentVersion, overClause.toString() ) ;
	if ( ! isComponentVersion )
		q = q.flatQuery ( false ) ;
  	return ( q ) ;
}

/*
 * Merge all boolean measures into one (with AND) and then remove them.

void mergeBooleanMeasures ( ) throws AppError
{
	 * ListString ls = new ListString() ;
	
	String	cond = null ;
	
	for ( DatasetComponent pr : this.measures ) {
		if ( pr.col_type.compareTo( "boolean" ) == 0 ) {
			cond = ( cond == null ? pr.sql_expr : cond + " AND " + pr.sql_expr ) ;				
			ls.add( pr.col_name ) ;
		}
	}
	if ( cond == null )
		AppError.TypeError( "Cannot find a boolean measure (condition)") ; 
	
	this.sql_where = cond ;
	
	for ( String dimName : ls )
		this.measures.remove( this.getMeasure( dimName ) ) ;	
}
*/

/*
 * Check whether the operand is a mono-measure dataset (or a scalar)
 */
final boolean isMultiMeasure ( ) {
	return ( this.measures.size() > 1 ) ;
}

/*
 * Check whether the operand is a mono-measure dataset (or a scalar)
 */
final boolean isScalar ( ) {
	return ( this.measures.size() == 1 && this.sqlFrom == null ) ;	// or: dims.size() == 0
}

/*
 * 
 */
void checkContainSystemMeasure ( String measure) throws VTLError
{
	if ( this.getMeasure(measure) != null )
		VTLError.TypeError( "Cannot create system measure " + measure + " because already existing in expression");
}

/*
 * Binary operators
 * print  aact  [ filter geo="IT" ]
 * both operands can be dataset or scalar (scalar to be combined with all measures of the other operand)
 
A := na_main ;
B := na_main + 1 ;
A+B
	na_main >10 and na_main < 100
	A := na_main >10 and na_main < 100 ; A
	na_main [ filter obs_value >= 10 and obs_value <= 100 ]
 */
void operatorBinaryCheckMeasures ( Query q2, String op, String typeResult, String typeOperand1, String typeOperand2, boolean isBooleanOp ) throws VTLError
{
	Query				q1 = this;

	if ( isBooleanOp && ( q1.measures.size() > 1 || q2.measures.size() > 1 ) )
		VTLError.TypeError( "Boolean operator: " + op + " cannot be applied to multimeasure operands");

	if ( q1.isScalar() ) {
		DatasetComponent	m1 = q1.getFirstMeasure() ;
		q1.removeMeasures();
		if ( ! checkTypeOperand ( typeOperand1, m1.compType ) )
			errorTypeOperand ( op, m1.compName, typeOperand1 , m1.compType ) ;
		for ( DatasetComponent m2 : q2.measures ) {
			if ( ! checkTypeOperand ( typeOperand2, m2.compType ) )
				errorTypeOperand ( op, m2.compName, typeOperand2 , m2.compType ) ;
			DatasetComponent	m = new DatasetComponent () ;
			if ( isBooleanOp ) {
				if ( q2.isScalar() || ( typeOperand1.equals( "scalar" ) && typeOperand2.equals( "scalar" ) ) )
					m.compName = "bool_var" ;
				m.compType = "boolean" ;
				m.sql_expr = Query.booleanBinaryExpr( m1.sql_expr, op, m2.sql_expr ) ;
			}
			else {
				m.compType = deriveType ( m2.compType, m1.compType ) ;
				if ( q2.isScalar() )
					m.compName = scalarMeasureDefaultName ( m.compType ) ;
				m.sql_expr = m1.sql_expr + " " + op + " " + m2.sql_expr ;
			}
			q1.measures.add( m ) ;
		}
		// q1.measures = q2.measures ;
	}	
	else {
		if ( ! q2.isScalar() )
			q1.checkIdenticalMeasures ( q2, op ) ;

		for ( DatasetComponent m1 : q1.measures ) {
			DatasetComponent	m2;
			if ( q2.isScalar() )
				m2 = q2.getFirstMeasure() ;
			else {
				if ( ( m2 = q2.getMeasure ( m1.compName ) ) == null )
					VTLError.TypeError ( "Operator: " + op + ", measure " + m1.compName + " is only in one operand" ) ;
			}
			if ( ! checkTypeOperand ( typeOperand1, m1.compType ) )
				errorTypeOperand ( op, m1.compName, typeOperand1 , m1.compType ) ;
			if ( ! checkTypeOperand ( typeOperand2, m2.compType ) )
				errorTypeOperand ( op, m2.compName, typeOperand2 , m2.compType ) ;
			
			if ( isBooleanOp ) {
				if ( typeOperand1.equals( "scalar" ) && typeOperand2.equals( "scalar" ) )
					m1.compName = "bool_var" ;
				m1.compType = "boolean" ;
				m1.sql_expr = Query.booleanBinaryExpr( m1.sql_expr, op, m2.sql_expr ) ;
			}
			else {
				m1.compType = deriveType ( m1.compType, m2.compType ) ;
				m1.sql_expr = m1.sql_expr + " " + op + " " + m2.sql_expr ;
			}			
		}
	}	
}

/*
	if ( (q1.sql_from != null && q2.sql_from != null && q1.sql_from.equals(q2.sql_from) )
			&& ( q1.sql_where != null && q2.sql_where != null ) && q1.sql_where.equals(q2.sql_where) )
		return ;
 * like iinerJoin but accepts also scalars
 */
void operatorBinaryJoin ( Query q2 ) throws VTLError
{
	StringBuffer		onCond = new StringBuffer ( ) ;
	DatasetComponent	dim2 ;
	Query				q1 = this ;
	
	// dimensions
	for ( DatasetComponent dim1 : q1.dims ) {			// join common dimensions
		if ( ( dim2 = q2.getDimension( dim1.compName ) ) != null ) {
			onCond.append( onCond.length() == 0 ? " ON (" : " AND " ).append( dim1.sql_expr + "=" + dim2.sql_expr ) ;
		}
	}
	onCond.append( ")" ) ;
	//if ( q1.sql_from != null && q2.sql_from != null && onCond.length() == 0)
	//	AppError.TypeError( "inner join: left and right operands must have at least a common dimension" ) ;				
	// add dimensions that are not in q1
	for ( DatasetComponent dim : q2.dims ) {
		if ( q1.getDimension( dim.compName ) == null )
			q1.addDimension( dim.compName, dim.compType, dim.compWidth, dim.sql_expr, dim.dim_values) ;
	}
	
	// sql_from
	if ( q2.sqlFrom != null ) {
		if ( q1.sqlFrom != null ) {
			if ( onCond.length() > 0 )
				q1.sqlFrom = q1.sqlFrom + " INNER JOIN " + q2.sqlFrom + onCond.toString() ;	
			else
				q1.sqlFrom = q1.sqlFrom + " CROSS JOIN " + q2.sqlFrom ;	
		}
		else
			q1.sqlFrom = q2.sqlFrom ;
	}
	
	// referenced tables
	q1.addReferencedDatasets(q2);
}

/*
 * Binary operators

	na_main [ filter obs_value > 0 ] + na_main [ filter obs_value < 10 ]
	1 > 2
	3 > 2
	true = false
	na_main > 10
	10 <= na_main
 */
Query operatorBinary ( Query q2, String op, String typeResult, String typeOperand1, String typeOperand2, boolean isBooleanOp ) throws VTLError
{
	Query	q1 = this.copy() ;
	// q2 = q2.copy();
	
	// 1. where part (uses the SQL expr)
	if ( isBooleanOp ) {
		if ( typeOperand1.equals( "scalar" ) && typeOperand2.equals( "scalar" )) {
			q1.sqlWhere = q1.getFirstMeasureSqlExpr() + " " + op + " " + q2.getFirstMeasureSqlExpr() ;					
		}
		else {					
			if ( op.equals ( "xor" ) )
				q1.sqlWhere = "(" + q1.sqlWhere + " OR " + q2.sqlWhere + ") AND NOT (" + q1.sqlWhere + " AND " + q2.sqlWhere + ")" ;
			else
				q1.sqlWhere = q1.sqlWhere + " " + op + " " + q2.sqlWhere ;
		}
	}
	else {
		if ( q2.sqlWhere != null ) 
			q1.addWherePart( q2.sqlWhere );		
	}
	if ( q2.doFilter )
		q1.setDoFilter();

	// 2. measures (first the measures because of the test on scalar)
	q1.operatorBinaryCheckMeasures ( q2, op, typeResult, typeOperand1, typeOperand2, isBooleanOp );
	
	// 3. dimensions
	q1.operatorBinaryJoin ( q2 ) ;

	// 4. attributes
	if ( isBooleanOp )
		q1.removeAttributes();
	else if ( q1.isScalar() || q2.isScalar() )
		q1.propagateAttributesUnary();
	else
		q1.propagateAttributesBinary ( q2 );
	
	return ( q1 ) ;
}

/*
 * Prepare error message when the type expected is <> type found
 */
static void errorTypeOperand ( String op, String compName, String expectedType, String actualType ) throws VTLError
{
	VTLError.TypeError( "Operator " + op + ", measure " + compName 
			+ " of type " + actualType + " has not the correct type: " + expectedType );
}

/*
 * Operator is prefix: not, - (unary)
 * if typeResult is null then the type returned is typeOperand
	A := na_main >10 and na_main < 100 ; not A
*/
public Query operatorUnary ( String op, String typeOperand, String typeResult, boolean isBooleanOp ) throws VTLError
{
	Query	q1 = this.copy() ;
	
	if ( isBooleanOp && this.measures.size() > 1 )
		VTLError.TypeError( "Operator: " + op + " cannot be applied to multimeasure operands");

	for ( DatasetComponent m1 : q1.measures ) {
		if ( ! checkTypeOperand ( typeOperand, m1.compType ) )
			errorTypeOperand ( op, m1.compName, typeOperand , m1.compType ) ;
		m1.compType = ( typeResult != null ? typeResult : typeOperand ) ;	// therefore "- 1" produces integer
		if ( isBooleanOp ) {
			m1.compName = "bool_var" ;
			q1.sqlWhere = op + " " + q1.sqlWhere ;
			m1.sql_expr = "CASE " + m1.sql_expr + " WHEN 'true' THEN 'false' WHEN 'false' THEN 'true' END";			
		}
		else
			m1.sql_expr = op + " " + m1.sql_expr ;			
	}
	
	if ( isBooleanOp )
		this.removeAttributes();
	else
		this.propagateAttributesUnary();
	
	return ( q1 ) ;
}


/*
 * Name is implicit
 */
public static final Query operatorConstant ( String compExpr, String compType ) throws VTLError
{
	//if ( compExpr.equals( "null" ) && compType.equals("null" ))
		//compExpr = "CAST(NULL AS NUMBER)";
	return ( operatorConstant ( scalarMeasureDefaultName ( compType ), compType, compExpr ) ) ;
}

/*
 * build constant expression with given name, type and expression.
 */
public static Query operatorConstant ( String compName, String compType, String compExpr ) throws VTLError
{
	Query q = new Query () ;
	q.addMeasure( compName, compType, compExpr );

	if ( compType.equals( "boolean" ))
		q.sqlWhere = compExpr.equals( "'true'" ) ? "(1=1)" : "(0=1)" ;

	return ( q ) ;
}

/*
 * Name of the measure returned for a scalar measure of given type. 
		string_var 
		num_var
		int_var
		time_var
		time_period_var
		date_var
		duration_var
		bool_var
 */
public static String scalarMeasureDefaultName ( String vtlType ) throws VTLError
{
	if ( Check.isPredefinedType ( vtlType ) ) {
		switch ( vtlType ) {
			case "boolean" : return ( "bool_var" ) ;
			case "number" : return ( "num_var" ) ;
			case "integer" : return ( "int_var" ) ;
			default : return ( vtlType + "_var" ) ;
		}
	}
	
	return ( scalarMeasureDefaultName ( baseType ( vtlType ) ) ) ;
}

/*
 * Return base type of user-defined type. 
 */
public static String baseType ( String userType ) throws VTLError
{
	Dataset ds ;
	
	if ( ( ds = Dataset.getDatasetDesc ( userType ) ) == null )
		VTLError.InternalError( "Type not found: " + userType );
	return ( ds.get_dimension(0).compType ) ;	
}

/*
 * predefined function
 * user defined types? any, number
 * all measures must be of the right type for the operator (typeOperand) otherwise a type error is raised.
 * These functions can maintain the viral attributes of the dataset.
 * A function has a first operand (dataset) and may have other arguments (non-datasets)
	'refcl.geo' [ x := match_characters ( label_en,"^A.*" ) ]
	nvl: maybe operates on 2 datasets
	TBD: length, match_characters and other functions that change the type
	boolean: can have several operands and the result is not necessarily named bool_var
*/
public Query operatorFunction ( String functionName, Vector<Query> arguments, boolean applyAttributePropagationUnary, String postfixSyntax ) throws VTLError
{		
	Query 	q1 = this.copy();
	int 	funtionIndex ;
	
	if ( ( funtionIndex = Parser.sqlFunctionIndex ( functionName ) ) < 0 )
		VTLError.InternalError( "Unknown predefined function: " + functionName ) ;

	int idxParm = 0 ;
	for ( Query q : arguments ) {
		if ( ! q.isScalar() )
			VTLError.TypeError( "Operator: " + functionName + ", argument is not a scalar" );
		String typeExpected = Parser.sqlFunctionTypeParameter ( funtionIndex, idxParm++ ) ;
		if ( ! checkTypeOperand ( typeExpected, q.getFirstMeasure().compType ) )
			errorTypeOperand ( functionName, "()", typeExpected, q.getFirstMeasure().compType ) ;		
	}

	String typeResult = Parser.sqlFunctionTypeResult ( funtionIndex ) ;
	String typeOperand = Parser.sqlFunctionTypeFirstOperand ( funtionIndex ) ;
	
	if ( typeResult.equals( "boolean") && ! q1.isMonoMeasure() )
		VTLError.TypeError( functionName, " can be applied only to mono-measure datasets");

	for ( DatasetComponent m1 : q1.measures ) {
		if ( ! checkTypeOperand ( typeOperand, m1.compType ) )
			errorTypeOperand ( functionName, m1.compName, typeOperand , m1.compType ) ;
		if ( functionName.compareTo( "count") == 0 ) {
			q1.removeMeasures();
			q1.addMeasure("int_var", "integer", "count(*)");
			break ;
		}
		else {
			m1.sql_expr = functionName + "(" + m1.sql_expr ;
			for ( Query q : arguments )
				m1.sql_expr = m1.sql_expr + "," + q.getFirstMeasure().sql_expr ;
			m1.sql_expr = m1.sql_expr + ")" + postfixSyntax ;
		}
		// TBD: in some cases the type of the measure is the type of the operand (ex: abs, mod)
		m1.compType = typeOperand.equals( "scalar") && typeResult.equals( "scalar" ) ? m1.compType : typeResult ;		// any?
		if ( typeResult.equals( "boolean" ) )
			m1.compName = "bool_var" ;
		switch ( functionName ) {
			case "length" : 
			case "match_characters" : 
			case "instr" : 
				if ( ! q1.isMonoMeasure() )
					VTLError.TypeError( functionName, " can be applied only to a mono-measure dataset");
				m1.compName = Query.scalarMeasureDefaultName( typeResult ) ;
		}
	}
	
	if ( typeResult.equals( "boolean") )
		q1.removeAttributes();
	else if ( applyAttributePropagationUnary )
		q1.propagateAttributesUnary();

	return ( q1 ) ;
}

/*
 * Returns SQL type of join.
 */
static String sqlJoinType ( String VTLTypeofJoin )
{
	switch ( VTLTypeofJoin ) {	
		case "inner_join" :	return ( " INNER JOIN " );
		case "left_join"  : return ( " LEFT OUTER JOIN " );
		case "full_join"  : return ( " FULL OUTER JOIN " );
		case "cross_join" : return ( " CROSS JOIN " );
		default : return ( "" ) ;
	}
}

/*
 * join this and q1: sql_from + ON condition, sql_where
 * "left_join", "full_join"		dimensions must be identical
 * 	"cross_join" 				no constraints
 * "inner_join" 				operands must have at least a common dimension
 * 								in the VTL manual: dimensions should be a subset of a dataset (not implemented)
 */
void join ( Query q2, ListString usingDims, String joinType ) throws VTLError
{
	StringBuffer		onCond = new StringBuffer ( ) ;
	DatasetComponent	dim2 ;
	boolean 			checkIdenticalDims = ( joinType.equals("left_join") || joinType.equals("full_join") ) ;

	for ( DatasetComponent dim1 : this.dims ) {			// join common dimensions
		if ( ( dim2 = q2.getDimension( dim1.compName ) ) != null ) {
			if ( usingDims == null || usingDims.contains( dim1.compName ) )
				onCond.append( onCond.length() == 0 ? " ON (" : " AND " ).append( dim1.sql_expr + "=" + dim2.sql_expr ) ;
		}
		else {
			if ( checkIdenticalDims )
				VTLError . TypeError ( joinType + ": the two datasets do not have the same dimensions"
						+ "\ndataset (1): " + this.stringDimensions ( ',' )
						+ "\ndataset (2): " + q2.stringDimensions ( ',' ) ) ;
		}
	}
	// add dimensions that are not in q1
	if ( ! checkIdenticalDims ) {
		for ( DatasetComponent dim : q2.dims ) {
			if ( this.getDimension( dim.compName ) == null )
				this.addDimension( dim.compName, dim.compType, dim.compWidth, dim.sql_expr, dim.dim_values) ;
		}		
		if ( joinType.equals("inner_join") && onCond.length() == 0)
			VTLError.TypeError( joinType + ": operands must have at least a common dimension" ) ;	
	}
	
	onCond.append( ")" ) ;

	//sql_from
	this.sqlFrom = this.sqlFrom + sqlJoinType ( joinType ) + q2.sqlFrom + onCond.toString() ;	
}


/*
 * inner join
{
  x2000 := aact_ali01 [ time = 2000 ]  ;
  x2001 := aact_ali01 [ time = 2001 ]  ;
  inner_join ( x2000, x2001 ) [ rename x2000#obs_value to v2000, x2001#obs_value to v2001; keep v2000, v2001 ]
}
{
  x2000 := aact_ali01 [ time = 2000 ]  ;
  x2001 := aact_ali01 [ time = 2001 ]  ;
  inner_join ( x2000, x2001 on geo ) [ rename itm_newa to itm2, x2000#obs_value to v2000, x2001#obs_value to v2001; keep v2000, v2001 ]
}
inner_join ( aact_ali01  , aact_ali02   on [ geo, itm_newa ] ) [ rename aact_ali01#obs_value to v1 ; 
	rename aact_ali02#obs_value to v2; keep v1,v2 ; rename aact_ali01#time to newtime ] [ pivot time, v1 ]
 NB: alias can be null when the method is called by inte_if
 */
void dimJoinInner ( Query q2 ) throws VTLError
{
	StringBuffer		onCond = new StringBuffer ( ) ;
	DatasetComponent	dim2 ;
	Query				q1 = this ; 
	
	for ( DatasetComponent dim1 : q1.dims ) {			// join common dimensions
		if ( ( dim2 = q2.getDimension( dim1.compName ) ) != null ) {
			onCond.append( onCond.length() == 0 ? " ON (" : " AND " ).append( dim1.sql_expr + "=" + dim2.sql_expr ) ;
		}
	}
	if ( onCond.length() == 0)
		VTLError.TypeError( "inner join: left and right operands must have at least a common dimension" ) ;	
	onCond.append( ")" ) ;
	// add dimensions that are not in q1
	for ( DatasetComponent dim : q2.dims ) {
		if ( q1.getDimension( dim.compName ) == null )
			q1.addDimension( dim.compName, dim.compType, dim.compWidth, dim.sql_expr, dim.dim_values) ;
	}
	
	//sql_from
	q1.sqlFrom = q1.sqlFrom + " INNER JOIN " + q2.sqlFrom + onCond.toString() ;	
	
	//sql_where
	if ( q2.sqlWhere != null)
		q1.addWherePart( q2.sqlWhere ) ;
	if ( q2.doFilter)
		q1.setDoFilter();
}

/*
 * Full outer join: condition to select the measures when the left key is empty.
 * Example: "geo IS NULL"
 */
String leftKeyEmpty ( Query q2) throws VTLError
{
	Query	q1 = this ;
	
	for ( DatasetComponent dim1 : q1.dims )
		if ( ! dim1.sql_expr.startsWith( "'" ) )
			return ( dim1.sql_expr + " IS NULL" ) ;
	
	VTLError.InternalError( "All dimensions have constant values");
	return ( null ) ;
}



/*
 * The dimensions of q1 are a subset of the dimensions of q2.
 */
void checkSubsetDimensions ( Query q2, String contextOperator ) throws VTLError
{
	boolean ret = true ;

	for ( DatasetComponent d : this.dims )
		if ( q2.getDimension(d.compName) == null ) {
			ret = false ;		
			break ;
		}

	if ( ret == false )
		VTLError . TypeError (contextOperator + ": the dimensions of dataset(1) must be all in dataset(2)"
			+ "\ndataset (1): " + this.stringDimensions ( ',' )
			+ "\ndataset (2): " + q2.stringDimensions ( ',' ) ) ;
}

/*
 * The two datasets must have the same dimensions.
 */
void checkIdenticalDimensions ( Query q2, String contextOperator ) throws VTLError
{
	boolean ret = true ;
	
	if ( this.dims.size() != q2.dims.size() )
		ret = false ;
	else
		for ( DatasetComponent d : this.dims )
			if ( q2.getDimension(d.compName) == null ) {
				ret = false ;		
				break ;
			}

	if ( ret == false )
		VTLError . TypeError (contextOperator + ": the two datasets do not have the same dimensions"
			+ "\ndataset (1): " + this.stringDimensions ( ',' )
			+ "\ndataset (2): " + q2.stringDimensions ( ',' ) ) ;
}

/*
 * The two datasets must have the same measures.
 * Only the name and number of dimensions/measure is checked. The type is not checked.
 */
public void checkIdenticalMeasures ( Query q2, String contextOperator ) throws VTLError
{
	Query	q1 = this ;
	boolean	error = false ;
			
	if ( q1.measures.size() != q2.measures.size() )
		error = true ;			
		
	for ( DatasetComponent m : q1.measures )
		if ( q2.getMeasure(m.compName) == null ) {
			error = true ;
			break ;
		}

	if ( error )
		VTLError . TypeError (contextOperator + ": the two datasets do not have the same measures"
				+ "\ndataset (1): " + this.stringMeasures ( )
				+ "\ndataset (2): " + q2.stringMeasures ( ) ) ;
}

/*
 * The two datasets must have the same attributes.
 * Only the name and number of dimensions/measure is checked. The type is not checked.
 */
public void checkIdenticalAttributes ( Query q2, String contextOperator ) throws VTLError
{
	Query	q1 = this ;
	boolean	error = false ;
			
	if ( q1.attributes.size() != q2.attributes.size() )
		error = true ;			
		
	for ( DatasetComponent m : q1.attributes )
		if ( q2.getAttribute (m.compName) == null ) {
			error = true ;
			break ;
		}

	if ( error )
		  VTLError.TypeError (contextOperator + ": the two datasets do not have the same attributes"
		+ "\ndataset (1): " + this.stringAttributes ( ',' )
		+ "\ndataset (2): " + q2.stringAttributes ( ',' ) ) ;
}

/*
 * Parenthesis ( ).
 */
public Query parenthesis ( ) throws VTLError
{
	Query q = this.copy();
	for ( DatasetComponent pro1 : q.measures )
		pro1.sql_expr = "(" + pro1.sql_expr + ")" ;
	if ( q.sqlWhere != null )
		q.sqlWhere = "(" + q.sqlWhere + ")" ;
	return ( q ) ;
}

/*
 * Create new Query, copy dimensions from this Query that are in listDimensions
 * listDimensions can be null
 */
public Query copyDimensions ( ListString listDimensions ) throws VTLError
{
	Query	q = new Query () ;
	for ( DatasetComponent d : this.dims ) {
		if ( listDimensions == null || listDimensions.contains( d.compName ) )
			q.addDimension (d.compName, d.compType, d.compWidth, d.sql_expr, d.dim_values);
	}
	q.sqlFrom = this.sqlFrom ;
	q.sqlWhere = this.sqlWhere ;
	return ( q ) ;
}

/*
 * creates a copy of this
 */
public Query copy ( ) throws VTLError
{
	Query	q = new Query () ;
	for ( DatasetComponent d : this.dims )
		q.addDimension (d.compName, d.compType, d.compWidth, d.sql_expr, d.dim_values);
	for ( DatasetComponent m : this.measures )
		q.addMeasure ( m.compName, m.compType, m.sql_expr, m.compWidth, m.canBeNull  );
	for ( DatasetComponent m : this.attributes )
		q.addAttribute ( m.compName, m.compType, m.sql_expr, m.compWidth, m.canBeNull, m.isViralAttribute  );
	q.sqlFrom = this.sqlFrom ;
	q.sqlWhere = this.sqlWhere ;
	q.referencedDatasets = new ListString ( this.referencedDatasets ) ;
	q.doFilter = this.doFilter ;
	return ( q ) ;
}

/*
 * creates a copy of this
 */
public Query refTo ( String object_name, String table_alias) throws VTLError
{
	Query	q = new Query () ;
	for ( DatasetComponent d : this.dims )
		q.addDimension (d.compName, d.compType, d.compWidth, table_alias + '.' + d.compName, d.dim_values);
	for ( DatasetComponent c : this.measures )
		q.addMeasure ( c.compName, c.compType, table_alias + '.' + c.compName, c.compWidth, c.canBeNull );
	for ( DatasetComponent c : this.attributes )
		q.addAttribute ( c.compName, c.compType, table_alias + '.' + c.compName, c.compWidth, c.canBeNull );
	q.sqlFrom = object_name + " " + table_alias ;
	q.referencedDatasets = new ListString ( this.referencedDatasets ) ;
	return ( q ) ;
}

/*
 * and, or, xor Expression
 */
static String invert ( String op ) throws VTLError
{
	switch ( op ) {
		case "=" : return ( "<>" ) ;
		case "<>" : return ( "=" ) ;
		case ">" : return ( "<=" ) ;
		case ">=" : return ( "<" ) ;
		case "<" : return ( ">=" ) ;
		case "<=" : return ( ">" ) ;
		default : VTLError.InternalError( "invert binary expr");
		return ( null ) ;
	}
}

/*
 * and, or, xor Expression
 */
static String booleanBinaryExpr ( String expr1, String op, String expr2 ) throws VTLError
{
	String res ;
	
	switch ( op ) {
		case "and" :
			res = " CASE " + expr1 + " WHEN 'false' THEN 'false' WHEN 'true' THEN " + expr2 + " END " ;
			break ;
		case "or" :
			res = " CASE " + expr1 + " WHEN 'true' THEN 'true' WHEN 'false' THEN " + expr2 + " END " ;
			break ;
		case "xor" :
			op = "<>" ;		// no break
		default :
			res = " CASE WHEN " + expr1 + op + expr2 + " THEN 'true' WHEN " 
						+ expr1 + invert ( op ) + expr2 + " THEN 'false' END " ;
	}
	return ( res ) ;
}

/*
 * Check if object in query can be updated.
 */
public void checkObjectForUpdate ( ) throws VTLError
{
	Db.checkConnectionNotReadOnly ( ) ;	
}

/*
 * Check if dataset can be updated.
 */
static void checkDatasetForUpdate ( String dsName ) throws VTLError
{
	Db.checkConnectionNotReadOnly ( ) ;	
}

/*
	Convert Query structure to Table structure.
*/
public Dataset convert2ds ( int objectId, String dsName, char object_type ) throws VTLError
{
	Dataset		ds ;
	Query		q = this.copy() ;
 
	ds = new Dataset ( dsName, object_type ) ;
	ds.setMeasures(q.measures) ;
	ds.setAttributes(q.attributes) ;
	ds.setDimensions( q.dims );
	ds.objectId = objectId ;
	
	return ( ds ) ;
}

/*
 * Set list of values of a dimension in the query (if list of values is empty). Used in pivot, hierarchy, fill_time_series.
 * throw exception if expression is based on more than 1 dataset.
	(na_main + na_main2)[ pivot time, obs_value ]
 */
void setValues ( DatasetComponent dim ) throws VTLError
{
	ListString 	ls ;
	
	// to test: dim.dim_values = new ListString() ;
	
	if ( dim.dim_values.size() > 0 )
		return ;
	
	if ( this.referencedDatasets.size() > 1 ) {
		// VTLError.RunTimeError( "Get list of values: expression is based on more than 1 dataset for dimension: " + dim.compName );
		String sqlQuery = this.build_sql_query( true, true ) ;
		ls = Db.sqlFillArray("SELECT DISTINCT " + dim.compName + " FROM (" + sqlQuery + ") ORDER BY 1") ;
		if ( ls.size() == 0 )	// empty dataset
			return ;
		dim.dim_values = ls ;
	}
	
	if ( this.referencedDatasets.size() == 0 )
		VTLError.InternalError( "Get list of values: no referenced datasets found" ) ;

	Dataset				ds = Dataset.getDatasetDesc ( this.referencedDatasets.firstElement() ) ;
	DatasetComponent	dim2 ;
	
	if ( ( dim2 = ds.getDimension( dim.compName ) ) == null )
		VTLError.InternalError( "Get list of values: component not found in referenced dataset" ) ;
	
	if ( dim2.dim_values.size() > 0 )
		dim.dim_values =  dim2.dim_values ;
	
	ls = Db.sqlFillArray("SELECT DISTINCT " + dim.compName + " FROM " + ds.sqlTableName + " ORDER BY 1") ;

	if ( Check.isValueDomainType( dim.compType ) ) {
		ListString ls2 = Dataset.getValuedomainCodeList( dim.compType ) ;
		if ( ls2 != null )
			ls = ls.sort_by_list( ls2 ) ;
	}
}

/*
 * Add to q.sql_where the current filters for dimensions contained in q descriptor.
 * Change list of dimension values.
 */
void filterQuery ( boolean filterDimValues ) throws VTLError
{
	RangeVariable	item ;

	for ( DatasetComponent	dim : this.dims ) {
	     item = RangeVariable.getFilter ( dim.compName ) ;
	     if ( item != null ) {
	    	 if ( filterDimValues )
		   		 dim.dim_values = dim.dim_values.join ( item.dimValues ) ;
	    	 this.addWherePart ( item.dimValues.sqlSyntaxInList( dim.sql_expr, "IN" ) ) ;
	    	 this.setDoFilter();
	     }
	}
}

/*
 * Add the current filters for dimensions contained in the left part of the update.
 */
void filterqueryForUpdate ( int dsObjectId, Query qExpr ) throws VTLError
{
	String				dim_name ;
	DatasetComponent	dsDim ;
	RangeVariable		item ;
	ListString			queryDimValues, dsDimvalues, resDimValues ;
	Query 				qLeft = this; 
  
	int dim_index = 1 ;				// index of the the first dimension in the meta base
	for ( DatasetComponent dim : qExpr.dims) {
		dim_name = dim.compName ;
		dsDim = qLeft.getDimension ( dim_name ) ;
		queryDimValues = dim.dim_values ;
		dsDimvalues = dsDim.dim_values ;
	
		item = RangeVariable.getFilter ( dim_name ) ;

		if ( dsDimvalues.size () == 0 ) {				// no positions defined in ds, take condition from filter  
			resDimValues = ( item == null ? null : item.dimValues.intersect ( queryDimValues, true ) ) ;
		}
		else if ( queryDimValues.size () == 0  ) {		// no positions defined in query
			resDimValues = ( item == null ? dsDimvalues : item.dimValues.intersect ( dsDimvalues, true ) ) ;
		}		
		else {
			if ( item == null ) {
				if ( dsDimvalues == queryDimValues )			// they denote the same vector
					resDimValues = null ;
				else if ( dsDimvalues.containsAll( queryDimValues ) )
					resDimValues = null ;
				else
					resDimValues = dsDimvalues.join( queryDimValues ) ;    		  
			}
			else {
				// build tmp array from intersection between filter and positions from (in) or complement (not in)
				resDimValues = item.dimValues.intersect ( dsDimvalues, true );
				resDimValues = resDimValues.join( queryDimValues );
			}
		}

		if ( resDimValues != null ) {
			if ( item == null && resDimValues.size ( ) > 1000 ) {
				qExpr.addWherePart ( dim.sql_expr + " IN (SELECT pos_code FROM " + Db.mdt_positions 
    				  		+ " WHERE object_id=" + dsObjectId + " AND dim_index=" + ( dim_index++ ) + ")" ) ;
			}
			else {
				dim.dim_values = resDimValues ;
				qExpr.addWherePart (  resDimValues.sqlSyntaxInList ( dim.sql_expr, "IN" ) ) ;		    		  
			}
		}
	}
}

/*
* If this query is a simple table with no where condition then return the sql_from otherwise a subquery
*/
public String getFromSubquery ( ) throws VTLError
{
	if ( this.sqlFrom.startsWith( "(" ) || ( this.sqlWhere != null ))
		return ( "(" + this.build_sql_query( true ) + ")" ) ;
	return ( sqlFrom ) ;
}


/*
 * Get dimension of type time_period.
 * TBD: check that query does not contain a second dimension with type time_period
 */
public DatasetComponent getTimePeriodDimension ( ) throws VTLError
{
	for ( DatasetComponent dim : this.dims )
		if ( Check.isPeriodDataType ( dim.compType ) )
			return( dim ) ;
	
	return ( null ) ;
}

/*
	period_indicator  ( { op } )
	
	can be used on dataset and components or scalar
	period_indicator  ( na_main )
	na_main [ filter period_indicator () = "A" ]	
	na_main [ filter period_indicator (time) = "A" ]	
	period_indicator ( cast ( "2000Q1", time_period ) )
 */
public Query periodIndicator ( ) throws VTLError
{
	DatasetComponent	timeDim ;
	String				sqlExpr ;
	Query				q1 = this.copy() ;
		
	if ( this.dims.size() == 0 && this.measures.size() == 1 ) {
		sqlExpr = this.getFirstMeasureSqlExpr() ;
		if ( ! this.getFirstMeasure().compType.equals( "time_period"))
			VTLError.TypeError ( "time_period: dataset has no dimension whose type is time_period" ) ;
	}
	else {
		timeDim = q1.getMandatoryTimeDimension ( "period_indicator" ) ;
		sqlExpr = timeDim.sql_expr ;
	}
		
	sqlExpr = "CASE length(" + sqlExpr + ") WHEN 4 THEN 'A' WHEN 10 THEN 'D' ELSE " 
				+ "substr(" + sqlExpr + ",5,1)" + " END " ;
	
	q1.removeMeasures();
	q1.removeAttributes();
	q1.addMeasure( "duration_var", "duration", sqlExpr );
	return ( q1 ) ;
}

/*
 * attempt ro raise error in SELECT statement:
WITH
  FUNCTION f ( ) RETURN integer IS 
  BEGIN
    raise_application_error (1, 'ee' )  ;
    RETURN ( 0 ) ;
  END;
SELECT * FROM dual
 */

/*
	cast ( "1000", integer ) 
	from: number, boolean, string with or without mask
*/
static void castToInteger ( DatasetComponent m, String format ) throws VTLError
{
	if ( format != null )
		VTLError.RunTimeError( "cast to integer: format not allowed" );

	switch ( m.compType ) {
		case "integer" : 		// nothing
			break ;
		case "number" : 
			m.sql_expr = "TRUNC(" + m.sql_expr + ")" ;
			break ;
		case "boolean" :  
			m.sql_expr = "CASE WHEN " + m.sql_expr + "='false' THEN 0 ELSE 1 END " ; 
			break ;
		case "string" :
			m.sql_expr = "TRUNC(" + m.sql_expr + ")" ;
			break ;
		default :
			VTLError.TypeError( "cast to integer, type of the operand must be: integer, number, boolean or string");
	}
	m.compType = "integer" ;
	m.compName = "int_var" ;		
}

/*
	cast ( "1.2", number, "DD.D" )
	from: number, boolean, string with or without mask
*/
static void castToNumber ( DatasetComponent m, String format ) throws VTLError
{
	switch ( m.compType ) {
		case "integer" : 		// nothing
			break ;
		case "number" : 		// nothing
			break ;
		case "boolean" :  
			if ( format != null )
				VTLError.RunTimeError( "cast boolean to integer/number: format not allowed" );
			m.sql_expr = "CASE WHEN " + m.sql_expr + "='false' THEN 0 ELSE 1 END " ; 
			break ;
		case "string" :
			if ( format == null )
				VTLError.RunTimeError( "cast string to number: format requested" );
			format = "FM" + format.replaceAll( "D", "9")  ;
			m.sql_expr = "TO_NUMBER(" + m.sql_expr + ",'" + format + "')" ;					
			break ;
		default :
			VTLError.TypeError( "cast to number, type of the operand must be: integer, number, boolean or string");
	}
	m.compType = "number" ;
	m.compName = "num_var" ;				
}

/*
	cast ( 0, boolean )
*/
static void castToBoolean ( DatasetComponent m, String format ) throws VTLError
{
	if ( format != null )
		VTLError.RunTimeError( "cast to boolean: format not allowed" );

	switch ( m.compType ) {
		case "number" : 
		case "integer" :
			m.sql_expr = "CASE WHEN " + m.sql_expr + "=0 then 'false' ELSE 'true' END " ;					
			break ;
		case "boolean" :	// nothing
			break;	
		default : VTLError.TypeError( "cast to boolean, type of the operand must be: integer, number or boolean");
	}
	m.compType = "boolean" ;
	m.compName = "bool_var" ;
}

/*
	cast ( "A", duration )
*/
static void castToDuration ( DatasetComponent m, String format ) throws VTLError
{
	switch ( m.compType ) {
		case "string" : 
			// TBD which mask?
			m.sql_expr = "CASE WHEN INSTR(" + m.sql_expr + ",'ASQMWD') > 0 THEN " + m.sql_expr + " ELSE NULL END " ;
			break ;
		case "duration" : 	// nothing
			break ;
		default : VTLError.TypeError( "cast to duration, type of the operand must be: string or duration");
	}
	m.compType = "duration" ;
	m.compName = "duration_var" ;
}

/*
	cast ( "20000727", date, "YYYYMMDD" ) 
	cast ( na_main#embargo_date, date, "YYYYMMDD" ) 
 */
static void castToDate ( DatasetComponent m, String format ) throws VTLError
{
	if ( format == null )
		VTLError.RunTimeError( "cast to date: format not specified" );
	
	switch ( m.compType ) {
		case "time" : 
		case "time_period" : 
			VTLError.InternalError( "cast to date: cast from time_period not yet implemented");
		case "string" :
			m.sql_expr = "TO_DATE(" + m.sql_expr + "," + format + ")" ;		
			break ;
		case "date" :		// nothing
			break ;
		default :
			VTLError.TypeError( "cast to date, type of the operand must be: time, time_period, date or string");
	}

	m.compType = "date" ;
	m.compName = "date_var" ;
}

/*
	cast ( "20000727", string, "YYYYMMDD" ) 
*/
static void castToString ( DatasetComponent m, String format ) throws VTLError
{

	switch ( m.compType ) {
		case "time" : 
		case "time_period" :	// nothing to do 
			break ;
		case "date" :
			if ( format == null )
				VTLError.RunTimeError( "cast to string: format not specified" );
			m.sql_expr = "TO_CHAR(" + m.sql_expr + "," + format + ")" ;		
			break ;
		case "duration" :
			// no changes
			break ;
		default :
			VTLError.TypeError( "cast to date, type of the operand must be: time, time_period, date or duration");
	}

	m.compType = "string" ;
	m.compName = "string_var" ;
}

/*
 * cast ( "2000Q1", time_period, "YYYY\QQ" ) 
 * TBD: format e.g. 
 */
static void castToTimePeriod ( DatasetComponent m, String format ) throws VTLError
{	
	switch ( m.compType ) {
		case "time" : 
		case "time_period" : 		// nothing to do
			break ;
		case "date" :
			if ( format != null )
				VTLError.RunTimeError( "cast to time_period: format not allowed" );
			m.sql_expr = "TO_CHAR(" + m.sql_expr + ",'YYYY')" ;		// return only the year
			break ;
		case "string" :
			// cast ( "2000M01", "YYYYMM" ) cast ( "2000M04", "YYYY\MMM" )
			if ( format == null )
				VTLError.RunTimeError( "cast to time_period: format requested" );
			int	idx ;
			if ( ( idx = format.indexOf( "\\" ) ) >= 0 ) {
				format = format.replaceFirst(format.substring( idx, idx + 2), "") ;
			}
			if ( ( idx = format.indexOf( "YYYY" ) ) < 0 ) 
				VTLError.RunTimeError( "cast to time_period: format does not contain YYYY" );
			String	sqlExpr = "SUBSTR(" + m.sql_expr + "," + (idx+1) + ",4)" ;
			String	subP[] = { "MM", "QQ", "SS", "Q", "S", "WW" } ;
			for ( String s : subP ) {
				if ( (idx = format.indexOf( s )) >= 0 ) {
					sqlExpr = sqlExpr + "+'" + s.charAt(0)+ "'+SUBSTR(" + m.sql_expr + "," + (idx+1) + "," + s.length() + ")" ;
					break ;
				}				
			}
			m.sql_expr = sqlExpr ;
			break ;
		default :
			VTLError.TypeError( "cast to time_period, type of the operand must be: string, date, time or time_period");
	}

	m.compType = "time_period" ;
	m.compName = "time_period_var" ;

	/*	
 * sql " select * from dual where  regexp_like ( '2001A', '^\d{4}A$' )  " ;
	String				sqlExpr = null, sqlPattern = null, period, freq ;
	if ( format.length() < 4 || ! format.startsWith( "YYYY") )
		VTLError.TypeError( "cast to time_period, bad format: " + format );
	
	if ( format.startsWith( "yyyy") ) {
		period = format.substring( 4 ) ;
		char c1, c2 ;
		switch ( period.length() ) {
			case 0 :													// annual
				sqlPattern = "'^\\d{4}$'" ;
				sqlExpr = d.sql_expr ;
				break ;
			case 1 :													// annual, s, q
				c1 = period.charAt(0) ;
				if ( c1 == 's' || c1 == 'q' ) {
					sqlPattern = "'^\\d{5}$'" ;
					freq = period.toUpperCase() ;
					sqlExpr = "substr(" + d.sql_expr +",1,4)||'" + freq + "'||substr(" + d.sql_expr + ",5,1)";					
				}
				else {
					sqlPattern = "'^\\d{4}" + c1 + "$'" ;
					sqlExpr = "substr(" + d.sql_expr +",1,4)";					
				}
				break ;
			case 2 :													// mm, Xs, Xq
				c1 = period.charAt(0) ;
				c2 = period.charAt(1) ;
				if ( c2 == 's' || c2 == 'q' ) {
					sqlPattern = "'^\\d{4}" + c1 + "\\d$'" ;
					freq = period.substring( 1 ).toUpperCase() ;
					sqlExpr = "substr(" + d.sql_expr +",1,4)||'" + freq + "'||substr(" + d.sql_expr + ",5,1)";					
				}
				else if ( period.equals( "mm" ) || period.equals( "ss" ) || period.equals( "qq" ) ) {
					sqlPattern = "'^\\d{6}$'" ;
					freq = period.substring( 1 ).toUpperCase() ;
					sqlExpr = "substr(" + d.sql_expr +",1,4)||'" + freq + "'||substr(" + d.sql_expr + ",5,2)";					
				}
				break ;
			case 3 :													// Xmm, Xss, Xqq
				c1 = period.charAt(0) ;
				String sub = period.substring ( 1 ) ;
				if ( sub.equals( "mm" ) || sub.equals( "ss" ) || sub.equals( "qq" ) ) {
					sqlPattern = "'^\\d{4}" + c1 + "\\d$'" ;
					freq = sub.substring( 1 ).toUpperCase() ;
					sqlExpr = "substr(" + d.sql_expr +",1,4)||'" + freq + "'||substr(" + d.sql_expr + ",6,2)";					
				}
				break ;			
			default :
				sqlPattern = null ;			
				sqlExpr = null ;
		}
	}
	
	if ( sqlExpr == null )
		VTLError.RunTimeError( "cast to time_period: format not yet implemented: " + format );
	
	d.sql_expr = sqlExpr ;
	q1.add_where_part( "regexp_like(" + d.sql_expr + "," + sqlPattern + ")" );
		
	return ( q1 );
	*/
}

/*
 * Fill in the list of time periods.
 * timeValues.size() > 0.
 */
static ListString fillTimePeriods ( ListString timeValues ) throws VTLError 
{
	String		min = "9999", max = "0000" ;
	char		refFreq ;
	
	if ( timeValues.size() == 0 )
		return ( new ListString()) ;
	
	refFreq = Check.getFrequency( timeValues.get(0) ) ;
	
	for ( String timeValue : timeValues ) {
		char cFreq = Check.getFrequency( timeValue ) ;
		if ( cFreq != refFreq )
			VTLError.TypeError( "fill_time_series, time period has different periods:"
									+ cFreq + "," + refFreq );	
		if ( min.compareTo( timeValue ) > 0 )
			min = timeValue ;
		if ( max.compareTo( timeValue ) < 0 )
			max = timeValue ;
	}

	return ( Check.buildTimePeriodInterval ( min, max ) ) ;
/*	switch ( freq ) {
		case "A" :
			int minTime = Integer.parseInt ( min ) ;
			int maxTime = Integer.parseInt ( max ) ;
			for ( int x = minTime; x <= maxTime; x ++ )
				ls.add ( x + "" ) ;
			break ;
		default:
			return ( timeValues ) ;
	}
	return ( ls ) ;*/
}

/*
	fill_time_series ( op { , limitsMethod } )
	limitsMethod ::= all | single

	maybe all can accept different frequencies
	fill_time_series ( na_main )
	fill_time_series ( na_main, single )
	fill_time_series ( op, all )
 */
public Query fillTimeSeries ( String limitMethod ) throws VTLError
{
	StringBuffer		withPeriods = new StringBuffer () ;
	StringBuffer		leftJoin = new StringBuffer () ;
	String				queryMinMaxPeriods ;
	String				timeDimName ;
	DatasetComponent	timeDim ;
	Query				q1 = this.copy() ;
	ListString			timeValues ;
	
	timeDim = q1.getMandatoryTimeDimension( "fill_time_series" ) ;
	timeDimName = timeDim.compName ;
	
	// TBD: build the full list of time periods of the given frequency 
	q1.setValues(timeDim) ;
	timeValues = timeDim.dim_values ;
	if ( timeValues.size() == 0 )		// empty dataset
		return ( this ) ;
	
	timeValues = fillTimePeriods ( timeValues ) ;

	for ( String v : timeValues ) {
		withPeriods.append( ( withPeriods.length() == 0 ? "(" : " UNION " ) 
								+ "SELECT '" + v + "' AS " + timeDimName + " FROM DUAL" ) ;
	}		
	withPeriods.append ( ")" ) ;				// sqlQuery = this.build_sql_query( true ) ;
	
	queryMinMaxPeriods = q1.stringDimensionsWithout( timeDimName ) ;
	
	queryMinMaxPeriods = "SELECT " + queryMinMaxPeriods + ",MIN(" + timeDimName + 
			") AS tmin$,MAX(" + timeDimName + ") AS tmax$" 
			+ " FROM " + q1.getFromSubquery () + " GROUP BY " + q1.stringDimensionsWithout( timeDimName ) ; ;
	
	for ( DatasetComponent dim : q1.dims ) {
		if ( leftJoin.length() == 0 )
			leftJoin.append( "LEFT JOIN (" + q1.build_sql_query( true )+ ") b$" ).append( " ON (" ) ;		
		else
			leftJoin.append( " AND " ) ;
		leftJoin.append( "a$." + dim.compName + "=b$." + dim.compName ) ;
	}
	leftJoin.append( ")" ) ;
	StringBuffer all = new StringBuffer (); 
	for ( DatasetComponent dim : q1.dims ) {
		if ( all.length() > 0 )
			all.append( ',' ) ;
		all.append( "a$." + dim.compName) ;
		dim.sql_expr = "c$." + dim.compName ;
	}
	for ( DatasetComponent c : q1.measures ) {
		all.append( ',' ).append( "b$." + c.compName ) ;
		c.sql_expr = "c$." + c.compName ;
	}
	for ( DatasetComponent c : q1.attributes ) {
		all.append( ',' ).append( "b$." + c.compName ) ;
		c.sql_expr = "c$." + c.compName ;
	}

	// take dims from a$
	// take prop from b$ 
	q1.sqlFrom = "(" //+ withPeriods 
		+ "SELECT " + all.toString() + " FROM (SELECT * FROM (" + queryMinMaxPeriods + ") CROSS JOIN " + withPeriods // T$1" 
		+ " WHERE " + timeDimName + " BETWEEN tmin$ AND tmax$) a$ " + leftJoin + ") c$" ;
	q1.sqlWhere = null ;

	return ( q1 );
}

/*
	flow_to_stock ( op )

	flow_to_stock ( na_main)
*/
public Query flowToStock ( ) throws VTLError
{
	Query				q1 = this.copy();
	DatasetComponent	timeDim ;
	String				sqlQuery, measure, dimsWithoutTime ;
	
	timeDim = q1.getMandatoryTimeDimension ( "flowToStock" ) ;
	
	measure = q1.getFirstMeasureSqlExpr() ;
	dimsWithoutTime = stringDimensionsWithout ( timeDim.compName ) ;
	q1.removeAttributes();
	sqlQuery = "SELECT " + q1.dimensionsSqlColumns() + "," 
			+ "SUM(" + measure + ") OVER ( PARTITION BY " + dimsWithoutTime 
			+ " ORDER BY " + timeDim.compName 
			+ " ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS " + q1.getFirstMeasure().compName
			+ " FROM " + q1.sqlFrom ;
	
	return(q1.flatQuery(false, sqlQuery)) ;
}

/*
	stock_to_flow ( op )
	
	stock_to_flow ( na_main )
*/
public Query stockToFlow ( ) throws VTLError
{
	Query				q1 = this.copy();
	DatasetComponent	timeDim ;
	String				sqlQuery, measure, dimsWithoutTime ;
	
	timeDim = q1.getMandatoryTimeDimension ( "stockToFlow" ) ;
	
	measure = q1.getFirstMeasureSqlExpr() ;
	dimsWithoutTime = stringDimensionsWithout ( timeDim.compName ) ;
	q1.removeAttributes();
	sqlQuery = "SELECT " + q1.dimensionsSqlColumns() +
			"," + measure + " - LAG (" + measure + ",1,0) OVER ( PARTITION BY " + dimsWithoutTime 
			+ " ORDER BY " + timeDim.compName 
			+ ") AS " + q1.getFirstMeasure().compName
			+ " FROM " + q1.sqlFrom ;
	
	return(q1.flatQuery(false, sqlQuery)) ;
}

static final String		frequencyCombinations[] = { "QA", "MA", "SA", "MQ" } ;
static final int		frequencyMultipliers [] = { 4, 12, 2, 3 } ;	

/*
	time_agg ( op )
	
	time_agg ( ds_bop )
*/
public Query timeAgg ( String str ) throws VTLError
{
	VTLError.InternalError( "time_agg: not yet implemented" );
	return ( null ) ;
}
	
/*
 * offset is a string containing an integer
 * timeshift  ( op , shiftNumber )
 * TBD: format YYYY-MM-DDTHH24:MI:SS
	timeshift ( "2000M01", 2 )
	timeshift ( "2000Q1", 2 )
	timeshift ( "2000-01-01", 10 )
	timeshift ( na_main, 1)
*/
public Query timeshift ( String offset ) throws VTLError
{
	Query				q1 = this.copy() ;
	DatasetComponent	timeDim = q1.getMandatoryTimeDimension ( "timeshift" ) ;	
	String 				t = timeDim.sql_expr ;
	String				ns = "(CASE SUBSTR(" + t + ",5,1) WHEN 'M' THEN 12 WHEN 'Q' THEN 4 WHEN 'S' THEN 2 WHEN 'W' THEN 52 END)" ;
	ListString			timeValues ;
	
	timeDim.sql_expr = 
		"CASE length(" + t + ")"
/* annual */	+ "WHEN 4 THEN TO_CHAR(SUBSTR(" + t + ",1,4)+" + offset + ")"
/* daily */		+ "WHEN 10 THEN TO_CHAR(TO_DATE(SUBSTR(" + t + ",1,4)||SUBSTR(" + t + ",6,2)||SUBSTR(" + t + ",9,2)"+ ",'YYYYMMDD')+" + offset +",'YYYY-MM-DD')" 
				+ "ELSE TRUNC((SUBSTR(" + t + ",1,4)*" + ns + "+SUBSTR(" + t + ",6)+" + offset + "-1)/" + ns + ")"
					+ " || SUBSTR(" + t + ",5,1) "
					+ " || TO_CHAR(mod ( ((SUBSTR(" + t + ",1,4)*12)+SUBSTR(" + t + ",6)+" + offset + "-1)," + ns + ")+1,CASE WHEN SUBSTR(" + t + ",5,1)='M' THEN 'FM09' ELSE 'FM9' END)"
		+ " END " ;
	
	if ( ( timeValues = timeDim.dim_values).size() > 0 ) {
		ListString	shiftedTimeValues = new ListString ();
		int			n = Integer.parseInt( offset ) ;
		for ( String v : timeValues )
			shiftedTimeValues.add( Check.buildTimePeriodOffset( v, n ) ) ;
		timeDim.dim_values = shiftedTimeValues ;
	}
	return ( q1 ) ;
}

/*
 * return the sql_expr of some dimensions of this, in the order of someDims.
 */
ListString listDimensionsSqlExpr ( ListString someDims )
{
	ListString			ls = new ListString ();

	for ( String dimName : someDims ) {			// keep the order of someDims
		DatasetComponent d ;
		if ( ( d = this.getDimension(dimName) ) != null )
			ls.add( d.sql_expr ) ;
	}

	return ( ls ) ;
}

/*
 * Return the sql_expr of the dimensions of this.
 */
ListString listDimensionsSqlExpr ( )
{
	ListString	ls = new ListString ();

	for ( DatasetComponent d : this.dims )
			ls.add( d.sql_expr ) ;

	return ( ls ) ;
}

/*
 * Change alias of components of q2.
 */
void reAlias ( Query q2, String alias, boolean onlyDimensions )
{
	for ( DatasetComponent d : q2.dims ) 
		this.addDimension(d.compName, d.compType, d.compWidth, alias + "." + d.compName, d.dim_values) ;
	if ( onlyDimensions )
		return ;
	for ( DatasetComponent c : q2.measures ) 
		this.addMeasure( c.compName, c.compType, alias + "." + c.compName, c.compWidth, c.canBeNull );
	for ( DatasetComponent c : q2.attributes ) 
		this.addMeasure( c.compName, c.compType, alias + "." + c.compName, c.compWidth, c.canBeNull );
}

/*
 * Create a copy of this with the components in the same order as defined in q2
 */
public Query copyOrderComponents ( Query q2 ) throws VTLError
{
	Query	q = new Query () ;
	
	for ( DatasetComponent d : q2.dims )
		q.dims.add( this.getDimension( d.compName) ) ;
	for ( DatasetComponent c : q2.measures )
		q.measures.add( this.getMeasure( c.compName) ) ;	
	for ( DatasetComponent c : q2.attributes )
		q.attributes.add( this.getAttribute( c.compName) ) ;
	q.sqlFrom = this.sqlFrom ;
	q.sqlWhere = this.sqlWhere ;
	q.referencedDatasets = this.referencedDatasets ;
	return ( q ) ;
}

/*
	exists_in ( op1, op2 { , retain } )
	retain ::= true | false | all
	
	op2 has all the identifier components of op1 or op1 has all the identifier components of op2
	
	exists_in ( na_main , derog_na_main, true )
 * at least 1 dimension in common
  	exists_in ( aact_ali02 , aact_ali01, all ) 
	exists_in ( aact_ali01 [ geo =IT, itm_newa=40000], aact_ali01 [ geo=FR, itm_newa=40000] )
 	setdiff ( aact_ali02 [ filter time = 2000 ] , aact_ali01 [ filter time = 2000 ; filter geo <> "IT"]  ) 
 	exists_in ( aact_ali02 [ filter time = 2000 ], aact_ali01 [ filter time = 2000 ; filter geo <> "IT"] )
 	exists_in ( aact_ali02 [ filter time = 2000 ], aact_ali01 [ filter time = 2000 ; filter geo <> "IT"] ) [ filter not bool_var  ]
 */
public Query exists_in ( Query q2, String retain ) throws VTLError
{
	Query		q1 = this.copy() ;
	ListString	commonDims ;
	boolean		in = true ;
	
	q1.removeMeasures();
	q1.removeAttributes();
	q1.addReferencedDatasets( q2 );

	switch ( retain ) {
		case "all" : 
			StringBuffer	onCond = new StringBuffer ( ) ;
			q2 = q2.flatQuery( true ) ;		// includes also sql_where
			for ( DatasetComponent dim2 : q2.dims ) {
				DatasetComponent dim1 ;
				if ( (dim1 = q1.getDimension(dim2.compName) ) == null )
					VTLError.TypeError( "exists_in: dimension " + dim2.compName + " of the right operand is not in the left operand" ) ;						
				onCond.append( onCond.length() == 0 ? " ON (" : " AND " ).append( dim1.sql_expr + "=" + dim2.sql_expr ) ;
			}
			onCond.append( ")" ) ;
			q1.sqlFrom = q1.sqlFrom + " LEFT OUTER JOIN " + q2.sqlFrom + onCond.toString() ;	
			q1.sqlWhere = q2.dims.firstElement().sql_expr + " IS NOT NULL" ;
			q1.setDoFilter();
			q1.addMeasure("bool_var", "boolean", "CASE WHEN " + q1.sqlWhere + " THEN 'true' ELSE 'false' END" ) ;
			return ( q1 ) ;
		case "true" : 
			in = true ; 
			break ; 
		case "false" : 
			in = false ; 
			break ;
		default : 
			VTLError.InternalError( "exists_in" );
	}

	commonDims = q1.listCommonDimensions(q2) ;
	if ( commonDims.size() == 0 )
		VTLError.TypeError( "exists_in: found no common dimensions");
	
	q1.sqlWhere = "(" + q1.listDimensionsSqlExpr(commonDims).toString( ',' ) + ")"
			+ ( in ? " IN " : " NOT IN " ) 
			+ "(" + q2.sqlQuerySomeDims( commonDims ) + ")" ;
	q1.setDoFilter();
	q1.addMeasure("bool_var", "boolean", ( in ? "'true'" : "'false'" ) ) ;			// return true or false
	
	return ( q1 ) ; 
}

/*
 * setdiff ( ds1, ds2 )
 * The operand Data Sets have the same Identifier, Measure and Attribute Components.

	setdiff ( na_main [ filter obs_value > 100 ], na_main [ filter obs_value > 100 ] )
  	setdiff ( fill_time_series ( aact_ali01 [ filter time<>2016 ] , "A" ) , aact_ali01 [ filter time<>2016] ) 
 */
public Query setdiff ( Query q2 ) throws VTLError
{
	Query		q1 = this.copy() ;
	
	q1.checkIdenticalAllComponents ( q2, "setdiff" ) ;
	
	q1.addWherePart( "(" + q1.dimensionsSqlColumns() + ") NOT IN ("
			+ q2.sqlQuerySomeDims( q1.listDimensions ( ) ) + ")" ) ;
	
	q1.setDoFilter();
	q1.addReferencedDatasets( q2 );

	return ( q1 ) ;
}

/*
 * symdiff ( ds1, ds2 )
 *
 * Implements the symmetric difference. The operand Data Sets have the same components

	symdiff ( na_main [sub ref_area = "DK" ], na_main [sub ref_area = "DK" ] ) ;
 */
public Query symdiff ( Query q2 ) throws VTLError
{
	Query	q1, q1_minus_q2, q2_minus_q1 ;
	q1 = this.copyOrderComponents( q2 ) ;
	q1.checkIdenticalAllComponents ( q2, "symdiff" ) ;	
	q1_minus_q2 = q1.setdiff( q2 ) ;
	q2_minus_q1 = q2.setdiff( q1 ) ;
	
	q1.pushQuery ( q1_minus_q2.build_sql_query ( false ) + " UNION ALL " + q2_minus_q1.build_sql_query ( false ) ) ;
	
	q1.setDoFilter();
	q1.addReferencedDatasets( q2 );

	return ( q1 ) ;
}


/*
 * intersect ( dsList )
 * dsList ::= ds { , ds }*
 * The operand Data Sets can contain Data Points having the same values of the Identifiers. To avoid 
 * duplications of Data Points in the resulting Data Set, those Data Points are filtered by chosing 
 * the Data Point belonging to the left most operand Data Set. 

	intersect ( na_main [filter obs_value > 10 ], na_main [filter obs_value < 15 ] ) ;
 */
public static Query intersect ( Vector<Query> queries ) throws VTLError
{
	Query		q = null ;
	
	for ( Query q2 : queries ) {
		if ( q == null ) {
			q = q2.copy() ;					// first query
		}
		else {
			// check dimensions, measures and attributes
			q.checkIdenticalAllComponents ( q2, "intersect" ) ;	
			q.addWherePart( "(" + q.dimensionsSqlColumns() + ") IN ("
					+ q2.sqlQuerySomeDims( q.listDimensions ( ) ) + ")" ) ;
			q.setDoFilter();
			q.pushQuery(q.build_sql_query(false));
		}
	}
	return ( q ) ;
}

/*
 * union ( dsList )
 * dsList ::= ds { , ds }*
 * The operand Data Sets can contain Data Points having the same values of the Identifiers. To avoid 
 * duplications of Data Points in the resulting Data Set, those Data Points are filtered by chosing 
 * the Data Point belonging to the left most operand Data Set. 

	union ( na_main [ calc identifier d := "A" ] , na_main [ calc identifier d := "B" ] )
	
  	x := na_main [sub time = 2014 ] ;
  	y := na_main [sub time = 2015 ] ;
  	union ( x, y ) 
  	union (na_main [sub time = 2014 ] , na_main [sub time = 2015 ] )
  	union (na_main [filter time = 2014 ] , na_main [filter time = 2015 ] )
  	union ( myview [filter time = 2014 ] , myview [filter time = 2015 ] )
  	na_main [filter time = 2014 or time = 2015 ]   	
 */
static Query union ( Vector<Query> queries ) throws VTLError
{
	Query		q = null ;
	String		myQueryString = "" ;
	boolean		sqlUnion = true ;
	
	if ( queries.size() == 1 )					// it is possible to have only one operand
		return ( queries.firstElement() ) ;
	
	for ( Query q2 : queries ) {
		if ( q == null )
			q = q2.copy() ;					// first query
		else {
			q.checkIdenticalAllComponents ( q2, "union" ) ;		// check dimensions, measures and attributes
			
			boolean empty_intersection = false ;
			
			for ( DatasetComponent d : q.dims ) {
				DatasetComponent dim2 = q2.getDimension ( d.compName ) ;
				ListString dim_values2 = dim2.dim_values ;
			    if ( dim_values2.size() > 0 ) {
				    if ( d.dim_values.size() > 0 && d.dim_values.isEmptyIntersection ( dim_values2 ) )
				    	empty_intersection = true ;
				    d.dim_values = d.dim_values.merge ( dim_values2 ) ;			    	
			    }
			}
			
			if ( empty_intersection == false )		// join	
				sqlUnion = false ;
			
			// add the datasets referenced in the queries
			q.addReferencedDatasets(q2);
		}
	}

	if ( sqlUnion ) {
		myQueryString = q.build_sql_query ( false ) ;
		int idx = 0 ;
		for ( Query q2 : queries ) {
			if ( idx++ > 0 )
				myQueryString = myQueryString + " UNION ALL " + q2.copyOrderComponents(q).build_sql_query ( false ) ;		
		}
		q.pushQuery ( myQueryString ) ;		
	}
	else {
		int idx = 0 ;
		for ( Query q2 : queries ) {
			if ( idx++ > 0 )
				q = q.unionOuterJoin ( q2 ) ;			
		}
	}

	return ( q ) ;
}

/*
 * outer join, called by union when at least a dimension has a non-empty intersection of values
 */
Query unionOuterJoin ( Query q2 ) throws VTLError
{
	StringBuffer		onCond = new StringBuffer ( ) ;
	DatasetComponent	c2 ;
			
	Query q1 = this.flatQuery( false ) ;
	q2 = q2.flatQuery( false ) ;
			
	String leftKeyEmpty = q1.leftKeyEmpty(q2) ;			// to be done as first step 

	for ( DatasetComponent dim1 : q1.dims ) {
		c2 = q2.getDimension(dim1.compName) ;
		onCond.append( onCond.length() == 0 ? " ON (" : " AND " ).append( dim1.sql_expr + "=" + c2.sql_expr ) ;
		dim1.sql_expr = "CASE WHEN " + leftKeyEmpty + " THEN " + c2.sql_expr + " ELSE " + dim1.sql_expr + " END" ;
	}
	onCond.append( ")" ) ;
	
	for ( DatasetComponent c : q1.measures ) {
		c2 = q2.getMeasure(c.compName) ;
		if ( c.compType.equals( "null") && ! c2.compType.equals( "null") ) {
			c.sql_expr = "CASE WHEN " + leftKeyEmpty + " THEN " + c2.sql_expr 
							+ " ELSE CAST(" + c.sql_expr + " AS " + Check.getSqlCastType(c2.compType) + ") END " ;					
		}
		else if ( c2.compType.equals( "null") && ! c.compType.equals( "null") ) {
			c.sql_expr = "CASE WHEN " + leftKeyEmpty + " THEN CAST(" + Check.getSqlCastType(c2.sql_expr) 
							+ " AS " + c2.compType + ") ELSE " + c.sql_expr + " END " ;					
		}
		else 
			c.sql_expr = "CASE WHEN " + leftKeyEmpty + " THEN " + c2.sql_expr + " ELSE " + c.sql_expr + " END " ;					
	}
	for ( DatasetComponent c : q1.attributes )
		c.sql_expr = "CASE WHEN " + leftKeyEmpty + " THEN " + q2.getAttribute(c.compName).sql_expr 
					+ " ELSE " + c.sql_expr + " END " ;
	//sql_from
	q1.sqlFrom = q1.sqlFrom + " FULL OUTER JOIN " + q2.sqlFrom + onCond.toString() ;	
	return ( q1 ) ;
}

/*
 * time_agg
 * frequency_to			convert to frequency_to
 * frequency_from		convert from frequency from
 * TBD: propagation of attributes
 */
void time_agg ( String freqTo, String freqFrom  ) throws VTLError
{
	String				period_expr, group_expr ;
	int					idxSub ;
	final String		subPeriods[] = { "AQ", "AM", "AS", "QM" } ;
	DatasetComponent 	timeDim ;
	
	if ( ( timeDim = this.getTimePeriodDimension ( ) )== null )
		VTLError.TypeError ( "time_agg: expression has no dimension of type time period" ) ;
	period_expr = timeDim.sql_expr ;
	
	idxSub = Sys.getPosition(subPeriods, freqTo + freqFrom ) ;
	if ( idxSub < 0 )
		VTLError.TypeError ( "time_agg: invalid combination of period: " + freqTo + " from " + freqFrom ) ;
	
    if ( freqTo.equals ( "A" ) )
    	group_expr = "substr(" + period_expr + ",1,4)" ;
    else if ( freqTo.equals ( "Q" ) )
		group_expr = "substr(" + period_expr + ", 1, 4) || 'Q' || ( trunc((substr(" + period_expr + ",6,2)-1)/3)+1)" ;
    else
    	group_expr = null ;
    
    timeDim.sql_expr = group_expr ;
    this.addWherePart( " substr(" + period_expr + ",5,1)='" + freqFrom + "'" ) ;	// TBD: monthly/daily  
    this.setDoFilter();
    //return (q1.flatQueryAggregate ( " GROUP BY " + q1.dimensionsSqlColumns() + clauseHaving ) ) ;
}

/*
 * Fill option of update statement. Add missing dimension to q in order that q_target and q have the same dimensions.
 */
public void fillMissingDims ( Dataset tab, Query q ) throws VTLError
{
	int			dim_index, tab_dim_index ;
	String		dim_name, table_alias ;
	DatasetComponent	tab_dim ;
	
	for ( dim_index = 0; dim_index < this.dims.size(); dim_index ++ ) {
		dim_name = this.dims.get( dim_index ).compName ;
		if ( q.getDimension(dim_name) == null ) {		// dim_name is in target tab and not in query
			if ( ( tab_dim_index = tab.getDimensionIndex(dim_name) ) < 0 )
				VTLError.InternalError( "update with fill option") ;
			tab_dim = tab.get_dimension(tab_dim_index) ;

			table_alias = Query.newAlias () ;
			if ( tab_dim.dim_values.size() == 0 )
				VTLError.RunTimeError( "update - fill option: object " + tab.dsName
											+ " does not have a defined code list for dimension " + dim_name )	;	
			q.addDimension(dim_name, tab_dim.compType, tab_dim.compWidth, table_alias + ".pos_code", tab_dim.dim_values) ;
			q.addFromPart ( Db.mdt_positions, table_alias ) ;
			q.addWherePart ( table_alias + ".object_id=" + tab.objectId + " AND " + table_alias + ".dim_index=" + ( tab_dim_index + 1 ) ) ;
		}
	}	
}

/*
 * Build string alias + column name, quoted and upper case.
 */
public final String aliasComponent ( String tableAlias, String compNname ) throws VTLError
{
	return ( tableAlias + ".\"" + compNname.toUpperCase() + "\"" ) ;
}

/*
 * Build a flat query with no aggregate functions and where clause.
 */
public Query flatQueryAggregate ( String groupBy) throws VTLError
{
	Query 	q ;
	String	alias ;
	
	q = new Query () ;

	alias = Query.newAlias () ;

	for ( DatasetComponent dim : this.dims )
		q.addDimension(dim.compName, dim.compType, dim.compWidth, aliasComponent ( alias, dim.compName), dim.dim_values) ;

	for ( DatasetComponent m : this.measures )
		q.addMeasure ( m.compName, m.compType, aliasComponent ( alias, m.compName), m.compWidth, m.canBeNull  ) ;

	for ( DatasetComponent m : this.attributes )
		q.addAttribute ( m.compName, m.compType, aliasComponent ( alias, m.compName), m.compWidth, m.canBeNull ) ;
	
	q.sqlFrom = "(" + this.build_sql_query( true ) + groupBy + ")" + alias ;
	q.referencedDatasets = this.referencedDatasets ;
	return ( q ) ;
}


/*
 * Modify this by pushing the query in a subquery.
 */
void pushQuery ( String myQuery ) throws VTLError
{
	String	alias = Query.newAlias () ;
	
	for ( DatasetComponent d : this.dims )						// dimensions
		d.sql_expr = alias + '.' + d.compName ;
	
	for ( DatasetComponent c : this.measures )					// measures
		c.sql_expr = alias + '.' + c.compName ;
	
	for ( DatasetComponent c : this.attributes )				// attributes
		c.sql_expr = alias + '.' + c.compName ;
	
	this.sqlFrom = "(" + myQuery + ")" + alias ;
	this.sqlWhere = null ;
	this.doFilter = false ;
}

/*
 * Build a flat query with no aggregate functions and where clause.
 */
Query flatQuery ( boolean onlyDims ) throws VTLError
{
	return ( this.flatQuery ( onlyDims, this.build_sql_query( true ) ) ) ;
}

/*
 * Build a flat query with no aggregate functions and where clause.
 */
Query flatQuery ( boolean onlyDims, String sqlQuery ) throws VTLError
{
	Query 	q ;
	String	alias ;
	
	q = new Query () ;

	alias = Query.newAlias () ;

	for ( DatasetComponent dim : this.dims )
		q.addDimension(dim.compName, dim.compType, dim.compWidth, aliasComponent ( alias, dim.compName), dim.dim_values) ;

	if ( ! onlyDims ) {
		for ( DatasetComponent m : this.measures )
			q.addMeasure ( m.compName, m.compType, aliasComponent ( alias, m.compName), m.compWidth, m.canBeNull ) ;

		for ( DatasetComponent m : this.attributes )
			q.addAttribute ( m.compName, m.compType, aliasComponent ( alias, m.compName), m.compWidth, m.canBeNull ) ;		
	}
	
	q.sqlFrom = "(" + sqlQuery + ")" + alias ;
	q.referencedDatasets = this.referencedDatasets ;
	this.sqlWhere = null ;
	this.doFilter = false ;
	return ( q ) ;
}

/*
 * Build a flat query without aggregates and joins.
 */
public Query flatQueryUnpivot ( String unpivotExpr ) throws VTLError
{
	Query q ;
	String	table_alias ;
	
	q = new Query () ;

	table_alias = Query.newAlias () ; 

	for ( DatasetComponent dim : this.dims ) {
		q.addDimension(dim.compName, dim.compType, dim.compWidth, table_alias + "." + dim.compName, dim.dim_values) ;
	}
	for ( DatasetComponent m : this.measures ) {
		q.addMeasure ( m.compName, m.compType, table_alias + "." + m.compName, m.compWidth, m.canBeNull ) ;
	}
	// operator unary?
	for ( DatasetComponent m : this.attributes ) {
		q.addAttribute ( m.compName, m.compType, table_alias + "." + m.compName, m.compWidth, m.canBeNull ) ;
	}
	
	q.sqlFrom = "( SELECT * FROM (" + this.build_sql_query( true ) + ")" + unpivotExpr + ")" + table_alias ;
	q.referencedDatasets = this.referencedDatasets ;
	return ( q ) ;
}

/*
 * Build strings to update dataset.
 * TBD: check that measures/attributes of dataset not included in query are not null
 */
String dsUpdateMeasuresAttributes ( Query q2 ) throws VTLError
{
	StringBuffer	strPivotColumns = new StringBuffer(), strPivotExpr = new StringBuffer() ;

	for ( DatasetComponent c2 : q2.measures ) {
	   if ( this.getMeasure ( c2.compName ) != null ) {
		   if ( c2.sql_expr.compareTo ( "NULL" ) == 0 ) {
			   if ( this.getMeasure ( c2.compName ).canBeNull == false )
		    		VTLError.TypeError ( "Cannot assign null to component defined as not null: " + c2.compName ) ;
		   }
		   else {
			   if ( strPivotColumns.length () > 0 ) {
				   strPivotColumns.append( "," ) ;
				   strPivotExpr.append( "," ) ;
			   }
			   strPivotColumns.append( c2.compName ) ;
			   strPivotExpr.append( c2.sql_expr ) ;				   
		   }
	   }
	}

	for ( DatasetComponent c2 : q2.attributes ) {
		   if ( this.getAttribute ( c2.compName ) != null ) {
			   if ( c2.sql_expr.compareTo ( "NULL" ) == 0 ) {
				   if ( this.getAttribute ( c2.compName ).canBeNull == false )
					   VTLError.TypeError ( "Cannot assign null to component defined as not null: " + c2.compName ) ;
			   }
			   else {
				   if ( strPivotColumns.length () > 0 ) {
					   strPivotColumns.append( "," ) ;
					   strPivotExpr.append( "," ) ;
				   }
				   strPivotColumns.append( c2.compName ) ;
				   strPivotExpr.append( c2.sql_expr ) ;				   
			   }
		   }
		}

	return ( strPivotColumns + " " + strPivotExpr ) ;
}

/*
 * Print query.
 */
String printQuery ( )
{
	StringBuffer str = new StringBuffer () ;
	for ( DatasetComponent d : this.dims ) 
		str.append ( "identifier " + d.compName + " " + d.compType + " {" + d.dim_values.toString(',') + "}\n" );
	for ( DatasetComponent c : this.measures ) 
		str.append ( "measure " + c.compName + " " + c.compType + "\n" );
	for ( DatasetComponent c : this.attributes ) 
		str.append ( (c.isViralAttribute ? "viral attribute " : "attribute " ) + c.compName + " " + c.compType + "\n" );
	return ( str.toString() ) ;
}

//end of class
}
