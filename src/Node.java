
/*
 * Interpreter: evaluate internal representation of expressions.
 */

import java.util.Vector;

// import jmdt.Command.Commands;

class Node {
	short  name ;
	short  info ; 
	String val ;
	Node   child ;
	Node   next ;

final Node child_1 ( ) { return ( this.child ) ; }
final Node child_2 ( ) { return ( this.child.next ) ; }
final Node child_3 ( ) { return ( this.child.next.next ) ; }
final Node child_4 ( ) { return ( this.child.next.next.next ) ; }
final Node child_5 ( ) { return ( this.child.next.next.next.next ) ; }

/*
 * Constructor. All other fields are null.
 */
Node ( short node_name )
{
	name = node_name ;
}

/*
 * Constructor
 */
Node ( short node_name, Node node_son )
{
	name = node_name ;
	child = node_son ;
}

/*
 * Constructor
 */
Node ( short node_name, String node_val )
{
	name = node_name ;
	val = node_val ;
}

/*
 * Constructor
 */
Node ( short node_name, String node_val, Node node_son )
{
	name = node_name ;
	val = node_val ;
	child = node_son ;
}

/*
 * Constructor
 */
Node ( short node_name, String node_val, short node_info )
{
	name = node_name ;
	val = node_val ;
	info = node_info ;
}

/*
 * Returns number of nodes in a list (linked via .next).
 */
int nodeListLength ( )
{
	int  n = 0 ;

	for ( Node p = this ; p != null ; p = p.next )
		n ++ ;
	return ( n ) ;
}

/*
 * Returns number of child nodes. this cannot be null.
 */
int countChildren ( )
{
	return ( this.child == null ? 0 : this.child.nodeListLength ( ) ) ;
}

/*
 * Append p_last as the last son of hd. this cannot be null.
 */
void addChild ( Node p_last )
{
	Node	p ;

	if ( this.child == null )
		this.child = p_last ;
	else {
		for ( p = this.child ; p.next != null ; p = p.next )
			;
		p.next = p_last ;
	}
}

/*
 * Return string name of the node.
 */
String nodeName ( )
{
	String s ;
	switch ( this.name ) {
		case Nodes.N_EMPTY 				: s= "(empty)" ; 				break ; 
		case Nodes.N_AND 				: s= "and" ; 					break ;
		case Nodes.N_CLAUSE_OP 			: s= "clause" ; 				break ;
		case Nodes.N_BETWEEN 			: s= "between" ; 				break ;
		case Nodes.N_UNARY_SUBTRACT 	: s= "(unary)-" ;  				break ;
		case Nodes.N_NOT_IN 			: s= "not_in" ; 				break ; 
		case Nodes.N_IN 				: s= "in" ;		 				break ; 
		case Nodes.N_DEFAULT_VALUE 		: s= "(default) _" ; 			break ; 	
		case Nodes.N_FALSE 				: s= "false" ; 					break ; 
		case Nodes.N_TRUE 				: s= "true" ; 					break ; 	
		case Nodes.N_NOT 	 			: s= "not" ; 					break ; 
		case Nodes.N_NULL 				: s= "null" ; 					break ; 
		case Nodes.N_OR 				: s= "or" ; 					break ;
		case Nodes.N_XOR 				: s= "xor" ; 					break ;
		case Nodes.N_UNION  			: s= "union" ; 					break ;			
		case Nodes.N_PLUS  				: s= "+" ; 						break ;
		case Nodes.N_SUBTRACT  			: s= "-" ; 						break ;
		case Nodes.N_MULTIPLY  			: s= "*" ; 						break ;
		case Nodes.N_DIVIDE 			: s= "/" ; 						break ;
		case Nodes.N_EQUAL 				: s= "=" ;  					break ;
		case Nodes.N_GT 				: s= ">" ;  					break ;
		case Nodes.N_LT 				: s= "<" ;  					break ;
		case Nodes.N_MEMBERSHIP 		: s= "#" ;  					break ;
		case Nodes.N_N_EQUAL 			: s= "<>" ;  					break ;
		case Nodes.N_GT_EQUAL 			: s= ">=" ;  					break ;
		case Nodes.N_LT_EQUAL 			: s= "<=" ;  					break ;
		case Nodes.N_CONCAT 			: s= "||" ;  					break ;
		case Nodes.N_TYPE_COMPONENT 	: s= "(component) " ; 			break ;
		case Nodes.N_TYPE_RULESET 		: s= "(ruleset)" ;  			break ;
		case Nodes.N_FLOW_TO_STOCK		: s= "flow_to_stock" ; 			break ;	
		case Nodes.N_STOCK_TO_FLOW 		: s= "stock_to_flow" ; 			break ;
		case Nodes.N_NUMBER 			: s= "(number) " 	;			break ;
		case Nodes.N_STRING 			: s= "(string) " 	;			break ;
		case Nodes.N_IDENTIFIER			: s= "(identifier) " ;			break ;
		case Nodes.N_FILL_TIME_SERIES 	: s= "fill_time_series" ; 		break ;	
		case Nodes.N_CLAUSE_SUBSCRIPT 			: s= "sub" ;  					break ;
		case Nodes.N_EXISTS_IN 			: s= "exists_in" ; 				break ;
		case Nodes.N_PERIOD_INDICATOR 	: s= "period_indicator" ; 		break ;	
		case Nodes.N_PARENTHESIS 		: s= "( )" ; 					break ;
		case Nodes.N_SQL_ROW_FUNCTION 	: s= "(datapoint operator) " ; 	break ;
		case Nodes.N_SQL_AGGREGATE_FUNCTION : s= "(aggregate) " 	; 	break ;
		case Nodes.N_SQL_ANALYTIC_FUNCTION 	: s= "(analytic) " ;		break ;
		case Nodes.N_GROUP_BY 			: s= "(group by)" ; 			break ;
		case Nodes.N_GROUP_EXCEPT 		: s= "(group except)" ; 		break ;
		case Nodes.N_DUMMY 				: s= ""  ;		 				break ;
		case Nodes.N_SORT_ASC_DESC		: s= "(asc/desc) " ;		 	break ;
		// case unused 					: s= "" ;  						break ;
		case Nodes.N_GROUP_ALL 			: s= "(group all)" ; 			break ;	
		case Nodes.N_ORDER_BY 			: s= "(order by)" ; 			break ;
		case Nodes.N_PARTITION_BY 		: s= "(partition by)" ; 		break ;
		case Nodes.N_TYPE_DATASET		: s= "dataset { }" ; 			break ;	
		case Nodes.N_TYPE_SCALAR 		: s= "(scalar type)" ; 			break ;
		case Nodes.N_USER_TYPE 			: s= "(user type)" ; 			break ;
		case Nodes.N_CAST		 		: s= "cast" ;  					break ;
		case Nodes.N_TIMESHIFT 			: s= "timeshift" ;  			break ;
		case Nodes.N_TYPE_CONSTRAINT 	: s= "(constraint)" ;  			break ;
		case Nodes.N_TYPE_NULL			: s= "(null)" ;			  		break ;
		// case unused 					: s= "" ;  						break ;		
		// case unused 					: s= "" ;  						break ;		
		// case unused 					: s= "" ;  						break ;		
		// case unused 					: s= "" ; 						break ;		
		case Nodes.N_POSITION 			: s= "(position) " ; 			break ;
		case Nodes.N_SET_SCALAR		 	: s= "(set scalar)" ; 			break ;	
		case Nodes.N_CLAUSE_APPLY		: s= "apply" ; 					break ;			
		case Nodes.N_TYPE_SET 			: s= "set" ; 					break ;
		case Nodes.N_COMPONENT_LIST 	: s= "(components)" ; 			break ;	
		// case unused					: s= "" ; 						break ;	
		case Nodes.N_SYMDIFF 			: s= "symdiff" ; 				break ;
		case Nodes.N_IF 				: s= "if" ; 					break ;	
		case Nodes.N_VTL_META_DATASET	: s= "(metadataset) " ; 		break ;
		case Nodes.N_GENERIC_COMPONENT	: s= "(component) " ;			break ;
		case Nodes.N_COMPONENT			: s= "(component) " ; 			break ;
		case Nodes.N_VARIABLE 			: s= "(variable) " ; 			break ;
		case Nodes.N_CHECK 				: s= "check" ; 					break ;
		case Nodes.N_CHECK_DATAPOINT	: s= "check_datapoint" ; 		break ;
		case Nodes.N_CHECK_HIERARCHY 	: s= "check_hierarchy" ; 		break ;			
		case Nodes.N_INTERSECT 			: s= "intersect" ; 				break ;
		case Nodes.N_JOIN_ARGUMENTS 	: s= "join arguments" ; 		break ;
		case Nodes.N_ISNULL		 		: s= "isnull" ; 				break ;				
		case Nodes.N_MATCH_CHARACTERS 	: s= "match_characters" ; 		break ;
		case Nodes.N_USER_OPERATOR_CALL : s= "(user operator call) " ;	break ;	
		case Nodes.N_HIERARCHY 			: s= "hierarchy" ; 				break ;			
		case Nodes.N_TIME_AGG 			: s= "time_agg" ; 				break ;
		case Nodes.N_SETDIFF 			: s= "setdiff" ; 				break ; 					
		case Nodes.N_VTL_ASSIGNMENT 	: s= ":=" ; 					break ;
		case Nodes.N_EVAL		 		: s= "eval" ; 					break ;
		case Nodes.N_JOIN		 		: s= "(join) " ; 				break ;
		case Nodes.N_CLAUSE_ASSIGNMENT	: s= ":=" ; 					break ;
		case Nodes.N_CLAUSE_AGGR		: s= "aggr" ; 					break ;
		case Nodes.N_CLAUSE_CALC 		: s= "calc" ; 					break ;
		case Nodes.N_CLAUSE_DROP 		: s= "drop" ; 					break ;
		case Nodes.N_CLAUSE_FILTER 		: s= "filter" ; 				break ;
		case Nodes.N_CLAUSE_KEEP 		: s= "keep" ;			 		break ;
		case Nodes.N_CLAUSE_RENAME 		: s= "rename" ; 				break ;
		case Nodes.N_CLAUSE_PIVOT 		: s= "pivot" ; 					break ;
		case Nodes.N_CLAUSE_UNPIVOT 	: s= "unpivot" ; 				break ;
		default : s = "" ;
	}
	return ( s + ( this.val == null ? "" : this.val ) ) ;
}

String printTree ( StringBuffer buff, int lev )
{
	int       		nam = this.name ;
	String    		nodeType ;
	final String 	pad = 
	"                                                                                                   " ;
	if ( nam != Command.Commands.N_STATEMENT_LIST ) {
		buff.append ( pad.substring ( 0, ( lev <= 30 ? 3*lev : 90) ) ) ;			// indentation
		
		// type of node
		if ( nam < 100 )
			nodeType = nodeName ( ) ;
		else {
			nodeType = null ;
			if ( nam >= 100 && nam < 300 ) {
				if ( nam == Command.Commands.N_TEMP_ASSIGNMENT )
					nodeType = ":=" ;
				else if ( nam == Command.Commands.N_PERS_ASSIGNMENT )
					nodeType = "<-" ;
				else for ( int idx = 1; idx < Command.Commands.NameCommands.length ; idx ++ )
						if ( nam == Command.Commands.NameCommands[ idx ] )
							nodeType = Command.Commands.text_cmds[ idx ] ;
			}
			if ( nodeType == null )
				nodeType = "(internal: " + nam + ")" ;			
		}
		// if ( lev > 0 && nam < Parser.SqlSyntax.length && Parser.SqlSyntax [ nam ].compareTo ( "" ) != 0 )nodeVal = Parser.SqlSyntax [ nam ] ;
		buff.append ( nodeType ).append ( ' ' ).append ( "\r\n" );									// newline	
	}
	
	for ( Node p = this.child ; p != null; p = p.next )
		p . printTree ( buff, lev + 1 ) ;
	
	return ( buff.toString() ) ;
}

public String printSyntaxTree ( )
{
	StringBuffer	buff = new StringBuffer () ;
	return ( this.printTree ( buff, 0 ) ) ;
}

static public String getOptionalVal ( Node p )
{
	return ( p != null && p.name != Nodes.N_EMPTY ? p.val : null ) ;
}

// ----------- interpreter

/*
 * Build a ListString containing the list of dimensions.
 */
ListString listDimensions ( ) throws VTLError
{
	ListString	ls = new ListString ( ) ;
	
	for ( Node p = this ; p != null ; p = p.next ) {
		if ( ls.contains(p.val))
			VTLError.TypeError("Duplicated dimension in list: " + p.val );
		ls.add ( p.val ) ;
	}
	
	return ( ls ) ;
}

/*
 * Evaluate expression (condition) to a scalar boolean value, used in if-then-else.
 */
boolean inteEvalCondition ( ) throws VTLError
{
	Query 	q ;
	String	str ;

	q = this.inte ( ) ;

	if ( q.dims.size () > 0 )
		VTLError.TypeError ( "Boolean expression is not scalar" ) ;

	str = Db.sqlGetValue( q.build_sql_query ( true ) ) ;
   
	return ( str.equals("true") ) ;
}

/*
 * Evaluate expression to produce an object name. It can be an identifier, variable name or a string value. 
 */
String evalObjectName ( ) throws VTLError
{
	String	object_name = this.val ;
	if ( object_name == null || object_name.length() == 0 )
		VTLError.RunTimeError( "Object name is null" ) ;
	if ( object_name.startsWith( "'" ) )
		return ( object_name.substring( 1, object_name.length() - 1 )) ;
	return ( object_name ) ;
	/*
	object_name = this.inteEvalScalar ( ) ;		// for N_IDENTIFIER inte_eval_scalar_value returns the val field 
	if ( object_name == null || object_name.length() == 0 )
		AppError.RunTimeError( "Object name is null" ) ;
	return ( object_name ) ;
	*/
}

/*
 * Evaluate expression and produce a scalar value. Check the type.
 * Don't query the db when the expression is a constant.
 * TBD: check the data type
	print aact_ali01 [ geo = IT ]
	x := "S1" ; myview[sub ref_sector = x ]
 */
String inteEvalScalar ( ) throws VTLError
{
	switch ( name ) {
		case Nodes.N_NUMBER :
			return ( this.val ) ;	
		case Nodes.N_STRING :
		case Nodes.N_POSITION :
			return ( this.val ) ; 				// Db.sql_quote_string ( this.val) ) ;
		case Nodes.N_NULL :
			return ( "" ) ;						// or "null" ? AppError . TypeError ( "Null value" ) ;
		default :
			Query q = this.inte ( ) ;
			if ( ! q.isScalar() )
				VTLError.TypeError( "Not a scalar value: " + this.val );
			return ( Db.sqlGetValue( q.build_sql_query ( true ) ) ) ;
	}  
}

String inteEvalInteger ( ) throws VTLError
{
	String s = this.inteEvalScalar( );
	Check.checkInteger( s ) ;
	return ( s ) ;
}

/*
 * Function call, predefined function.
 * length returns a measure having the same name of the operand
	power ( aact_ali01, 2 )
	current_date()
	TBD: mod can take two operand datasets (exception)
*/
Query inteRowFunction ( ) throws VTLError
{
	String 			functionName = this . val ;
	Query			q ;
	Vector<Query>	arguments = new Vector<Query> () ;
	  
	if ( this.child == null )		// operator: current_date
	  return ( Query.operatorConstant ( functionName, "date" ) ) ;			
  
	q = this.child.inte ( ) ;		// first operand is a dataset or scalar

	for ( Node p = this.child.next ; p != null ; p = p.next ) {
		if ( p.name == Nodes.N_DEFAULT_VALUE )
			;	// nothing, the only exception is substr and is managed by the parser
		else {
			arguments.add( p.inte () ) ;
		}
	}

	return ( q.operatorFunction ( functionName, arguments, true, "" ) ) ;
}

/*
 * Am I in a clause, like in ds [ ... ]
 */
static boolean isWithinClause ( ) throws VTLError
{
	return ( Env.getVarIndex ( "$this" ) >= 0 ) ;
}

/*
 * get dataset operand of a clause, like in ds [ ... ]
 */
static Query getImplicitDataset ( String op ) throws VTLError
{
	int	varIndex = Env.getVarIndex ( "$this" ) ;
	if ( varIndex < 0 )							// || Env.getVarType ( varIndex ) != null )
		VTLError.InternalError ( op + ": cannot find dataset parameter in a clause" ) ;
	return ( Env.getQueryValue ( varIndex ) ) ;		// operand dataset (specified as clause operand)
}

/*
 * Function call, analytic function.
 * if partition by is missing then the query is partitioned by the dimensions not specified in order by
 * Example:
	avg ( aact_ali01 [geo=IT] ) over ( partition by itm_newa order by time rows between 2 preceding and 2 following )
	lag ( aact_ali01 [geo=IT], 1, 0 ) over ( partition by itm_newa order by time )
   on components:
	aact_ali01 [ aa := avg ( obs_value ) over ( partition by itm_newa order by time ) ]
	{ a =  aact_ali01 (geo=IT) , b = rank () over ( partition by itm_newa order by time ) } ;
	
	print { a =  aact_ali01 [geo=IT] , b = rank () over ( partition by itm_newa order by time ) } ;
 * 	print { a =  aact_ali01 (geo=IT) , b = avg ( aact_ali01 (geo = IT ) ) over ( rows between 2 preceding and 2 following ) } ;
  	print  { obs_value = refin.avia_paoc(unit ="PASS",tra_meas ="PASS_BRD",geo ="IT")  , 
 	avg_value =  avg ( refin.avia_paoc(unit ="PASS",tra_meas ="PASS_BRD",geo ="IT")   ) over  ( rows between 1 preceding and 1 following ) } 
	filter by obs_value > 0 and abs ( obs_value - avg_value ) > abs ( obs_value ) * 10  ;
	Only functions with 1 argument
	TBD: partition and order by can be expressions

  	aact_ali01 [ ee := avg ( obs_value ) over ( partition by itm_newa,geo order by time rows between 2 preceding and 2 following ) ]
	lag ( aact_ali01, 1, 0 ) over ( partition by itm_newa,geo order by time )
	lag ( aact_ali01, 1, 0 ) over ( order by time )
	aact_ali01 [ v1 := lag ( obs_value, 1, 0 ) over ( order by time ) ]

*/
Query inteAnalyticFunction ( ) throws VTLError
{
	String 			analyticFunction = this.val  ;
	Node   			p ;
	Node   			p_arguments = this.child, p_partition, p_order_by , p_windowing_clause ;
	Query 			q ;
	StringBuffer	windowClause = new StringBuffer () ;
	ListString		lsPartition = new ListString(), lsOrderBy = new ListString (), lsAscDesc = new ListString () ;
	Vector<Query>	arguments = new Vector<Query> () ;
	boolean			isComponentVersion ;
	
	if ( p_arguments.child == null ) {						// function with no arguments: rank
		q = getImplicitDataset ( analyticFunction ) ;
		isComponentVersion = true ;
	}
	else {
		Query qop = p_arguments.child.inte ( ) ;			// first operand is a dataset or scalar
		if ( qop.isScalar() ) {
			q = getImplicitDataset ( analyticFunction ).copy() ; 	// operand dataset (specified as clause operand)
			if ( q.measures.size() == 1 )
				q.getFirstMeasure().sql_expr = qop.getFirstMeasure().sql_expr ;											// nothing
			isComponentVersion = true ;
		}
		else {
			q = qop;										// operand dataset
			isComponentVersion = false ;
		}
		for ( p = p_arguments.child.next ; p != null ; p = p.next )
			arguments.add( p.inte() ) ;
	}

	// N_PARTITION_BY
	p_partition = p_arguments.next ;
	if ( p_partition.child != null ) {
		for ( p = p_partition.child ; p != null ; p = p.next )
			lsPartition.add( p.val ) ;	
	}

	// N_ORDER_BY
	p_order_by = p_partition.next ;
	for ( p = p_order_by.child ; p != null ; p = p.next.next ) {
		lsOrderBy.add ( p.val ) ;			
		lsAscDesc.add ( p.next.val ) ;
	}

  	// N_WINDOWING_CLAUSE
  	p_windowing_clause = p_order_by.next ;
  	if ( p_windowing_clause.child != null ) {
  		String	tmp ;
  		windowClause.append ( " " + 
  		( p_windowing_clause.val.equals( "range" ) ? "range" : "rows") ) ;	// rows / range is attached to N_DUMMY
  		windowClause.append ( " between " ) ;
  		p = p_windowing_clause.child ;
  		if ( p.name != Nodes.N_EMPTY )
  			tmp = p.inteEvalInteger ( ) + " " ;
  		else
  			tmp = "" ;
  		p = p.next ;
  		windowClause.append ( tmp + p.val ) ;	// preceding / following / ...
  		windowClause.append ( " AND " ) ;
  		p = p.next ;
  		if ( p.name != Nodes.N_EMPTY )
  			tmp = p.inteEvalInteger ( ) + " " ;
  		else
  			tmp = "" ;
  		p = p.next ;
  		windowClause.append ( tmp + p.val ) ;	// preceding / following / ...
    }
  	
	q = q.analyticFunction ( analyticFunction, isComponentVersion, arguments,
				lsPartition, lsOrderBy, lsAscDesc, windowClause.toString() ) ;
 
  	return ( q ) ;
}

/*
 * group by, group expect, group all, nothing
 */
void inteGroupByExceptAll ( Query q) throws VTLError
{
	Node p = this ;
	switch ( p.name ) {
	  	case Nodes.N_GROUP_BY :
	  		// group by - group by these dimensions and delete the other dimensions 
			ListString	groupbyDims = p.child.listDimensions() ;
	  		for ( String dimName : groupbyDims ) {
	  			if ( q.getDimension ( dimName ) == null )
	               VTLError.TypeError ( "Dimension " + dimName + " not found in expression " ) ;
	  		}
	  		for ( String dimName : q.listDimensions() ) {
	  			if ( ! groupbyDims.contains( dimName ) )
			  		q.dims.remove( q.getDimension( dimName ) ) ;
	  		}
	  		break ;
	
	  	case Nodes.N_GROUP_EXCEPT :
	  		// group except - delete these dimensions and group by the other dimensions
	  		for ( String dimName : p.child.listDimensions() ) {
	  			DatasetComponent dim = q.getDimension ( dimName ) ;
	  			if ( dim == null )
	               VTLError.TypeError ( "Dimension " + dimName + " not found in expression " ) ;
	  			q.dims.remove( dim ) ;
	  		}
	  		// TBD: check whether all dimensions have been removed
	        break ;
	
		case Nodes.N_GROUP_ALL :
			// group all - group by all dimensions
				p = p.child ;
				if ( p.name != Nodes.N_TIME_AGG )		// done at parser level
					VTLError.TypeError( "group all, only the time_agg operator is allowed");
				if ( p.countChildren()  != 2 )
					VTLError.TypeError( "group all, only time_agg with 2 arguments is allowed");
			  	String frequency_to = p.child.inteEvalScalar ( ) ;
				String frequency_from = p.child_2().inteEvalScalar ( ) ;
			  	q.time_agg ( frequency_to, frequency_from ) ;
			  	break;
	
		case Nodes.N_EMPTY :
			// no group specification - remove all dimensions
			q.dims.removeAllElements() ;		
			break ;
	
		default:
			VTLError.InternalError( "Call aggregate operator - case not found: " + p . name ) ;
	}
}

/*
	op [ aggr aggrClause { groupClause } ]
	aggrClause ::= { aggrRole } aggrComp := aggrExpr { , { aggrRrole } aggrComp:= aggrExpr }*
	groupClause ::= 
		group by groupingId {, gropuingId }*
		| group except groupingId {, groupingId }*
		| group all time_agg ( )
	{ having havingCondition }
	aggrRole::=  measure  |  attribute  |  viral attribute

	na_main [ aggr x1 := sum ( obs_value ) group by time ]
	na_main [ aggr x1 := sum ( obs_value ), x2 := avg (obs_value) group by time ]
	na_main [ aggr obs_value := sum(obs_value) group by time having count() >1 ]
	returns error:
		na_main [ aggr obs_status := sum(obs_value) group by time having count() >1 ]
*/
Query inteClauseAggr ( Query q ) throws VTLError
{
	Query			q2 ;
	Node			p ;
	
	q.removeMeasures();						// remove all measures
	
	p = this.child.child ;	// list of N_CLAUSE_CALC
	do {
		String	compRole, compName ;					
		if ( p.name != Nodes.N_CLAUSE_CALC )
			VTLError.InternalError( "aggr: not a calc node: " + p.name + " " + this.name );
		compRole = (p.child.name == Nodes.N_EMPTY ? null : p.child.val);
		compName = p.child_2().val ;
		q2 = p.child_3().inte () ;
		String compvalue = q2.getFirstMeasure().sql_expr ;		// TBD mono-measure
		String compType = q2.getFirstMeasure().compType ;
		q.calcAddRedefineComponent ( compRole, compName, compType, compvalue, p.child_3().child == null ) ;
	} while ( ( p = p.next ) != null ) ;
	
	q.propagateAttributesAggregateFunction();
	
	this.child_2().inteGroupByExceptAll(q);
	
	p = this.child_3() ;
	String	havingCond ;
  	if ( p.name == Nodes.N_EMPTY )
  		havingCond = " HAVING COUNT(*) > 0" ;
  	else {
  		Query qHaving = p.inte( ) ;		
  		if ( ! qHaving.getFirstMeasure().compType.equals("boolean"))
  			VTLError.TypeError( "having: boolean expression expected");
  		havingCond = " HAVING " + qHaving.sqlWhere ;
  	}
  	
	return ( q.flatQueryAggregate ( q.dims.size() == 0 ? "" :
			" GROUP BY " + q.dimensionsSqlColumns() + havingCond ) ) ;
}

/*
	aggrOperator ( op )  { groupingClause } 
	aggOperator ::= avg | count | max | median | min | stddev_pop | stddev_samp | sum | var_pop | var_samp 
	groupingClause ::=  
		group by groupingId {, groupingId}* 
		| group except groupingId {, groupingId}* 
		| group all time_agg ( time_agg ( periodIndTo { , periodIndFrom } 
												{ , op  } { , first | last }  ) 
	{ having havingCondition }

	sum ( na_main group by time )
	sum ( na_main group by time having count() > 1 )
	sum ( na_main[ calc attribute obs_status := "" ] group by time having count() >1 )
	sum ( na_main group all time_agg ( "A", "Q" ) )
	sum ( na_main group all time_agg ( "A", "Q" ) having count() = 4 )
	sum ( na_main group by time having count() >= 2 )
	sum ( na_main group by time having sum( obs_value ) >= 10 )
	
	alternative way to aggregate e.g. from any period to annual
	sum ( na_main [ calc identifier t := substr ( time, 1, 4 ) ]  ) along t
	however: how to compute the quarter related to a month?
	sum ( na_main [ calc identifier t := substr ( time, 1, 4 ) || "Q" || (substr ( time, 5, 2 ) / 4 + 1) ]  ) along t
*/
Query inteAggregateFunction ( ) throws VTLError
{
	String 		groupFunction = this.val, havingCond ;
	Node   		p ;
	Query  		q, qcopy ;
	
	p = this.child ;// dummy node
	Vector<Query>	arguments = new Vector<Query> () ;
	if ( groupFunction.equals( "count" ) && p.child == null ) {
		Query qCount = new Query () ;
		qCount.addMeasure( "int_var", "integer", "count(*)" ) ;
		return ( qCount) ;
	}
	q = p.child.inte() ;
	qcopy = q.copy();				// for the having clause if ( this.child_3().name != Nodes.N_EMPTY )

	q = q.operatorFunction ( groupFunction, arguments, false, "" ) ;	// no other arguments
	
	q.propagateAttributesAggregateFunction();
	
	this.child_2().inteGroupByExceptAll(q);

  	p = this.child_3() ;
  	if ( p.name == Nodes.N_EMPTY )
  		havingCond = " HAVING COUNT(*) > 0" ;
  	else {
		Env.addfunctionCall("having");
		qcopy.addToEnvironment( );
  		Query qHaving = p.inte( ) ;
		Env.removefunctionCall();
		
  		if ( ! qHaving.getFirstMeasure().compType.equals("boolean"))
  			VTLError.TypeError( "having: boolean expression expected");
  		havingCond = " HAVING " + qHaving.sqlWhere ;
  	}
  	
  	if ( q.dims.size() == 0 && Node.isWithinClause() )
  		return ( q ) ;
  	
  	return ( q.flatQueryAggregate ( q.dims.size() == 0 ? "" :
  						" GROUP BY " + q.dimensionsSqlColumns() + havingCond ) ) ;

  	/* maybe this optimization is done by oracle
  	if ( ( idx = q.agg_from_where_groupby.indexOf( my_agg_from_where_groupby ) ) >= 0 ) {
		// found an expression with same components
		// to distinguish names referring to the same function
  		col_name = col_name + q.agg_select.get(idx).length() ;
		q.agg_select.set(idx,  q.agg_select.get(idx) + "," + my_expr + " AS " + col_name ) ;
		table_alias = "g" + idx					;		// use "g" + idx as table alias for this temporary table
	  	q.set_properties( col_name, "number", table_alias + "." + col_name ) ;
		return ;
  	}*/
}

/*
	between ( e1, e2, e3 )
	
	na_main [ filter between (obs_value, 40, 100 ) ]
	between (na_main#obs_value, 44, 44 ) 
 */
Query inte_between ( ) throws VTLError
{	
	Query				q1, q2, q3 ;
	DatasetComponent	m1 ;

	q1 = this.child.inte ( ).copy() ;		// first operand is a dataset or scalar
	m1 = q1.getFirstMeasure() ;
	if ( q1.isMultiMeasure() )
		VTLError.TypeError( "between can be applied only to mono-measure boolean operands");
	
	q2 = this.child_2().inte();
	q3 = this.child_3().inte();

	Query.checkTypeOperand( m1.compType , q2.getFirstMeasure().compType ) ;
	Query.checkTypeOperand( m1.compType , q3.getFirstMeasure().compType ) ;
	m1.compName = "bool_var" ;
	m1.compType = "boolean" ; 

	q1.sqlWhere = q1.sqlWhere + " BETWEEN " + q2.sqlWhere + " AND " + q3.sqlWhere ;
	m1.sql_expr = "CASE WHEN " + q1.sqlWhere + " THEN 'true' WHEN NOT (" 
											+ q1.sqlWhere + ") THEN 'false' END " ;

	q1.removeAttributes();
	return ( q1 ) ;
}

/*
	isnull ( op )
	
	isnull ( na_main )
	isnull ( na_main#obs_value )
	na_main [ filter isnull ( obs_value ) ]
 */
Query inte_isnull ( ) throws VTLError
{	
	Query				q1 ;
	DatasetComponent	m1 ;

	q1 = this.child.inte ( ).copy() ;		// first operand is a dataset or scalar
	
	if ( q1.isMultiMeasure() )
		VTLError.TypeError( "isnull can be applied only to a mono-measure boolean operand");

	m1 = q1.getFirstMeasure() ;
	
	m1.compName = "bool_var" ;
	m1.compType = "boolean" ; 

	q1.sqlWhere = m1.sql_expr + " IS NULL " ;
	q1.getFirstMeasure().sql_expr = "CASE WHEN " + q1.sqlWhere + " THEN 'true' ELSE 'false' END " ;

	q1.removeAttributes();
	return ( q1 ) ;
}

/*
	match_characters ( op , pattern )

	na_main [ filter match_characters ( obs_status, "s" ) ]
	match_characters ( na_main#obs_status, "s" )
*/
Query inte_match_characters ( ) throws VTLError
{	
	Query				q1, q2 ;
	DatasetComponent 	m1, m2 ;
	
	q1 = this.child.inte ( ).copy() ;		// first operand is a dataset or scalar
	q2 = this.child_2().inte();
	m1 = q1.getFirstMeasure() ;
	m2 = q2.getFirstMeasure() ;
	
	if ( q1.isMultiMeasure() || q2.isMultiMeasure() )
		VTLError.TypeError( "match_characters can be applied only to mono-measure boolean operands");

	Query.checkTypeOperand( "string" , m1.compType ) ;
	Query.checkTypeOperand( "string" , m2.compType ) ;
	m1.compName = "bool_var" ;
	m1.compType = "boolean" ; 
	
	q1.sqlWhere = "REGEXP(" + m1.sql_expr + "," + m2.sql_expr + ")" ;
	q1.getFirstMeasure().sql_expr = 
		"CASE WHEN " + q1.sqlWhere + " THEN 'true' WHEN NOT (" + q1.sqlWhere + ") THEN 'false' END " ;
	
	q1.removeAttributes();
	return ( q1 ) ;
}

/*
 * Membership operator "#"
 * Cannot be used with dimensions.
	aact_ali01 # obs_value
	aact_ali01 # obs_status
	aact_ali01 # obs_decimals
	aact_ali01 # geo
*/
Query inte_membership ( ) throws VTLError
{
	String				selectedComp ;
	DatasetComponent	pro ;
	DatasetComponent	dim ;
	Query				q1 ;
	
	selectedComp = this.child.next.val ;

	if ( this.child.name == Nodes.N_VARIABLE ) { // is this a dataset within a join?
		int		varIndex ;
		if ( ( varIndex = Env.getVarIndex ( this.child.val + "#" + selectedComp ) ) >= 0 ) {
			q1 = Query.operatorConstant(selectedComp, (String)Env.getVarType(varIndex), (String)Env.getScalarValue(varIndex)) ;
			return ( q1 ) ;
		}
	}
	
	q1 = this.child.inte( ).copy();
		
	if ( ( pro = q1.getMeasure( selectedComp ) ) != null ) {
		q1.removeMeasures(); 
		q1.addMeasure ( selectedComp, pro.compType, pro.sql_expr ) ; 		
	}
	else if ( ( dim = q1.getDimension( selectedComp ) ) != null) {
		q1.removeMeasures();
		q1.addMeasure( Query.scalarMeasureDefaultName ( dim.compType ), dim.compType, dim.sql_expr ) ; 		
	}
	else if ( ( pro = q1.getAttribute( selectedComp ) ) != null ) {
		q1.removeMeasures() ;						// attribute becomes a measure
		q1.addMeasure ( Query.scalarMeasureDefaultName ( pro.compType ), pro.compType, pro.sql_expr ) ;
	}
	else
	  	VTLError.TypeError ( "Dataset component " + selectedComp + " not found in expression" ) ;

	q1.propagateAttributesUnary();
	return ( q1 ) ;
}

/*
 * print expression to file or screen
	
	print na_main order by ref_area, time desc;
 */
public String inteOrderBy ( Query q ) throws VTLError
{
	StringBuffer 	str = new StringBuffer () ;
	
	for ( Node p = this; p != null ; p = p.next ) {
		if ( str.length() > 0 )
			str.append( ',' ) ;
		if ( ! q.hasComponent ( p.val ) )
			VTLError.TypeError( "order by: " + p.val + " is not a component of the dataset");
		str.append ( p.val ).append ( ' ' ).append( p.next.val ) ;
		p = p.next ;
	}
	
	return ( str.toString() ) ;
}

/*
	op [ calc { calcRole } calcComp := calcExpr { , { calcRole } calcComp := calcExpr }* ]
	op [ aggr aggrClause { groupClause } ]
	op [drop compList ]
	op [keep compList]
	op [filter booleanCondition ]
	op [rename c1 to c2 { , c1 to c2 }* ]
	op [pivot id1 , me1 ]
	op [unpivot id1 , me1 ]
	ds [sub ident = value { , ident = value }* ]
	
	na_main [calc x1 := obs_value + 100, x2 := obs_value + 200 ]
	na_main [drop obs_value ]			// error
	na_main [drop obs_status ]
	na_main [keep obs_value]
	na_main [keep obs_status ]			// error
	na_main [filter obs_value > 40 and obs_status = "A" ]
	na_main [rename obs_value to x1, obs_status to x2 ]
	na_main [pivot ref_sector , obs_value ]
	na_main [pivot ref_sector , obs_value ] [unpivot ref_sector , obs_value ]
	na_main [sub ref_area = "DK", time = 2014 ]
	
	calcRole ::=  identifier  |  measure  | attribute  |  viral attribute
	aggrClause ::= { aggrRole } aggrComp := aggrExpr { , { aggrRrole } aggrComp:= aggrExpr }*
	aggrRole::=  measure  |  attribute  |  viral attribute

 */
Query inteClauseOperator ( ) throws VTLError
{
	Node 	hd ;
	Query 	q  ; 
	
	// dataset operand
	q = this.child.inte( ).copy() ;
	if ( q.isScalar() )
		VTLError.TypeError( "operand of clauses is not a dataset");
	
	// clause operator
	hd = this.child_2() ;

	switch ( hd.name ) {
    	case Nodes.N_CLAUSE_SUBSCRIPT :
    		ListString	lsNames = new ListString () ;
    		ListString	lsValues = new ListString () ;
    		for ( Node p1 = hd.child ; p1 != null ; p1 = p1.next ) {
    			lsNames.add ( p1.child.val ) ;
    			lsValues.add ( p1.child.next.inteEvalScalar ( ) ) ;
    		}
    		q.subscript ( lsNames, lsValues ) ;
    		break ;
		case Nodes.N_CLAUSE_DROP :
			q.dropComponents ( hd.child.listDimensions () ) ;
			break ;
		case Nodes.N_CLAUSE_KEEP :
			q.keepComponents ( hd.child.listDimensions () ) ;
			break ;
		case Nodes.N_CLAUSE_CALC :
			Env.addfunctionCall("calc");
			q.addToEnvironment( );
			hd.inteClauseCalc ( q ) ;
			Env.removefunctionCall();
			break;
		case Nodes.N_CLAUSE_AGGR :
			Env.addfunctionCall("aggr");
			q.copy().addToEnvironment( );
			q = hd.inteClauseAggr ( q ) ;
			Env.removefunctionCall();
			break;
		case Nodes.N_CLAUSE_FILTER :
			Env.addfunctionCall("filter");
			q.addToEnvironment( );
			Query q1 = hd.child.inte ( ) ;
			Env.removefunctionCall();
			q1.checkBooleanMonoMeasure ( "filter" ) ;
			q.addWherePart( q1.sqlWhere ) ;
			q.setDoFilter();
			break ;
		case Nodes.N_CLAUSE_RENAME :
			ListString 	listFrom = new ListString (), listTo = new ListString () ;
			for ( Node pun = hd.child; pun != null; pun = pun.next ) {
				listFrom.add (pun.val) ;
				pun = pun.next ;
				listTo.add(pun.val) ;
			}
			q.renameComponents ( listFrom, listTo ) ;
			break ;
		case Nodes.N_CLAUSE_PIVOT :
			DatasetComponent pivotDim = q.getDimension( hd.child.val ) ;
			if ( pivotDim == null )
				VTLError.TypeError( "pivot: cannot find dimension " + hd.child.val );
			DatasetComponent pivotMeasure = q.getMeasure( hd.child.next.val ) ;
			if ( pivotMeasure == null )
				VTLError.TypeError( "pivot: cannot find measure " + hd.child.next.val );
			q.pivot( pivotDim, pivotMeasure );
			break ;
			
		case Nodes.N_CLAUSE_UNPIVOT :
			String foldDim = hd.child.val ;
			if ( q.getDimension( foldDim ) != null )
				VTLError.TypeError( "unpivot: duplicate dimension " + foldDim );
			String foldMeasure = hd.child.next.val ;
			if ( q.getMeasure( foldMeasure ) != null )
				VTLError.TypeError( "unpivot: duplicate measure " + foldMeasure );
			q.unpivot ( foldDim, foldMeasure ) ;
			break ;

		default : VTLError.InternalError( "unknown clause operator: " + hd.name ) ;
	}

	return ( q ) ;
}

/*
 * Delete all measures/attributes whose name contains "#" followed by compName
 * return the role of the component found
 * raise error if the component found is a dimension
 */
static boolean removeMembershipComponents ( Query q, String compName ) throws VTLError
{
	String		cc = "#" + compName ;
	ListString	ls = new ListString ();
	boolean		found = false ;
	
	// measures
	for ( DatasetComponent c : q.measures ) {
		if ( c.compName.contains( cc ) ) {
			found = true ;
			ls.add (c.compName) ;
		}
	}
	for ( String s : ls )
		q.measures.remove(q.getMeasure(s)) ;
	
	// attributes
	for ( DatasetComponent c : q.attributes ) {
		if ( c.compName.contains( cc ) ) {
			found = true ;
			ls.add (c.compName) ;
		}
	}
	for ( String s : ls )
		q.attributes.remove(q.getAttribute(s)) ;
	/*
	// dimensions
	for ( DatasetComponent c : q.dims ) {
		if ( c.compName.equals( compName ) || c.compName.contains( cc ) ) {
			VTLError.TypeError( "Cannot redefine an identifier component (" + compName +")" );
		}
	}
	*/
	return ( found ) ;
}

/*
 * Build dimensions of the join expression.
 * add the SQL FROM and WHERE clauses ( INNER JOIN, LEFT OUTER JOIN,FULL OUTER JOIN, CROSS JOIN )
 * inner_join: 
 * left_join:
 * full_join
 * cross_join
 * 
 * cross_join ( myview#obs_value as a, myview [ rename obs_value to v1 ] [ keep [ v1 ] ] as b )
 * 
 * cross_join ( myview#obs_value as a, myview [ rename obs_value to v1 ] [ keep [ v1 ] ] as b 
	rename a#ref_sector to x1, a#activity to x2, a#time to x3 )
 */
static Query joinDimensions ( Vector <Query> queries, String joinType, ListString usingDims, ListString aliases ) throws VTLError
{
	Query	qRes ;
	
	switch ( joinType ) {	
		case "inner_join" :	
		case "left_join"  : 
		case "full_join" :
			// inner_join ( ds_bop [ ref_area="DE"] as d1, ds_bop [ ref_area="IT"] as d2 apply d1 + d2 )
			// left_join ( myview  as ds1, myview  as ds2 apply ds1 + ds2 * 100 drop [ ds1#obs_status, ds2#obs_status] )
			qRes = queries.get(0).copyDimensions( usingDims );
			for ( Query q2 : queries ) {
				if ( q2 != queries.get(0) ) {
					qRes.join( q2, usingDims, joinType );
					if ( q2.sqlWhere != null)
						qRes.addWherePart( q2.sqlWhere ) ;	
					if ( q2.doFilter )
						qRes.setDoFilter();
				}
			}
			break ;
		case "cross_join" : 
			if ( usingDims != null )
				VTLError.TypeError( "cannot specify \"using\" with cross_join");
			qRes = new Query ( ) ;
			for ( int idx = 0; idx < queries.size(); idx ++ ) {
				Query q2 = queries.get(idx);
				for ( DatasetComponent d : q2.dims )
					qRes.addDimension( aliases.get(idx) + "#" + d.compName, d.compType, d.sql_expr, d.dim_values);
				qRes.sqlFrom = ( qRes.sqlFrom == null ? q2.sqlFrom : qRes.sqlFrom + " CROSS JOIN " + q2.sqlFrom ) ;
				if ( q2.sqlWhere != null )
					qRes.addWherePart( q2.sqlWhere ) ;
				if ( q2.doFilter )
					qRes.setDoFilter();
			}
			break ;
		default : 
			qRes = null ;
	}

	return ( qRes ) ;
}

/*
 * if role = identifier then expression must be a constant value
 */
void inteClauseCalc ( Query q ) throws VTLError
{
	for ( Node p = this.child ; p != null ; p = p.next ) {
		String	compRole, compName ;
		compRole = (p.child.name == Nodes.N_EMPTY ? null : p.child.val);
		compName = p.child_2().val ;
		Query q2 = p.child_3().inte () ;
		String compType = q2.getFirstMeasure().compType ;
		String compvalue = q2.getFirstMeasure().sql_expr ;		// TBD mono-measure
		q.calcAddRedefineComponent ( compRole, compName, compType, compvalue, p.child_3().child == null ) ;
	}
}

/*
 * 2 children: join clause and clauses
	joinOp ( ds1 { as alias1 }, … ,dsN { as aliasN }
		{ using usingComp }
		{ filter filterCondition }
		{ apply applyExpr | calc calcClause | aggr aggrClause { groupingClause } }
		{ keep comp {, comp }* | drop comp {, comp }*  }
		{ rename copFrom to cmpTo { , cmpFrom to cmpTo }* }			
	)
	joinOp ::= { inner_join | left_join| full_join | cross_join }1

	inner_join ( na_main as a , na_main as b )						// error: duplicated component
	inner_join ( na_main as a , na_main as b keep a#obs_value )
	inner_join ( na_main as a , na_main as b drop a#obs_value )		// error
	inner_join ( na_main as a , na_main as b calc v1 := a#obs_value + b#obs_value )		// error
	inner_join ( na_main as a , na_main as b calc obs_value := a#obs_value + b#obs_value keep obs_value )
	inner_join ( na_main as a , na_main as b calc v1 := 1, v2 := "A" )
	inner_join ( na_main[sub time = 2014] as a , na_main[sub time = 2014] as b 
		calc x1 := a#obs_value + b#obs_value, x2 := "A" 
		keep x1, x2 )
	inner_join ( na_main[sub time = 2014] as a , na_main[sub time = 2014] as b
		aggr x1 := sum (a#obs_value) group by ref_sector )
*/
Query inteJoin ( ) throws VTLError
{
	Node 			p ;
	Query 			qRes, q2  ; 
	ListString		aliases = new ListString() ;
	ListString		usingDims = null ;
	Vector <Query>	queries = new Vector <Query> ( ) ;
	String			joinType = this.val ;
	
	for ( p = this.child.child; p != null; p = p.next.next ) {
		q2 = p.inte() ;
		if ( q2.isScalar() )
			VTLError.TypeError( "Operand of join is not a dataset");
		queries.add( q2 ) ;
		aliases.add( p.next.name == Nodes.N_EMPTY ? p.val : p.next.val ) ;		// can be empty (identifier) or alias
	}
	usingDims = ( this.child_2().name == Nodes.N_EMPTY ? null : this.child_2().child.listDimensions() ) ;
	
	qRes = joinDimensions ( queries, joinType, usingDims, aliases ) ;
	// add measures and attributes
	for ( int idx = 0; idx < queries.size(); idx ++ ) {
		for ( DatasetComponent c2 : queries.get(idx).measures ) 
			qRes.addMeasure ( aliases.get(idx) + "#" + c2.compName, c2.compType, c2.sql_expr );
		for ( DatasetComponent c2 : queries.get(idx).attributes ) 
			qRes.addAttribute ( aliases.get(idx) + "#" + c2.compName, c2.compType, c2.sql_expr );		
	}

	Env.addfunctionCall( "join" );
	// add the datasets and dimensions to the environment
	//for ( int idx = 0; idx < queries.size(); idx ++ )
		//Env.addVar ( aliases.get(idx), queries.get(idx), null ) ;
	qRes.addToEnvironment();
	// add the components specified in the using list
	if ( usingDims != null ) {
		for ( String dim : usingDims ) {
			if ( qRes.getDimension( dim ) == null )
				VTLError.TypeError( "using: dimensions must be common to all datasets (" + usingDims.toString() + ")");
			//Env.addVar ( d.compName, d.compType, d.sql_expr ) ;
		}		
	}
	
	// process the clauses
	p = this.child_3() ;
	if ( p.name != Nodes.N_CLAUSE_OP )
		VTLError.InternalError( "join: clause operators");
	p = p.child ;	// NB: p can be null (no clauses)
	if ( p != null && p.name == Nodes.N_CLAUSE_FILTER ) {
		Query q1 = p.child.inte ( ) ;
		q1.checkBooleanMonoMeasure ( "filter" ) ;
		qRes.addWherePart( q1.sqlWhere ) ;
		qRes.setDoFilter();
		p = p.next ;	// go to the next clause
	}
	if ( p != null && p.name == Nodes.N_CLAUSE_APPLY ) {
		q2 = p.child.inte () ;
		for ( DatasetComponent c : q2.measures ) {
			removeMembershipComponents ( qRes, c.compName ) ;				// find role and delete components
			qRes.addMeasure( c.compName, c.compType, c.sql_expr ) ;
		}
		// 	inner_join ( ds_bop as d1, ds_bop as d2 apply d1 + d2 )
		p = p.next ;	// go to the next clause
	}
	if ( p != null && p.name == Nodes.N_CLAUSE_CALC ) {	
		p.inteClauseCalc ( qRes ) ;
		for ( Node pCalc = p.child ; pCalc != null ; pCalc = pCalc.next )
			removeMembershipComponents ( qRes, pCalc.child_2().val ) ;
		p = p.next ;	// go to the next clause
	}
	
	if ( p != null && p.name == Nodes.N_CLAUSE_AGGR ) {
		qRes = p.inteClauseAggr ( qRes ) ;
		p = p.next ;	// go to the next clause
	}

	if ( p != null && p.name == Nodes.N_CLAUSE_KEEP ) {
		qRes.keepComponents (  p.child.listDimensions () ) ;
		p = p.next ;	// go to the next clause
	}
	if ( p != null && p.name == Nodes.N_CLAUSE_DROP ) {
		// inner_join ( na_main as d1, na_main as d2 calc x := 1 drop d1#obs_value, d2#obs_value )
		for ( String c : p.child.listDimensions () ) {
			if ( ! removeMembershipComponents(qRes, c) )
				qRes.dropComponent ( c ) ;			
		}
		p = p.next ;	// go to the next clause
	}
	if ( p != null && p.name == Nodes.N_CLAUSE_RENAME ) {
		ListString renameFrom, renameTo ;
		renameFrom = new ListString () ;
		renameTo = new ListString () ;
		for ( Node pun = p.child; pun != null; pun = pun.next ) {
			renameFrom.add (pun.val) ;
			pun = pun.next ;
			renameTo.add(pun.val) ;
		}
		qRes.renameComponents ( renameFrom, renameTo ) ;
		
		p = p.next ;	// go to the next clause
	}
	Env.removefunctionCall(); 		

	qRes.joinReturnCheckQuery ( joinType, usingDims );

	return ( qRes ) ;
}

/*
 * NB: conflict with the if statement
 * then, else parts must have the same components
 * if can be a scalar or a dataset joinable to the then part
 	print if 1=2 then  1 else 2
 	print if 1=2 then  aact_ali01 else aact_ali02[ unit=I05]
	print if aact_ali01 # obs_value > 100 then  aact_ali01 else aact_ali02[ unit=I05] 
	( if 2 > 3 then null else 3 )
 */
Query inteIfThenElse ( ) throws VTLError 
{
	Query			q_cond, q_then, q_else ;
	String			cond ;

	q_cond = this.child.inte ();
	q_then = this.child.next.inte ().copy() ;
	q_else = this.child.next.next.inte ();
	
	q_cond.checkBooleanMonoMeasure ( "if-then-else" ) ;
	cond = q_cond.sqlWhere ;		// .getFirstMeasure().sql_expr ; 
	
	if ( q_then.isScalar() && q_else.isScalar() ) {
		if ( ! q_cond.isScalar() )
			VTLError.TypeError( "if-then-else: condition must be scalar when then and else part are scalar");
		
		DatasetComponent m_then = q_then.getFirstMeasure() ;
		DatasetComponent m_else = q_else.getFirstMeasure() ;
		m_then.compType = Query.deriveType ( m_then.compType, m_else.compType ) ;
		m_then.sql_expr = "CASE WHEN " + cond + " THEN " + m_then.sql_expr + " ELSE " + m_else.sql_expr + " END " ;
	}
	else {
		q_then.checkIdenticalDimensions( q_else, "if-then-else" ) ;
		q_then.checkIdenticalDimensions( q_cond, "if-then-else" ) ;
		
		q_then.checkIdenticalMeasures ( q_else, "if-then-else" ) ;
		q_then.checkIdenticalAttributes ( q_else, "if-then-else" ) ;
		q_then.dimJoinInner( q_else ) ;
		q_then.dimJoinInner( q_cond );
		for ( DatasetComponent m_then : q_then.measures ) {
			DatasetComponent m_else = q_else.getMeasure( m_then.compName ) ;
			m_then.sql_expr = "CASE WHEN " + cond + " THEN " + m_then.sql_expr + " ELSE " + m_else.sql_expr + " END " ;
			m_then.compType = Query.deriveType ( m_then.compType, m_else.compType ) ;
		}
		for ( DatasetComponent m_then : q_then.attributes ) {
			DatasetComponent m_else = q_else.getAttribute( m_then.compName ) ;
			m_then.sql_expr = "CASE WHEN " + cond + " THEN " + m_then.sql_expr + " ELSE " + m_else.sql_expr + " END " ;
			m_then.compType = Query.deriveType ( m_then.compType, m_else.compType ) ;
		}
	}

	return ( q_then ) ;
}

/*
 * Evaluate expression to produce a list of scalar values.
 * a list object cannot be null
 * dataType can be null (means do not check the values)
*/
ListString inteSetScalar ( String scalarType ) throws VTLError
{
	ListString	myList ;
	Node		p ;

	p = this.child ;

	switch ( this.name ) {
    	case Nodes.N_SET_SCALAR :
    		myList = new ListString ( p == null ? 0 : p.nodeListLength ( ) ) ;
    		for ( ; p != null; p = p . next ) {
    			String tmp ;
    			tmp = p.inteEvalScalar ( ) ;
    			if ( tmp == null || tmp.length () == 0 )
    				VTLError.RunTimeError ( "Found item in list whose value is null" ) ;
    			if ( myList.indexOf ( tmp ) >= 0 )
    				VTLError.RunTimeError ( "Item duplicated in set scalar: " + tmp ) ;
    			myList.add ( tmp ) ;
    		}
    		if ( scalarType != null )
    			Check.checkLegalValues ( scalarType, myList ) ;
    		break ;
    	  
    	case Nodes.N_IDENTIFIER :
    		myList = Dataset.getValuedomainCodeList( this.val ) ;
    		break ;
    	     	 
    	case Nodes.N_VARIABLE :
    		int 	idx = Env.getVarIndex( this.val ) ;
    		String	sType = Check.baseTypeOfSetType ( (String) Env.getVarType( idx ) ) ;
    		Query.checkTypeOperand( "in", sType, scalarType ) ;
    		myList = Env.getSetScalarValue( idx ) ;
    		break ;
    	     	 
    	default :
    		VTLError.InternalError ( "Unknown list operator: " + this . name ) ;
    		myList = null ;
	}

	return ( myList ) ;
}

/*
 * "in" and "not in" operator.
 	na_main#ref_area in { "IT", "FR" } 
 	na_main[ filter ref_area in { "IT", "FR" } ] 
	op can be " IN " or " NOT IN "
 */
Query inteIn ( String sqlOp ) throws VTLError
{
	ListString		ls ;
	Query			q1 = this.child.inte( ).copy() ;
	String			compExpr ;

	q1.checkMonoMeasure( this.val );

	ls = this.child.next.inteSetScalar ( q1.getFirstMeasure().compType ) ;
	q1.sqlWhere = ls.sqlSyntaxInList(q1.getFirstMeasure().sql_expr, sqlOp.toUpperCase()) ;
	compExpr = "CASE WHEN " + q1.sqlWhere + " THEN 'true' WHEN NOT (" + q1.sqlWhere + ") THEN 'false' END" ;
	q1.removeMeasures();
	q1.addMeasure( "bool_var", "boolean", compExpr );	
	q1.removeAttributes();
	
	return ( q1 ) ;
}

/*
 * Identifier (object_name)
 */
Query inte_identifier ( ) throws VTLError
{
	Dataset	ds ;
	int		var_index ;
	String	ide = this.val ;
	
	if ( ( var_index = Env.getVarIndex( ide ) ) >= 0 ) {
		if ( Env.getVarType ( var_index ) == null )
			return ( Env.getQueryValue (var_index ) ) ;
		return ( Query.operatorConstant ( ide, (String) Env.getVarType ( var_index ), Env.getScalarValue(var_index) ) ) ;
	}
	else {
		ds = Dataset.getDatasetDesc ( ide ) ; 
		return ( ds.convert2query() ) ;
	}
}

/*
 * Variable.
A := na_main ;
B := A ;
inner_join(A,B calc obs_value := A#obs_value + 1 keep [obs_value] )
x := "DK" ;  na_main [ filter ref_area = x ]
 */
public Query inteVariable ( ) throws VTLError
{
	int 		varIndex ;
	Object		varType ;
	String		varName = this.val ;
	
    if ( ( varIndex = Env.getVarIndex ( varName ) ) >= 0 ) {		// local variable
        varType = Env.getVarType ( varIndex ) ;
        if ( varType instanceof Query ) {
        	Query qorig = (Query) varType;
        	if (Env.getScalarValue ( varIndex ) == null)			// it is a parameter, not a variable (assignment)
        		return ( qorig ) ;
        	// it is a variable (assignment)
        	if ( qorig.sqlFrom == null )							// scalar or multiple measures with 0 dimensions
        		return ( qorig ) ;
        	Query q = qorig.refTo (varName, Query.newAlias () );
        	return ( q ) ;
        }
        else {
        	return ( Query.operatorConstant ( Env.getScalarValue ( varIndex ), varType.toString() )) ;
        }
    }
    
    // top level variable
	if ( ( varIndex = Env.getVarIndexTopLevel ( varName ) ) < 0 )
		VTLError.InternalError ("Unknown variable: " + varName + "\n(" + Env.printEnvironment () + ")" ) ;
    varType = Env.getVarTypeTopLevel ( varIndex ) ;
    if ( ! ( varType instanceof Query ) )
    	VTLError.InternalError ("Top level variable (bad query): " + varName + "\n(" + Env.printEnvironment () + ")" ) ;
	Query qorig = (Query) varType;
	if ( qorig.sqlFrom == null )						// scalar or multiple measures with 0 dimensions
		return ( qorig ) ;
	Query q = qorig.refTo (varName, Query.newAlias () );
	return ( q ) ;
}

/*
 * Variable.
A := na_main ;
B := A ;
inner_join(A,B calc obs_value := A#obs_value + 1 keep [obs_value] )
 */
public Query inteGeneriComponent ( ) throws VTLError
{
	int 		varIndex ;
	Object		varType ;
	String		varName = this.val ;
	
    if ( ( varIndex = Env.getVarIndex ( varName ) ) < 0 ) 
    	VTLError . InternalError ("Unknown component: " + varName + "\n(" + Env.printEnvironment () + ")" ) ;
    
    varType = Env.getVarType ( varIndex ) ;
   	return ( Query.operatorConstant ( Env.getScalarValue ( varIndex ), varType.toString() )) ;
}

/*
 * check
	check ( op { errorcode errorcode } { errorlevel errorlevel } 
		{ imbalance imbalance } { output } )
	output ::= all | invalid 
 */
Query inteCheck (  ) throws VTLError
{
	Query 	q ;
	Node	p = this.child ;
	String	errorcode, errorlevel, optionOutput ;
	Query	imbalance ;
	
	q = p.inte ( ) ;													// 1st operand - dataset
	p = p.next ;
	errorcode = ( p.name == Nodes.N_EMPTY ? null : p.inteEvalScalar( ) ) ;
	p = p.next ;
	errorlevel = ( p.name == Nodes.N_EMPTY ? null : p.inteEvalScalar( ) ) ;	
	p = p.next ;
	imbalance = ( p.name == Nodes.N_EMPTY ? null : p.inte( ) ) ;	
	p = p.next ;
	optionOutput = p.val ;
	
	return ( q.checkSingle ( errorcode, errorlevel, imbalance, optionOutput ) ) ;
}

/*
 * check_datapoint
	check_datapoint ( op , dpr { components listComp } { output output } )
	output ::= invalid | all | all_measures
 */
Query inteCheckDataPoint ( ) throws VTLError
{
	Query 		q ;
	Node		p = this.child ;
	String		dprName ;
	ListString	components ;
	String		output ;
	
	q = p.inte ( ) ;													// 1st operand - dataset
	p = p.next ;
	dprName = p.val ;
	p = p.next ;
	components = ( p.name == Nodes.N_EMPTY ? null : p.child.listDimensions() ) ;
	p = p.next ;
	output = p.val ;
	return ( DatapointRuleset.checkDataPoint ( q, dprName, components, output ) ) ;	
}

/*
	check_hierarchy ( op , hr 
	{ condition condComp { condComp }* } 
	{ rule ruleComp }
	{ mode } { input } { output } )	
	mode ::= non_null | non_zero | partial_null | partial_zero | always_null | always_zero 
	input ::=  dataset | dataset_priority 
	output ::= invalid | all | all_measures
 */
Query inteCheckHierarchy ( ) throws VTLError
{
	Query 		q ;
	Node		p = this.child ;
	ListString	condComps ;
	String		rsName, ruleComp, mode, input, output ;
	
	// dataset
	q = p.inte ( ) ;
	p = p.next ;
	
	// ruleset name
	rsName = p.val ;
	p = p.next ;
	
	// condition components
	condComps = ( p.name == Nodes.N_EMPTY ? null : p.child.listDimensions() ) ;
	p = p.next ;

	// rule component
	ruleComp = ( p.name == Nodes.N_EMPTY ? null : p.val ) ;
	p = p.next ;
	// mode, input, output
	mode = ( p.name == Nodes.N_EMPTY ? null : p.val ) ;
	p = p.next ;
	input = ( p.name == Nodes.N_EMPTY ? null : p.val ) ;
	p = p.next ;
	output = ( p.name == Nodes.N_EMPTY ? null : p.val ) ;

	return ( HierarchicalRuleset.checkHierarchy ( q, rsName, condComps, ruleComp, mode, input, output ) ) ;	
}

/*
	hierarchy ( op , hr { condition condComp { , condComp }* } { rule ruleComp } 
					{ mode } { input } { output } )
	mode ::= non_null | non_zero | partial_null | partial_zero | always_null | always_zero
	input ::= rule | rule_priority | dataset
	output ::= computed | all
*/
Query inteHierarchy ( ) throws VTLError
{
	Query 		q ;
	Node		p = this.child ;
	ListString	condComps ;
	String		rsName, ruleComp, mode, input, output ;
	
	// dataset
	q = p.inte ( ) ;
	p = p.next ;
	
	// ruleset name
	rsName = p.val ;
	p = p.next ;
	
	// condition components
	condComps = ( p.name == Nodes.N_EMPTY ? null : p.child.listDimensions() ) ;
	p = p.next ;
	
	// rule component
	ruleComp = ( p.name == Nodes.N_EMPTY ? null : p.val ) ;
	p = p.next ;
	// mode, input, output
	mode = ( p.name == Nodes.N_EMPTY ? null : p.val ) ;
	p = p.next ;
	input = ( p.name == Nodes.N_EMPTY ? null : p.val ) ;
	p = p.next ;
	output = ( p.name == Nodes.N_EMPTY ? null : p.val ) ;
	
	return ( HierarchicalRuleset.hierarchy(q, rsName, condComps, ruleComp, mode, input, output ) ) ;	
}

/*
	eval   (  extRoutineName ( { argument }  { , argument }* )
				language language
				returns  outputType )
	eval ( get_value ( "SELECT 'aa' FROM DUAL" ) language "SQL" returns string )
	Possible new functions:
	ds := eval ( load ( "filepath", ds, "merge/replace", "autoextend", "separator" ) language "OS" returns dataset )
	s := eval ( mdt_merge_flags ( "E", "PR" ) language "PL/SQL" returns string )
 */
Query inteEval ( ) throws VTLError
{
	Query 			q1 ;
	Node			p = this.child ;
	String			functionName, language, returnType, resultValue ;
	StringBuffer	arguments = new StringBuffer ();
	
	functionName = p.val ;
	for ( Node parg = p.child; parg != null ; parg = parg.next ) {
		if ( arguments.length() > 0 )
			arguments.append( "," ) ;
		if ( parg.name == Nodes.N_STRING )
			arguments.append( "'" + parg.val + "'" ) ;
		else {
			q1 = parg.inte() ;
			arguments.append( q1.getFirstMeasure().sql_expr ) ;			
		}
	}
	
	q1 = p.next.inte() ;
	language = q1.getFirstMeasure().sql_expr.replaceAll( "'", "") ;
	returnType = p.next.next.val ;
	switch ( language ) {
		case "PL/SQL" :
			if ( ! returnType.equals( "string" ) )
				VTLError.RunTimeError( "Operator eval: bad return type" );
			
			resultValue = "'" + Db.sqlGetValue( "SELECT " + functionName 
					+ "(" + arguments.toString() + ") FROM dual", true ) + "'" ;
			break ;
		case "SQL" :
			int numArguments = p.countChildren() ;
			if ( functionName.equals( "get_value") ) {
				if ( numArguments != 1 )
					VTLError.RunTimeError( "Operator eval: get_value has 1 parameter" );
				if ( ! returnType.equals( "string") )
					VTLError.RunTimeError( "Operator eval: get_value returns string" );
				resultValue = "'" + Db.sqlGetValue( arguments.toString(), true ) + "'" ;				
			}
			else {
				VTLError.RunTimeError( "Operator eval: not implemented " + functionName );
				resultValue = null ;
			}
			break ;
		default :
			VTLError.RunTimeError( "Operator eval: bad language " + language );
			resultValue = null ;
	}
	
	return ( Query.operatorConstant( resultValue, returnType) ) ;
}

/*
dataset component ::= role scalartype compName
scalartype ::= name constraint null/not null
*/
Query makeDatasetType ( ) throws VTLError
{
	Query	q = new Query () ;
	for ( Node p = this.child;  p != null ; p = p.next ) {
		String 	compName = p.child_3().val ;
		String	compType = p.child_2().child.val ;
		boolean nullable = ( p.child_2().child_3().val == "null" ) ;
		switch ( p.child.val ) {
			case "identifier" : q.addDimension( compName, compType, null); break ;
			case "measure" : 	q.addMeasure(compName, compType, compName, nullable) ; break ;
			case "attribute" : 	q.addAttribute( compName, compType, compName, nullable) ; break ;
			case "viral attribute" : q.addViralAttribute( compName, compType, compName, nullable) ; break ;
			default : VTLError.InternalError( "Bad component type");
		}
	}
	return ( q ) ;
}
/*
 * Evaluate the constraint part of a scalar type.
 * A boolean condition is allowed only if scalara type is a valuedomain.
 */
ListString evalConstraint ( String vdName ) throws VTLError
{
	if ( this.name != Nodes.N_EMPTY ) {
		if ( this.name == Nodes.N_SET_SCALAR )
			return ( this.inteSetScalar( vdName ) ) ;
		else {
			if ( ! Check.isValueDomainType( vdName ) )
				VTLError.TypeError( "Type constraint: boolean condition is allowed only on a valuedomain");
			Env.addfunctionCall( "Type constraint");
			Env.addVar( "value", vdName, vdName ) ;
			Query q = this.inte() ;
			Env.removefunctionCall();
			q.checkBooleanMonoMeasure( "Type constraint");
			return ( Db.sqlFillArray( " SELECT " + vdName + " FROM " + vdName + " WHERE " + q.sqlWhere ) ) ;
		}	
	}
	
	return ( new ListString () ) ;
}

/*
dataset component ::= role scalartype compName
scalartype ::= name constraint null/not null
*/
void checkArgDatasetType ( Query q ) throws VTLError
{
	for ( Node p = this.child;  p != null ; p = p.next ) {
		String 	compName = p.child_3().val ;
		String	compType = p.child_2().child.val ;
		boolean nullable = ( p.child_2().child_3().val == "null" ) ;
		switch ( p.child.val ) {
			case "identifier" : 
				if ( q.getDimension(compName) == null )
					VTLError.TypeError( "User defined operator: cannot find component " + compName );
				break;
			case "measure" : 
				if ( q.getMeasure(compName) == null )
					VTLError.TypeError( "User defined operator: cannot find component " + compName );
				break;
			case "attribute" : 
			case "viral attribute" :
				if ( q.getAttribute(compName) == null )
					VTLError.TypeError( "User defined operator: cannot find component " + compName );
				break;
			default : VTLError.InternalError( "Bad component type");
		}
	}
}

/*
 * User function call.
 * TBD: dataType = ruleset
	define operator mytest ( x integer, y integer ) is 
		 x+y
	end define operator;
	mytest( 1, 2)
	mytest( 1 )
 */
Query inteUserOperatorCall (  ) throws VTLError
{
	UserFunction		fun ;
	Node 				hd = this, p ;
	String				function_name ;
	int					var_index, num_arguments, num_parameters ;
	UserFunctionParm	parm ;
	Query				q ;
	String				var_type ;

	function_name = hd.val ;
	fun = UserFunction.getUserDefinedOperator ( function_name ) ;				  // fun.hd_body.printMdtSyntaxTree ();		// debug

	if ( fun.hd_body == null )
		VTLError.InternalError("User-defined operator call - bad function: " + function_name ) ;
	  
	// evaluate function arguments, put them on the stack
	num_arguments = hd.countChildren () ;
	num_parameters = fun.parameters.size() ;
	if ( num_arguments != num_parameters )
		VTLError.RunTimeError ( "User-defined operator " + function_name + " is called with wrong number of arguments: " + num_arguments  
				  	+ " instead of " + num_parameters )  ;
	// but the last parameters can be omitted
	  
	Env.addfunctionCall(function_name);
	  
	for ( p = hd.child, var_index = 0; p != null; p = p.next, var_index ++ ) {
		parm = fun.parameters.get ( var_index ) ;

		switch ( parm.parmType.name ) {
			case Nodes.N_TYPE_SCALAR :
				var_type = parm.parmType.val ;
				if ( p.name == Nodes.N_DEFAULT_VALUE ) {
					if ( parm.defaultValue != null )
						q = Query.operatorConstant ( parm.defaultValue, var_type ) ;
					else {
						VTLError.RunTimeError ( "User defined operator " + function_name 
									+ ": parameter " + parm.name + "is not optional" ) ;	
						q = null ;
					}
				}
				else
					q = p.inte () ;
				if ( ! Query.checkTypeOperand( var_type, q.getFirstMeasure().compType))
					Query.errorTypeOperand ( "User defined operator call: " + function_name , parm.name, (String)var_type , q.getFirstMeasure().compType ) ;
				Env.addVar( parm.name, var_type, q.getFirstMeasure().sql_expr ) ;
				break ;

			case Nodes.N_TYPE_SET :
				var_type = parm.parmType.child.val ;
				Env.addVar ( parm.name, "set " + var_type, p.inteSetScalar ( var_type ) ) ;
				break ;
				
			case Nodes.N_TYPE_DATASET :
				Query qParam = Command.buildQueryFromParameter ( parm.parmType ) ;
				q = p.inte () ;
				qParam.checkHasAllComponents(q);
				Env.addVar ( parm.name, q, null ) ;
		}
	}

	// execute function - list of statements
	try { 
		if ( fun.isOperator() )
			q = fun.hd_body.child.inte();
		else {			
			for ( Node pCmd = fun.hd_body.child ; pCmd != null; pCmd = pCmd . next ) {
			      if ( ! Command.eval_cmd ( pCmd, false ) )
			    	  break ;
			}
			q = Env.getReturnValue() ;
			if ( q == null && fun.return_type != null )
				VTLError.RunTimeError( "User function: " + Env.functionCalls.lastElement ( ).function_name + " does not return a value" ) ;
			if ( q != null && fun.return_type == null )
					VTLError.RunTimeError( "User function: " + Env.functionCalls.lastElement ( ).function_name + " returns a value but a return type is not declared" ) ;
		}
		Env.removefunctionCall();		// TBD: restore filters
	}
	catch ( VTLError e ) {
		Env.removefunctionCall();
		throw e ;
	}
	  
	return ( q ) ;
}

/*
	cast ( v , scalarType { , mask } )
	cast ( "2000Q1", time_period ) 
 */
Query inteCast ( Query q, String dataType, String format ) throws VTLError 
{
	q.checkMonoMeasure("cast" );
	q = q.copy() ;
	DatasetComponent m = q.getFirstMeasure() ;

	switch ( dataType ) {
		case "integer" :	Query.castToInteger( m, format ) ;			break ;
		case "number" :		Query.castToNumber( m, format ) ;			break ;
		case "boolean" :	Query.castToBoolean( m, format ) ;			break ;
		case "time" :													// time is treated as a synonym for time_period
		case "time_period" :Query.castToTimePeriod ( m, format ) ;		break ;
		case "date" :		Query.castToDate( m, format ) ;				break ;
		case "string" :		Query.castToString( m, format ) ;			break ;
		case "duration" :	Query.castToDuration( m, format ) ;			break ;
		default :
			if ( Check.isValueDomainType(dataType)) {
				// cast ( "IT", ref_area )
				if ( format != null )
					VTLError.TypeError( "cast to valuedomain: mask is not allowed" ) ;
				if ( q.dims.size() > 0 )
					VTLError.InternalError( "cast to valuedomain: only scalar value parameter is implemented" ) ;
				String	v = m.sql_expr ;
				if ( v.startsWith("'"))
					v = v.substring(1, v.length() -1 ) ;
				Check.checkLegalValue( dataType, v );
				String baseType = Dataset.getValuedomainBaseType(dataType) ;
				m.compType = baseType ;
				m.compName = Query.scalarMeasureDefaultName( baseType ) ;	
			}
			else
				VTLError.TypeError( "cast: unknown scalar type name " + dataType ) ;			
	}
	q.propagateAttributesUnary();
	return ( q ) ;
}

/*
 * Check that the dimensions are declared as condition dimensions.
 */
void checkDimensionsUsed ( ListString variable_names ) throws VTLError
{
	Node 		p ;
	if ( this.name == Nodes.N_VARIABLE ) {
		if ( variable_names == null || variable_names.indexOf( this.val ) < 0 ) 
			VTLError.RunTimeError( "Dimension " + this.val + " is used in a condition but has not been declared" ) ;
	}
	else {
		for ( p = this.child; p != null; p = p.next )
			p.checkDimensionsUsed ( variable_names ) ;
	}
}

/*
 * period_indicator  ( { op } )
 * can be called with no argument (q == null) within a filter/calc 
 * can be used on dataset, on component and scalar
	na_main [ filter period_indicator() = "A" ]
	na_main [ filter period_indicator(time) = "A" ]
	period_indicator( na_main )
	period_indicator( na_main#time )
 */
Query intePeriodIndicator ( Query q ) throws VTLError
{
	if ( q == null ) {
		Query 		q1 = getImplicitDataset ( "period_indicator" ) ;
		return ( q1.periodIndicator ( )	) ;
	}
	else {
		return ( q.periodIndicator ( )	) ;
	}		
}
/*
 * VTL block
	{ x := 10 ; y := x * 2 ; z := x * 50; z * y }
	{ x := aact_ali01#obs_value ; x + 1 }
	{ x := aact_ali01#obs_value ; y := aact_ali02#obs_value; x + y [ unit = "I05"]   }
	Error (duplicate variable)
	{ x := 10 ; x := 2 ; z := x * 50; z * y }
Query inte_block ( ) throws VTLError
{
	Query qres = null ;
	Env.addfunctionCall( "block" );
	for ( Node p = this.child ;  p != null ; p = p.next ) {
		if ( p.name == Nodes.N_VTL_ASSIGNMENT ) {
			String	var_name = p . val ;
			Query	qval = p.child.inte () ;  
			Env.addVar ( var_name, null, qval ) ;		
		}
		else
			qres = p.inte ( ) ;
	}
	Env.removefunctionCall();
	return ( qres ) ;
}
*/

Vector<Query> listDatasets ( ) throws VTLError
{
	Vector<Query> queries = new Vector<Query> () ;
	for ( Node p = this.child; p != null ; p = p.next )
		queries.add( p.inte ( ) ) ;
	return ( queries ) ;
}

/*
 * Interpreter, main function.
 * refin.obs_status - we need the base type
 * type any
 * type boolean
 */
public Query inte ( ) throws VTLError
{
	Query q1, q2 ;
	
	switch ( name ) {
	case Nodes . N_PARENTHESIS :
		return ( this.child.inte().parenthesis () ) ;	
		
	case Nodes . N_PLUS  :
	case Nodes . N_SUBTRACT  :
	case Nodes . N_MULTIPLY  :
	case Nodes . N_DIVIDE :
		q1 = child.inte ( ) ;
		q2 = child.next.inte ( ) ;
		return ( q1.operatorBinary ( q2, Parser.SqlSyntax [ name ], "number", "number", "number", false ) ) ;

    case Nodes . N_CONCAT :
		q1 = child.inte ( ) ;
		q2 = child.next.inte ( ) ;
		return ( q1.operatorBinary ( q2, Parser.SqlSyntax [ name ], "string", "string", "string", false ) ) ;
		// TRUE FALSE

	case Nodes.N_EQUAL :
	case Nodes.N_GT :
	case Nodes.N_LT :
	case Nodes.N_N_EQUAL :
	case Nodes.N_GT_EQUAL :
	case Nodes.N_LT_EQUAL :
		q1 = child.inte ( ) ;
		q2 = child.next.inte ( ) ;
		return ( q1.operatorBinary ( q2, Parser.SqlSyntax [ name ], "boolean", "scalar", "scalar", true ) ) ;
	
	case Nodes.N_AND :
	case Nodes.N_OR :
	case Nodes.N_XOR :
		q1 = child.inte ( ) ;
		q2 = child.next.inte ( ) ;
		return ( q1.operatorBinary ( q2, Parser.SqlSyntax [ name ], "boolean", "boolean", "boolean", true ) ) ;
		
    case Nodes.N_BETWEEN :
    	return ( this.inte_between () ) ;

    case Nodes.N_ISNULL :
    	return ( this.inte_isnull () ) ;

    case Nodes.N_MATCH_CHARACTERS :
    	return ( this.inte_match_characters () ) ;

    case Nodes.N_NUMBER :				// 1.0 should be integer ?
    	return ( Query.operatorConstant ( this.val, ( this.val.indexOf( '.' ) >= 0 ? "number" : "integer" ) ) ) ; 

    case Nodes.N_STRING :
    	return ( Query.operatorConstant ( Db.sqlQuoteString ( this.val ), "string") ) ;

    case Nodes.N_NULL :
    	return ( Query.operatorConstant ( "null", "null" ) ) ;		

    case Nodes.N_FALSE :
    	return ( Query.operatorConstant ( "'false'", "boolean" ) ) ;	

    case Nodes.N_TRUE :
    	return ( Query.operatorConstant ( "'true'", "boolean" ) ) ;	

    case Nodes.N_UNARY_SUBTRACT :
		q1 = child.inte ( ) ;
		return ( q1.operatorUnary( Parser.SqlSyntax [ name ], "number", null, false ) ) ;
		
	case Nodes.N_NOT :
		q1 = child.inte ( ) ;
		return ( q1.operatorUnary( Parser.SqlSyntax [ name ], "boolean", "boolean", true ) ) ;
		
    case Nodes.N_POSITION :		// as this is now a legal identifier
    case Nodes.N_IDENTIFIER :
    	return ( this.inte_identifier ( ) ) ;
		
    case Nodes.N_MEMBERSHIP :
    	return ( this.inte_membership ( ) ) ;
    
    case Nodes.N_VARIABLE :
    	return ( this.inteVariable( ) ) ;

    case Nodes.N_GENERIC_COMPONENT :
    	return ( this.inteVariable( ) ) ;

    case Nodes.N_IN :
		return ( inteIn ( "in" ) ) ;

    case Nodes.N_NOT_IN :
		return ( inteIn ( "not in" ) ) ;

    case Nodes.N_IF :
    	return ( inteIfThenElse ( ) ) ;
    
    case Nodes.N_SQL_ROW_FUNCTION :
    	return ( this.inteRowFunction ( ) ) ;

    case Nodes.N_USER_OPERATOR_CALL  :
		return ( this.inteUserOperatorCall() ) ; 

	// end of operators applicable to both dataset and component
	    
    case Nodes.N_CLAUSE_OP :
		return ( this.inteClauseOperator ( ) ) ;		
		
    case Nodes.N_JOIN:
		return ( this.inteJoin ( ) ) ;		
		
    case Nodes.N_SQL_AGGREGATE_FUNCTION :
    	return ( this.inteAggregateFunction ( ) ) ;

    case Nodes.N_SQL_ANALYTIC_FUNCTION :
    	return ( this.inteAnalyticFunction ( ) ) ;

    case Nodes.N_SETDIFF  :
    	q1 = this.child.inte ( ) ;
    	q2 = this.child.next.inte ( ) ;
		return ( q1.setdiff ( q2 ) );
    
    case Nodes.N_SYMDIFF  :
    	q1 = this.child.inte ( ) ;
    	q2 = this.child.next.inte ( ) ;
		return ( q1.symdiff ( q2 ) );

    case Nodes.N_EXISTS_IN  :
		q1 = child.inte ( ) ;
		q2 = child.next.inte ( ) ;
		return ( q1.exists_in ( q2, ( this.val == null ? "all" : this.val )) );
   
    case Nodes.N_UNION  :
		return ( Query.union ( this.listDatasets ( ) ) ) ;
		
    case Nodes.N_INTERSECT  :
		return ( Query.intersect ( this.listDatasets ( ) ) ) ;
		
    case Nodes.N_CHECK  :
    	return ( this.inteCheck ( ) ) ;

    case Nodes.N_CHECK_DATAPOINT  :
    	return ( this.inteCheckDataPoint ( ) ) ;

    case Nodes.N_CHECK_HIERARCHY  :
    	return ( this.inteCheckHierarchy ( ) ) ;

    case Nodes.N_HIERARCHY :
    	return ( this.inteHierarchy ( ) ) ;

    case Nodes.N_EVAL  :
		return ( this.inteEval ( ) ) ;
		
    case Nodes.N_CAST :
    	q1 = child.inte ( ) ;
    	return ( inteCast ( q1, child.next.val, ( child.next.next == null ? null : child.next.next.val ) ) ) ;

    case Nodes.N_FILL_TIME_SERIES :
    	q1 = child.inte ( ) ;
    	return ( q1.fillTimeSeries ( child.next == null ? "all" : child.next.val ) ) ;
    	
    case Nodes.N_FLOW_TO_STOCK :
    	q1 = child.inte ( ) ;
    	return ( q1.flowToStock ( ) ) ;

    case Nodes.N_STOCK_TO_FLOW :
    	q1 = child.inte ( ) ;
    	return ( q1.stockToFlow ( ) ) ;

    case Nodes.N_TIMESHIFT :
    	q1 = child.inte ( ) ;
    	return ( q1.timeshift ( child.next.inteEvalInteger ( ) ) ) ;
    	
    case Nodes.N_TIME_AGG :
    	q1 = child.inte ( ) ;
    	return ( q1.timeAgg ( child.next.inteEvalScalar( ) ) ) ;
    	
    case Nodes.N_PERIOD_INDICATOR :		// TBD optional parameter
    	q1 = ( child == null ? null : child.inte ( ) ) ;
    	return ( intePeriodIndicator ( q1 ) ) ;

    case Nodes.N_VTL_META_DATASET :	
    	return ( Dataset.vtl_meta_dataset(this.val) ) ;

    case Nodes.N_EMPTY : 					// should only appear in "update x with empty"
    	VTLError.InternalError( "Bad expression: empty node") ;
    	return ( (Query)null );
    	
    default :
    	VTLError.InternalError ( "inte - case not found: " + name ) ;
    	return ( (Query)null );
	}
}


/*
 * Unparse list function. Called by rowOpMDTsyntax.
 */
public void unparseList ( StringBuffer str ) throws VTLError
{
	switch ( this.name ) {
	    case Nodes.N_SET_SCALAR :
		  	str.append ( "{" ) ;
		  	for ( Node p = this.child; p != null; p = p.next ) {
		  		p.unparseOp(str) ;
		  		if ( p.next != null )
		  			str.append ( "," ) ;
		  	}
		  	str.append ( "}" ) ;
		  	break ;
	    case Nodes.N_IDENTIFIER :
	    case Nodes.N_VARIABLE :
	    	str.append( this.val ) ;
	    	break ;
	    default :
	    	  VTLError.InternalError ( "unparseList, unknown operator: " + this.name ) ;
	}
}

/*
 * Run the interpreter on this node and return the sql where condition. Used in the rulesets.
 */
final String sqlWhereCondition ( ) throws VTLError
{	
	return ( this.inte( ).sqlWhere ) ;
}

/*
 * Builds the MDT syntax used in the when condition of ruleset or in define valuedomain subset
 */
void unparseOp ( StringBuffer str ) throws VTLError
{	
	switch ( name ) {
		case Nodes.N_PLUS  :
		case Nodes.N_SUBTRACT  :
		case Nodes.N_MULTIPLY  :
		case Nodes.N_DIVIDE :
		case Nodes.N_CONCAT :
	    case Nodes.N_AND :
		case Nodes.N_OR :
		case Nodes.N_XOR :
		case Nodes.N_EQUAL :
		case Nodes.N_GT :
		case Nodes.N_LT :
		case Nodes.N_N_EQUAL :
		case Nodes.N_GT_EQUAL :
		case Nodes.N_LT_EQUAL :
			child.unparseOp ( str ) ;
			str.append ( " " ).append( Parser.SqlSyntax [ name ] ).append( " " ) ;
			child.next.unparseOp ( str ) ;
			break ;

		case Nodes.N_NOT :
			child.unparseOp ( str.append ( " not " ) ) ;
			break ;
	
	    case Nodes.N_UNARY_SUBTRACT :
			child.unparseOp ( str.append ( Parser.SqlSyntax [ name ] ) ) ;
			break ;
	    	
	    case Nodes.N_IDENTIFIER :
	    case Nodes.N_VARIABLE :
	    	str.append ( this.val ) ;
	    	break ;    	
	    	// str.append ( str.append ( "CV(" ).append( this.val ).append( ")" ) ) ; // used in MODEL RULES
	
		case Nodes.N_PARENTHESIS :
			str.append ( "( " ) ;
			child.unparseOp ( str ) ;
			str.append ( " )" ) ;
			break ;
	
	    case Nodes.N_NUMBER :
			str.append ( val ) ;
			break ;

	    case Nodes.N_STRING :
	    case Nodes.N_POSITION :
    		str.append ( '"' + this.val + '"' ) ;			// MDT syntax
			break ;

	    case Nodes.N_NULL :
			str.append ( "null" ) ;
			break ;
   
	    case Nodes.N_SQL_ROW_FUNCTION :
	    	str.append ( this.val ).append( "( " ) ;
	    	for ( Node p = this.child ; p != null ; p = p.next ) {	
	    		p.unparseOp ( str ) ;
	    		if ( p.next != null )
	        		str.append ( ", " ) ;    
	    	}
	    	str.append( " )" ) ;
	    	break ;
	
	    case Nodes.N_IF :
	    	str.append ( " if " ) ;
	  		this.child.unparseOp ( str ) ;	
			str.append ( " then " ) ;
	  		this.child_2().unparseOp ( str ) ;	
			str . append ( " else " ) ;
	  		this.child_3().unparseOp ( str ) ;	
	    	break ;
	
	    case Nodes.N_BETWEEN :
			str.append ( " between ( " ) ;
			child.unparseOp ( str ) ;
			str.append ( ", " ) ;
			child.next . unparseOp ( str ) ;
			str.append ( ", " ) ;
			child.next.next.unparseOp ( str ) ;
			str.append ( ")" ) ;
			break ;

	    case Nodes.N_ISNULL :
	    	str.append ( " isnull( " ) ;
	    	child.unparseOp ( str ) ;
	    	str.append( ")") ;
			break ;

	    case Nodes.N_IN :
	    	child.unparseOp ( str ) ;
	    	str.append ( " in " ) ;
			child.next.unparseList ( str ) ;
			break ;
	    	
	    case Nodes.N_NOT_IN :
	    	child.unparseOp ( str ) ;
	    	str.append ( " not in " ) ;
			child.next.unparseList ( str ) ;
			break ;
	
	    case Nodes.N_EMPTY : // should only appear in "update x with empty"
	    	VTLError .InternalError("rowOpSQLsyntax - found empty node") ;
	    	break ;
		
	    default :
	    	VTLError.InternalError ( "rowOpSQLsyntax - case not found: " + name ) ;
    }
}

}

/*
 * Check whether the object exists.
 *
Query inte_object_exists ( ) throws VTLError
{
	String 	objectName ;
	
	if ( this.val != null && this.child == null )
		objectName = this.val ;													
	else
		objectName = this.child.eval_object_name() ;							

	return ( Query.operatorConstant (  VTLObject.object_exists ( objectName ) ? "(0=0)" : "(0<>0)" , "string") ) ;
}
*/