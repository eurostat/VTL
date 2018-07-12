import java.util.Vector;
public class Parser {


static final Node newnode ( short node_name, String val )
{
	return ( new Node ( node_name, val ) ) ;
}

static final Node newnode ( short node_name )
{
	return ( new Node ( node_name ) ) ;
}

static final Node newnode ( short node_name, String val, Node node_son )
{
	return ( new Node ( node_name, val, node_son ) ) ;
}

static final Node newnode ( short node_name, Node son )
{
	return ( new Node ( node_name, son ) ) ;
}

static final Node newnode ( short node_name, String val, short node_info )
{
	return ( new Node ( node_name, val, node_info ) ) ;
}

static final String ownershipSymbol = "\\" ;

// ----------- parser
/* 
   Priority. 0 max, 7 min. 0 for prefix operators.
	0	+ (unary) - (unary) if 
		null number string identifier 
		function application
		clause [ ]
	1	#
	2	not (prefix)- (prefix)+
	3	* / 
	4	+ - || 
	5	= <> < > <= >= in not_in
	6 	and
	7   or xor 
*/

static final int MinOpPriority = 7 ;
static final int Priority [ ] = {
	0, 0, 6, 1, 5, 2, 0, 5, 5, 0,				 
	0, 0, 2, 0, 7, 7, 0, 0, 4, 4,
	3, 3, 5, 5, 5, 1, 5, 5, 5, 4,			
} ;

static final String SqlSyntax [ ] = {
	"(empty)", "", "and", "[", "", "-", "", "not_in", "in", "",
	"", "", "not", "null", "or", "xor", "", "", "+", "-",
	"*", "/", "=", ">", "<", "#", "<>", ">=", "<=", "||",
} ;

// Convert Tokens to Nodes 
static final short NodeName [ ] = { 
	Nodes.N_AND , 			// Y_AND 		=  0 ,
	Nodes.N_OR , 			// Y_OR 		=  1 ,
	Nodes.N_XOR ,			// Y_XOR		=  2 ,
	Nodes.N_NOT ,			// Y_NOT 		=  3 ,
	Nodes.N_IN , 			// Y_IN 		=  4 ,
	Nodes.N_NOT_IN , 		// Y_NOT_IN		=  5 ,
	Nodes.N_NULL ,			// Y_NULL 		=  6 ,
	Nodes.N_PLUS ,			// Y_PLUS	    =  7 , // +
	Nodes.N_SUBTRACT ,		// Y_SUBTRACT	=  8 , // -
	Nodes.N_MULTIPLY ,	 	// Y_MULTIPLY	=  9 , // *
	Nodes.N_DIVIDE , 		// Y_DIVIDE		= 10 , // /
	Nodes.N_EQUAL , 		// Y_EQUAL 		= 11 , // =
	Nodes.N_GT ,			// Y_GT			= 12 , // >
	Nodes.N_LT ,			// Y_LT			= 13 , // <
	Nodes.N_MEMBERSHIP,		// Y_SHARP		= 14 , // #
	Nodes.N_N_EQUAL ,		// Y_N_EQUAL	= 15 , // <>
	Nodes.N_GT_EQUAL ,		// Y_GT_EQUAL 	= 16 , // >=
	Nodes.N_LT_EQUAL ,		// Y_LT_EQUAL	= 17 , // <=
	Nodes.N_CONCAT ,		// Y_CONCAT		= 18 , // ||
	Nodes.N_CLAUSE_OP,		// Y_BRAC_OPEN	= 19 , // [
} ;

/*
 * Static enviroment for parsing variables.
 */
static	ListString		stack_var_name = new ListString () ;
static	Vector <Short>	stack_var_type = new Vector <Short> () ;

static void resetStack ()
{
	stack_var_name.setSize ( 0 ) ;
	stack_var_type.setSize ( 0 ) ;
}

static void addStack ( String varName, short nodeType, boolean check ) throws VTLError
{
	if ( check && stack_var_name.indexOf ( varName ) >= 0 )
		VTLError.TypeError ( "Duplicate variable " + varName ) ;
	stack_var_name.add ( varName ) ;
	stack_var_type.add ( new Short ( nodeType ) ) ;
}

static void removeStack ( int nVars )
{
	for ( int idx = 0; idx < nVars; idx ++ ) {
		stack_var_name.remove( stack_var_name.size() - 1 ) ;
		stack_var_type.remove( stack_var_type.size() - 1 ) ;		
	}
}

/*
 * Make data object name: identifier or variable.
 * a global variable takes priority over a component
 */
static Node makeDataObjectName ( String ide ) throws VTLError
{
	int	varIndex ;
	
	if ( ( varIndex = stack_var_name.indexOf ( ide ) ) >= 0 )
	    return ( Parser.newnode ( Nodes.N_VARIABLE, ide, (short) varIndex ) ) ;
	
	if ( stack_var_name.size() > 0 && stack_var_name.lastElement().equals("$this"))
	    return ( Parser.newnode ( Nodes.N_GENERIC_COMPONENT, ide, (short) (stack_var_name.size() - 1 )) ) ;
	 
	return ( Parser.newnode ( Nodes.N_IDENTIFIER, ide ) ) ;
}

/*
 * Parse object name: object name can be an identifier or a variable.
 */
static Node parseObjectName ( boolean look_for_variable ) throws VTLError
{
	String ide = Lex.readObjectName () ;
	return ( makeDataObjectName ( ide ) ) ;
}

/*
 * Read an expression with infix operators.
 * 		browse aact_ali01 convert geo in ( IT,FR) and itm_newa in (40000, 41000) to flow in ( CREDIT,DEBIT) ;
 */
static Node infix( int level ) throws VTLError
{
	Node   	p ;
	short  	name ;
	int		tok ;
	
	if ( level == 0 )
		return ( prefix() ) ;

	p = infix( level - 1 );
	
	// while ( ( name = NodeName [ Lex.scan() ] ) >= 0 && Priority [ name ] == level ) {
	while ( ( ( tok = Lex.scan() ) < NodeName.length ) && ( name = NodeName [ tok ] ) >= 0 && Priority [ name ] == level ) {
		switch ( name ) {
			case Nodes.N_MEMBERSHIP :										// syntax: ds # component
				p = Parser.newnode( name, p ) ;    
				p.addChild( parseComponentName ( false ) );
				break ;
			case Nodes.N_CLAUSE_OP :										// syntax: ds [ clauseOp ]
				p = parseClauseOp ( p ) ;
				break ;
			case Nodes.N_IN :												// syntax: expr in set scalar
			case Nodes.N_NOT_IN :											// syntax: expr not_in set scalar
				p = Parser.newnode ( name, p  ) ;
				p.child.next = Parser.parseScalarSet ( ) ;
				break ;
			default :
				p = Parser.newnode( name, p ) ;    
				p.child.next = infix( level - 1 ) ;	
		}
	}
	
	Lex.unscan();
	// if ( p.name == Nodes.N_BETWEEN && p.child.next.name != Nodes.N_AND ) VTLError.RunTimeError ( "Expected: and after between" ) ;
	// print 'refin.aact_ali01' [ filter obs_value between 1 and 10 ]

	return(p);
}

/*
 * Parse prefix operator.
*/
static Node prefix () throws VTLError
{
	Node   p  ;
	int    tok;
	String ide;

	tok = Lex . scan() ;								// Session.debug ( "tok: " + tok ) ;

	switch ( tok ) {
		case Tokens.Y_IDENTIFIER: 
		case Tokens.Y_QUOTEDNAME: 
			Lex.unscan () ;
			ide = Lex.readObjectName ( ) ;
			if ( Lex.readOptionalToken ( Tokens.Y_PAR_OPEN ) )
				p = parseFunction ( ide ) ;	  				
			else {
				switch ( ide ) {
					case "if" : p = parseIf () ; 								break ;
					case "true" : p = Parser.newnode(Nodes.N_TRUE) ; 			break ;
					case "false" : p = Parser.newnode(Nodes.N_FALSE) ; 			break ;
					case "vtl_all_objects" :
					case "vtl_user_objects" :
					case "vtl_all_datasets" :
					case "vtl_user_datasets" :p = Parser.newnode ( Nodes.N_VTL_META_DATASET, ide ) ;	break ;
					default : p = makeDataObjectName ( ide ) ;
				}
			}			
			break;
			
  		case Tokens.Y_PLUS     : 
  			p = infix( Priority [ Nodes.N_UNARY_SUBTRACT ] ) ;                     
  			break;

  		case Tokens.Y_SUBTRACT    : 
  			p = Parser.newnode( Nodes.N_UNARY_SUBTRACT, infix( Priority [ Nodes.N_UNARY_SUBTRACT ] ) ); 
  			break;

  		case Tokens.Y_NOT     : 
  			p = Parser.newnode( Nodes.N_NOT, infix( Priority [ Nodes.N_NOT ] ) ) ;  
  			break;

  		case Tokens.Y_PAR_OPEN    : 
  			p = Parser.newnode ( Nodes.N_PARENTHESIS, parseExpression ( ) ) ;
  			Lex.readToken ( Tokens.Y_PAR_CLOSE );
  			break;
 
  		case Tokens.Y_NUMBER    : 
  			p = Parser.newnode( Nodes.N_NUMBER, Lex.TokenString );
  			break;

  		case Tokens.Y_STRING    : 
  			p = Parser.newnode( Nodes.N_STRING, Lex.TokenString.length() > 0 ? Lex.TokenString : null ) ;
  			break ;

  		case Tokens.Y_POSITION    : 
  			p = Parser.newnode( Nodes.N_POSITION, Lex.TokenString );
  			break;

  		case Tokens.Y_NULL    : 
  			p = Parser.newnode( Nodes.N_NULL );
  			break;
  		
  		default : 
  			p = null ;
  			if ( tok < Lex.Keywords.size() )
  				VTLError.SyntaxError( "Bad symbol: " + Lex.Keywords.get( tok ) ) ;
  			else
  				VTLError.InternalError( "Unknown symbol: " + tok + " (internal code)" ) ;
    }

	return p ;
}

/*
 * Parse expression.
 */
static Node parseExpression ( ) throws VTLError
{
	return ( infix ( MinOpPriority ) ) ;
}

/*
 * Parse condition.
 */
static final Node parseCondition ( ) throws VTLError
{
	return ( infix ( MinOpPriority ) ) ;
}

static Node parseNumber ( ) throws VTLError
{
  	return ( Parser.newnode ( Nodes.N_NUMBER, Lex.readNumber() ) ) ;
}

static Node parseIdentifier ( ) throws VTLError
{
	return ( Parser.newnode ( Nodes.N_IDENTIFIER, Lex.readIdentifier ( ) ) ) ;
}

static Node parseComponentName ( ) throws VTLError
{
	return ( Parser.newnode ( Nodes.N_COMPONENT, Lex.readIdentifier ( ) ) ) ;
}

/*
 * Parse component. Optional: accept ds#component (can be used in keep, drop, rename within a join)
 */
static Node parseComponentName ( boolean acceptMembership ) throws VTLError
{
	String ide = Lex.readIdentifier ( ) ;
	if ( acceptMembership && Lex.readOptionalToken ( Tokens.Y_SHARP )) {
		ide = ide + "#" + Lex.readIdentifier ( ) ;		
	}
	return ( Parser.newnode ( Nodes.N_COMPONENT, ide ) ) ;
}

static Node parseObjectName ( ) throws VTLError
{
	return ( Parser.newnode ( Nodes.N_IDENTIFIER, Lex.readObjectName ( ) ) ) ;
}

/*
create or replace integrity rule ir_bleu ( geo ) is
	(BLEU,AA) = LU + BE ;
	BNL = BLEU + NL ;
end integrity rule ;
 */
static Node parsePosition ( ) throws VTLError
{
	Node  p ;
	switch ( Lex . scan() ) {
	  	case Tokens . Y_NUMBER :
	  		p = Parser.newnode ( Nodes.N_NUMBER, Lex.TokenString ) ;
	  		break ;
	    case Tokens . Y_IDENTIFIER :
	    case Tokens . Y_STRING :
	    case Tokens . Y_POSITION :
	    	p = Parser.newnode ( Nodes.N_STRING, Lex.TokenString ) ;
	    	break ;
	    default :
	        VTLError . SyntaxError ( "Bad position: " + Lex.TokenString ) ;
	    	return ( null ) ;
	}
	
	return ( p ) ;
}

/*
 * Parse (possibly empty) list of expressions separated by separator 
 * and terminated by terminator
 */
static Node parseListExpr ( int separator, int terminator ) throws VTLError
{
	Node hd, p;
	
	if ( Lex.readOptionalToken ( terminator ))
		return ( null ) ;
	
	hd = p = parseExpression () ;
	while ( Lex.readOptionalToken ( separator ) ) {
		p.next = parseExpression ();
		p = p.next ;	  
	}

	Lex.readToken ( terminator ) ;

	return(hd);
}

/*
 * Parse non empty list of expressions separated by separator. No terminator character.
 */
static Node parseListExpr ( int separator ) throws VTLError
{
	Node hd, p;
	
	hd = p = parseExpression () ;
	while ( Lex.readOptionalToken ( separator ) ) {
		p.next = parseExpression ();
		p = p.next ;	  
	}

	return(hd);
}

/*
 * Parse list of expressions
 * the underscore _ is used to tell VTL to use the default value
 */
static Node parseArguments ( int separator, int terminator ) throws VTLError
{
	Node hd, p;
	
	if ( Lex.readOptionalToken ( Tokens.Y_PAR_CLOSE ))
		return ( null ) ;
	if ( Lex.readOptionalToken ( Tokens.Y_UNDERSCORE ) )
		p = Parser.newnode( Nodes.N_DEFAULT_VALUE ) ;
	else
		p = parseExpression () ;
	hd = p ;
	while ( Lex.readOptionalToken ( separator ) ) {
		if ( Lex.readOptionalToken ( Tokens.Y_UNDERSCORE ) )
			p.next = Parser.newnode( Nodes.N_DEFAULT_VALUE ) ;
		else
			p.next = parseExpression ();
		p = p.next ;	  
	}

	Lex.readToken ( terminator ) ;

	return(hd);
}

/*
 * Parse expression if-then-else
 */
static Node parseIf ( ) throws VTLError
{
	Node     hd = Parser.newnode ( Nodes.N_IF ) ;
	
	hd.addChild ( parseCondition () ) ;
	Lex.readIdeKeyword ( "then" ) ;
	hd.addChild ( parseExpression () ) ;	
	Lex.readIdeKeyword ( "else" ) ;
	hd.addChild ( parseExpression () ) ;

	return ( hd ) ;
}

static Node parseDimList ( ) throws VTLError
{
	Node hd = null, p = null ;

	hd = p = parseIdentifier () ;
	while ( Lex.readOptionalToken ( Tokens.Y_COMMA ) ) {
		p.next = parseIdentifier ();
		p = p.next ;	  
	}

	return(hd);
}

/*
 * Product of variables
 */
static Node parseVariablesProduct ( ) throws VTLError
{
	Node hd = null, p = null ;

	hd = p = parseIdentifier () ;
	while ( Lex.readOptionalToken ( Tokens.Y_MULTIPLY ) ) {
		p.next = parseIdentifier ();
		p = p.next ;	  
	}

	return(hd);
}

/*
 * Parse order by list.
 */
static Node parseByList ( ) throws VTLError
{
	Node hd = null, p = null;

	Lex.readIdeKeyword ( "by" ) ;

	do {
	    if ( hd == null )
	    	hd = p = parseExpression ();
	    else {
			p.next = parseExpression ();
			p = p.next ;
		}
	  
	    if ( Lex.readOptionalIdeKeyword ( "asc" ) )
	    	p.next = Parser.newnode ( Nodes.N_SORT_ASC_DESC, "ASC" ) ;
	    else if ( Lex.readOptionalIdeKeyword ( "desc" ) )
	    	p.next = Parser.newnode ( Nodes.N_SORT_ASC_DESC, "DESC" ) ;
	    else
	    	p.next = Parser.newnode ( Nodes.N_SORT_ASC_DESC, "ASC" ) ;		// default
	    p = p.next;
	} while ( Lex.readOptionalToken ( Tokens.Y_COMMA ) ) ;

	return ( hd ) ;
}

/*
 * Parse windowing clause (analytic functions)
 */
static Node parseWindowingClause ( ) throws VTLError
{
	Node   	p ;
	
	if ( Lex.readOptionalIdeKeyword( "current" )) {
	   	Lex.readIdeKeyword ( "row" ) ;
		p = makeEmptyNode ( ) ;
		p.next = Parser.newnode ( Nodes.N_STRING, "current row" ) ;
	  }
	else if ( Lex.readOptionalIdeKeyword( "unbounded" )) {
		p = makeEmptyNode ( ) ;
		if ( Lex.readOptionalIdeKeyword ( "preceding" ) )
			p.next = Parser.newnode ( Nodes.N_STRING, "unbounded preceding" ) ;
		else if ( Lex.readOptionalIdeKeyword ( "following" ) )
			p.next = Parser.newnode ( Nodes.N_STRING, "unbounded following" ) ;
		else 
			VTLError . SyntaxError ( "Bad windowing clause (expecting: preceding or following)" ) ;
	}
	else {
		p = parseExpression () ;
		if ( Lex.readOptionalIdeKeyword ( "preceding" ) )
			p.next = Parser.newnode ( Nodes.N_STRING, "preceding" ) ;
		else if ( Lex.readOptionalIdeKeyword ( "following" ) )
			p.next = Parser.newnode ( Nodes.N_STRING, "following" ) ;
		else
			VTLError . SyntaxError ( "Bad windowing clause (expecting: preceding or following)" ) ;
	}

  return ( p ) ;
}
/*
 * p_arguments	argument list
 * ide			function name
 * print sum ( aact.obs_value ) over ( partition by geo, itm_newa order by time ) ;
 */

static final String[] WindowNotAllowed = { "lag", "lead", "rank", "ratio_to_report", "median" } ;
static final String[] OrderbyNotAllowed = { "ratio_to_report", "median" } ;

/*
  Syntax :
	analyticOperator ( { arguments } 
		over ( [ partition_by_clause ] [ order_by_clause ] [ windowing_clause ] ) )
	analyticOperator  ::=  avg | count | max | median | min | stddev_pop | stddev_samp 
		| sum | var_pop | var_samp | first_value | lag | last_value | lead | rank | ratio_to_report
	partition_by_clause = partition by expression , ...
	order_by_clause = order by expression asc | desc , ...
	windowing_clause = rows | range
		between expression1 preceding | following 
			| current row | unbounded preceding | unbounded following
		and expression2 preceding | following 
			| current row | unbounded preceding | unbounded following

  Examples:
  print sum ( bop ) over ( partition by geo order by period ) ;
  print {obs_value = aact_ali01, analytic_function = lag(aact_ali01, 1, 0 ) over ( partition by itm_newa,geo order by time  ) } ;
  print lag(aact_ali01 (geo=IT), 1, 0 ) over ( partition by itm_newa order by time  ) ;
  print lag(aact_ali01 (geo=IT), 1, 0 ) over ( partition by itm_newa order by time rows between 1 preceding and 1 following ) ;
  print lag(aact_ali01, 1, 0 ) over ( partition by itm_newa order by time rows between 1 preceding and 1 following ) ;
*/
static void parseOver ( Node hd, String op ) throws VTLError
{
	String tmp ;
	Node   p_partition, p_orderby, p_windowing_clause ;

  	Lex.readToken ( Tokens.Y_PAR_OPEN ) ;

  	p_partition = Parser.newnode ( Nodes.N_DUMMY, "over" ) ;
	if ( Lex.readOptionalIdeKeyword ( "partition" ) ) {
		Lex.readIdeKeyword( "by" );
		p_partition.child = parseDimList ( ) ; 		
	}

	p_orderby = Parser.newnode ( Nodes.N_DUMMY ) ;
	if ( Lex.readOptionalIdeKeyword ( "order" ) ) {
		if ( Sys.getPosition(OrderbyNotAllowed, op ) >= 0 )
			VTLError.TypeError( "Analytic function " + op + ": window not allowed" );
		p_orderby.child = parseByList ( ) ;
	}

	p_windowing_clause = Parser.newnode ( Nodes.N_DUMMY ) ;
	tmp = Lex.nextTokenString () ;
	if ( tmp.equals ( "data" ) || tmp.equals ( "range" ) ) {
		if ( Sys.getPosition(WindowNotAllowed, op ) >= 0 )
			VTLError.TypeError( "Analytic function " + op + ": window not allowed" );
		if ( tmp.equals ( "data" ) ) {
			Lex.readIdeKeyword( "points" );
			p_windowing_clause.val = "data points" ;			
		}
		else
			p_windowing_clause.val = "range" ;
		Lex.readIdeKeyword ( "between" ) ;
		p_windowing_clause .child = parseWindowingClause ( ) ;
		Lex.readToken ( Tokens.Y_AND ) ;
		p_windowing_clause .child . next . next = parseWindowingClause ( ) ;
	} else
		Lex . unscan ( ) ;
  
  	Lex.readToken ( Tokens . Y_PAR_CLOSE ) ;
  	
  	hd.addChild( p_partition ) ;
  	hd.addChild( p_orderby ) ;
  	hd.addChild( p_windowing_clause ) ;
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
 */
static Node parseAggregateAnalyticCall ( String ide ) throws VTLError
{
	Node hd, pargs ;
	hd = Parser.newnode ( Nodes.N_SQL_AGGREGATE_FUNCTION, ide ) ;
	// count in a having clause
	if ( Lex.readOptionalToken( Tokens.Y_PAR_CLOSE ) && ide.equals( "count" ) ) {
		hd.addChild( Parser.newnode ( Nodes.N_DUMMY, "arguments" ) );
		hd.addChild( Parser.makeEmptyNode() ) ;		
		return ( hd ) ;
	}
	if ( ide.equals( "rank") ) {
		hd.name = Nodes.N_SQL_ANALYTIC_FUNCTION ;
		hd.addChild( Parser.newnode ( Nodes.N_DUMMY, "arguments" ) ) ;
		Lex.readIdeKeyword( "over");
		parseOver ( hd, ide ) ;
		Lex.readToken( Tokens.Y_PAR_CLOSE );
		return ( hd ) ;
	}
	
	pargs = parseListExpr ( Tokens.Y_COMMA ) ;						// TBD: count()
	hd.addChild( Parser.newnode ( Nodes.N_DUMMY, "arguments", pargs ) );
	
	// only 1 parameter, no need to check n. parameters
	
	if (  Lex.readOptionalIdeKeyword ( "group" ) ) {
		if ( Lex.readOptionalIdeKeyword ( "by" ) ) {
			hd.addChild( Parser.newnode ( Nodes.N_GROUP_BY, parseDimList ( ) ) ) ;
		}
		else if ( Lex.readOptionalIdeKeyword ( "except" ) ) {
			hd.addChild( Parser.newnode ( Nodes.N_GROUP_EXCEPT, parseDimList ( ) ) ) ;
		}
		else if ( Lex.readOptionalIdeKeyword ( "all" ) ) {
			Lex.readIdeKeyword ( "time_agg" ) ;
			Lex.readToken( Tokens.Y_PAR_OPEN );
			Node p = parseArguments ( Tokens.Y_COMMA, Tokens.Y_PAR_CLOSE ) ;
			if ( ! ( p.nodeListLength () >= 1 && p.nodeListLength () <= 4) )
				VTLError.SyntaxError( "time_agg must have 1 to 4 arguments");
			hd.addChild( Parser.newnode ( Nodes.N_GROUP_ALL, 
					Parser.newnode(Nodes.N_TIME_AGG, p) ) ) ;
		}		
		else
			VTLError.SyntaxError( "aggregate operator: only (by, except, all) can follow group");
		
		hd.addChild ( Lex.readOptionalIdeKeyword ( "having" ) ? Parser.parseExpression() : Parser.makeEmptyNode () ) ;
	}
	else if ( Lex.readOptionalIdeKeyword ( "over" ) ) {
		hd.name = Nodes.N_SQL_ANALYTIC_FUNCTION ;
		parseOver ( hd, ide ) ;
	}
	else {
		// empty nodes: group by + having
		hd.addChild ( Parser.makeEmptyNode() ) ;
		hd.addChild ( Parser.makeEmptyNode() ) ;	
	}
	Lex.readToken( Tokens.Y_PAR_CLOSE );
	return ( hd ) ;
}

/*
 * Parse subscript.
 * Y_PAR_OPEN has been parsed
 * the first identifier has been parsed
 * the dimension value must be a string scalar expression (string constant must be quoted) 
 */
static Node parseSubscript ( ) throws VTLError
{
	Node		hd, p, pExpr ;
	String 		ide ;
    
	hd = Parser.newnode ( Nodes.N_CLAUSE_SUBSCRIPT ) ;    	
	for ( ; ; ) {
		ide = Lex.readIdentifier ( ) ;
		p = Parser.newnode ( Nodes.N_EQUAL, Parser.newnode( Nodes.N_COMPONENT,ide ) ) ;
    	Lex.readToken ( Tokens.Y_EQUAL ) ;
    	pExpr = parseExpression () ;
    	// if ( pExpr.name == Nodes.N_GENERIC_COMPONENT )		pExpr.name = Nodes.N_STRING ;
		p.addChild( pExpr ) ;			// priority?
    	hd.addChild( p ) ;
    	if ( ! Lex.readOptionalToken ( Tokens.Y_COMMA ) )
    		break ;
    }

    return ( hd ) ;
}

/*
 * Make empty node. 
 */
static final Node makeEmptyNode ( )
{
	return ( Parser.newnode( Nodes.N_EMPTY ) ) ;
}

/*
 * Parse enumerated options of a parameter. 
 * The default is the first element.
 */
static final Node parseOption ( String options [] ) throws VTLError
{
	for ( String str : options ) {
		if ( Lex.readOptionalIdeKeyword( str ) )
			return ( Parser.newnode( Nodes.N_STRING, str ) ) ;
	}
	return ( Parser.newnode( Nodes.N_STRING, options[0] ) ) ;
}

static final String optionsCheckOutput [ ] = { "all", "invalid" } ;

/*
	check ( op { errorcode errorcode } { errorlevel errorlevel } 
		{ imbalance imbalance } { output } )
	output ::= all | invalid
	
	check ( na_main#obs_value > 1000 errorcode "Bad value" errorlevel "Warning" ) ;
 */
static Node parseCheck ( ) throws VTLError
{
	Node hd = Parser.newnode( Nodes.N_CHECK ) ;
	hd.addChild( Parser.parseExpression() ) ;
	hd.addChild( Lex.readOptionalIdeKeyword( "errorcode") ? parseExpression() : makeEmptyNode ( ) );
	hd.addChild( Lex.readOptionalIdeKeyword( "errorlevel") ? parseExpression() : makeEmptyNode ( ) );
	hd.addChild( Lex.readOptionalIdeKeyword( "imbalance") ? parseExpression() : makeEmptyNode ( ) );
	hd.addChild( parseOption ( optionsCheckOutput ) ) ;
	Lex.readToken( Tokens.Y_PAR_CLOSE );
	return ( hd ) ;
}

/*
	check_datapoint ( op , dpr { components listComp } { output output } )
	output ::= invalid | all | all_measures
	
	check_datapoint ( na_main, dpr_313_zero_negative_values )
 */
static final String optionsCheckDatapointOutput [ ] = { "invalid", "all", "all_measures" } ;

static Node parseCheckDatapoint ( ) throws VTLError
{
	Node hd = Parser.newnode( Nodes.N_CHECK_DATAPOINT ) ;
	hd.addChild( parseExpression() );
	Lex.readToken( Tokens.Y_COMMA );
	hd.addChild( Parser.parseObjectName() );
	hd.addChild( Lex.readOptionalIdeKeyword( "components") ? Parser.newnode ( Nodes.N_COMPONENT_LIST, parseDimList() ) : makeEmptyNode ( ) );
	hd.addChild( parseOption ( optionsCheckDatapointOutput ) ) ;
	Lex.readToken( Tokens.Y_PAR_CLOSE );
	return ( hd ) ;
}

/*
 * Parse check_hierarchy
	check_hierarchy ( op , hr 
	{ condition condComp { condComp }* } 
	{ rule ruleComp }
	{ mode } { input } { output } )	
	mode ::= non_null | non_zero | partial_null | partial_zero | always_null | always_zero 
	input ::=  dataset | dataset_priority 
	output ::= invalid | all | all_measures
 */
static final String optionsCheckHierarchyMode [ ] = { "non_null", "non_zero", 
					"partial_null", "partial_zero", "always_null", "always_zero" } ;
static final String optionsCheckHierarchyInput [ ] = { "dataset", "dataset_priority" } ;
static final String optionsCheckHierarchyOutput [ ] = { "invalid", "all", "all_measures" } ;

static Node parseCheckHierarchy ( ) throws VTLError
{
	Node hd = Parser.newnode( Nodes.N_CHECK_HIERARCHY ) ;
	hd.addChild( parseExpression() );
	Lex.readToken( Tokens.Y_COMMA );
	hd.addChild( Parser.parseObjectName() );
	hd.addChild( Lex.readOptionalIdeKeyword( "condition") ? Parser.newnode ( Nodes.N_COMPONENT_LIST, parseDimList() ) : makeEmptyNode ( ) );
	hd.addChild( Lex.readOptionalIdeKeyword( "rule") ? Parser.parseIdentifier() : makeEmptyNode ( ) );
	hd.addChild( parseOption ( optionsCheckHierarchyMode ) ) ;
	hd.addChild( parseOption ( optionsCheckHierarchyInput ) ) ;
	hd.addChild( parseOption ( optionsCheckHierarchyOutput ) ) ;
	Lex.readToken( Tokens.Y_PAR_CLOSE );
	return ( hd ) ;
}

/*
	hierarchy ( op , hr { condition condComp { , condComp }* } { rule ruleComp } 
					{ mode } { input } { output } )
	mode ::= non_null | non_zero | partial_null | partial_zero | always_null | always_zero
	input ::= rule | rule_priority | dataset
	output ::= computed | all
 */
static final String optionsHierarchyMode [ ] = { "non_null", "non_zero", 
					"partial_null", "partial_zero", "always_null", "always_zero" } ;
static final String optionsHierarchyInput [ ] = { "rule", "rule_priority", "dataset" } ;
static final String optionsHierarchyOutput [ ] = { "computed", "all" } ;

static Node parseHierarchy ( ) throws VTLError
{
	Node hd = Parser.newnode( Nodes.N_HIERARCHY ) ;
	hd.addChild( parseExpression() );
	Lex.readToken( Tokens.Y_COMMA );
	hd.addChild( Parser.parseObjectName() );
	hd.addChild( Lex.readOptionalIdeKeyword( "condition") ? parseDimList() : makeEmptyNode ( ) );
	hd.addChild( Lex.readOptionalIdeKeyword( "rule") ? Parser.parseIdentifier() : makeEmptyNode ( ) );
	hd.addChild( parseOption ( optionsHierarchyMode ) ) ;
	hd.addChild( parseOption ( optionsHierarchyInput ) ) ;
	hd.addChild( parseOption ( optionsHierarchyOutput ) ) ;
	Lex.readToken( Tokens.Y_PAR_CLOSE );
	return ( hd ) ;
}

/*
	eval   (  extRoutineName ( { argument }  { , argument }* )
				language language
				returns  outputType )
 */
static Node parseEval ( ) throws VTLError
{
	Node hd = Parser.newnode( Nodes.N_EVAL ) ;
	Node p = Parser.newnode( Nodes.N_DUMMY, "arguments" ) ;
	
	p.val = Lex.readIdentifier() ;
	Lex.readToken( Tokens.Y_PAR_OPEN );
	p.addChild( parseArguments ( Tokens.Y_COMMA, Tokens.Y_PAR_CLOSE ) ) ;
	hd.addChild( p );
	Lex.readIdeKeyword( "language");
	hd.addChild( Parser.parseExpression() ) ;
	Lex.readIdeKeyword( "returns");
	hd.addChild( Parser.parseType() ) ;
	Lex.readToken( Tokens.Y_PAR_CLOSE );
	return ( hd ) ;
}

/*
 * Parse operator call (predefined operator or user-defined operator)
 * 	non-functional operators:
 * 		join, hierarchy, check, check_datapoint, check_hierarchy
 * 		aggregate operators
 * 		analytic operators
 * functional operators:
 *  	V	VTL function
 *  	S	Set function
 *  	R	SQL row function
 *  	A	SQL analytic function
 *  	G	SQL aggregate function
 *  	user-defined operator
 *    Y_PAR_OPEN has been read by prefix()
 */
static Node parseFunction ( String ide ) throws VTLError
{
	// non-functional syntax
	switch ( ide ) {
		case "inner_join" :
		case "left_join" :
		case "full_join" :
		case "cross_join" :
			return ( parseJoin ( ide ) ) ;
		
		case "check" :
			return ( Parser.parseCheck() ) ;
			
		case "check_datapoint" :
			return ( Parser.parseCheckDatapoint() ) ;
			
		case "check_hierarchy" :
			return ( Parser.parseCheckHierarchy() ) ;
			
		case "hierarchy" :
			return ( Parser.parseHierarchy() ) ;

		case "eval" :
			return ( Parser.parseEval() ) ;
	}
	
	// functional syntax
	return ( parseFunctionalOperatorCall ( ide ) ) ;
}

/*
 * Read optional component role. Return null (nothing read) if not found.
 */
static String readOptionalComponentRole ( ) throws VTLError
{
	String	roleIde = Lex.readIdentifier ( ) ;
	
	switch ( roleIde ) {
		case "identifier" :
		case "measure" :
		case "attribute" :
			return ( roleIde ) ;
		case "viral" :
			Lex.readIdeKeyword( "attribute" );
			roleIde = "viral attribute" ;
			return ( roleIde ) ;
		default :
			Lex.unscan () ;
			return ( null ) ;
	}
}
/*
	op [ calc { calcRole } calcComp := calcExpr { , { calcRole } calcComp := calcExpr }* ]
	calcRole ::= identifier | measure | attribute | viral attribute
 */
static Node parseClauseCalc ( ) throws VTLError
{
	Node	hd, p ;
	String	roleIde ;
	hd = Parser.newnode ( Nodes.N_CLAUSE_CALC ) ;
	do {
		p = Parser.newnode ( Nodes.N_CLAUSE_ASSIGNMENT ) ;
		roleIde = readOptionalComponentRole ( ) ;
		if ( roleIde != null )		// it is a component role
			p.addChild( Parser.newnode ( Nodes.N_STRING, roleIde ) ) ;
		else
			p.addChild(Parser.makeEmptyNode() ) ;
		p.addChild( Parser.parseIdentifier() ) ;
		Lex.readToken ( Tokens.Y_TEMP_ASSIGNMENT ) ;
		p.addChild ( parseExpression () ) ;
		hd.addChild( p ) ;		
	}
	while ( Lex.readOptionalToken( Tokens.Y_COMMA ) ) ;
	
	return ( hd ) ;
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

*/
static Node parseClauseAggr ( ) throws VTLError
{
	Node	hd, p, pdummy ;
	String	roleIde ;
	hd = Parser.newnode ( Nodes.N_CLAUSE_AGGR ) ;
	pdummy = Parser.newnode ( Nodes.N_DUMMY, "aggr clause" ) ;
	hd.addChild( pdummy ) ;		
	do {
		p = Parser.newnode ( Nodes.N_CLAUSE_CALC ) ;
		roleIde = readOptionalComponentRole ( ) ;
		if ( roleIde != null ) {		// it is a component role
			if ( roleIde.equals( "identifier") )
				VTLError.SyntaxError( "aggr: role cannot be identifier");
			p.addChild( Parser.newnode ( Nodes.N_STRING, roleIde ) ) ;
		}
		else
			p.addChild( Parser.makeEmptyNode() ) ;
		p.addChild( Parser.parseIdentifier() ) ;
		Lex.readToken ( Tokens.Y_TEMP_ASSIGNMENT ) ;
		p.addChild ( parseExpression () ) ;
		pdummy.addChild( p );
	}
	while ( Lex.readOptionalToken( Tokens.Y_COMMA ) ) ;
	
	// group
	if (  Lex.readOptionalIdeKeyword ( "group" ) ) {
		if ( Lex.readOptionalIdeKeyword ( "by" ) ) {
			hd.addChild( Parser.newnode ( Nodes.N_GROUP_BY, parseDimList ( ) ) ) ;
		}
		else if ( Lex.readOptionalIdeKeyword ( "except" ) ) {
			hd.addChild( Parser.newnode ( Nodes.N_GROUP_EXCEPT, parseDimList ( ) ) ) ;
		}
		else if ( Lex.readOptionalIdeKeyword ( "all" ) ) {
			Lex.readIdeKeyword ( "time_agg" ) ;
			Lex.readToken( Tokens.Y_PAR_OPEN );
			Node args = parseArguments ( Tokens.Y_COMMA, Tokens.Y_PAR_CLOSE ) ;
			if ( ! ( p.nodeListLength () >= 1 && args.nodeListLength () <= 4) )
				VTLError.SyntaxError( "time_agg must have 1 to 4 arguments");
			hd.addChild( Parser.newnode ( Nodes.N_GROUP_ALL, Parser.newnode(Nodes.N_TIME_AGG, args) ) ) ;
		}		
		else
			VTLError.SyntaxError( "aggregate operator: only (by, except, all) can follow group");
		// having
		hd.addChild ( Lex.readOptionalIdeKeyword ( "having" ) ? Parser.parseExpression() : Parser.makeEmptyNode () ) ;
	}
	else {
		// empty nodes: group by + having
		hd.addChild ( Parser.makeEmptyNode() ) ;
		hd.addChild ( Parser.makeEmptyNode() ) ;	
	}
	return ( hd ) ;
}

static Node parseComponentList ( short nodeName, boolean acceptMembership ) throws VTLError 
{
	Node p = Parser.newnode ( nodeName ) ;
	do p.addChild( parseComponentName ( acceptMembership ) ) ;
		while ( Lex.readOptionalToken ( Tokens.Y_COMMA ) ) ;
	
	return ( p ) ;
}

/*
	joinOp ( ds1 ds1 { as alias1 } { , dsN { as aliasN } }* { using usingComp }
		{ filter filterCondition }
		{ apply applyExpr | calc calcClause | aggr aggrClause { groupingClause } }
		{ keep comp {, comp }* | drop comp {, comp }*  }
		{ rename copFrom to cmpTo { , cmpFrom to cmpTo }* } )
	joinOp ::= inner_join | left_join| full_join | cross_join
	calcClause 	::=  { calcRole } calcComp := calcExpr { , { calcRole } calcComp := calcExpr }*
	calcRole    ::= identifier | measure | attribute | viral attribute
	aggrClause 	::=  { aggrRole } aggrComp := aggrExpr { , { aggrRole } aggrComp := aggrExpr }* 
	aggrRole    ::=  { measure | attribute | viral attribute }1

 * 	inner_join ( ds_bop as d1, ds_bop as d2 
  				filter ref_area = "IT" 
				calc obs_value := d1#obs_value + d2#obs_value, obs_status = d1#obs_status || d2#obs_status
				)
	// Syntax error:
	inner_join ( ds_bop as d1, ds_bop as d2 filter ref_area = "IT" filter ref_area = "IT" )
 */
static Node parseJoinClause () throws VTLError
{
	Node	hd = Parser.newnode( Nodes.N_CLAUSE_OP ) ;
	
	if ( Lex.readOptionalIdeKeyword( "filter") )
		hd.addChild( Parser.newnode( Nodes.N_CLAUSE_FILTER , parseExpression () ) ) ;				
	
	if ( Lex.readOptionalIdeKeyword( "calc") )
		hd.addChild( parseClauseCalc ( ) ) ;
	else if ( Lex.readOptionalIdeKeyword( "aggr") )
		hd.addChild( parseClauseAggr () ) ;
	else if ( Lex.readOptionalIdeKeyword( "apply") )					// apply, calc are mutually exclusive
		hd.addChild( Parser.newnode( Nodes.N_CLAUSE_APPLY , parseExpression () ) ) ;				

	if ( Lex.readOptionalIdeKeyword( "keep") )					// keep, drop are mutually exclusive
		hd.addChild( parseComponentList ( Nodes.N_CLAUSE_KEEP, true ) ) ;			
	else if ( Lex.readOptionalIdeKeyword( "drop") )
		hd.addChild( parseComponentList ( Nodes.N_CLAUSE_DROP, true ) ) ;
	
	if ( Lex.readOptionalIdeKeyword( "rename") ) {
		Node p = Parser.newnode ( Nodes.N_CLAUSE_RENAME ) ;
		do {
			p.addChild( parseComponentName ( true ) ) ; 
			Lex.readIdeKeyword( "to" ) ;
			p.addChild( parseComponentName ( false ) );
		}
		while ( Lex.readOptionalToken ( Tokens.Y_COMMA ) ) ;
		hd.addChild( p ) ;
	}
	return ( hd ) ;
}

/*
 * Parse join expression
 	join expr ::= [ inner_join | left_join | full_join | cross_join ] 
 	join_expr ( { ds { as ds } { , ds { as ds } } * ) 
 			{ on dim { , dim } * } }
 			{ filter ... }
	
	
	inner_join, left_join, full_join, cross_join
	|
	expression -> alias -> ... -> N_DUMMY or N_EMPTY
	alias: N_IDENTIFIER or N_EMPTY
	inner_join ( aact_ali01 , aact_ali02 rename aact_ali01#obs_value to obs_value ; keep obs_value )
	inner_join (na_main as a, na_main as b keep [ a#obs_value])
 */
static Node parseJoin ( String joinType ) throws VTLError
{
	Node      	hd, pargs ; 
	ListString	ls = new ListString();
	
	// join clause
	hd = Parser.newnode ( Nodes.N_JOIN, joinType ) ;
	pargs = Parser.newnode( Nodes.N_JOIN_ARGUMENTS ) ;
	for ( ; ; ) {
		Node dsExpr = parseExpression() ;
		Node pAlias ;
		if ( Lex.readOptionalIdeKeyword( "as" ) ) {
			pAlias = Parser.parseIdentifier ( ) ;
			ls.addUnique( pAlias.val, joinType);
		}
		else {
			if ( dsExpr.name != Nodes.N_IDENTIFIER && dsExpr.name != Nodes.N_VARIABLE )
				VTLError.TypeError( "join: an alias must be specified for an expression" );
			pAlias = makeEmptyNode ( ) ;	
			ls.addUnique( dsExpr.val, joinType);
		}
		pargs.addChild( dsExpr );
		pargs.addChild( pAlias );
		if ( ! Lex.readOptionalToken ( Tokens.Y_COMMA ) )
			break;
	}
	hd.addChild( pargs );
	for ( String s : ls )
		Parser.addStack( s, Nodes.N_USER_TYPE, false );
	
	if ( Lex.readOptionalIdeKeyword ( "using" ) ) {
		if ( joinType.equals( "cross_join" ) )
			VTLError.SyntaxError( "It is not allowed to specify: using in combination with: cross_join");	
		hd.addChild ( Parser.newnode ( Nodes.N_COMPONENT_LIST, parseDimList ( ) ) ) ;		
	}
	else
		hd.addChild ( makeEmptyNode ( ) ) ;
	
	// change to test the new syntax
	hd.addChild ( Parser.parseJoinClause() ) ;
	Lex.readToken ( Tokens.Y_PAR_CLOSE ) ;		
	return ( hd ) ;
	
	/* previous version:
	Lex.readToken ( Tokens.Y_PAR_CLOSE ) ;
	
	if ( Lex.readOptionalToken ( Tokens.Y_BRACKET_OPEN ) )
		return ( Parser.parseClauses ( hd ) ) ;
	// else build a clauses node with no children
	
	hd.next = Node.newnode ( Nodes.N_CLAUSE_OPERATORS ) ;
	return ( Node.newnode ( Nodes.N_CLAUSE_LIST, hd ) ) ; 
	*/
}
	// clauses
	// Lex.readToken ( Tokens.Y_BRACKET_OPEN ) ;
	// return ( Parser.parseClauses ( p ) ) ;

static String readOptionalMember ( boolean isJoin ) throws VTLError
{
	String ide = Lex.readIdentifier ( ) ;
	if ( isJoin && Lex.readOptionalToken ( Tokens.Y_SHARP )) {
		ide = ide + "#" + Lex.readIdentifier ( ) ;		
	}
	return ( ide ) ;
}

/*
 * Parse body of clauses:
 * the [ is already parsed
 * 	calc_clause := { identifier | measure | attribute } compName = k
 * 	drop_clause ::= drop { cmp { , cmp } * }
 * 	keep_clause  ::= keep { cmp { , cmp } * }
 * 	filter_clause ::= filter boolean-expression | dpr
 * 	rename_clause ::= rename cmp to cmp { , cmp to cmp }
 * 	pivot_clause ::= pivot dim , msr to elem { , elem }
 * 	unpivot_clause ::=  unpivot elem { , elem } to dim , msr
 * cmp can be identifier or object.identifier
	print 'refin.aact_ali01' [ filter geo = "IT" ]
	print 'refin.aact_ali01' [ sub geo = "IT" ]
	print ( 'refin.aact_ali01' [sub geo = "IT", itm_newa=40000 ] ) + 'refin.aact_ali01' [sub geo = "FR", itm_newa=40000 ]

	N_CLAUSE_LIST
	      |
	expression -> N_CLAUSE_OPERATORS
						|
						filter, ...
	or
	N_CLAUSE_LIST
	      |
	EXPR -> N_CLAUSE
 */
static Node parseClauseOp ( Node p_expr ) throws VTLError
{
	String		op ;
	Node      	hd, p ;
	
	hd = Parser.newnode ( Nodes.N_CLAUSE_OP, p_expr ) ; 
	
	Parser.addStack( "$this", Nodes.N_USER_OPERATOR_CALL, false);	// "$this" means that all identifiers must be treated as variables (components)
	
	switch ( op = Lex.readIdentifier ( ) ) {
		case "calc" :
			hd.addChild( parseClauseCalc ( ) ) ;
			break ;
		case "sub" : 
			p = parseSubscript ( ) ;
			hd.addChild( p ) ;
			break ;
		case "filter" : 
			hd.addChild( Parser.newnode ( Nodes.N_CLAUSE_FILTER, parseExpression() ) ) ;
			break ;
		case "aggr" :
			hd.addChild( parseClauseAggr ( ) ) ;
			break ;
		case "drop" : 
			hd.addChild( parseComponentList ( Nodes.N_CLAUSE_DROP, false ) ) ;
			break ;
		case "keep" : 
			hd.addChild( parseComponentList ( Nodes.N_CLAUSE_KEEP, false ) ) ;
			break ;			
		case "rename" : 
			p = Parser.newnode ( Nodes.N_CLAUSE_RENAME ) ;
			do {
				p.addChild( parseComponentName ( false ) ) ; 
				Lex.readIdeKeyword( "to" ) ;
				p.addChild( parseComponentName ( false ) );
			}
			while ( Lex.readOptionalToken ( Tokens.Y_COMMA ) ) ;
			hd.addChild( p ) ;
			break ;			
		case "pivot" : 
			p = Parser.newnode ( Nodes.N_CLAUSE_PIVOT ) ;
			p.child = parseIdentifier () ;
			Lex.readToken ( Tokens.Y_COMMA ) ;
			p.child.next = parseIdentifier () ; ;
			hd.addChild( p ) ;
			break ;
		case "unpivot" : 
			p = Parser.newnode ( Nodes.N_CLAUSE_UNPIVOT ) ;
			p.child = parseIdentifier () ;
			Lex.readToken ( Tokens.Y_COMMA ) ;
			p.child.next = parseIdentifier () ; ;
			hd.addChild( p ) ;
			break ;
		default : 
			VTLError.SyntaxError( "bad clause operator: " + op );
	}

	Parser.removeStack(1);
	Lex.readToken ( Tokens .Y_BRACKET_CLOSE ) ;
	
	return ( hd ) ; 
}

// parse types

/*
 * parse optional data length (create dataset)
 */
static Node parseDataLength ( String compType ) throws VTLError
{
	Node		p ;

	if ( compType.equals ( "string" ) ) {
		if ( Lex.scan () != Tokens.Y_PAR_OPEN )
			VTLError.SyntaxError ( "Data length required for type string" ) ;
		p = Parser.parseNumber () ;
		Lex.readToken ( Tokens.Y_PAR_CLOSE ) ;
	}
	else
		p = Parser.makeEmptyNode() ;
	
	return ( p ) ;
}

/*
	scalarType ::= basicScalarType | valueDomainName | setName 
					{ scalarTypeConstraint } 
					{ null | not null }
	basicScalarType ::= scalar | number | integer | string 
			| boolean | time | date | time_period | duration
	scalarTypeConstraint ::= [ booleanCondition ] | { scalarLiteralList }
	
	syntax tree:
	N_TYPE_SCALAR (or N_USER_TYPE) typeName
		constraint
		null
*/
public static Node parseScalarType ( ) throws VTLError
{
	Node	hd ;
	String	ide ;
	
	ide = Lex.readIdentifier() ;
	hd = Check.isPredefinedType( ide ) ? Parser.newnode( Nodes.N_TYPE_SCALAR, ide )
			: Parser.newnode( Nodes.N_USER_TYPE, ide ) ;
	
	if ( Lex.readOptionalToken( Tokens.Y_BRACKET_OPEN)) {
		hd.addChild ( Parser.parseExpression() ) ;
		Lex.readToken( Tokens.Y_BRACKET_CLOSE ) ;
	}
	else if ( Lex.readOptionalToken( Tokens.Y_BRACE_OPEN))
		hd.addChild ( Parser.newnode ( Nodes.N_SET_SCALAR, parseListExpr ( Tokens.Y_COMMA, Tokens.Y_BRACE_CLOSE ) ) ) ;
	else
		hd.addChild( Parser.makeEmptyNode() );
	
	if ( Lex.readOptionalIdeKeyword ( "null" ) )
		hd.addChild( Parser.newnode( Nodes.N_TYPE_NULL, "null" ) ) ;
	else if ( Lex.readOptionalIdeKeyword ( "not" ) ) {
		Lex.readIdeKeyword ( "null" ) ;
		hd.addChild( Parser.newnode( Nodes.N_TYPE_NULL, "not null" ) ) ;
	}
	else
		hd.addChild( Parser.newnode( Nodes.N_TYPE_NULL ) ) ;
	
	return ( hd ) ;
}

/*
	dataType ::= scalarType | componentType | datasetType | universalSetType | operatorType  | rulesetType
	scalarType ::= { basicScalarType | valueDomainName | setName } { scalarTypeConstraint } { { not } null }
	basicScalarType ::= scalar | number | integer | string | boolean | time | date | time_period | duration
	scalarTypeConstraint ::= 	[ valueBooleanCondition ] | { scalarLiteral { , scalarLiteral }* }	
	componentType ::=  	componentRole  { < scalar type > }
	componentRole ::=   component | identifier | measure | attribute | viral attribute
	datasetType ::=   	dataset { componentConstraint  { , componentConstraint  }* } 
	componentConstraint ::= 	componentType  { componentName | multiplicityModifier }
	multiplicityModifier ::=  	_ { + | * }
	universalSetType ::=  set { < scalarType > }
	operatorType ::=  	paramResultType { * paramResultType }* } ->  paramResultType
		paramResultType ::=	scalarType | datasetType | universalSetType | rulesetType
	rulesetType ::= 		ruleset | dpRuleset | hrRuleset
	dpRuleset ::= 	datapoint 
				| datapoint_on_valuedomains { valueDomainName  { * valueDomainName  }* }
				| datapoint_on_variables { variableName { * variableName }*  }
	hrRuleset ::= 	hierarchical 
	| hierarchical_on_valuedomains { valueDomainName 
	 				{ ( valueDomainName * { valueDomainName }* ) } }
	| hierarchical_on_variables { variableName { ( variableName * { variableName }* ) }  }
					variableName ::= name 

	valueDomainName, setName, componentName ::= name

	dataset component ::= role <scalartype> compName
	scalartype ::= typeName constraint null/not null
	TBD: dimension cannot be null
*/
public static Node parseType ( ) throws VTLError
{
	String		tmp ;
	Node		hd, p ;

	if ( Lex.readOptionalIdeKeyword( "dataset") ) {			// dataset
		hd = Parser.newnode( Nodes.N_TYPE_DATASET ) ;
		Lex.readToken( Tokens.Y_BRACE_OPEN ) ;
		do {
			p = Parser.newnode( Nodes.N_TYPE_COMPONENT ) ;
			if ( ( tmp = readOptionalComponentRole ( ) ) == null )
				VTLError.SyntaxError( "Expected: component role");
			p.addChild( Parser.newnode( Nodes.N_STRING, tmp ));
			Lex.readToken( Tokens.Y_LT ) ;
			p.addChild( parseScalarType () ) ; 
			Lex.readToken( Tokens.Y_GT ) ;
			// component name
			if ( Lex.readOptionalToken( Tokens.Y_UNDERSCORE )) {
				if ( Lex.readOptionalToken( Tokens.Y_MULTIPLY ))
					tmp = "_*" ;
				else if ( Lex.readOptionalToken( Tokens.Y_PLUS ))
					tmp = "_+" ;
				else
					tmp = "_" ;
				p.addChild( Parser.newnode( Nodes.N_STRING, tmp ) ) ;
			}
			else
				p.addChild( parseIdentifier () );
			hd.addChild(p);
		} while ( Lex.readOptionalToken( Tokens.Y_COMMA ) ) ;
		Lex.readToken( Tokens.Y_BRACE_CLOSE ) ;
	}
	else if ( Lex.readOptionalIdeKeyword ( "set" ) ) {
		Lex.readToken( Tokens.Y_LT ) ;
		hd = Parser.newnode( Nodes.N_TYPE_SET, parseScalarType () ) ;
		Lex.readToken( Tokens.Y_GT ) ;
	}
	else if ( Lex.readOptionalIdeKeyword( "component") 
				|| readOptionalComponentRole ( ) != null ) {
		Lex.readToken( Tokens.Y_LT ) ;
		hd = Parser.newnode( Nodes.N_TYPE_COMPONENT, parseScalarType () ) ;
		Lex.readToken( Tokens.Y_GT ) ;
	}
	else if ( Lex.readOptionalIdeKeyword( "ruleset") ) {
		hd = Parser.newnode( Nodes.N_TYPE_RULESET, "ruleset" ) ;		
	}
	else if ( Lex.readOptionalIdeKeyword( "datapoint") ) {
		hd = Parser.newnode( Nodes.N_TYPE_RULESET, "datapoint" ) ;
	}
	else if ( Lex.readOptionalIdeKeyword( "datapoint_on_valuedomains") ) {
		hd = Parser.newnode( Nodes.N_TYPE_RULESET, "datapoint_on_valuedomains", parseVariablesProduct ( ) ) ;				
	}
	else if ( Lex.readOptionalIdeKeyword( "datapoint_on_variables") ) {
		hd = Parser.newnode( Nodes.N_TYPE_RULESET, "datapoint_on_variables", parseVariablesProduct ( ) ) ;				
	}
	else if ( Lex.readOptionalIdeKeyword( "hierarchical") ) {
		hd = Parser.newnode( Nodes.N_TYPE_RULESET, "hierarchical" ) ;				
	}
	else if ( Lex.readOptionalIdeKeyword( "hierarchical_on_valuedomains" ) ) {
		hd = Parser.newnode( Nodes.N_TYPE_RULESET, "hierarchical_on_valuedomains", parseVariablesProduct ( ) ) ;				
	}
	else if ( Lex.readOptionalIdeKeyword( "hierarchical_on_variables" ) ) {
		hd = Parser.newnode( Nodes.N_TYPE_RULESET, "hierarchical_on_variables", parseVariablesProduct ( ) ) ;
	}
	else
		hd = parseScalarType () ;

	return ( hd ) ;
}

// end parse types

/*
 * Parse set of positions.
 */
static Node parseScalarSet ( ) throws VTLError
{
	Node	p ;
  
	if ( Lex.readOptionalToken( Tokens.Y_BRACE_OPEN )) 
    	p = Parser.newnode ( Nodes.N_SET_SCALAR, parseListExpr ( Tokens.Y_COMMA, Tokens.Y_BRACE_CLOSE ) ) ;
	else
		p = parseObjectName ( true ) ;
	
	return ( p ) ;
}

/*
 * "(" already parsed
 * 
 */
static Node parseFunctionalOperatorCall ( String opName ) throws VTLError
{
	int		numArgs, idx = 0, opIndex ;
	Node 	hd ;
	String	opCategory, opParameters, parType ;
	boolean	isList, isUserDefinedOp ;
	
	if ( ( opIndex = sqlFunctionIndex( opName ) ) < 0 ) {
		hd = Parser.newnode ( Nodes.N_USER_OPERATOR_CALL, opName ) ;  
		isList = isUserDefinedOp = true ;
		opParameters = "M" ;
	}
	else {
		opCategory = Parser.functionalOperator[opIndex][1] ;
		if ( opCategory.contains( "A" ) || opCategory.contains( "G" ) ) 
			return ( parseAggregateAnalyticCall ( opName ) ) ;
		opParameters = Parser.functionalOperator[opIndex][3] ;		
		if ( opCategory.equals( "V" ) || opCategory.equals( "S" ) )
			hd = Parser.newnode ( setNodeNames [opIndex] ) ;
		else if ( opName.equals ( "between" ) )					// exception
			hd = Parser.newnode ( Nodes.N_BETWEEN ) ;
		else if ( opName.equals ( "isnull" ) )					// exception
			hd = Parser.newnode ( Nodes.N_ISNULL ) ;
		else if ( opName.equals ( "match_characters" ) )		// exception
			hd = Parser.newnode ( Nodes.N_MATCH_CHARACTERS ) ;
		else
			hd = Parser.newnode ( Nodes.N_SQL_ROW_FUNCTION, opName ) ;
		isList = opParameters.equals("+") ;
		isUserDefinedOp = false ;
	}
	
	numArgs = isList ? 100 : opParameters.length() ;
	
	if ( numArgs == 0 ) {
		Lex.readToken( Tokens.Y_PAR_CLOSE ) ;
		return ( hd ) ;
	}
	if ( numArgs == 1 && opParameters.charAt(0) == 'o' ) {
		if ( Lex.readOptionalToken( Tokens.Y_PAR_CLOSE ) )
			return ( hd ) ;
	}
	
	do {
		idx ++ ;
		if ( idx > numArgs )
			VTLError.SyntaxError ( "Too many arguments (" + idx + " instead of " + numArgs + ") for operator: " + opName ) ;
		
		parType = ( isList ? "M" : Parser.functionalOperator[opIndex][3 + idx] ) ; // i.e. 4-1
		
		if ( Lex.readOptionalToken ( Tokens.Y_UNDERSCORE ) ) {
			if ( ( isList && ! isUserDefinedOp ) || opParameters.charAt(idx-1) == 'M' )
				VTLError.SyntaxError ( "Parameter is not optional for operator: " + opName ) ;
			if ( idx == 2 && opName.equals( "substr" ))
				hd.addChild( Parser.newnode( Nodes.N_NUMBER, "1" ));// diff. from Oracle
			hd.addChild( Parser.newnode( Nodes.N_DEFAULT_VALUE ) ) ;	
		}
		else if ( parType.startsWith( " " ) ) {
			String	option = Lex.readIdentifier() ;
			if ( ! parType.contains ( ' ' + option + ' ' ) )
				VTLError.SyntaxError( "Operator: " + opName + ", bad parameter " + option );
			hd.val = option ;
		}
		else {
			if ( Lex.readOptionalToken( Tokens.Y_BRACE_OPEN ) )
				hd.addChild ( Parser.newnode ( Nodes.N_SET_SCALAR, parseListExpr ( Tokens.Y_COMMA, Tokens.Y_BRACE_CLOSE ) ) ) ;
			else
				hd.addChild( parseExpression () ) ;				
		}
	} while ( Lex.readOptionalToken ( Tokens.Y_COMMA ) ) ;
	
	Lex.readToken( Tokens.Y_PAR_CLOSE ) ;	
	
	if ( ( ! isList ) && idx < numArgs &&  opParameters.charAt(idx) == 'M' )
		VTLError.SyntaxError ( "Too few arguments (" + + idx + " instead of " + numArgs + ") for operator: " + opName ) ;

	return ( hd ) ;
}

/*
 * SQL functions. These functions are translated to SQL functions.
 * /* Cat. can be: 
 * V	VTL operator
 * S	Set operator
 * R 	single-row function
 * A	analytic function
 * AG	analytic/aggregate function
 * 
 * Parms:
 * M	1 mandatory argument
 * o	1 optional argument
 * +	any number of arguments
 * NB: the type of the result of abs and mod depends on the type of the operands 
 */
static final String[] functionalOperator [] = {
//Operator				Cat.	Returns		Parms	Parm 1		Parm 2		Parm 3		Parm 4
{ "cast", 				"V",	"scalar", 	"MMo",	"scalar",	"type",		"string"				} ,
{ "period_indicator",	"V",	"set", 		"o",	"scalar",										} ,
{ "fill_time_series", 	"V",	"set", 		"Mo",	"dataset",	" single all "						} ,
{ "flow_to_stock", 		"V",	"set", 		"M",	"dataset",										} ,
{ "stock_to_flow", 		"V",	"set", 		"M",	"dataset",										} ,
{ "timeshift", 			"V",	"set", 		"MM",	"dataset",	"integer"							} ,
{ "time_agg", 			"V",	"scalar", 	"Mooo",	"scalar",	"scalar",	"scalar",	" first last " } ,
{ "setdiff", 			"S",	"dataset", 	"MM",	"dataset",	"dataset"							} ,
{ "symdiff", 			"S",	"dataset", 	"MM",	"dataset",	"dataset"							} ,
{ "union", 				"S",	"dataset", 	"+",	"dataset"										} ,
{ "intersect", 			"S",	"dataset", 	"+",	"dataset"										} ,
{ "exists_in", 			"S",	"dataset", 	"MMo",	"dataset",	"dataset",	" all true false "		} ,
{ "abs", 				"R",	"number", 	"M",	"number" 										} ,
{ "ln", 				"R",	"number", 	"M",	"number" 										} ,
{ "sqrt", 				"R",	"number", 	"M",	"number" 										} ,
{ "ceil", 				"R",	"integer", 	"M",	"number" 										} ,
{ "floor", 				"R",	"integer", 	"M",	"number" 										} ,
{ "exp", 				"R",	"number", 	"M",	"number" 										} ,
{ "mod", 				"R",	"number", 	"MM",	"number", 	"number" 							} ,
{ "power", 				"R",	"number", 	"MM",	"number", 	"number" 							} ,
{ "log", 				"R",	"number", 	"MM",	"number", 	"number" 							} ,
{ "round",	 			"R",	"integer", 	"Mo",	"number", 	"integer" 							} ,
{ "trunc", 				"R",	"integer", 	"Mo",	"number", 	"integer" 							} ,
{ "length", 			"R",	"integer", 	"M",	"string" 										} ,
{ "lower", 				"R",	"string", 	"M",	"string" 										} ,
{ "upper", 				"R",	"string", 	"M",	"string" 										} ,
{ "trim", 				"R",	"string", 	"M",	"string" 										} ,
{ "ltrim", 				"R",	"string", 	"M",	"string" 										} ,
{ "rtrim", 				"R",	"string", 	"M",	"string" 										} ,
{ "match_characters",	"R",	"boolean", 	"MM",	"string", 	"string" 							} ,
{ "substr",				"R",	"string", 	"Moo",	"string", 	"integer", 	"integer" 				} ,
{ "replace",			"R",	"string", 	"MMo",	"string", 	"string", 	"string" 				} ,
{ "instr",				"R",	"integer", 	"MMoo",	"string", 	"string", 	"integer", 	"integer" 	} ,
{ "nvl",				"R",	"scalar", 	"MM",	"scalar", 	"scalar" 							} ,
{ "isnull",				"R",	"boolean", 	"M",	"scalar" 										} ,
{ "current_date",		"R",	"date", 	"" 														} ,
{ "between", 			"R",	"boolean", 	"MMM",	"scalar",	"scalar",	"scalar"				} ,
{ "avg", 				"AG",	"number",	"M",	"number" 										} ,
{ "count", 				"AG",	"integer",	"M", 	"scalar"										} ,
{ "max", 				"AG",	"scalar",	"M", 	"scalar"										} ,
{ "median", 			"AG",	"number",	"M", 	"number"										} ,
{ "min", 				"AG",	"scalar",	"M", 	"scalar"										} ,
{ "stddev_pop",			"AG",	"number",	"M", 	"number"										} ,
{ "stddev_samp",		"AG",	"number",	"M", 	"number"										} ,
{ "sum",				"AG",	"number",	"M", 	"number"										} ,
{ "var_pop",			"AG",	"number",	"M", 	"number"										} ,
{ "var_samp",			"AG",	"number",	"M", 	"number"										} ,
{ "first_value",		"A",	"scalar",	"M", 	"scalar"										} ,
{ "last_value",			"A",	"scalar",	"M", 	"scalar"										} ,
{ "lag",				"A",	"scalar",	"MMo", 	"scalar", 	"integer",	"scalar"				} ,
{ "lead",				"A",	"scalar",	"MMo", 	"scalar", 	"integer",	"scalar"				} ,
{ "rank",				"A",	"integer",	""	 													} ,
{ "ratio_to_report",	"A",	"number",	"M", 	"number"										} ,
} ;

static final short[] setNodeNames = { 
	Nodes.N_CAST ,
	Nodes.N_PERIOD_INDICATOR ,
	Nodes.N_FILL_TIME_SERIES ,
	Nodes.N_FLOW_TO_STOCK ,
	Nodes.N_STOCK_TO_FLOW ,
	Nodes.N_TIMESHIFT ,
	Nodes.N_TIME_AGG ,
	Nodes.N_SETDIFF, 
	Nodes.N_SYMDIFF, 
	Nodes.N_UNION, 
	Nodes.N_INTERSECT, 
	Nodes.N_EXISTS_IN
} ;

static final String sqlFunctionTypeResult ( int pred_function_index ) {
	return( functionalOperator [ pred_function_index ] [2] ) ;
}
static final String sqlFunctionTypeFirstOperand ( int pred_function_index ) {
	return( functionalOperator [ pred_function_index ] [4] ) ;
}
static final String sqlFunctionTypeParameter( int pred_function_index, int opIndex ) {
	return( functionalOperator [ pred_function_index ] [5 + opIndex] ) ;
}

static int sqlFunctionIndex( String function_name, String functionCategory )
{
	int arraydim = functionalOperator.length ;
	for ( int idx = 0; idx < arraydim ; idx ++ ) {
		if ( function_name.equals ( functionalOperator [ idx ] [0] ) ) {
			if ( functionalOperator [ idx ] [1].contains ( functionCategory ) )
				return ( idx ) ;	
			else
				break ;
		}		
	}
	return ( -1 ) ;
}

static int sqlFunctionIndex( String function_name )
{
	int arraydim = functionalOperator.length ;
	for ( int idx = 0; idx < arraydim ; idx ++ ) {
		if ( function_name.equals ( functionalOperator [ idx ] [0] ) )
			return ( idx ) ;
	}
	return ( -1 ) ;
}

// end class
}
