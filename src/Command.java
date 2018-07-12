
/*
 * VTL commands. Parsing and evaluation.
 */

import java.util.Vector;

class Command {

/*
 * Commands
 */
interface Commands {
static final short 			

N_OBJECT_DESCRIPTION	= 100 ,
N_COPY          		= 101 ,
N_DROP          		= 102 ,
N_RANGE	        		= 103 ,
N_FOR           		= 104 ,
N_CASE            		= 105 ,
N_LOAD          		= 106 ,
N_PRINT_EXPR       		= 107 ,
N_RENAME        		= 108 ,
N_SQL           		= 110 ,
N_GRANT	        		= 111 ,
N_REVOKE	        	= 112 ,
N_PURGE					= 113 ,
N_TRY_CATCH				= 114 ,
N_THROW					= 115 ,
N_RETURN				= 116 ,
N_TEMP_ASSIGNMENT		= 117 ,	
N_PERS_ASSIGNMENT		= 118 ,
N_RESTORE				= 119 ,
N_STATEMENT_LIST		= 120 ,

										// commands
N_CREATE_DATAPOINT_RULESET	= 130 ,
N_DEFINE_DATAPOINT_RULESET	= 131 ,
N_CREATE_HIERARCHICAL_RULESET  = 132 ,
N_DEFINE_HIERARCHICAL_RULESET  = 133 ,
N_CREATE_OPERATOR  			= 134 ,
N_DEFINE_OPERATOR 			= 135 ,
N_DEFINE_OPERATOR_ARGS		= 136 ,		// components of define operator
N_DEFINE_OPERATOR_BODY		= 137 ,
N_CREATE_DATAPOINT_RULE		= 138 ,		// components of define datapoint ruleset
N_CREATE_RULE_EQUATION		= 139 ,		// components of define hierarchical ruleset
N_CREATE_RULE_EQUATION_ITEMS= 140 ,		// components of define hierarchical ruleset
N_CREATE_VALUEDOMAIN 		= 141 ,
N_CREATE_SYNONYM  			= 142 ,
N_CREATE_DATASET  			= 143 ,
N_CREATE_VIEW  				= 144 ,
N_DEFINE_VIEW 				= 145 ,
N_CREATE_FUNCTION			= 146 ,	
N_DEFINE_FUNCTION			= 147 ,
N_CREATE_FUNCTION_ARGS		= 148 ,
N_CREATE_FUNCTION_BODY		= 149 ,
N_CREATE_DATASET_LIKE  		= 150 ,
N_CREATE_VALUEDOMAIN_SUBSET	= 151 ,
N_DEFINE_VALUEDOMAIN_SUBSET	= 152 ,
										// options in alter command
N_ALTER				  		= 160 ,
N_ALTER_DROP_COMPONENT		= 166 ,
N_ALTER_MOVE				= 169 ,
N_ALTER_RENAME				= 170 ,
N_ALTER_ADD_COMPONENTS		= 171 ,
N_ALTER_MODIFY_COMPONENTS	= 172 ,
N_ALTER_STORAGE_OPTIONS 	= 178 ,
N_ALTER_COMPONENT_TYPE		= 180 ,

N_CASE_WHEN					= 181 ,
N_CASE_ELSE					= 182 ,

N_ALTER_USER_PROFILE		= 190 ;		

static final String text_cmds [] = {
	"description", "copy", "drop", "range", "for", "case", "load", "print", "rename", "sql", 
	"grant", "revoke", "purge", "try", "throw", "return", "restore"
} ;
static final ListString TextCommands = new ListString ( text_cmds ) ;

static final int NameCommands [] = {
	N_OBJECT_DESCRIPTION, N_COPY, N_DROP, N_RANGE, N_FOR, N_CASE, N_LOAD,N_PRINT_EXPR,N_RENAME, N_SQL, 
	N_GRANT, N_REVOKE, N_PURGE, N_TRY_CATCH, N_THROW, N_RETURN, N_RESTORE
} ;

}

/*
 * Read object type including synonym ( Y ).
 */
static char  readObjectType ( ) throws VTLError
{
	String str ;

	switch ( str = Lex.readIdentifier() ) {
		case "hierarchical" : 	Lex.readIdeKeyword ( "ruleset" ) ;	return ( VTLObject.O_HIERARCHICAL_RULESET ) ;
		case "datapoint" : 		Lex.readIdeKeyword ( "ruleset" ) ; 	return ( VTLObject.O_DATAPOINT_RULESET ) ;
		case "valuedomain" : 	return ( VTLObject.O_VALUEDOMAIN ) ;
		case "operator" : 		return ( VTLObject.O_OPERATOR ) ;
		case "function" : 		return ( VTLObject.O_FUNCTION ) ;
		case "dataset" : 		return ( VTLObject.O_DATASET ) ;
		case "view" : 			return ( VTLObject.O_VIEW ) ;
		case "synonym" : 		return ( VTLObject.O_SYNONYM ) ;
	}

	VTLError.SyntaxError ( "Bad object type: " + str ) ;
	return ( '*' ) ;
}

/*
 * Syntax tree:
	(component) role
		component name
		N_TYPE_SCALAR (or N_USER_TYPE) typeName
			constraint
			null
		width (or empty)

	define dataset ds1 is
	identifier	ref_area	ref_area	;
	identifier	time	time_period ;
	measure	obs_value	number	 ;
	attribute	obs_status	obs_status
	end dataset ; 
*/
static Node parseDatasetComponent ( ) throws VTLError
{
	Node	p = Parser.newnode( Nodes.N_TYPE_COMPONENT ) ;
	
	String	componentRole ;

	// read component role (identifier, measure, attribute)
	if ( ( componentRole = Parser.readOptionalComponentRole ( ) ) == null )
		VTLError.SyntaxError( "Expected: component role");
	p.val = componentRole ;
	
	// read component name
	p.addChild( Parser.parseComponentName() ) ;
	
	// read component type
	Node pType = Parser.parseScalarType () ;
	if ( pType.child_2().val != null && pType.child_2().val.equals ( "null") )
		VTLError.SyntaxError( "identifier component cannot be null");
	p.addChild( pType ) ;
	
    // read dimension width ( data length )
    p.addChild( Parser.parseDataLength ( pType.val ) ) ;
	
	return ( p ) ;
}
	
/*
 * Returns a N_ALTER_STORAGE_OPTIONS node containing the sql storage options. 
 * Syntax:
 * [ organization index [ compress ] ]
 * [ tablespace tablespace_name ]
 * [ storage ( [ initial size_initial ] [ next size_next ] ) ]
 * [ pctfree percentage_free ]
 * [ pctused percentage_used ]
 */
static String readStorageOptions ( ) throws VTLError
{
    StringBuffer 	sql_storage_options = new StringBuffer () ;
    
    for ( ; ; ) {
    	if ( Lex.readOptionalIdeKeyword( "tablespace" )) {
    		sql_storage_options . append ( "tablespace " ) . append ( Lex.readIdentifier() ) ;    		
    	}
    	else if ( Lex.readOptionalIdeKeyword( "organization")) {
    		sql_storage_options . append ( "organization " ).append (  Lex.readIdentifier() ) ;
    		if ( Lex.readOptionalIdeKeyword( "compress"))
    			sql_storage_options . append ( " compress" ) ;
    		sql_storage_options . append ( " " ) ;
    	}
    	else if ( Lex.readOptionalIdeKeyword( "pctfree")) {
    		sql_storage_options.append ( "pctfree " ).append ( Lex.readNumber() ) ;
    	}
    	else if ( Lex.readOptionalIdeKeyword( "pctused")) {
    		sql_storage_options.append ( "pctused " ).append ( Lex.readNumber() ) ;
    	}
    	else if ( Lex.readOptionalIdeKeyword( "storage")) {
    		Lex.readToken ( Tokens.Y_PAR_OPEN ) ;
    		sql_storage_options.append ( "storage (" ) ;
    		if ( Lex.readOptionalIdeKeyword( "initial") )
    			sql_storage_options.append ( "initial " ) . append (  Lex.nextTokenString() ) ;
    		if ( Lex.readOptionalIdeKeyword( "next"))
    			sql_storage_options.append ( "next " ) . append (  Lex.nextTokenString() ) ;
       		Lex.readToken ( Tokens.Y_PAR_CLOSE ) ;   	
       		sql_storage_options.append ( ")" ) ;
       	}
    	else
    		break ;
    	
    	sql_storage_options.append ( " " ) ;
    }
    return ( sql_storage_options.toString() ) ;
}

/*
 * Parse data object definition - VTL syntax ( create statement ).
 	create/define dataset
 	create/define valuedomain
 	create/define view
create dataset ds (
identifier i integer ;
measure m integer
) 
 */
static Node parseCreateDataset ( char object_type, String objectName, boolean create_or_replace ) throws VTLError
{
	Node	hd ;

	switch ( object_type ) {
	    case VTLObject.O_DATASET : 
	    	if ( Lex.readOptionalIdeKeyword( "like" )) {
		    	hd = Parser.newnode ( Commands.N_CREATE_DATASET_LIKE, objectName, Parser.parseObjectName() ) ;
	    		Lex.readIdeKeyword ( "end" ) ;	    		
	    		Lex.readIdeKeyword ( "dataset" ) ;	
	    		return ( hd ) ;
	    	}
	    	hd = Parser.newnode ( Commands.N_CREATE_DATASET, objectName ) ; 
	    	break ;
	    case VTLObject.O_VALUEDOMAIN : 
	    	if ( Lex.readOptionalIdeKeyword( "subset" ) ) {
	    		Lex.readIdeKeyword( "of" ) ;
	    		hd = Parser.newnode ( (create_or_replace ? Commands.N_DEFINE_VALUEDOMAIN_SUBSET 
	    													: Commands.N_CREATE_VALUEDOMAIN_SUBSET), objectName ) ; 
	    		hd.addChild( Parser.parseScalarType() ) ;
	    		if ( hd.child.name != Nodes.N_USER_TYPE )
	    			VTLError.SyntaxError( "valuedomain subset: can only be subset of a valuedomain");
	    		// check null/not null
	    		Lex.readIdeKeyword ( "end" ) ;	    		
	    		Lex.readIdeKeyword ( "valuedomain" ) ;	  
	    		return ( hd ) ;
	    	}
	    	else
	    		hd = Parser.newnode ( Commands.N_CREATE_VALUEDOMAIN, objectName ) ; 
	    	break ;
	    default : hd = null ;
	}
	
	Lex.readIdeKeyword( "is" ) ;
	for ( ; ; ) {
		hd.addChild( parseDatasetComponent( ) ) ;
		if ( Lex.readOptionalIdeKeyword ( "end" ) )
			break ;
      	Lex.readToken ( Tokens.Y_SEMICOLON ) ;
		if ( Lex.readOptionalIdeKeyword ( "end" ) )
			break ;
	}		
	
	Lex.readIdeKeyword( object_type == VTLObject.O_DATASET ? "dataset" : "valuedomain" ) ;
	
	return ( hd ) ;
}

/*
 * define operator
	define operator  operator_name ( { parameter { , parameter }* } )
		{ returns outputType }
	is operatorBody
	end define operator
	parameter::= parameterName parameterType { default parameterDefaultValue }

	function_args	return_type	function_body	text_of_body
	the function body (text) is attached to a temporary node (this node must not be saved in the syntax tree)
  	define operator f ( x number, y number ) returns integer 
  	is x + y
  	end operator
  	objectType : F or ...
  	
  	define operator f ( s string, ss set <string> ) returns boolean is s in ss end operator ; 
	f ( "1", { "ee" } )
	TBD f ( 1, { "ee" } )
  	
  	define operator f2 ( ds dataset { identifier<ref_area> r, measure <number> obs_value } ) returns number is 
  		count ( ds ) 
  	end operator ; 
	f ( na_main )
	
  	define function fun1 ( n number ) returns number is 
  		x := n * 100 ;
  		return ( x + 1 ) 
  	end function ; 

*/
static Node parseDefineOperator ( String object_name, char objectType, boolean create_or_replace ) throws VTLError
{
	Node		hd, p, hdParms, hdBody, hdReturnType, p_type, p_default ;
	String		var_name ;
	int 		markStartText ;
	boolean		isOperator = ( objectType == VTLObject.O_OPERATOR ) ;

	Parser.resetStack() ;
	markStartText = Lex.inputTextMarkStart ( ) ;
  
	if ( isOperator )
		hd = Parser.newnode ( ( create_or_replace ? Commands.N_DEFINE_OPERATOR : Commands.N_CREATE_OPERATOR ), object_name ) ;
	else
		hd = Parser.newnode ( ( create_or_replace ? Commands.N_DEFINE_FUNCTION : Commands.N_CREATE_FUNCTION ), object_name ) ;
		
	hdParms = Parser.newnode ( Commands.N_DEFINE_OPERATOR_ARGS ) ;
	hdBody = Parser.newnode ( Commands.N_DEFINE_OPERATOR_BODY ) ;

	// function arguments
	Lex.readToken ( Tokens.Y_PAR_OPEN ) ;

	if ( Lex.readOptionalToken ( Tokens.Y_PAR_CLOSE ) )
		; // no arguments
	else {
		do {
			p = Parser.parseIdentifier () ;
			p_type = Parser.parseType () ;
			var_name = p.val ;		// variable name
			Parser.addStack ( var_name, p_type.name, true ) ;
	
			if ( Lex.readOptionalIdeKeyword( "default" )) {
				if ( p_type.name != Nodes.N_TYPE_SCALAR)
					VTLError.SyntaxError("define operator: default value can be defined only for scalar value" );
				p_default = Parser.parseExpression() ;
				if ( p_default.name != Nodes.N_STRING && p_default.name != Nodes.N_NUMBER )
					VTLError.SyntaxError("define operator: default value can be only a constant scalar value" );
			}
			else
				p_default = Parser.makeEmptyNode ( ) ;
			Node	parm = Parser.newnode( Nodes.N_DUMMY, "parameter" ) ;
			
			parm.addChild( p );
			parm.addChild( p_type );
			parm.addChild( p_default );
			
	        hdParms.addChild ( parm ) ;
		} while ( Lex.readOptionalToken ( Tokens.Y_COMMA ) ) ;
		Lex.readToken ( Tokens.Y_PAR_CLOSE ) ;
	}
 	
	// return type is optional
	hdReturnType = Lex.readOptionalIdeKeyword ( "returns" ) ? Parser.parseType () 
							: Parser.makeEmptyNode() ;
	
	Lex.readIdeKeyword( "is" );						
	if ( objectType == VTLObject.O_OPERATOR )
		hdBody.addChild( Parser.parseExpression() ) ;
	else {
		Node p_statement_list = Parser.newnode( Commands.N_STATEMENT_LIST ) ;
		for ( ; ;  ) {
			p_statement_list.addChild ( parseCmd () ) ;
			if ( Lex.readOptionalIdeKeyword ( "end" ) ) {
				Lex.unscan();
				break ;
			}
		  	Lex.readToken ( Tokens.Y_SEMICOLON ) ;
			if ( Lex.readOptionalIdeKeyword ( "end" ) ) {
				Lex.unscan();
				break ;
			}
		}
		hdBody.child = p_statement_list ;
	}
		
	Lex.readIdeKeyword ( "end" );		// Lex.readIdeKeyword ( "define" );			correction proposed to RM
	Lex.readIdeKeyword ( objectType == VTLObject.O_OPERATOR ? "operator" : "function" );
	
	hd.addChild ( hdParms ) ;
	hd.addChild ( hdReturnType ) ;
	hd.addChild ( hdBody ) ;
	hd.addChild( Parser.newnode(Nodes.N_DUMMY, 
								"define " + ( objectType == VTLObject.O_OPERATOR ? "operator " : "function " ) 
										+ object_name + " ( " + Lex.inputTextGet ( markStartText ) ) ) ;
	
	return ( hd ) ;
}

static Node variableOrValuedomain ( ) throws VTLError
{
	String	tmp = null ;
	if ( Lex.readOptionalIdeKeyword( "variable") )
		tmp = "variable" ;		// defined on variables
	else if ( Lex.readOptionalIdeKeyword( "valuedomain") )
		tmp = "valuedomain" ;	// defined on value domains
	else
		VTLError.SyntaxError( "expected: valuedomain or variable");
	return ( Parser.newnode( Nodes.N_DUMMY, tmp ));
}

/*
	define datapoint ruleset rsName ( dpRulesetSignature ) is
		dpRule { ; dpRule }*
	end datapoint ruleset 
	dpRulesetSignature ::= valuedomain listValueDomains | variable listVariables
	listValueDomains ::= valueDomain { as vdAlias } { , valueDomain { as vdAlias } }*
	listVariables  ::= variable { as varAlias } { , variable { as varAlias } }*
	dpRule  ::= { ruleName : } { when antecedentCondition then } consequentCondition
	{ errorcode  errorCode  } { errorlevel  errorLevel  }

 	define datapoint ruleset test_dpr ( variable obs_value ) is
		obs_value > 10000 errorcode "zzzz";
	end datapoint ruleset
	check_datapoint ( na_main, test_dpr )
*/
static Node parseDefineDatapointRuleset ( String object_name, boolean create_or_replace) throws VTLError
{
  	Node		hd, p, pWhen, pThen, pCondVariables, pRuleid ;
	
	hd = Parser.newnode ( ( create_or_replace ? Commands.N_DEFINE_DATAPOINT_RULESET : Commands.N_CREATE_DATAPOINT_RULESET ), object_name ) ;
	
	Lex.readToken ( Tokens.Y_PAR_OPEN ) ;
	
	hd.addChild( variableOrValuedomain ( ) ) ;
	
	pCondVariables = parseDimListAddStack( ) ;

	hd.addChild( pCondVariables ) ;
	
	Lex.readToken ( Tokens.Y_PAR_CLOSE ) ;

	Lex.readIdeKeyword ( "is" ) ;
	
	for ( ; ; ) {
	    p = Parser.newnode ( Commands.N_CREATE_DATAPOINT_RULE ) ;
	    
		if ( Lex.readOptionalIdeKeyword ( "when" ) ) {
			pRuleid = Parser.makeEmptyNode() ;
			pWhen = Parser.parseCondition () ;
			Lex.readIdeKeyword ( "then" ) ;
			pThen = Parser.parseCondition () ;
		}
		else {
			pThen = Parser.parseCondition () ;
			if ( pThen.name == Nodes.N_IDENTIFIER && Lex.readOptionalToken( Tokens.Y_COLON )) {
				pRuleid = pThen ;
				pWhen = Lex.readOptionalIdeKeyword ( "when" ) ? Parser.parseCondition () 
							: Parser.makeEmptyNode() ;
				pThen = Parser.parseCondition () ;
			}
			else {
				pWhen = Parser.makeEmptyNode() ;
				pRuleid = Parser.makeEmptyNode() ;				
			}
		}
		
		if ( pWhen.name != Nodes.N_EMPTY && pCondVariables.name == Nodes.N_EMPTY )
			VTLError.SyntaxError( "found when without condition");

		p.addChild( pRuleid );
		p.addChild( pWhen );
		p.addChild( pThen );

		// TBD: should be literal
		p.addChild( Lex.readOptionalIdeKeyword ( "errorcode" ) ? Parser.parseExpression () 
					: Parser.makeEmptyNode() ) ;
		// TBD: should be literal
		p.addChild( Lex.readOptionalIdeKeyword ( "errorlevel" ) ? Parser.parseExpression () 
					: Parser.makeEmptyNode() ) ;
		
		hd.addChild( p ) ;
		if ( Lex.readOptionalIdeKeyword ( "end" ) )
			break ;
      	Lex.readToken ( Tokens.Y_SEMICOLON ) ;
		if ( Lex.readOptionalIdeKeyword ( "end" ) )
			break ;
	}
    Lex.readIdeKeyword ( "datapoint" );
    Lex.readIdeKeyword ( "ruleset" );
    Parser.resetStack();						// reset stack variables
    return ( hd ) ;
}

/*
 * Used by define ruleset.
 */
static Node parseDimListAddStack ( ) throws VTLError
{
	Node hd = Parser.newnode( Nodes.N_DUMMY), p ;

	do  {
		p = Parser.parseIdentifier ();
		p.next = Lex.readOptionalIdeKeyword( "as" ) ? Parser.parseIdentifier () : Parser.makeEmptyNode() ;			
		hd.addChild( p );	  
	} while ( Lex.readOptionalToken ( Tokens.Y_COMMA ) ) ;

	for ( Node p1 = hd.child; p1 != null ; p1 = p1.next ) {
		Parser.addStack ( (p1.next.name == Nodes.N_EMPTY ? p1.val : p1.next.val), Nodes.N_TYPE_SCALAR, false ) ;
		p1 = p1.next ;
	}

	return(hd);
}

/*
	define hierarchical ruleset rsName ( hrSignature ) is 
		hrRule { ; hrRule }*
	end hierarchical ruleset
	hrSignature  ::= vdSignature | varSignature 
	vdSignature ::= valuedomain { condition vdCondSignature } rule ValueDomain
	vdCondSignature ::= ValueDomain { as vdAlias } { , ValueDomain { as vdAlias } }*
	varSignature ::= variable { condition varCondSignature } rule Variable
	varCondSignature ::= Variable { as vdAlias } { , Variable { as vdAlias } }*
	hrRule ::= { ruleName : } codeItemRelation { errorcode errorCode } { errorlevel errorLevel }
	codeItemRelation ::= { when leftCondition then } leftCodeItem = | > | < | >= | <= 
		+ | -   rightCodeItem  { [ rightCondition ] } { + |- rightCodeItem { [ rightCondition ] } }*

	equation:
	left_dim right_dim dim_condition properties create_equation												create_equation			etc
				condition comment left_part eq_type pos pos pos pos pos		etc
	define hierarchical ruleset ir_bleu ( ref_area ,flow ) is
		BLEU = LU + BE ;
	end integrity rule ;
*/
static Node parseDefineHierarchicalRuleset ( String object_name, char object_type, boolean createOrReplace) throws VTLError
{
	String	sign ;
	Node	hd, p_eq, pEquationSymbol, pCondVariables, pItems, ruleVariable, pWhen = null, pRuleid, pLeftSide, p ;

	hd = Parser.newnode ( createOrReplace ? Commands.N_DEFINE_HIERARCHICAL_RULESET : Commands.N_CREATE_HIERARCHICAL_RULESET, object_name ) ;
	
	Lex.readToken ( Tokens.Y_PAR_OPEN ) ;

	hd.addChild( variableOrValuedomain ( ) ) ;
		
	pCondVariables = Lex.readOptionalIdeKeyword( "condition") ? parseDimListAddStack( ) 
			: Parser.makeEmptyNode() ;
	
	Lex.readIdeKeyword( "rule" ) ;
	ruleVariable = Parser.parseIdentifier() ;

	Lex.readToken ( Tokens.Y_PAR_CLOSE ) ;
	Lex.readIdeKeyword ( "is" ) ;
	
	hd.addChild( pCondVariables ) ;
	hd.addChild( ruleVariable ) ;

	for ( int equation_num = 1; ; equation_num++ ) {
		p = Parser.newnode( Commands.N_CREATE_RULE_EQUATION ) ;
		if ( Lex.readOptionalIdeKeyword( "when" ) ) {
	    	pWhen = Parser.parseCondition() ;
	    	pRuleid = Parser.makeEmptyNode() ;
			Lex.readIdeKeyword( "then" ) ;
			pLeftSide = Parser.parsePosition () ;		
		}
		else {
			pLeftSide = Parser.parsePosition () ;		// returns N_STRING	
			if ( pLeftSide.name == Nodes.N_IDENTIFIER && Lex.readOptionalToken( Tokens.Y_COLON )) {
				pRuleid = pLeftSide ;
				if ( Lex.readOptionalIdeKeyword ( "when" ) ) {
					pWhen = Parser.parseCondition () ;
					Lex.readIdeKeyword( "then" ) ;
				}
				else {
					pWhen = Parser.makeEmptyNode() ;
				}
				pLeftSide = Parser.parsePosition () ;
			}
			else {
				pWhen = Parser.makeEmptyNode() ;
				pRuleid = Parser.makeEmptyNode() ;				
			}
		}

		if ( pWhen.name != Nodes.N_EMPTY && pCondVariables.name == Nodes.N_EMPTY )
			VTLError.SyntaxError( "found when without condition");

		// type of equation (=, <>, >, etc)
		switch ( Lex . scan () ) {
		  case Tokens.Y_EQUAL : 
		  case Tokens.Y_GT_EQUAL : 
		  case Tokens.Y_LT_EQUAL :
		  case Tokens.Y_GT : 
		  case Tokens.Y_LT : 
		    pEquationSymbol = Parser.newnode ( Nodes.N_STRING , Lex.TokenString ) ;	
		    break ;
		  default :
		    VTLError.SyntaxError ("Line " + equation_num + ": bad equation operator: " + Lex.TokenString ) ;
		    pEquationSymbol = null ;
		}

		pItems = Parser.newnode( Commands.N_CREATE_RULE_EQUATION_ITEMS ) ;

		boolean first = true ;
		while ( true ) {
			if ( Lex.readOptionalToken ( Tokens.Y_PLUS ) )
				sign = "+" ;
			else if ( Lex.readOptionalToken ( Tokens.Y_SUBTRACT ) )
				sign = "-" ;
			else {
				if ( ! first )
					break ;
				first = false ;
				sign = "+" ;
			}

			p_eq = Parser.parsePosition () ;
			p_eq.val = sign + p_eq.val ;
			pItems.addChild( p_eq );
		}
		
		// errorcode, errorlevel
		Node pErrorcode, pErrorlevel ;
		if ( Lex.readOptionalIdeKeyword ( "errorcode" ) ) {
			pErrorcode = Parser.parseExpression () ; 				// should be literal
			pErrorlevel = Lex.readOptionalIdeKeyword ( "errorlevel" ) ? Parser.parseExpression () // should be literal
					: Parser.makeEmptyNode() ;
		}
		else if ( Lex.readOptionalIdeKeyword ( "errorlevel" ) ) {
			pErrorcode = Parser.makeEmptyNode() ;
			pErrorlevel = Parser.parseExpression () ; 					// should be literal
		}
		else {
			pErrorcode =  Parser.makeEmptyNode() ;
			pErrorlevel = Parser.makeEmptyNode() ;          					
		}
			
		p.addChild( pRuleid );
		p.addChild( pWhen );
		p.addChild( pLeftSide );
		p.addChild( pEquationSymbol );
		p.addChild( pItems );				
		p.addChild( pErrorcode );				
		p.addChild( pErrorlevel );				
		hd.addChild( p ) ;

		if ( Lex.readOptionalIdeKeyword ( "end" ) )
			break ;

		Lex.readToken ( Tokens.Y_SEMICOLON ) ;
      			
		if ( Lex.readOptionalIdeKeyword ( "end" ) )
			break ;
    }

	Lex.readIdeKeyword ( "hierarchical" );
	Lex.readIdeKeyword ( "ruleset" );
	
	return ( hd ) ;
}

/*
 * var x string; create table x like aact_ali01;
 */
static Node parseCreate ( boolean create_or_replace ) throws VTLError
{
	Node		hd ;
	String 		object_name ;
	char		object_type ;
	int 		markStartText ;

	object_type = readObjectType ( ) ;
	
	object_name = Lex.readObjectName () ;

	switch ( object_type ) {
	    case VTLObject.O_VALUEDOMAIN :
	    case VTLObject.O_DATASET :
	        hd = parseCreateDataset ( object_type, object_name, create_or_replace ) ;
	        break ;
	    case VTLObject.O_VIEW :
	    	Lex . readIdeKeyword ( "is" ) ;
	    	markStartText = Lex.inputTextMarkStart ( ) ;
	    	hd = Parser.newnode ( create_or_replace ? Commands.N_DEFINE_VIEW : Commands.N_CREATE_VIEW, object_name ) ;
	    	hd.child = Parser.parseExpression () ;
	    	Lex.readIdeKeyword ( "end" );		// Lex.readIdeKeyword ( "define" );			correction proposed to RM
	    	Lex.readIdeKeyword ( "view" );
	    	hd.addChild ( Parser.newnode( Nodes.N_STRING, Lex.inputTextGet ( markStartText ) ) ) ;	
	    	break ;
	    case VTLObject.O_OPERATOR :
	    	hd = parseDefineOperator ( object_name, VTLObject.O_OPERATOR, create_or_replace ) ;
	    	break ;
	    case VTLObject.O_FUNCTION :
	    	hd = parseDefineOperator ( object_name, VTLObject.O_FUNCTION, create_or_replace ) ;
	    	break ;
	    case VTLObject.O_DATAPOINT_RULESET :
	    	hd = parseDefineDatapointRuleset ( object_name, create_or_replace ) ;
	    	break ;
	    case VTLObject.O_HIERARCHICAL_RULESET :
	    	hd = parseDefineHierarchicalRuleset ( object_name, object_type, create_or_replace ) ;
	    	break ;
	
	    case VTLObject.O_SYNONYM :
	    	hd = Parser.newnode ( Commands.N_CREATE_SYNONYM, object_name ) ;
	    	Lex.readIdeKeyword ( "for" ) ;
	    	hd.child = Parser.parseObjectName () ;
	    	break ;
	
	    default:
	    	hd = null ;
	}

	return ( hd ) ;
}

/*
	case when 2 > 1 then
		print 2 ;
		else
		print 3 ;
	end

	case 
		when 1 > 2 then print "one" 
		when 2 > 4 then print "two"
	end
	case 
		when 1 > 2 then print "one" 
		when 2 > 4 then print "two"
		else print "three"
	end
 */
static void parseCase ( Node hd ) throws VTLError
{
	Node     p_when, p_else ;
	
	Lex.readIdeKeyword ( "when" ) ;
	do {
		p_when = Parser.newnode(Commands.N_CASE_WHEN) ;
		hd.addChild(p_when);
		p_when.child = Parser.parseCondition () ;	
		Lex.readIdeKeyword ( "then" ) ;
		for ( ; ; ) {
			p_when.addChild( parseCmd ( ) ) ;	
			if ( Lex.readOptionalIdeKeyword ( "when" ) || Lex.readOptionalIdeKeyword ( "else" ) 
															|| Lex.readOptionalIdeKeyword ( "end" )) {
				Lex.unscan () ;
				break ;
			}
			Lex.readToken(Tokens.Y_SEMICOLON) ;
		}
	} while ( Lex.readOptionalIdeKeyword ( "when" ) ) ;

	if ( Lex.readOptionalIdeKeyword ( "else" ) ) {
		p_else = Parser.newnode(Commands.N_CASE_ELSE) ;
		hd.addChild(p_else);
		for ( ; ; ) {
			p_else.addChild( parseCmd ( ) ) ;	
			if ( Lex.readOptionalIdeKeyword ( "end" ) ) {
				Lex.unscan();
				break ;
			}
			Lex.readToken(Tokens.Y_SEMICOLON);
		}
	}
	Lex.readIdeKeyword ( "end" ) ;
	Lex.readIdeKeyword ( "case" ) ;
}

static void parseIf ( Node hd ) throws VTLError
{
  Node		p_then, p_else = null ;

  // condition clause
  hd.child = Parser.parseExpression ( ) ;

  // then clause
  Lex.readIdeKeyword ( "then" ) ;

  hd.child.next = p_then = Parser.newnode ( Commands . N_STATEMENT_LIST ) ;

  for ( ; ; ) {
	  p_then.addChild( parseCmd ( ) ) ;
	  if ( Lex . readOptionalIdeKeyword ("else" ) ) {
		  p_else = Parser.newnode ( Commands . N_STATEMENT_LIST ) ;
		  for ( ; ; ) {
			  p_else.addChild ( parseCmd ( ) ) ;
			  if ( Lex.readOptionalIdeKeyword ( "end" ) )
				  break ;
		  }  
		  break ;
	  }
			  
	  if ( Lex.readOptionalIdeKeyword ( "end" ) )
		  break ;
  }
  
  p_then.next = p_else ;
}

/*
 * try - catch.
 */
static void parse_try ( Node hd ) throws VTLError
{
  Node		p_try, p_catch ;

  hd.child = p_try = Parser.newnode ( Commands.N_STATEMENT_LIST ) ;
  p_catch = Parser.newnode ( Commands.N_STATEMENT_LIST ) ;
  p_try.next = p_catch; 
  
  for ( ; ; ) {
	  p_try.addChild( parseCmd ( ) ) ;
	  if ( Lex . readOptionalIdeKeyword ( "catch" ) ) {
		  Lex.readToken( Tokens.Y_PAR_OPEN ) ;
		  hd.val = Lex.readIdentifier() ;
		  Lex.readIdeKeyword( "string" );					// string is mandatory
		  Lex.readToken( Tokens.Y_PAR_CLOSE ) ;
		  for ( ; ; ) {
			  p_catch.addChild ( parseCmd ( ) ) ;
			  if ( Lex.readOptionalIdeKeyword ( "end" ) )
				  break ;
		  }  
		  break ;
	  }			  
	  if ( Lex.readOptionalIdeKeyword ( "end" ) )
		  break ;
  }
  
  if ( p_catch == null )
	  VTLError.SyntaxError( "found try without catch" ) ;
}

/*
 *  dsName { subspace } { filter } <- ds2
 *  
	na_main2 [ sub ref_area = "DK" ] <- na_main [ sub ref_area = "DK" ]
*/
static void checkSyntaxPermAssignment ( Node hd ) throws VTLError
{
	Node	pLeft = hd.child ;
	
	if ( pLeft.name == Nodes.N_IDENTIFIER )
		return ; // OK
	
	if ( pLeft.name == Nodes.N_CLAUSE_OP ) {
		if ( pLeft.child.name == Nodes.N_IDENTIFIER && pLeft.child_2().name == Nodes.N_CLAUSE_SUBSCRIPT )
			return ; // OK
		if ( pLeft.child.name == Nodes.N_IDENTIFIER && pLeft.child_2().name == Nodes.N_CLAUSE_FILTER )
			return ; // OK
		if ( pLeft.child.name == Nodes.N_CLAUSE_OP && pLeft.child_2().name == Nodes.N_CLAUSE_FILTER ) {
			Node p = pLeft.child ;
			if ( p.child.name == Nodes.N_IDENTIFIER && p.child_2().name == Nodes.N_CLAUSE_SUBSCRIPT )
				return ; // OK
		}
	}

	VTLError.SyntaxError( "Permanent assignment: identifier, subspace or filter expected") ;
}

/*
 * Alter object ( or alter user profile).
	alter na_main modify identifier ref_area ref_area { "DE", "DK", "IT", "FR", "GB", "ES", "PT", "SK", "SI" } ;
 */
static Node parseAlter ( ) throws VTLError
{
	Node		hd, p ;
	String 		objectName ;

	// alter user profile
	if ( Lex.readOptionalIdeKeyword  ( "user" ) ) {
		Lex.readIdeKeyword ( "profile" ) ;
     	Lex.readIdeKeyword ( "set" ) ;
     	hd = Parser.newnode ( Commands.N_ALTER_USER_PROFILE, Lex.readIdentifier ( ) ) ;
     	Lex.readToken ( Tokens.Y_EQUAL ) ;
     	hd.child = Parser.parseExpression () ;
     	return ( hd ) ;
	} 

  	objectName = Lex.readObjectName () ;
    
	hd = Parser.newnode ( Commands.N_ALTER, Parser.makeDataObjectName ( objectName ) ) ;
  		
	switch ( Lex.nextIdeKeyword() ) {
		case "storage" :
		    p = Parser.newnode ( Commands.N_ALTER_STORAGE_OPTIONS, readStorageOptions ( ) ) ;
			break ;
		case "modify" :
	  		p = Parser.newnode( Commands.N_ALTER_MODIFY_COMPONENTS, parseDatasetComponent( ) ) ;
	  		break ;
		case "add" :
	  		p = Parser.newnode( Commands.N_ALTER_ADD_COMPONENTS, parseDatasetComponent( ) ) ;
	  		break ;
		case "drop" :
	  		p = Parser.newnode ( Commands.N_ALTER_DROP_COMPONENT, Lex.readIdentifier () ) ;	  
	  		break ;
		case "move" :
	  		p = Parser.newnode ( Commands.N_ALTER_MOVE, Lex.readIdentifier ()  ) ;
	  		p.addChild ( Lex.readOptionalIdeKeyword ( "before" ) ? Parser.parseIdentifier () : Parser.makeEmptyNode ( ) ) ;
			break ;
		case "rename" :
	      	p = Parser.newnode ( Commands.N_ALTER_RENAME, Lex.readIdentifier () ) ;
	      	Lex . readIdeKeyword ( "to" ) ;
	      	p.addChild( Parser.parseIdentifier () ) ;
	      	break ;
	    default :
	  		p = null ;
	  		VTLError . SyntaxError ( "Alter command, expected: add | drop | modify | move | rename | storage" ) ;
	}
 
  	hd.addChild ( p ) ;
  
  	return ( hd ) ;
}

/*
 * Parse command.
 */
static Node parseCmdList ( ) throws VTLError
{
	Node p ;
	
	p = Parser.newnode(Commands.N_STATEMENT_LIST ) ;
	
	while ( true ) {
		p.addChild( parseCmd ( ) ) ;
		Lex.readToken( Tokens.Y_SEMICOLON ) ;
		if ( Lex.endOfInput() )
			break ;  
	} ;
		  	  
	return ( p ) ;
}

/*
 * Parse command.
 */
static Node parseCmd ( ) throws VTLError
{
	String		keyword ;
	int  		tok ;
	Node		hd, p ;  

	keyword = Lex.nextTokenString() ;
	
	tok = Commands.TextCommands.indexOf( keyword ) ;	// Basic.getPosition( Commands.text_cmds, keyword ) ;

	if ( tok < 0 ) {
		short lineNumber = Lex.getInputLineNumber () ;		
		if ( keyword.equals ( "alter" ) )
			hd = parseAlter () ;
		else if ( keyword.equals ( "create" ) ) {
			hd = parseCreate ( false ) ; 
		}
		else if ( keyword.equals ( "define" ) ) {
			hd = parseCreate ( true ) ;    	 		// synonym for "create or replace"    	 
		}
		else {
			Lex.unscan();
			p = Parser.parseExpression() ;
			
			if ( Lex.readOptionalToken(Tokens.Y_TEMP_ASSIGNMENT )) {
				if ( p.name != Nodes.N_IDENTIFIER )
					VTLError.SyntaxError( "Left side of assignment must be an identifer" );
				hd = Parser.newnode(Commands.N_TEMP_ASSIGNMENT, p ) ;
				hd.addChild( Parser.parseExpression() );
		    	Parser.addStack ( p.val, Nodes.N_USER_TYPE, true ) ;
			}
			else if ( Lex.readOptionalToken(Tokens.Y_PERS_ASSIGNMENT)) {
				hd = Parser.newnode(Commands.N_PERS_ASSIGNMENT, p ) ;
				hd.addChild( Parser.parseExpression() );
				checkSyntaxPermAssignment ( hd ) ;
			}
			else {
				hd = Parser.newnode( Commands.N_PRINT_EXPR, p ) ;
				hd.addChild( Parser.makeEmptyNode ( ) ) ;
				hd.addChild( Parser.makeEmptyNode ( ) ) ;									
			}
		}
		hd.info = lineNumber ;  
	}
	else {
		if ( VTLMain.vtlOnly )
			VTLError.InternalError( "Option vtlonly: cannot execute statements other than := and <-");
		tok = Commands.NameCommands [ tok ] ; 
		hd = p = Parser.newnode ( ( short ) tok ) ;
		hd.info = (short)Lex.getInputLineNumber () ;		// loss of precision?
  
		switch ( tok ) {
			case Commands.N_OBJECT_DESCRIPTION   :
				Lex . readIdeKeyword ( "of" ) ;
		        hd.child = Parser.parseObjectName ( true ) ;
		        Lex . readIdeKeyword ( "is" ) ;
		        Lex.readToken( Tokens.Y_BRACKET_OPEN );
		        do {
		        	hd.addChild( Parser.parseIdentifier() );
		        	Lex.readToken(Tokens.Y_EQUAL);
		        	hd.addChild( Parser.parseExpression() );
		        }
		        while ( Lex.readOptionalToken(Tokens.Y_COMMA)) ;
		        Lex.readToken( Tokens.Y_BRACKET_CLOSE );
		        break ;

			case Commands.N_COPY      :
				hd.child = Parser.parseObjectName ( true ) ;
				Lex.readIdeKeyword ( "to" ) ;
				hd.addChild( Parser.parseObjectName ( true ) ) ;
				break ;

			case Commands.N_DROP      :
				p.val = readObjectType () + "" ;	
				p.addChild( Parser.parseObjectName ( true ) ) ;
				p.addChild( Lex.readOptionalIdeKeyword ( "purge" ) ? Parser.newnode ( Nodes.N_IDENTIFIER, "purge") 
	    			  						: Parser.makeEmptyNode ( ) ) ;
				break ;

			case Commands.N_RESTORE   :
				p.addChild( Parser.parseObjectName ( true ) ) ;
				if ( Lex.readOptionalIdeKeyword ( "rename" ) ) {
		    		Lex.readIdeKeyword ( "to" ) ;
		    		p.addChild( Parser.parseIdentifier() ) ;
		    	}
		    	else
		    		p.addChild( Parser.makeEmptyNode());
				break ;
				
			case Commands.N_RANGE    :
				if ( Lex.readOptionalToken ( Tokens.Y_BRACE_OPEN ) )
					Lex.readToken ( Tokens.Y_BRACE_CLOSE ) ;			// reset all filters
				else {
					p.child = Parser.parseIdentifier ( ) ;
					p.child.next = Parser.parseScalarSet () ;
				}
				break;

			case Commands . N_FOR       :
				p.val = Lex.readIdentifier ( ) ;
				Parser.addStack ( p.val, Nodes.N_TYPE_SCALAR, true ) ;
	
				Lex . readIdeKeyword ( "in" ) ;
				p.child = Parser . parseScalarSet () ;
				Lex.readIdeKeyword ( "do" ) ;
				p = p .child ;
				while ( true ) {
					p . next = parseCmd () ;
					p = p . next ;
					if ( Lex.readOptionalIdeKeyword ( "end" ) ) {
		               Lex . readIdeKeyword ( "for" ) ;
		               break ;
					}
				}
				Parser.removeStack ( 1 ) ;
				break ;

			case Commands . N_CASE        :
				parseCase ( hd ) ;
				break ;
			case Commands.N_THROW :
				Lex.readToken( Tokens.Y_PAR_OPEN );
				hd.child = Parser.parseExpression( ) ;
				Lex.readToken( Tokens.Y_PAR_CLOSE );
				break ;

			case Commands . N_LOAD      :
				hd.child = Parser.parseExpression ();
				Lex . readIdeKeyword ( "into" ) ;
				hd.addChild ( Parser.parseObjectName( true ) ) ;
				hd.addChild( Parser.newnode ( Nodes.N_STRING ) );
				hd.addChild( Parser.newnode ( Nodes.N_STRING ) ) ;
				hd.addChild( Parser.newnode ( Nodes.N_STRING ) ) ;
				if ( Lex . readOptionalIdeKeyword ( "merge" ) ) 
					p.child.next.next.val = "merge" ;
				else if ( Lex . readOptionalIdeKeyword ( "replace" ) ) 
					p.child.next.next.val = "replace" ;
				if ( Lex . readOptionalIdeKeyword ( "autoextend" ) )
					p.child.next.next.next.val = "autoextend"  ;
				if ( Lex . readOptionalIdeKeyword ( "fields" ) ) {
					Lex.readIdeKeyword ( "separated" ) ;
					Lex.readIdeKeyword ( "by" ) ;
					p.child.next.next.next.next.val = Lex.nextIdeKeyword() ;  ;
				}
				else
					p.child.next.next.next.next.val = "\t"  ;
				break ;

			case Commands.N_PRINT_EXPR :
				// print expression { order by expression { asc | desc } , ...   } [ to filename ]
				hd.child = Parser.parseExpression ( ) ;

				// optional order by
				hd.addChild( Lex.readOptionalIdeKeyword ( "order" ) ? 
							Parser.newnode ( Nodes.N_DUMMY, Parser.parseByList ( ) ) : Parser.makeEmptyNode() ) ;
				// optional write to file
				hd.addChild( Lex.readOptionalIdeKeyword( "to" ) ? Parser.parseExpression() : Parser.makeEmptyNode ( ) ) ;
				break;

	      	case Commands . N_RENAME    :
	      		p .child = Parser.parseObjectName () ;
	      		Lex . readIdeKeyword ( "to" ) ;
	      		p .child . next = Parser.parseObjectName () ;
	      		break ;

	      	case Commands.N_SQL       :
	      		hd.child = Parser.parseExpression ( ) ;
	      		break;

	      	case Commands.N_TRY_CATCH       :
	      		parse_try ( hd ) ;
	      		break ;

	      	case Commands.N_RETURN :
	      		if ( ! ( Lex.readOptionalToken( Tokens.Y_SEMICOLON ) || Lex.readOptionalIdeKeyword( "end" ) ) )
	      			p.child = Parser.parseExpression() ;
	      		break ;

	      	case Commands.N_GRANT :
	      		p.child = Parser.parseIdentifier() ;
	      		Lex . readIdeKeyword ( "on" ) ;
	      		p.child.next = Parser . parseObjectName () ;
	      		Lex . readIdeKeyword ( "to" ) ;
	      		p.child.next.next = Parser.parseDimList ( ) ;
	      		break ;
    	  
	      	case Commands.N_REVOKE :
	      		p.child = Parser.parseIdentifier() ;
	      		Lex . readIdeKeyword ( "on" ) ;
	      		p.child.next = Parser . parseObjectName () ;
	      		Lex . readIdeKeyword ( "from" ) ;
	      		p.child.next.next = Parser.parseDimList ( ) ;
	      		break ;
    	  
	      	case Commands.N_PURGE :
	    		Lex.readIdeKeyword ( "recyclebin" ) ;
	    		p.val = "recyclebin" ;
	    		p.child = Parser.makeEmptyNode ( ) ;
	    		break ;

	      	default :
	      		VTLError . InternalError ( "Parse command, case not found: " + keyword ) ;
	      		break;
    	}
  
  	}
	
  	// Lex . readToken ( Tokens . Y_SEMICOLON ) ;

  	return ( hd ) ;
}

// end of parser of statements - evaluate

/*
 * Eval expression. p points to expr.
 */
static final Query eval_expression ( Node p ) throws VTLError
{
	return ( p.inte ( ) ) ;
}

static boolean eval_for ( Node hd, boolean compile_only ) throws VTLError
{
	int			var_index ;
	String		var_name ;
	ListString 	my_list ;
	Node		p ;
	boolean		continue_execution = true ;

	var_name = hd . val ;

	hd = hd .child ;

	my_list = hd . inteSetScalar ( null ) ;
	hd = hd . next ;
  
	var_index = Env.addVar ( var_name, "string", null ) ;

 	for ( String var_value : my_list ) {
 		Env . setValue ( var_index, var_value ) ;
        for ( p = hd; p != null && continue_execution; p = p . next )
        	continue_execution = eval_cmd ( p, compile_only ) ;
        if ( continue_execution == false )
        	break ;
 	}

 	Env . removeVar ( var_index ) ;
  
 	return ( continue_execution ) ;
}

/*
 * try print 1 ; print 3/0 ; catch print 2 ; end try ;
 	try 
 		print 1 ; 
 		print 3/0 ; 
 	catch ( string error_message ) 
 		print "ERROR " || error_message ; 
 		throw ( error_message ) ;
 	end try ;
 * try print 1 ; print 3/0 ; catch ( error_message string ) print "ERROR " || error_message ; end try ;
 */
static boolean eval_try ( Node hd, boolean compile_only ) throws VTLError
{
	Node		p ;
	boolean		continue_execution = true ;
	int			idx ;

	try {
	    for ( p = hd.child.child; p != null && continue_execution; p = p . next )				// try statement list
	    	continue_execution = eval_cmd ( p, compile_only ) ;
	}
	catch ( Exception e ) {
		idx = Env.addVar(hd.val, "string", e.toString() ) ;											// variable declaration
	    for ( p = hd.child.next.child; p != null && continue_execution; p = p . next )			// catch statement list
	    	continue_execution = eval_cmd ( p, compile_only ) ;	
	    Env.removeVar( idx ) ;
	}
  return ( continue_execution ) ;
}

/*
 * Syntax tree:
	(component) role
		component name
		N_TYPE_SCALAR (or N_USER_TYPE) typeName
			constraint		can be N_SET_SCALAR, boolean condition or N_EMPTY
			null
		width (or empty)
 */
static DatasetComponent evalDatasetComponent ( Node p ) throws VTLError
{
	DatasetComponent	c ;
	String				cType ;
	Node				pWidth, pType ;
	
	c = new DatasetComponent () ;
	
	if ( p.val.equals( "viral attribute" ) )
		c.isViralAttribute = true ;

	c.compName = p.child.val ;
	pType = p.child_2() ;
	c.compType = cType = pType.val ;
	
	String	existingDataType = Dataset.getVariableDataType( c.compName, false) ;
	if ( existingDataType != null ) {
		if ( ! existingDataType.equals( c.compType) )
			VTLError.TypeError( "calc: type of component " + c.compName + " is different from type of existing component " + existingDataType );
	}

	if ( pType.name == Nodes.N_USER_TYPE ) {
		if ( cType.contains ( Parser.ownershipSymbol ) )
			cType = VTLObject.getFullDataType ( cType ) ;
	}
	c.dim_values = pType.child.evalConstraint( cType ) ;		// constraint: list of values (e.g. for a dimension)	
	c.canBeNull = ( pType.child_2().val == null || pType.child_2().val.equals( "null") ) ;
		
	// length (for string data type)
	pWidth = p.child_3(); 
	if ( pWidth.name != Nodes.N_EMPTY ) {
	    if ( ! cType.equals ( "string" ) )					// this check is carried out also in the parser
	    	c.compWidth = Integer.parseInt ( pWidth.val ) ; 
	}
	
	return ( c ) ;
}

/*
 * Create a data object descriptor from a CREATE node.
 * valuedomain: check that object_name=dim_name
 * check uniqueness of component names
 */
static Dataset makeDatasetDesc ( Node hd_create, String dsName, char object_type ) throws VTLError
{
	Dataset	ds ;

	ds = new Dataset ( dsName, object_type ) ;

	for ( Node p = hd_create.child ; p != null ; p = p.next ) {
		switch ( p.val ) {
			case "identifier" : ds.getDims().add ( evalDatasetComponent ( p ) ) ; break ;
			case "measure" : 	ds.getMeasures().add ( evalDatasetComponent ( p ) ) ; break ;
			case "attribute" : 	
			case "viral attribute" : 
				ds.getAttributes().add ( evalDatasetComponent ( p ) ) ; 
				break ;
			default : VTLError.InternalError( "create: bad component type");
		}
	}
	
	ds.checkComponentUniqueness () ;

	return ( ds ) ;  
}

/*
 * Create view.

	define view my_view as aact_ali01 [ sub geo = IT ]
 */
static void evalDefineView ( Node hd, boolean compile_only ) throws VTLError
{
	Query		q ;
	Dataset		ds ;
	boolean		is_new_object = false ;
	boolean 	create_or_replace = ( hd.name == Commands.N_DEFINE_VIEW ) ;
	String		view_text, objectName ;
	int			idx_end, objectId = 0 ;
	char		object_type = VTLObject.O_VIEW ;
	boolean 	created = false;
	
    q = eval_expression ( hd.child ) ;
    
    if ( compile_only )
    	return ;
    
    objectName = Check.checkObjectOwner ( hd.val ) ;
    
    if ( hd.child.next == null )
    	VTLError.InternalError ( "Create view: cannot find view definition" ) ;
 
	if ( ! create_or_replace )
		Check.checkNewObjectName( objectName );
	
    Dataset.create_sql_view ( q, objectName ) ;
    
    view_text = hd.child.next.val ;
    if ( view_text.charAt( 0 ) == '\n' )	
    	view_text = view_text.substring ( 1 ) ;
    else if ( view_text.charAt( 0 ) == '\r' && view_text.charAt( 1 ) == '\n' )	
    	view_text = view_text.substring ( 2 ) ;

    if ( ( idx_end = view_text . lastIndexOf ( ";") ) < 0 )
        view_text = view_text . substring ( 0 ) ;
    else
    	view_text = view_text . substring ( 0, idx_end ) ;
    
	try {
		if ( create_or_replace )
	 	   	objectId = VTLObject.createReplaceObjectId ( objectName, object_type ) ;
	    is_new_object = ( objectId == 0 ) ;
		if ( is_new_object ){
			objectId = VTLObject.createObjectId( objectName, object_type ) ;
			created = true;
		}
		
		ds = q.convert2ds( objectId, objectName, object_type ) ;
	
		ds.saveDatasetdesc ( ) ;
		
		Db.insertSource ( objectId, view_text ) ; //	insert text of view
		
		// insert referenced objects
		VTLObject.saveReferencedObjects ( objectId, q.referencedDatasets ) ;
		
		// save syntax tree of the expression 
		UserFunction.saveSyntaxTree ( objectId, hd ) ;
		  
		if ( ! is_new_object )
			VTLObject.setObjectModified ( objectId ) ;
		
		Db.sqlCommit ( ) ;
	}
	catch ( Exception e ) {
		Db.sqlRollback() ;
		if ( is_new_object && created ) {
			if (VTLObject.object_exists( objectName ))
				VTLObject.objectTrueDrop( objectName ) ;
			if ( Db.sqlUserTableExists ( objectName ) )
				Db.sqlExec ( "DROP VIEW " + objectName ) ;			
		}
		VTLError.RunTimeError(e.toString()) ;
	}
}


/*
 * Create data object: dataset, valuedomain or view.
 * TBD: is it the name of a dataset?
 */
static void evalCreateDatasetLike ( Node hd, boolean compile_only ) throws VTLError
{
	VTLObject.copyObject ( hd.evalObjectName( ), hd.child.evalObjectName( ), false, compile_only ) ;
}

/*
 * Create valuedomain subset.
 	create valuedomain geo subset of ref_area 
 	end valuedomain
 	define valuedomain geo subset of ref_area { "IT", "FR", "DE" }
 	end valuedomain
 	define valuedomain geo subset of ref_area [ value in { "IT", "FR", "DK" } ]
 	end valuedomain 	
 */
static void evalCreateValueDomainSubset ( Node hd, boolean compile_only ) throws VTLError
{
	String				vdName = hd.val, vdParentName = hd.child.val ;
	Dataset				vd, vdParent ;
	boolean 			create_or_replace = ( hd.name == Commands.N_DEFINE_VALUEDOMAIN_SUBSET ) ;
	Node				pConstraint = hd.child.child ;
	DatasetComponent	dim ;
	
	Check.checkObjectOwner ( vdName ) ;

	if ( compile_only )
		return ;
	
	vdParent = Dataset.getDatasetDesc( vdParentName ) ;
	vd = new Dataset ( vdParent ) ;
	vd.dsName = vdName ;
	vd.sqlTableName = vdName ;
	dim = vd.dims.firstElement() ;
	dim.compName = vdName ;
	dim.compType = vdParentName ;
	dim.dim_values = pConstraint.evalConstraint ( vdParentName ) ;
	
	if ( ! create_or_replace )
		Check.checkNewObjectName( vdName );
	
	String sql_create_view = "CREATE OR REPLACE VIEW " + vdName + "(" + vd.stringAllComponents ( ',' ) 
		+ ") AS ( SELECT " + vdParent.stringAllComponents ( ',' ) 
	  	+ " FROM " + vdParentName 
	  	+ ( dim.dim_values.size() > 0 ? " WHERE " + dim.dim_values.sqlSyntaxInList( vdParentName, " IN " ) : "" )
	  	+ ")" + " WITH READ ONLY" ;
	 
	// create view
	Db.sqlExec ( sql_create_view ) ;
	  
	// grant select privileges
	Privilege.grant_select ( vdName ) ;

   	try {
   		boolean	is_new_object ;
   		int objectId = 0 ;
		if ( create_or_replace )
	 	   	objectId = VTLObject.createReplaceObjectId ( vdName, VTLObject.O_VALUEDOMAIN ) ;
	    is_new_object = ( objectId == 0 ) ;
		if ( is_new_object )
			objectId = VTLObject.createObjectId( vdName, VTLObject.O_VALUEDOMAIN ) ;
   		
   		vd.objectId = objectId ;	
   		vd.saveDatasetdesc ( ) ;
   		UserFunction.saveSyntaxTree ( objectId, hd ) ;
		
		// insert referenced objects
   		ListString ls = new ListString( ) ;
   		ls.add( vdParentName ) ;
		VTLObject.saveReferencedObjects ( objectId, ls ) ;
		
		if ( ! is_new_object )
			VTLObject.setObjectModified ( objectId ) ;
	  	Db.sqlCommit ( ) ;
   	}
   	catch ( Exception e ) {
	  	Db.sqlRollback() ;
	  	if (VTLObject.object_exists(vdName))
		  	VTLObject.objectTrueDrop( vdName ) ;
	  	else {
		  	if (Db.sqlUserTableExists( vdName ) )
			  	Db.sqlExec ( "DROP VIEW " + vdName + " PURGE" ) ;
	  }
	  VTLError.RunTimeError(e.toString()) ;
  	}
}

/*
 * Create data object: dataset or valuedomain.
 * it is not allowed to replace an existing object.
 */
static void evalCreateDataObject ( Node hd, char object_type, boolean compile_only ) throws VTLError
{
	Dataset		ds ;
	String		dsName = hd.val ;

	Check.checkObjectOwner ( dsName ) ;
	Check.checkNewObjectName ( dsName ) ;

	ds = makeDatasetDesc ( hd, dsName, object_type ) ; // dimensions, measures, attributes

   	if ( object_type == VTLObject.O_VALUEDOMAIN ) {
   		if ( ds.dims.size() != 1 )
   			VTLError.TypeError( "create valuedomain: only one dimension expected");
   		if ( ! ds.dims.firstElement().compName.equals ( ds.dsName ) )
   			VTLError.TypeError( "create valuedomain: dimension must be identical to the valuedomain name " + ds.dsName );
   	}

	if ( compile_only )
		return ;
	
	ds.createSqlTable ( true ) ;

   	try {
   		ds.objectId = VTLObject.new_object ( ds.dsName, "(New)", object_type, "" ) ;	
   		ds.saveDatasetdesc ( ) ;
	  	Db.sqlCommit ( ) ;
   	}
   	catch ( Exception e ) {
	  	Db.sqlRollback() ;
	  	if (VTLObject.object_exists(ds.dsName))
		  	VTLObject.objectTrueDrop( ds.dsName ) ;
	  	else {
		  	if (Db.sqlUserTableExists( ds.dsName ) )
			  	Db.sqlExec ( "DROP TABLE " + ds.dsName + " PURGE" ) ;
	  }
	  VTLError.RunTimeError(e.toString()) ;
  	}
}

/*
 * build Query descriptor from dataset type
 */
static Query buildQueryFromParameter ( Node hd ) throws VTLError
{
	Query q = new Query () ;
	Node pRole, pType, pName ;
	if ( hd.name != Nodes.N_TYPE_DATASET )
		VTLError.InternalError( "Not a dataset type");
	for ( Node p = hd.child; p != null; p = p.next) {
		pRole = p.child ;
		pType = p.child_2() ;
		pName = p.child_3() ;
		switch ( pRole.val ) {
			case "identifier": q.addDimension(pName.val, pType.val, new ListString());
			case "measure" 	 : q.addMeasure(pName.val, pType.val, null );
			case "viral attribute" : 
			case "attribute" : q.addAttribute(pName.val, pType.val, null);
		}
	}	
	return ( q ) ;
}
/*
 * Check type of body expression wrt declared result type
 */
static void checkOperator ( Node pParameters, Node pResultType, Node pBody, String objectName, char objectType ) throws VTLError
{
	if ( objectType == VTLObject.O_OPERATOR ) {
		Env.addfunctionCall( "define operator" );
		for ( Node pParam = pParameters ; pParam != null; pParam = pParam.next ) {
			Node pType = pParam.child_2() ;
			switch ( pType.name ) {
				case Nodes.N_TYPE_SCALAR :
					Env.addVar(pParam.child.val, pType.val, null ) ;
					break ;
				case Nodes.N_TYPE_SET :
					Env.addVar(pParam.child.val, "set " + pType.child.val, new ListString() ) ;
					break ;
				case Nodes.N_TYPE_DATASET :
					Env.addVar(pParam.child.val, buildQueryFromParameter ( pType ), null) ;
					break ;
			}
		}
		Query	q = pBody.inte() ;
		if ( pResultType.name != Nodes.N_EMPTY ) {
			switch ( pResultType.name ) {
				case Nodes.N_TYPE_SCALAR :
					String 	scalarTypeExpected = pResultType.val ;
					String	t = q.getFirstMeasure().compType ;
					Query.checkTypeOperand ( "define operator " + objectName, scalarTypeExpected, t ) ;
					break ;
					
				case Nodes.N_TYPE_DATASET :
					Query qParam = Command.buildQueryFromParameter ( pResultType ) ;
					q.checkHasAllComponents(qParam);
			}
		}
		Env.removefunctionCall();
	}
	Env.removefunctionCall() ;
}

/*
 * define operator.

	define operator f ( x integer ) returns integer is x + 1 end operator
	define operator f ( s string, ss set <string> ) returns boolean is s in ss end operator
 * the function body (text) is temporarily stored in a node ( hd.next ) that is not saved
 * Example:
   internal form:
   userfunction
   args return_type body text
   object type can be user-defined operator or function
    define operator f2 ( ds dataset { identifier<ref_area> r, measure <number> obs_value } ) returns number is 
  		count ( ds ) 
  	end operator ; 
	f2 ( na_main )
 */
static void evalDefineOperator ( Node hd, char objectType, boolean compile_only ) throws VTLError
{
	boolean		createOrReplace ;
	String		objectName = hd.val, bodyText ;
	Node 		hdBody ;

	if ( compile_only )
		return ;
	
	createOrReplace = objectType == VTLObject.O_OPERATOR ? ( hd.name == Commands.N_DEFINE_OPERATOR ) 
													: ( hd.name == Commands.N_DEFINE_FUNCTION ) ;
			
	objectName = Check.checkObjectOwner ( objectName ) ;

	hdBody = hd.child_3() ;								

	checkOperator ( hd.child.child, hd.child_2(), hdBody.child, objectName, objectType ) ;

	if ( objectType == VTLObject.O_FUNCTION && hdBody.child.name != Commands.N_STATEMENT_LIST )
		VTLError.InternalError( "User-defined function " + objectName + ": bad internal format");
	bodyText = hdBody.next.val  ;
	hdBody.next = null ;									// remove the temporary DUMMY node

    try {
    	UserFunction.saveUserDefinedOperator ( objectName, objectType, createOrReplace, bodyText, hd ) ;
    	Db.sqlCommit() ;	
    }
	catch ( Exception e ) {
		Db.sqlRollback() ;
		VTLError.RunTimeError( e.toString() ) ;
	}
}

/*
	ruleid when then errorcode errorlevel
	all can be empty apart from then

 	define datapoint ruleset test_dpr ( variable obs_value ) is
		obs_value > 10000 errorcode "zzzz";
	end datapoint ruleset
	check_datapoint ( na_main, test_dpr )
*/
static DatapointRuleset evalDatapointRuleset ( Node hd ) throws VTLError
{
	DatapointRuleset	dpr ;
	HorizontalRule		hr ;
	Node 				p, pWhen, pThen, pError ;
	
	dpr = new DatapointRuleset ( ) ;
	dpr.rulesetName = hd.val ;
	if ( hd.child.val == null )
		VTLError.InternalError( "datapoint ruleset saved in old format - please re-create it" );

	// variables - skip dummy node
	// int n_variables = hd.child.countChildren() ;		// AS
	dpr.variables = new ListString ( ) ;
	dpr.valuedomains = new ListString ( ) ;
	dpr.definedOnVariable = hd.child.val.equals( "variable" ) ;
	
	for ( Node p1 = hd.child_2().child ; p1 != null; p1 = p1.next ) {
		// to copy the variable name in the sql query
		if ( dpr.definedOnVariable ) {
			dpr.variables.add( p1.val ) ;								// variable
			String tmp = Dataset.getVariableDataType ( p1.val ) ;	
			dpr.valuedomains.add( tmp ) ;
			if ( p1.next.name != Nodes.N_EMPTY )
				VTLError.TypeError( "datapoint ruleset: cannot use an alias for a variable" );
		}
		else {
			if ( p1.next.name == Nodes.N_EMPTY )
				VTLError.TypeError( "datapoint ruleset: must use an alias for a valuedomain" );
			dpr.variables.add( p1.next.val ) ;							// alias
			dpr.valuedomains.add( p1.val ) ;				
		}
		p1 = p1.next ;
	}
		
	dpr.rules = new Vector <HorizontalRule> ( ) ;

	for ( p = hd.child_3(); p != null ; p = p.next ) {
		hr = new HorizontalRule ( ) ;
		
		hr.ruleid = ( p.child.name == Nodes.N_EMPTY ? null : p.child.val ) ;	// ruleid
		
		pWhen = p.child_2() ;													// antecedent "when" condition (can be empty)
		hr.p_precondition = pWhen.name == Nodes.N_EMPTY ? null : pWhen ;				

		pThen = pWhen.next ;													// consequent "then" condition
		hr.p_condition = pThen ;													
		
		pError = pThen.next ;													// errorcode
		hr.errorcode = pError.name == Nodes.N_EMPTY ? null : pError.val ;
		
		pError = pError.next ;													// errorlevel
		hr.errorlevel = pError.name == Nodes.N_EMPTY ? null : pError.val ;

		dpr.rules.add( hr ) ; 
	}
	
	dpr.syntaxTree = hd ;
	
	return ( dpr ) ;
}

/*
 * Create hierarchical ruleset.
 */
static void evalDefineDatapointRuleset ( Node hd, boolean compile_only ) throws VTLError
{
	DatapointRuleset	dpr = Command.evalDatapointRuleset( hd );
    
	dpr.rulesetName = Check.checkObjectOwner ( dpr.rulesetName ) ;

	Env.addfunctionCall( "datapoint ruleset");

	// type checking
	for ( int idx = 0; idx < dpr.variables.size(); idx ++ )
		Env.addVar ( dpr.variables.get(idx), dpr.valuedomains.get(idx), null ) ; 	

	for ( HorizontalRule hr : dpr.rules ) {		
		if ( hr.p_precondition != null )
			hr.p_precondition.sqlWhereCondition() ;
		hr.p_condition.sqlWhereCondition() ;
	}
    Env.removefunctionCall(); 

    if ( compile_only )
    	return ;
    
    boolean create_or_replace = ( hd.name == Commands.N_DEFINE_DATAPOINT_RULESET ) ;

    dpr.saveRuleset ( create_or_replace ) ;
}

/*
 * Create hierarchical ruleset
	define hierarchical ruleset test_sector ( variable rule ref_sector ) is	
		S1=S11+S12+S13+S14+S15+S1N
	end hierarchical ruleset
 */
static HierarchicalRuleset evalHierarchicalRuleset ( Node hd ) throws VTLError
{
    String				item ;
    Node				pItems, pErr ;
    VerticalRule		eq ;
    HierarchicalRuleset	ir = new HierarchicalRuleset( ) ;

	if ( hd.child.val == null )
		VTLError.InternalError( "hierarchical ruleset saved in old format - please re-create it" );

    ir.rulesetName = hd.val ;
    ir.definedOnVariable = hd.child.val.equals( "variable" ) ;
	ir.condValuedomains = new ListString ( ) ;
	ir.condVariables = new ListString ( ) ;
	Env.addfunctionCall( "hierarchical ruleset");
	if ( hd.child_2().name != Nodes.N_EMPTY ) {
		for ( Node p1 = hd.child_2().child ; p1 != null; p1 = p1.next ) {
			// to copy the variable name in the sql query
			if ( ir.definedOnVariable ) {
				ir.condVariables.add( p1.val ) ;								// variable
				String tmp = Dataset.getVariableDataType ( p1.val ) ;	
				ir.condValuedomains.add( tmp ) ;
				if ( p1.next.name != Nodes.N_EMPTY )
					VTLError.TypeError( "hierarchical ruleset: cannot use an alias for a variable" );
				Env.addVar ( p1.val, tmp, null ) ;
			}
			else {
				if ( p1.next.name == Nodes.N_EMPTY )
					VTLError.TypeError( "hierarchical ruleset: must use an alias for a valuedomain" );
				Env.addVar ( p1.next.val, p1.val, null ) ;
				ir.condVariables.add( p1.next.val ) ;							// alias
				ir.condValuedomains.add( p1.val ) ;				
			}
			p1 = p1.next ;
		}		
	}

	if ( ir.definedOnVariable ) {
	    ir.ruleDimension = hd.child_3().val ;
	    ir.ruleDimensionType = Dataset.getVariableDataType ( ir.ruleDimension ) ;		
	}
	else {
		ir.ruleDimension = hd.child_3().val ;
		ir.ruleDimensionType = hd.child_3().val ;
	}

    // rules (-3 because variable/valuedomain, condition and rule dimension)
    ir.equations = new VerticalRule [ hd.countChildren() - 3 ] ;
    int numItem = 0 ; 
    for ( Node p = hd.child_4() ; p != null ; p = p.next ) {
    	ir.equations[numItem++] = eq = new VerticalRule() ;
    	eq.ruleid = ( p.child.name == Nodes.N_EMPTY ? null : p.child.val ) ;			// ruleid
    	eq.p_condition = ( p.child_2().name == Nodes.N_EMPTY ? null : p.child_2() ) ;	// condition
    	eq.left_part = p.child_3().val;				// left side of equation		
    	eq.equation_type = p.child_4().val ;		// equation type
    	pItems = p.child_5() ;
    	pErr = pItems.next ;
    	eq.errorcode = ( pErr.name == Nodes.N_EMPTY ? null : pErr.val ) ;		// null if name = N_EMPTY  
    	pErr = pErr.next ;
    	eq.errorlevel = ( pErr.name == Nodes.N_EMPTY ? null : pErr.val ) ;		// null if name = N_EMPTY
    	
    	eq.num_items = pItems.countChildren() ;
    	eq.factors = new int [ eq.num_items ] ;
    	eq.pos_codes = new ListString(eq.num_items) ;
    	int idx = 0 ;
	    for ( Node p_eq = pItems.child ; p_eq != null ; p_eq = p_eq.next ) {
		    item = p_eq.val ;
		    eq.factors[ idx ++ ] = ( item.charAt (0 ) == '+' ? 1 : -1 ) ;
		    eq.pos_codes.add ( item.substring ( 1 ) ) ;
		}
    }
    Env.removefunctionCall(); 
    ir.syntaxTree = hd ;												// the syntax tree
	return( ir ) ;
}

/*
 * Create hierarchical ruleset.
 */
static void evalDefineHierarchicalRuleset ( Node hd, boolean compile_only ) throws VTLError
{
    HierarchicalRuleset ir = evalHierarchicalRuleset ( hd ) ;
    
    ir.rulesetName = Check.checkObjectOwner ( ir.rulesetName ) ;

    if ( compile_only )
    	return ;
    
    boolean create_or_replace = ( hd.name == Commands.N_DEFINE_HIERARCHICAL_RULESET ) ;

    ir.saveRuleset ( create_or_replace ) ;
}

/* 
 * Statement: 
 * 
 * 
	ds1 { subspace } { filter } <- ds2
		
	na_main [ sub ref_area = "DE" ] <- na_main [ sub ref_area = "DK" ] ;
	na_main [ sub ref_area = "IT" ] <- na_main [ sub ref_area = "DK" ] ;
	na_main [ sub ref_area = "FR" ] <- na_main [ sub ref_area = "DK" ] ;
	na_main [ sub ref_area = "GB" ] <- na_main [ sub ref_area = "DK" ] ;
	na_main [ sub ref_area = "ES" ] <- na_main [ sub ref_area = "DK" ] ;
	na_main [ sub ref_area = "PT" ] <- na_main [ sub ref_area = "DK" ] ;
	na_main [ sub ref_area = "SK" ] <- na_main [ sub ref_area = "DK" ] ;
	na_main [ sub ref_area = "SI" ] <- na_main [ sub ref_area = "DK" ] ;
	
*/
static void evalPersistentAssignment ( Node hd, boolean compile_only ) throws VTLError
{
	Query 		qLeft, qExpr ;
	ListString	lsNames = new ListString () ;
	ListString	lsValues = new ListString () ;
	Node		pLeft, pClause ;
	boolean		hasFilter = false ;
	
	qExpr = hd.child_2().inte() ;

	pLeft = hd.child ;
	qLeft = pLeft.inte().copy();

	if ( pLeft.name == Nodes.N_CLAUSE_OP ) {
		pClause = pLeft.child_2();
		if ( pClause.name == Nodes.N_CLAUSE_SUBSCRIPT ) {
			for ( Node p1 = pClause.child ; p1 != null ; p1 = p1.next ) {
				lsNames.add ( p1.child.val ) ;
				lsValues.add ( p1.child.next.inteEvalScalar ( ) ) ;	// value auto. checked by the subscript
			}
		}
		else if ( pClause.name == Nodes.N_CLAUSE_FILTER ) {
			hasFilter = true ;
			if ( pLeft.child.name == Nodes.N_CLAUSE_OP ) {
				pClause = pLeft.child.child_2() ;
				if ( pClause.name == Nodes.N_CLAUSE_SUBSCRIPT ) {
					for ( Node p1 = pClause.child ; p1 != null ; p1 = p1.next ) {
						lsNames.add ( p1.child.val ) ;
						lsValues.add ( p1.child.next.inteEvalScalar ( ) ) ;
					}				
				}
			}
		}
		else
			VTLError.InternalError( "Persistent assignment: identifier, subspace or filter expected");
	}

	if ( compile_only )
  		return ;
	
  	Db.dbUpdate( qLeft, lsNames, lsValues, hasFilter, qExpr ) ;
}

/*
	range ref_sector { "S1" } ; na_main
 */
static void evalRange ( Node p ) throws VTLError
{
	if ( p.child == null )
		RangeVariable.resetAllRanges ( ) ;
	else {
		String	var = p.child.val ;
		String 	vd = Dataset.getVariableDataType ( var ) ;
		RangeVariable.setFilter ( var, p.child_2().inteSetScalar ( vd ) ) ;
	}
}

/*
 * Eval alter command.
 */
static void evalAlter ( Node hd, boolean compile_only ) throws VTLError
{
	String 				object_name, dim_name ;
	Dataset				ds ;
	Node				p, p_option ;
	DatasetComponent 	c ;
	
	ds = Dataset.getDatasetDesc ( hd.child.evalObjectName() ) ;		// can be an identifier, variable name or a string value
	
	if ( compile_only )
		return ;
	
	object_name = ds.sqlTableName ;		// object_name can be different from tab.table_name (real object name)
	p_option = hd.child_2() ;
	
	dim_name = p_option.val ;
	p = p_option.child ;
	
	if ( p_option.name == Commands.N_ALTER_MODIFY_COMPONENTS )
		Privilege.checkPrivilegeAddPositions( ds.dsName, ds.objectId ) ;
	else
		Check.checkObjectOwner ( object_name ) ;

	try {
		switch ( p_option.name ) {
			case Commands.N_ALTER_STORAGE_OPTIONS :
				ds.setStorageOptions ( p_option.val ) ;
				break ;		
			case Commands.N_ALTER_DROP_COMPONENT :
				ds.alterDropComponent ( dim_name ) ;
				break ;
			case Commands.N_ALTER_RENAME :
				ds.alterRenameComponent ( dim_name, p.val ) ;
				break ;
			case Commands.N_ALTER_MOVE :
				ds.alterMoveDimension ( dim_name, p.val ) ;
				break ;
			case Commands.N_ALTER_ADD_COMPONENTS :
				c = evalDatasetComponent ( p ) ;
				if ( p.val.equals( "identifier" ))
					ds.alterAddDimension ( c ) ;				
				else
					ds.alterAddMeasureAttribute ( p.val, c ) ;				
				break ;
			case Commands.N_ALTER_MODIFY_COMPONENTS :
				c = evalDatasetComponent ( p ) ;
				if ( p.val.equals( "identifier" ))
					ds.alterModifyDimension ( c ) ;				
				else
					ds.alterModifyMeasureAttribute ( p.child.val, c ) ;				
				break ;

			default :	
				VTLError.InternalError ("eval alter - case not found: " + p_option.name ) ; 
		}
		
		VTLObject.setObjectModified ( ds.objectId ) ;
		Db.sqlCommit ( ) ;
		Dataset.removeTableDesc ( ds.objectId ) ;
	}
	catch ( Exception e ) {
		Db.sqlRollback() ;
		VTLError.RunTimeError( e.toString() ) ;
	}
}

static void prepareprint ( Query q )
{
	for ( DatasetComponent c : q.dims ) {
		if ( c.compType.equals( "date"))
			c.sql_expr = "TO_CHAR(" + c.sql_expr + ",'YYYY-MM-DD\"T\"HH24:MI:SS')" ;
	}
	for ( DatasetComponent c : q.measures ) {
		if ( c.compType.equals( "date"))
			c.sql_expr = "TO_CHAR(" + c.sql_expr + ",'YYYY-MM-DD\"T\"HH24:MI:SS')" ;
	}
	for ( DatasetComponent c : q.attributes ) {
		if ( c.compType.equals( "date"))
			c.sql_expr = "TO_CHAR(" + c.sql_expr + ",'YYYY-MM-DD\"T\"HH24:MI:SS')" ;
	}	
}

/*
 * Eval print expr.
 * Test cases:
	print aact_ali01 order by geo, time
	print aact_ali01 to "file1.txt"
	range ref_area { "DK", "FR" } ; na_main [ filter obs_value > 0 ]
 */
static void evalPrintExpr ( Node p, boolean compile_only ) throws VTLError
{
	Query	q ;
	String	sqlQuery, orderBy ;

	q = eval_expression ( p.child ) ;

	prepareprint ( q ) ;
	
	orderBy = ( p.child_2().name == Nodes.N_EMPTY ? q.dimensionsOrderBy() : p.child_2().child.inteOrderBy ( q )  ) ;
	if ( orderBy.length() > 0 )
		orderBy = " ORDER BY " + orderBy ;
	
	sqlQuery = q.build_sql_query ( true, true ) + orderBy ;
	
	if ( p.child_3().name != Nodes.N_EMPTY ) {
		// write data to file
		Db.sqlUnload( sqlQuery, p.child_3().inteEvalScalar( ) );
		sqlQueryCommandQuery = null ;
		sqlQueryCommandResult = null ;				
	}
	else {
		sqlQueryCommandQuery = q ;
		sqlQueryCommandResult = sqlQuery ;		
	}
}

static String sqlQueryCommandResult ;
static Query sqlQueryCommandQuery ;

interface ComponentType {
	static final int 
	COMP_UNKNOWN	    = 0 ,
	COMP_DIMENSION      = 1 ,
	COMP_MEASURE		= 2 ,
	COMP_ATTRIBUTE      = 3 ;
}

static void setLastQueryToNull ( )
{
	sqlQueryCommandQuery = null ;
}

static String getLastQueryCompName ( int idx )
{
	if ( sqlQueryCommandQuery == null )
		return ( "" ) ;

	int n_dims = sqlQueryCommandQuery.dims.size() ;
	int n_measures = sqlQueryCommandQuery.measures.size() ;
	
	if ( idx < n_dims )
		return ( sqlQueryCommandQuery.dims.get( idx ).compName ) ;
	if ( idx < n_dims + n_measures )
		return ( sqlQueryCommandQuery.measures.get( idx - n_dims ).compName ) ;

	return ( sqlQueryCommandQuery.attributes.get( idx - (n_dims + n_measures )).compName ) ;
}

static boolean[] lastQueryColumnIsInteger ;

static void setLastQueryColumnIsInteger ( ) throws VTLError
{
	if ( sqlQueryCommandQuery == null )
		return ;
	int numColumns = sqlQueryCommandQuery.dims.size() + sqlQueryCommandQuery.measures.size()
					+ sqlQueryCommandQuery.attributes.size() ;

	lastQueryColumnIsInteger = new boolean [ numColumns ] ;
	
	int	idx = 0 ;
	for ( DatasetComponent c : sqlQueryCommandQuery.dims )
		lastQueryColumnIsInteger[idx++] = Check.getSqlCastType(c.compType).equals("integer") ;
	for ( DatasetComponent c : sqlQueryCommandQuery.measures )
		lastQueryColumnIsInteger[idx++] = Check.getSqlCastType(c.compType).equals("integer") ;
	for ( DatasetComponent c : sqlQueryCommandQuery.attributes )
		lastQueryColumnIsInteger[idx++] = Check.getSqlCastType(c.compType).equals("integer") ;
}

static final boolean getLastQueryColumnIsInteger ( int idx )
{
	return ( lastQueryColumnIsInteger[idx] ) ;
}

static String getLastQueryStructure ( )
{
	return ( sqlQueryCommandQuery == null ? "" : sqlQueryCommandQuery.printQuery() ) ;
}

static String getLastQueryReferencedTables ( )
{
	return ( sqlQueryCommandQuery == null ? "" : sqlQueryCommandQuery.referencedDatasets.toString('\n') ) ;
}

static int getLastQueryCompRole ( int idx )
{
	if ( sqlQueryCommandQuery == null )
		return ( ComponentType.COMP_UNKNOWN ) ;

	int n_dims = sqlQueryCommandQuery.dims.size() ;
	int n_measures = sqlQueryCommandQuery.measures.size() ;
	
	if ( idx < n_dims )
		return ( ComponentType.COMP_DIMENSION ) ;
	if ( idx < n_dims + n_measures )
		return ( ComponentType.COMP_MEASURE ) ;
	return ( ComponentType.COMP_ATTRIBUTE ) ;
}

/*
 * Eval comment command.
 */
static void evalObjectDescription ( Node hd, boolean compile_only ) throws VTLError
{
	Query   q = new Query() ;
	String	object_name ;
	
	if ( compile_only ) 
		return ;

	object_name = hd.child.evalObjectName ( ) ;
	for ( Node p = hd.child_2(); p != null ; p = p.next ) {
		Query	q2 = p.next.inte() ;
		q.addMeasure( p.val, q2.measures.firstElement().compType, q2.measures.firstElement().sql_expr );
	}

	VTLObject.setObjectDescription ( object_name, q ) ;		
}

/*
 * Eval predefined functions (Node.predFunctions)
 */
static void evalSql ( Node p, boolean compile_only ) throws VTLError
{
	String		sql_query ;
	
	if ( compile_only ) 
		return ;

	p = p.child ;
	
	sql_query = p.inteEvalScalar ( ) ;

	if ( sql_query == null )
		VTLError.RunTimeError( "sql: string is null" ) ;
	
	sql_query = sql_query.trim ().replace ( '\r', ' ' ).replace ( '\n', ' ' ).replace ( '\t', ' ' ) ;

	if ( sql_query.length() == 0 )
		VTLError.RunTimeError( "sql: string is empty" ) ;

	if ( sql_query.toUpperCase().startsWith ( "SELECT " ) || sql_query.toUpperCase().startsWith("WITH ") ) {
		sqlQueryCommandResult = sql_query ;
		sqlQueryCommandQuery = null ;			
		// VTLError.RunTimeError( "SQL query must be a SELECT statement: (" + sqlCmd + ")" );
	}
	else {
		sqlQueryCommandResult = null ;
		sqlQueryCommandQuery = null ;	
		VTLError.RunTimeError( "sql: only SELECT statement is allowed" );
		/*
		 * Db.sqlExec ( sql_query ) ;
		 */
	}
}

/*
 * Evaluate hd (single statement or statement list) .
 * Error messages should include the line number stored in p.info
 * Return true if execution continues
 * Line number is in p. info
*/
static boolean eval_cmd ( Node p, boolean compile_only ) throws VTLError
{  
  Env.setLineNumberCurrentEnv ( p.info ) ;
  
  switch ( p.name ) {
	  case Commands.N_TEMP_ASSIGNMENT :
		  Query	qval = p.child_2().inte () ;  
		  Env.addVar ( p.child.val, qval, qval.build_sql_query(true ) ) ;
		  break ;

	  case Commands.N_PERS_ASSIGNMENT :
		  evalPersistentAssignment ( p, compile_only ) ;
 		  break ;
		  
	  case Commands.N_RANGE :
		  evalRange ( p );
		  break ;
  
      case Commands.N_ALTER :
	   	  evalAlter ( p, compile_only ) ;
		  break ;

      case Commands.N_OBJECT_DESCRIPTION   :
    	  evalObjectDescription ( p, compile_only ) ;
    	  break;

      case Commands.N_COPY      :
    	  VTLObject.copyObject ( p.child_1().evalObjectName(), p.child_2().evalObjectName(), true, compile_only ) ;    		  
    	  break ;

      case Commands.N_CREATE_VALUEDOMAIN    :
    	  evalCreateDataObject ( p, VTLObject.O_VALUEDOMAIN, compile_only ) ;
    	  break ;
      	 
      case Commands.N_DEFINE_VALUEDOMAIN_SUBSET    :
      case Commands.N_CREATE_VALUEDOMAIN_SUBSET    :
    	  evalCreateValueDomainSubset ( p, compile_only ) ;
    	  break ;
      	 
      case Commands.N_CREATE_DATASET    :
    	  evalCreateDataObject ( p, VTLObject.O_DATASET, compile_only ) ;
    	  break ;

      case Commands.N_CREATE_DATASET_LIKE    :
    	  evalCreateDatasetLike ( p, compile_only ) ;
    	  break ;

      case Commands.N_CREATE_VIEW    :
      case Commands.N_DEFINE_VIEW    :
    	  evalDefineView ( p,compile_only ) ;
    	  break ;
    	  
      case Commands.N_CREATE_DATAPOINT_RULESET    :
      case Commands.N_DEFINE_DATAPOINT_RULESET    :
    	  evalDefineDatapointRuleset ( p,compile_only ) ;
    	  break ;

      case Commands.N_CREATE_FUNCTION    :
      case Commands.N_DEFINE_FUNCTION    :
    	  evalDefineOperator ( p, VTLObject.O_FUNCTION, compile_only ) ;
    	  break ;

      case Commands.N_CREATE_OPERATOR    :
      case Commands.N_DEFINE_OPERATOR    :
    	  evalDefineOperator ( p, VTLObject.O_OPERATOR, compile_only ) ;
    	  break ;

      case Commands.N_CREATE_HIERARCHICAL_RULESET    :
      case Commands.N_DEFINE_HIERARCHICAL_RULESET    :
    	  evalDefineHierarchicalRuleset ( p, compile_only ) ;
    	  break ;
      
      case Commands.N_CREATE_SYNONYM    :
    	  VTLObject.create_synonym ( p.val, p .child . val, compile_only ) ;
    	  break ;

      case Commands.N_DROP      :
    	  VTLObject.objectDrop ( p.val.charAt (0), p.child.evalObjectName ( ), (p.child.next != null && p.child.next.val != null ), compile_only ) ;
    	  break ;

      case Commands.N_RESTORE   :
    	  VTLObject.objectRestore ( p.child.evalObjectName ( ), p.child_2().name == Nodes.N_EMPTY ? null 
    			  													: p.child_2().evalObjectName ( ) ) ;
    	  break ;
    	  
      case Commands.N_FOR       :
    	  if ( ! eval_for ( p, compile_only ) )
    		  return ( false ) ;
    	  break ;

      case Commands.N_CASE      :
/*
case 
	when 1 > 2 then print "one" 
	when 2 > 4 then print "two"
	else print "three"
end
*/
    	  for ( p = p.child ; p != null ; p = p.next ) {
        	  if ( p.name == Commands.N_CASE_ELSE ) {
    			  if ( ! eval_cmd ( p.child, compile_only ) && ! compile_only )
    				  return ( false ) ;        		  
        	  }
        	  else {
        		  if ( p.child.inteEvalCondition ( ) ) {
        			  for ( Node p1 = p.child_2().next ; p1 != null ; p1 = p1.next )
            			  if ( ! eval_cmd ( p, compile_only ) && ! compile_only )
            				  return ( false ) ; 
        			  break ;
        		  }
        	  }
    	  }
    	  break ;
	   
      case Commands.N_LOAD      :
    	  String tmp = p.child.inteEvalScalar ( ) ;
    	  if ( p.countChildren() <= 4 ) {
        	  if ( p.child.next.next.next == null )    		  
        		  Db.loadDataFile(tmp, p.val, p.child.next.val, ( p.child.next.next.val != null ? true : false ), "\t", compile_only ) ;
        	  else   		  
        		  Db.loadDataFile(tmp, p.val, p.child.next.val, ( p.child.next.next.val != null ? true : false ), p.child.next.next.next.val, compile_only ) ;    		  
    	  }
    	  else {
    		  String	ide = p.child.next.evalObjectName() ;
        	  Db.loadDataFile(tmp, ide, p.child.next.next.val, ( p.child.next.next.next.val != null ? true : false ), p.child.next.next.next.next.val, compile_only ) ;    		      		  
    	  }
    	  break ;

      case Commands.N_PRINT_EXPR    :
    	  evalPrintExpr ( p, compile_only ) ;
    	  break;

      case Commands.N_RENAME    :
    	  p = p .child ;
    	  VTLObject . objectRename ( p . val, p . next . val, compile_only ) ;
    	  break ;
    	  
      case Commands.N_SQL :
    	  evalSql ( p, compile_only ) ;
    	  break;

      case Commands.N_STATEMENT_LIST :
		   for ( p = p .child; p != null; p = p . next )
			   if ( ! eval_cmd ( p, compile_only ) && ! compile_only )
		          	return ( false ) ;
		   break;
      case Commands.N_RETURN :
    	  if ( ! compile_only ) {
    		  if ( p.child != null )
    			  Env.computeReturnValue ( p.child.inte() ) ;
    		  return ( false ) ;
    	  }
    	  break ;
      case Commands.N_TRY_CATCH :
    	  eval_try ( p, compile_only ) ;
    	  break;
	   
      case Commands.N_THROW :
    	  if ( ! compile_only )
    		  VTLError.UserDefinedError ( p.child.inteEvalScalar ( ) ) ;
    	  break;
	   
      case Commands.N_ALTER_USER_PROFILE :
    	  if ( ! compile_only )
    		  UserProfile.setValue ( p.val, p.child.inteEvalScalar ( ) ) ;
    	  break;
	   
      case Commands.N_GRANT :
    	  if ( ! compile_only )
    		  Privilege.grant ( p.child.val, p.child.next.val, p.child.next.next.listDimensions ( ) ) ;
    	  break;

      case Commands.N_REVOKE :
    	  if ( ! compile_only )
    		  Privilege.revoke ( p.child.val, p.child.next.val, p.child.next.next.listDimensions ( ) ) ;
    	  break;

      case Commands.N_PURGE :
    	  if ( ! compile_only )
    		  VTLObject.purgeRecycleBin ( ) ;
    	  break;

      default :
    	  VTLError.InternalError ( "eval_cmd - case not found: " + p . name ) ;
  	}
 
  	return ( true ) ;
}

static String lastSyntaxTree = "" ;

/*
 * Main interface of the VTL interpreter.
 */
public static String eval ( String cmd, boolean compile_only ) throws VTLError
{
	Node 	hd ;
	long	elapsedTimeStart ;
	
	elapsedTimeStart = System.currentTimeMillis ( ) ;		// Basic . reset_elapsed_time ( ) ;

	sqlQueryCommandResult = null ;
	sqlQueryCommandQuery = null ;
	lastQueryColumnIsInteger = null ;
	lastSyntaxTree = null ;

	// parser
	Lex.initScan ( cmd ) ;							// initialise scanner
    Parser.resetStack () ;									// initialise stack (parser)	    
    hd = parseCmdList ( ) ; 
	lastSyntaxTree = hd.printSyntaxTree ( ) ;		// prepare for the UI

	// execute
    if ( compile_only == false ) {
    	Audit.start() ;
    	Query.resetAlias () ;
    	Env.newStackFunctionCalls () ;
    	RangeVariable.resetAllRanges() ;
    	
    	eval_cmd ( hd , false ) ; 
    	
    	Audit.finish ( cmd ) ;
    	
    	History.addHistory ( cmd, (int) ( System.currentTimeMillis ( ) - elapsedTimeStart ) / 1000 ) ;
    	setLastQueryColumnIsInteger () ;
    	return ( sqlQueryCommandResult ) ;
    } 
    return ( null ) ;
}

}
