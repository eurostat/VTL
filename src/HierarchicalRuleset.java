

/*
 * Hierarchical (vertical) ruleset.
 */

import java.util.Vector;

class VerticalRule { 
	String		ruleid ;
	String		left_part ;
	int			left_part_index ;
	int			num_items ;
	String		equation_type ;
	ListString	pos_codes ;
	int 		pos_indexes [] ;
	int 		factors [] ;
	String		errorcode ;
	String		errorlevel ;
	Node		p_condition ;

	/*
	 * Return text of equation.
	 */
	String getEquationText ( ) throws VTLError
	{	
		VerticalRule eq = this ;
		StringBuffer	str = new StringBuffer () ;
		
		str.append( eq.left_part ).append(" ").append( eq.equation_type ).append(" ") ;

		for ( int idx = 0; idx < eq.pos_codes.size(); idx ++ ) {
			if ( eq.factors[idx] ==  1 ) {
				if (idx > 0 )
					str.append("+") ;
				str.append( eq.pos_codes.get(idx ) ) ;
			}
			else {
				str.append("-").append( eq.pos_codes.get(idx )).append( " " ) ;
			}
		}

		return ( str.toString() ) ;
	}
}

public class HierarchicalRuleset {
	String			rulesetName ;
	int				objectId ;
	VerticalRule	equations [] ;
	ListString		allRuleDimValues ;			// this is not known in advance (before running the select statement)
	ListString		condVariables ;				// variable (or alias) used in the condition 
	ListString		condValuedomains ;			// value domain of variable (or alias) used in the condition	
	String			ruleDimension ;				// dimension used in the rule
	String			ruleDimensionType ;			// type of the dimension used in the rule
	Node			syntaxTree ;				// syntax tree of the ruleset
	boolean			definedOnVariable ;			// defined on variable or valuedomain
/*
 * Return number of equations.
 */
public final int numberOfEquations (  )
{
  	return ( this.equations.length ) ;
}

String getDefinition ( ) throws VTLError
{
	StringBuffer		syntax = new StringBuffer ( 100 ) ;

	syntax.append( "define hierarchical ruleset " + this.rulesetName + " ( " )
						.append( this.definedOnVariable ? "variable" : "valuedomain" ) ;

	if ( this.condValuedomains.size() > 0 )
		syntax.append( " condition " ).append( 
				(this.definedOnVariable ? this.condVariables : this.condValuedomains).toString( ',' ) ) ;
	
	syntax.append( " rule " ).append( this.ruleDimension ).append( " ) is\r\n" ) ;		

	int idx = 0 ;
	for ( VerticalRule	eq : this.equations ) {
		if ( idx++ > 0 )
			syntax.append ( " ;\n" ) ;
		syntax.append ( "/* " ).append ( idx ).append ( " */\t" ) ;
		
		if ( eq.ruleid != null )
			syntax.append ( eq.ruleid ).append ( " : " ) ;
		
		if ( eq.p_condition != null && eq.p_condition.name != Nodes.N_EMPTY ) {
			syntax.append( "when " ) ;
			eq.p_condition.unparseOp( syntax ) ;					
			syntax.append( " then " ) ;
		}

		syntax.append ( eq.getEquationText( ) ) ;		

		if ( eq.errorcode != null )
			syntax.append(" errorcode \"" + eq.errorcode + "\"") ;
		if ( eq.errorlevel != null )
			syntax.append(" errorlevel \"" + eq.errorlevel + "\"") ;		
	}
	
	syntax.append( "\nend hierarchical ruleset" ) ;
		
	return ( syntax.toString() ) ;
}

/*
 * Get descriptor of hierarchical ruleset.
 */
static public HierarchicalRuleset getRuleset ( String rule_name ) throws VTLError
{
	HierarchicalRuleset	ir = new HierarchicalRuleset () ;
	VTLObject			obj ;
	Node				hd  ;

	obj = VTLObject.getObjectDescSpecific( rule_name, VTLObject.O_HIERARCHICAL_RULESET ) ;
	
	hd = UserFunction.getSyntaxTree( obj.objectId ) ;
	ir = Command.evalHierarchicalRuleset ( hd ) ;
	ir.objectId = obj.objectId ;
	return( ir );
	
	/* old
	int				equation_num, item_num, pos_index, num_equations,factor ;
	String			tmp, pos_code, where_object_id, sql_query ;
	ListString		dim_values ;
	Statement 		st ;
	ResultSet 		rs ;
	VerticalRule	eq ;
	Node 			p ;

	ir = new HierarchicalRuleset () ;
	ir.rulesetName = rule_name ;
	ir.objectId = obj.object_id ;

	where_object_id = " WHERE object_id=" + obj . object_id ;


	if ( ( hd = UserFunction.getSyntaxTree( Db.mdt_syntax_trees, ir.objectId) ) != null ) {
		HierarchicalRuleset irr = Command.evalHierarchicalRuleset ( hd ) ;
		return( irr );
	}
	else {
		// dimensions
		tmp = Db.sqlGetValue ( "SELECT column_type FROM " + Db . mdt_dimensions + where_object_id + " AND dim_index=1" ) ;
		if ( tmp == null ) {
			int 	i_start, i_end ;
			// not found in mdt_dimensions - extract dimension name from object definition
			tmp = obj.get_source() ; // not found => fails 
			i_start = tmp.indexOf( "(" ) + 1 ;
			i_end = tmp.indexOf( ")" ) ;
			// Sys.println (" Start:" + i_start + "," + i_end ) ;
			if ( i_start < 0 || i_end < 0 )
				VTLError.InternalError( "Cannot find left dimension of " + rule_name + " (source)" ) ;
			tmp = tmp.substring(i_start, i_end ) ;
		}
		ir.ruleDimension = tmp ;
		
		// condition  dimensions

		if ( ( tmp = Db.sqlGetValue ( "SELECT column_type FROM " + Db.mdt_dimensions + where_object_id + " AND dim_index=2" ) ) != null ) {
			ir.condDimensions = new ListString ( 1 ) ;
			ir.condDimensions.add( tmp ) ;				
		}
		else
			ir.condDimensions = null ;
	}

	tmp = Db.sqlGetValue ( "SELECT count(*) FROM " + Db.mdt_equations + where_object_id ) ;
	num_equations = Integer . parseInt ( tmp )  ;
	ir . equations = new VerticalRule [ num_equations ] ;

	if ( num_equations == 0 ) {
		return ( ir ) ;
	}
		
	// get all dimension values in left part or right part
	sql_query = "SELECT left_part FROM " + Db . mdt_equations + where_object_id
    	+ " UNION SELECT pos_code FROM " + Db . mdt_equations_items + where_object_id 
    	+ " ORDER BY 1" ;

	dim_values = Db . sqlFillArray ( sql_query ) ;
	
	// get mdt_equation
	// constant_part: not used
	sql_query = "SELECT left_part,constant_part,num_items,equation_type,cond_lowerbound,cond_upperbound,equation_comment"
		+ " FROM " + Db . mdt_equations 
		+ where_object_id + " ORDER BY equation_number" ;
	
	try {
		st = Db . DbConnection . createStatement () ;
		rs = st . executeQuery ( sql_query ) ;
		rs . setFetchSize ( 100 ) ;

		equation_num = 0 ;

		while ( rs . next () ) {
			eq = new VerticalRule () ;

			pos_code = rs.getString ( 1 ) ;
			pos_index = dim_values.indexOf ( pos_code ) ; // Collections.binarySearch(dim_values, pos_code) ;	// 
			if ( pos_index < 0 )
				VTLError . InternalError ( "Bad index for left part: " + pos_code ) ;
      
			eq . left_part = pos_code ;
			eq . left_part_index = pos_index ;

			eq . num_items = rs . getInt ( 3 ) ;
			tmp = rs . getString ( 4 ) ;
			eq . equation_type = tmp ;
			eq . pos_codes = new ListString ( eq . num_items ) ;
			eq . pos_codes.setSize( eq . num_items ) ;
			eq . pos_indexes = new int [ eq . num_items ] ; 
			eq . factors = new int [ eq . num_items ] ; 
			ir . equations [ equation_num ] = eq ;
			
			String str = rs.getString( 7 ) ;
			// TAB separator between errorcode and errolevel
			if ( str != null && str.indexOf("	") >= 0 ) {
				eq.errorcode = str.substring( 0, str.indexOf("	") ) ;
				eq.errorlevel = str.substring( str.indexOf("	") + 1 ) ;
			}
			else {
				eq.errorcode = str ;
				eq.errorlevel = null ;
			}
			equation_num ++ ;
			eq.ruleid = "R" + equation_num ;		// starts from 1 - should be done in the syntax
		}
		rs . close () ;
		st . close () ;
	}
	catch ( SQLException e ) {
		VTLError . SqlError  ( e . toString () + "\nSQL query:\n" + sql_query ) ;
	}

	// get mdt_equations_items
	sql_query =	"SELECT pos_code, factor, item_number, equation_number FROM " + Db.mdt_equations_items 
		+ where_object_id + " ORDER BY equation_number,item_number" ;
	
	try {
		st = Db.DbConnection.createStatement () ;
		rs = st.executeQuery ( sql_query ) ;
		rs.setFetchSize ( 1000 ) ;

		while ( rs . next () ) {
			item_num = rs.getInt ( 3 ) - 1 ;// array index starts at 0 in java
			equation_num = rs.getInt ( 4 ) - 1 ;

			pos_code = rs.getString ( 1 ) ;
			pos_index = dim_values.indexOf ( pos_code ) ;	// Collections.binarySearch(dim_values, pos_code) ;	// dim_values . indexOf ( pos_code ) ;
			if ( pos_index < 0 )
				VTLError.InternalError ( "Bad index for right part: " + pos_code ) ;

			factor = rs.getInt ( 2 ) ;
			ir . equations [ equation_num ].pos_codes.set ( item_num, pos_code ) ;
			ir . equations [ equation_num ].pos_indexes [ item_num ] = pos_index ;
			ir . equations [ equation_num ].factors [ item_num ] = factor ;
		}
  
		rs . close () ;
		st . close () ;
	}
	catch ( SQLException e ) {
		VTLError . SqlError  ( e . toString () + "\nSQL query:\n" + sql_query ) ;
	}

	ir .allRuleDimValues = dim_values ;

	if ( hd != null ) {
		if ( num_equations != ( hd.countChildren() - 3 ) )
			VTLError.InternalError ( "Get rule desc: different number of equations: " + num_equations  + " and " + ( hd.countChildren() - 3 ) );
		p = hd.child.next.next.next ;
		
		for ( equation_num = 0 ; equation_num < num_equations ; equation_num ++ ) {
			ir.equations[equation_num].p_condition = ( p.child.name == Nodes.N_EMPTY ? null : p.child ) ;			// set to null if Node.name == N_EMPTY
			p = p.next ;
		}
	}
	
	return ( ir ) ;
	*/
}

void findLoop ( String left_part, ListString pos_codes, ListString leftSides ) throws VTLError
{
	int		idx ;
	
	for ( String str : pos_codes ) {
		if ( str.equals( left_part ) )
			VTLError.TypeError( "Found loop in the hierachical ruleset: " + str );
		if ( ( idx = leftSides.indexOf( str ) ) >= 0 ) {
			if ( pos_codes == this.equations[idx].pos_codes )		// equal by address value
				VTLError.TypeError( "Found loop in the hierachical ruleset: " + str );
			findLoop ( left_part, this.equations[idx].pos_codes, leftSides ) ;
		}
	}
}

/*
 * Save vertical ruleset
 * Check positions is performed in the calling function eval_create_rule.

	define hierarchical ruleset test_sector ( variable rule ref_sector ) is	
		S1=S11+S12+S13+S14+S15+S1N
	end hierarchical ruleset
	define hierarchical ruleset hr_sector_test ( variable rule ref_sector ) is	
		S11DO=S11001+S11002	;
		S1ZO=S11+S13	;
	end hierarchical ruleset
 */
void saveRuleset ( boolean create_or_replace ) throws VTLError
{
	int					equation_num ;
  	VerticalRule		eq ;
	String				object_name, ruleDim ;
	boolean				is_new_object ;

  	object_name = this.rulesetName ;
  
  	ruleDim = this.ruleDimension ;	
  	// end
  	// check all items
	for ( equation_num = 0 ; equation_num < this.equations.length ; equation_num ++ ) {
		eq = this.equations [ equation_num ] ;
		// check that the dimensions used in a condition are declared
		if ( eq.p_condition != null )
			eq.p_condition.checkDimensionsUsed ( this.condValuedomains ) ;	
	  	 
		Check.checkLegalValue ( ruleDim, eq.left_part )  ;

		for ( String str : eq.pos_codes ) {
			Check.checkLegalValue ( ruleDim, str ) ;
		}
	}
	
	// check for a loop
	for ( VerticalRule q : this.equations ) {
		ListString leftSides = this.listLeftSides() ;
		this.findLoop ( q.left_part, q.pos_codes, leftSides ) ;
	}
	
  	object_name = Check.checkObjectOwner ( object_name ) ;
	
  	this.objectId = 0 ;
  	if ( create_or_replace )
  		this.objectId = VTLObject.createReplaceObjectId ( object_name, VTLObject.O_HIERARCHICAL_RULESET ) ;
  	is_new_object = ( this.objectId == 0 ) ;
  	if ( is_new_object )
  		this.objectId = VTLObject.createObjectId ( object_name, VTLObject.O_HIERARCHICAL_RULESET ) ;
  	
	try {
	  	UserFunction.saveSyntaxTree ( this.objectId, this.syntaxTree ) ;
	  	if ( ! is_new_object )
	  		VTLObject.setObjectModified ( this.objectId ) ;	  
	  	Db.sqlCommit ( ) ;
	}
	catch ( Exception e ) {
		Db.sqlRollback() ;
		VTLError.RunTimeError( e.toString() ) ;
	}

	/*
 	try {
 	PreparedStatement 	pstmt ;
  	String				tmp, sql_query ;
  	sql_query = "INSERT INTO " + Db . mdt_user_equations 
		+ "(object_id,left_part,equation_type,num_items,equation_number,equation_comment,computation_level" + ")VALUES(" + object_id + ",?,?,?,?,?,0)" ;
	pstmt = Db.DbConnection.prepareStatement( sql_query ) ;

	for ( equation_num = 0 ; equation_num < this.equations.length ; equation_num ++ ) {
		eq = this.equations [ equation_num ] ;
		pstmt.setString( 1, eq.left_part ) ;
		pstmt.setString( 2, eq.equation_type ) ;
		pstmt.setInt( 3, eq.num_items ) ;
		pstmt.setInt( 4, equation_num + 1 ) ;
		String strError ;
		if ( eq.errorcode == null && eq.errorlevel ==  null)
			strError = null ;
		else if ( eq.errorcode == null && eq.errorlevel !=  null)
			strError = "\t" + eq.errorlevel ;
		else if ( eq.errorcode != null && eq.errorlevel ==  null)
			strError = eq.errorcode ;
		else
			strError = eq.errorcode + "	" + eq.errorlevel ;
		
		pstmt.setString( 5, strError ) ;
  		pstmt.executeUpdate () ;
	}
	
	sql_query = "INSERT INTO " + Db . mdt_user_equations_items + "(object_id,pos_code,factor,item_number,equation_number)" + "VALUES(" + object_id + ",?,?,?,?)" ; 
	pstmt = Db.DbConnection.prepareStatement( sql_query ) ;
	
	for ( equation_num = 0 ; equation_num < this.equations.length ; equation_num ++ ) {
		eq = this.equations [ equation_num ] ;
  		
  		for ( int idx = 0; idx < eq.num_items; idx ++ ) {
  			pstmt.setString( 1, eq.pos_codes.get( idx ) ) ;
  			pstmt.setInt( 2, eq.factors [ idx ] ) ;
  			pstmt.setInt( 3, idx + 1 ) ;
  			pstmt.setInt( 4, equation_num + 1 ) ;
  			pstmt.executeUpdate () ;
  		}
	}
	pstmt.close ( ) ;
	
  	if ( Check . isPredefinedType ( this.ruleDimension ) )
  		tmp = "_d1" ;
  	else
  		tmp = "_d1" ; // this.ruleDimension.get( 0 ) ;
  	Db . sqlExec ( "INSERT INTO " + Db . mdt_user_dimensions + "(object_id,dim_name,dim_index,dim_type,column_type)VALUES("
  		+ object_id + ",'" + tmp + "',1,VTLObject.O_HIERARCHICAL_RULESET,'" + this.ruleDimension + "')" ) ; 
  		
  	if ( this.condDimensions != null && this.condDimensions.size() > 0 ) {
  	  	if ( Check . isPredefinedType ( this.condDimensions.get(0) ) )
  	  		tmp = "_d2" ;
  	  	else
  	  		tmp = this.condDimensions.get(0) ;
  		Db . sqlExec ( "INSERT INTO " + Db . mdt_user_dimensions + "(object_id,dim_name,dim_index,dim_type,column_type)VALUES("
  	  			+ object_id + ",'" + tmp + "',2,VTLObject.O_HIERARCHICAL_RULESET,'" + this.condDimensions.get(0) + "')" ) ;  		
  	}
	*/
}

ListString listLeftSides ( )
{
	ListString ls = new ListString( this.equations.length );
	for ( VerticalRule eq : this.equations) {
		ls.add( eq.left_part ) ; // + " AS '" + eq.left_part + "'" ) ;
	}
	return ( ls ) ;
}


ListString listRuleids ( )
{
	ListString ls = new ListString( this.equations.length );
	for ( VerticalRule eq : this.equations) {
		ls.add( eq.ruleid ) ; // + " AS '" + eq.left_part + "'" ) ;
	}
	return ( ls ) ;
}

/*
 * Top level call: original_idx_rule must be -1
 * if recursive = true then implement the "dataset_priority" option
 * if recursive = false then implement the "dataset" option
 * TBD: verify that the equation symbol is "=" to get the recursive formula
 */
String checkRightSideFormula ( boolean recursive, boolean setZeroValue, int idx_rule, ListString leftSides, int original_idx_rule ) throws VTLError
{
	String 			tmp ;
	int				left_index ;
	StringBuffer	str = new StringBuffer () ;
	
	if ( original_idx_rule == idx_rule )
		VTLError.TypeError( "Found cyclical definition: " + this.equations[original_idx_rule].left_part 
								+ this.equations[idx_rule].left_part );
	VerticalRule eq = this.equations [ idx_rule ] ;

	if ( eq.p_condition != null ) {
		tmp = eq.p_condition.inte().measures.get(0).sql_expr;
		str.append ( "CASE WHEN " + tmp + " THEN " ) ;
	}

	for ( int x = 0; x < eq.num_items; x ++ ) {
		if ( x > 0 )
			str.append ( eq.factors[x] > 0 ? '+' : '-' ) ;
		// if a code appears in more than 1 left side then take the first
		String posCode = eq.pos_codes.get(x) ;
		if ( recursive && ( left_index = leftSides.indexOf( posCode ) ) >= 0 ) {
			str.append ( "NVL(\"" + posCode + "\"," 
								+ checkRightSideFormula ( true, setZeroValue, left_index, leftSides, original_idx_rule ) + ")" ) ;			
		}
		else {
			if ( setZeroValue )
				str.append ( "NVL(\"" ).append( posCode ).append( "\",0)" ) ;
			else
				str.append ( '"' ).append( posCode ).append( '"' ) ;
		}
	}	

	if ( eq.p_condition != null)		// ELSE part ?
		str.append ( " END " ) ;	


	return ( str.toString() ) ;
}

/*
 * get dim and dim values
 * verify that ruleComp is defined on the correct valuedomain
 */
DatasetComponent getPivotDimension ( Query q, String ruleComp, String op ) throws VTLError
{
	DatasetComponent 	dim ;
	String				dimName = this.ruleDimension ;
	// pivot dimension
	if ( this.definedOnVariable ) {
		// defined on variable
		if ( ( dim = q.getDimension( dimName ) ) == null )
			VTLError.TypeError( op + ": cannot find dimension " + dimName + " in the operand dataset" ) ;			
		if ( ruleComp != null ) {
			if ( ! dimName.equals(ruleComp ))
				VTLError.TypeError( op + ": " + dimName + " must be equal to " + ruleComp ) ; 
		}
	}
	else {
		// defined on valuedomain
		if ( ruleComp == null ) {
			dim = q.getDimensionWithGivenType( dimName ) ;	// raise exception in that method
		}
		else {
			if ( ( dim = q.getDimension( ruleComp ) ) == null )
				VTLError.TypeError( op + ": cannot find identifier " + ruleComp + " in the operand dataset" ) ;			
			if ( ! dim.compType.equals(dimName))
				VTLError.TypeError( op + ": type of " + ruleComp + " is not correct (expected: " + dimName + ")" ) ;
		}
	}
	
	// get dim values
	q.setValues ( dim ) ;

	return ( dim ) ;
}

/*
	check_hierarchy ( op , hr 
	{ condition condComp { condComp }* } 
	{ rule ruleComp }
	{ mode } { input } { output } )	
	mode ::= non_null | non_zero | partial_null | partial_zero | always_null | always_zero 
	input ::=  dataset | dataset_priority 
	output ::= invalid | all | all_measures
	
	dataset 		take value from the operand dataset 
	dataset_prority	if value is null then take value from a rule
	
	non_null	 the result Data Point is produced when all items exist and are not NULL

	define hierarchical ruleset test_sector ( variable rule ref_sector ) is	
		S1=S11+S12+S13+S14+S15+S1N
	end hierarchical ruleset
	check_hierarchy ( na_main , test_sector dataset_priority ) 
	
	define hierarchical ruleset ir_geo_itm_newa ( varaible rule itm_newa ) is
		40000 = 41000 + 42000
	end vertical ruleset ;
	check_hierarchy ( na_main , hr_sector dataset_priority )
	
	Optimization: compare number with string (e.g., itm_newa=40000 )
 */
public static Query checkHierarchy ( Query q , String rsName,
		ListString condComps, String ruleComp, String mode, String input, String output) throws VTLError
{
  	HierarchicalRuleset	ir ;
  	int					idx_rule = 0;
	DatasetComponent	pivotDim = null ;
  	StringBuffer		rules = new StringBuffer() ;
  	String				pivotMeasure = q.getFirstMeasure().compName, sql_pivot ;
  	DatasetComponent	m = q.getFirstMeasure() ;
  	ListString			leftSides ;

  	ir = getRuleset ( rsName ) ;
  	if ( ! ( mode.equals( "non_null" ) || mode.equals( "partial_null" ) ) )
  		VTLError.InternalError( "check_hierarchy, option not yet implemented:" + mode );
 
  	if ( ir.equations.length == 0 )
  		return ( q ) ;
  	
  	// check: 1 measure of type numeric
  	q.checkMonoMeasure( "check_hierarchy", "number");
  	
	// condition components - add variables to environment to copy the variable name in the sql query			
	if ( ir.condValuedomains.size() > 0 ) {
		Env.addfunctionCall("check_hierarchy");
		if ( ir.definedOnVariable ) {
			q.checkVariables ( ir.condVariables ) ;  				
		  	if ( condComps != null ) {
		  		q.checkVariables ( condComps ) ;		  		
				for ( int idx = 0; idx < ir.condVariables.size(); idx ++ ) {
					if ( ! ir.condVariables.get(idx).equals( condComps.get(idx) ) )
						VTLError.TypeError( "check_hierarchy: condition component different from ruleset (ruleset defined on valuedomain)");
				}
			}
			for ( int idx = 0; idx < ir.condValuedomains.size(); idx ++ ) {
				Env.addVar ( ir.condVariables.get(idx), ir.condValuedomains.get(idx), ir.condVariables.get(idx) ) ;					
			}
		}
		else {
			if ( condComps == null )
				VTLError.TypeError( "check_hierarchy: condition components are not specified (ruleset defined on valuedomain)");
			for ( int idx = 0; idx < ir.condValuedomains.size(); idx ++ ) {
				String vd = ir.condValuedomains.get(idx) ;
				String var = condComps.get(idx) ;
				if ( ! ( Dataset.getVariableDataType( var ).equals(vd)) )
					VTLError.TypeError( "check_hierarchy: bad valuedomain of variable: " + var + ", should be: " + vd );
				Env.addVar ( ir.condVariables.get(idx), vd, var ) ;		// alias, valuedomain, variable								
			}
		}
	}

	// pivot dimension and dim values
	pivotDim = ir.getPivotDimension ( q, ruleComp, "check_hierarchy" ) ;
  	
	// ruleid of each equation
	int idx = 1 ;
  	for ( VerticalRule eq : ir.equations )
  		if ( eq.ruleid == null )
  			eq.ruleid = rsName.toUpperCase() + "_" + idx++ ;

  	// start
  	Vector <VerticalRule> vr = new Vector <VerticalRule> ( ir.equations.length ) ;
  	for ( VerticalRule eq : ir.equations ) {
  		if ( ! pivotDim.dim_values.isEmptyIntersection ( eq.pos_codes) )
  			vr.add(eq) ;
	}
  	ir.equations = (VerticalRule[]) vr.toArray ( new VerticalRule[vr.size()]  ) ;
  	ListString allv = new ListString() ;
  	for ( VerticalRule eq : ir.equations ) {
  	  	if ( ! allv.contains( eq.left_part ) )  
  	  		allv.add(eq.left_part) ;
  	  	for ( String s : eq.pos_codes )
  	  		if ( ! allv.contains( s ) )  
  	  			allv.add(s) ;					// allv = allv.merge(eq.pos_codes) ;
  	}

	ir.allRuleDimValues = allv ;
  	leftSides = ir.listLeftSides() ;
  	// end
  	// just to test: ir.all_dim_values = new ListString();
	if ( ir.allRuleDimValues.size() == 0 ) {
		VTLError.InternalError( "No values of dimension " + pivotDim.compName + " are in the hierarchical ruleset"); 			// no rules are executed
		sql_pivot = null ;
	}
	
	rules.append( "SELECT " ).append ( q.stringDimensionsWithout ( pivotDim.compName ) ) ;
	for ( VerticalRule eq : ir.equations ) {
		rules.append( "," ).append( ir.checkRightSideFormula ( 
				( input.equals( "dataset_priority") ), 
				( mode.equals( "partial_null" ) ), idx_rule, leftSides, -1 ) )
				.append( " AS " + eq.ruleid ) ;				
		idx_rule ++ ;
	}
	
	for ( VerticalRule eq : ir.equations ) {
		rules.append( "," + eq.left_part ) ;
	}
	
	// change
	q = q.copy();
  	q.removeMeasures();
  	q.addMeasure(m.compName, m.compType, m.sql_expr );
	// q.addAttribute("orig_value", m.compType, "1") ;
	sql_pivot = q.pivotQuery ( pivotMeasure, pivotDim.compName, ir.allRuleDimValues ) ;

	rules.append( " FROM (" + sql_pivot + ")" ) ;

	if ( ir.condValuedomains.size() > 0 )
		Env.removefunctionCall() ;
	
  	StringBuffer	pivotDimValue = new StringBuffer( "CASE ruleid " );
  	StringBuffer	errorcode = new StringBuffer( "CASE ruleid " ) ;
  	StringBuffer	errorlevel = new StringBuffer( "CASE ruleid " ) ;
	StringBuffer 	originalValue = new StringBuffer ( "CASE ruleid " ) ;
	ListString		ruleids = new ListString() ;

  	for ( VerticalRule eq : ir.equations ) {
  		String			rid = eq.ruleid ;
 		ruleids.add( rid ) ;
		originalValue.append( " WHEN '" + rid + "' THEN " + eq.left_part ) ;
		pivotDimValue.append( " WHEN '" + rid + "' THEN '" + eq.left_part + "'" ) ;
		if ( eq.errorcode != null )
			errorcode.append( " WHEN '" + rid + "' THEN '" + eq.errorcode + "'" ) ;
		else
			errorcode.append( " WHEN '" + rid + "' THEN '" + eq.getEquationText( ) + "'" ) ;	// left part is the default
		if ( eq.errorlevel != null )
			errorlevel.append( " WHEN '" + rid + "' THEN '" + eq.errorlevel + "'" ) ;
	}
	pivotDimValue.append( " END " ) ;
	errorcode.append( " END " ) ;
	errorlevel.append( " END " ) ;
	originalValue.append( " END " ) ;

	Query q1 = new Query () ;
	String		table_alias = Query.newAlias () ;

	q1.sqlFrom = "(" + q.unpivotQuery(pivotMeasure, rules.toString(), "ruleid", ruleids) + ")" + table_alias ;
	// was pivotMeasure
	for ( DatasetComponent dim : q.dims ) {
		dim.sql_expr = table_alias + "." + dim.compName ;
		q1.dims.add (dim);
		if ( dim.compName.equals( pivotDim.compName ) )
			dim.sql_expr = pivotDimValue.toString() ;
	}
	
	q1.addDimension("ruleid", "string", 0, table_alias + ".ruleid", ir.listRuleids() ) ;
	
	// measures returned by check_hierarchy
	// all and all_measures: return the data points where the code appears in the leftside
	switch ( output ) {
		case "invalid" :
			q1.addMeasure(pivotMeasure, "number", originalValue.toString() ) ;		
			q1.sqlWhere = "(" + originalValue.toString() + " <> " + pivotMeasure + ")" ;
			q1.setDoFilter();
			break ;
		case "all_measures" :
			q1.addMeasure(pivotMeasure, "number", originalValue.toString() ) ;		
			q1.addMeasure( "bool_var", "boolean", "CASE WHEN " + originalValue.toString() + " = " + pivotMeasure 
					+ " THEN 'true' ELSE 'false' END");		
			break ;
		case "all" :
			q1.addMeasure( "bool_var", "boolean", "CASE WHEN " + originalValue.toString() + " = " + pivotMeasure 
													+ " THEN 'true' ELSE 'false' END");		
	}
	q1.addMeasure("imbalance", "number", "(" + originalValue.toString() + " - " + pivotMeasure + ")" );
	q1.addMeasure("errorcode", "string", ( errorcode.indexOf( " THEN " ) < 0 
					? "NULL" : errorcode.toString() ) ); 		// no errorcodes
	q1.addMeasure("errorlevel", "string", ( errorlevel.indexOf( " THEN " ) < 0 
					? "NULL" : errorlevel.toString() ) ); 		// no errorlevels
	
	q1.referencedDatasets = new ListString ( q.referencedDatasets ) ;

	return ( q1 ) ;
}

/*
 * Top level call: original_idx_rule must be -1
 */
String computeRightSideFormula ( boolean recursive, boolean setZeroValue, int idx_rule, ListString leftSides, int original_idx_rule ) throws VTLError
{
	String 			tmp ;
	int				left_index ;
	StringBuffer	str = new StringBuffer () ;
	
	if ( original_idx_rule == idx_rule )
		VTLError.TypeError( "Found cyclical definition: " + this.equations[original_idx_rule].left_part 
								+ this.equations[idx_rule].left_part );
	VerticalRule eq = this.equations [ idx_rule ] ;

	if ( eq.p_condition != null ) {
		tmp = eq.p_condition.inte().measures.get(0).sql_expr;
		str.append ( "CASE WHEN " + tmp + " THEN " ) ;
	}

	// TBD: a code can appear more than once in the list of left sides
	for ( int x = 0; x < eq.num_items; x ++ ) {
		if ( x > 0 )
			str.append ( eq.factors[x] > 0 ? '+' : '-' ) ;
		if ( ( left_index = leftSides.indexOf( eq.pos_codes.get(x) ) ) >= 0 ) {
			str.append ( "NVL(" + checkRightSideFormula ( recursive, setZeroValue, left_index, leftSides, original_idx_rule )
									+ ",\"" + eq.pos_codes.get(x) + "\")" ) ;
		}
		else
			str.append ( '"' ).append( eq.pos_codes.get(x) ).append( '"' ) ;
	}	

	if ( eq.p_condition != null)		// ELSE part ?
		str.append ( " END " ) ;	


	return ( str.toString() ) ;
}

/*
	hierarchy ( op , hr { condition condComp { , condComp }* } { rule ruleComp } 
					{ mode } { input } { output } )
	mode ::= non_null | non_zero | partial_null | partial_zero | always_null | always_zero
	input ::= rule | rule_priority | dataset
	output ::= computed | all
	
	define hierarchical ruleset test_sector ( variable rule ref_sector ) is	
		S1=S11+S12+S13+S14+S15+S1N
	end hierarchical ruleset
	hierarchy ( na_main, test_sector rule_priority )
 * use hierarchical ruleset to compute the left sides of the equations
 * when the same element is the left part of multiple left sides: use the first equation (ignore the others)
 * use the computed value if not null otherwise use the value of the left side in the dataset (the check operator implements the opposite)
 * aggregate (na_namei, hr_sector)
test := na_main [sto="B1G",activity="C",freq="A",adjustment="N",ref_area="DK",accounting_entry="B",counterpart_area="W2", counterpart_sector="S1",instr_asset="_Z", expenditure="_Z", unit_measure="XDC", prices="L", transformation="N", ref_year_price=2010 ] #obs_value ;
aggregate ( test , hr_sector)
 */
public static Query hierarchy ( Query q, String ruleName, 
		ListString condComps, String ruleComp,
		String mode, String input, String output ) throws VTLError
{
	HierarchicalRuleset	ir ;
	int					idx_rule = 0;
	DatasetComponent	pivot_dim ;
	StringBuffer		rules = new StringBuffer() ;
	String				pivotMeasure = q.getFirstMeasure().compName, sql_pivot ;
	DatasetComponent	m = q.getFirstMeasure() ;
	ListString			leftSides ;
		
	ir = getRuleset ( ruleName ) ;
  	if ( ! ( mode.equals( "non_null" ) || mode.equals( "partial_null" ) ) )
  		VTLError.InternalError( "hierarchy, option not yet implemented:" + mode );
  	if ( ! output.equals( "computed" ) )
  		VTLError.InternalError( "hierarchy, option not yet implemented:" + output );

	leftSides = ir.listLeftSides() ;
	
	if ( ir.equations.length == 0 )
		VTLError.RunTimeError( "Hierarchical ruleset: " + ruleName + " contains no rules" ) ;
	
	// remove measures <> from the selected measure
	q = q.copy();
	q.removeAttributes();
	q.removeMeasures();
	q.addMeasure(m.compName, m.compType, m.sql_expr );
	
	// add variables to environment
	if ( ir.condValuedomains.size() > 0 ) {
		Env.addfunctionCall("hierarchy");
		for ( String var : ir.condValuedomains )			
			Env.addVar ( var, "string", var ) ;				// to copy the variable name in the sql query
	}
	
	// pivot dimension and dim values
	pivot_dim = ir.getPivotDimension ( q, ruleComp, "hierarchy" ) ;
	
	// start
	Vector <VerticalRule> vr = new Vector <VerticalRule> ( ir.equations.length ) ;
	for ( VerticalRule eq : ir.equations ) {
	if ( ! pivot_dim.dim_values.isEmptyIntersection ( eq.pos_codes) )
		vr.add(eq) ;
	}
	ir.equations = (VerticalRule[]) vr.toArray ( new VerticalRule[vr.size()]  ) ;
	ListString allv = new ListString() ;
	for ( VerticalRule eq : ir.equations ) {
	if ( ! allv.contains( eq.left_part ) )  
		allv.add(eq.left_part) ;
	for ( String s : eq.pos_codes )
		if ( ! allv.contains( s ) )  
			allv.add(s) ;					// allv = allv.merge(eq.pos_codes) ;
	}
	
	ir.allRuleDimValues = allv ;
	leftSides = ir.listLeftSides() ;
	// end
	// just to test: ir.all_dim_values = new ListString();
	if ( ir.allRuleDimValues.size() == 0 ) {
		VTLError.InternalError( "No values of dimension " + pivot_dim.compName + " are in the hierarchical ruleset"); 			// no rules are executed
		sql_pivot = null ;
	}
	
	sql_pivot = q.pivotQuery ( pivotMeasure, pivot_dim.compName, ir.allRuleDimValues ) ;
	
	rules.append( "SELECT " ).append ( q.stringDimensionsWithout ( pivot_dim.compName ) ) ;
	for ( VerticalRule eq : ir.equations ) {
		if ( leftSides.indexOf( eq.left_part) < idx_rule )
			;	// take only the first definition
		else
			rules.append( "," ).append( ir.computeRightSideFormula ( 
					(input.equals("dataset_priority")),
					(mode.equals( "partial_null")),
					idx_rule, leftSides, -1 ) ).append( " AS " + eq.left_part ) ;		
		idx_rule ++ ;
	}
	rules.append( " FROM (" + sql_pivot + ")" ) ;
	
	if ( ir.condValuedomains.size() > 0 )
		Env.removefunctionCall() ;
	String s = q.unpivotQuery(pivotMeasure, rules.toString(), pivot_dim.compName, leftSides ) ; //ir.all_dim_values) ;
	q.pushQuery( s );
	
	return ( q ) ;
}

/*
 * Aggregate function using vertical ruleset. 
	Examples:
	print sum(aact_ali01 (itm_newa=40000, time = 2000) ) using my_rule where time="2005" and itm_newa = "40000" and geo in ( "IT", "FR", "BE","DE","IE" );
	print sum( bop_its_det ( geo = IT, time = 2005, flow=CREDIT, partner=WORLD ) ) using rule_post ;
	print compute ( bop_its_det ( geo = IT, time = 2005, flow=CREDIT, partner=WORLD ) ) using rule_post ;
	print sum ( bop_its_det ( geo = IT, time = 2005, flow=CREDIT, partner=WORLD ) ) using rule_post ;

	compute: can be total/partial, factor (+ or -) is inserted into the formula	
	aggr_function: can be total/partial, factor is ignored in the formula

   This function uses an Oracle Model. Example:
   
	SELECT geo, itm_newa, time, s as obs_value
	FROM aact_ali01
	MODEL
	PARTITION BY  ( itm_newa, time )
	DIMENSION BY ( geo )
	MEASURES ( obs_value AS s  )
	KEEP NAV
	UNIQUE DIMENSION
	RULES UPSERT SEQUENTIAL ORDER
	(
	   s[ 'IT' ]  = NVL ( s [ 'FR' ] + s [ 'LU' ] , s[ 'IT' ] ) ,
	   s[ 'DE' ]  = NVL ( s [ 'BE' ] + s [ 'UK' ] , s[ 'DE' ] ) ,
	   s[ 'SK' ]  = NVL ( s [ 'BE' ] + s [ 'LU' ] , s[ 'SK' ] )
	)
	ORDER BY geo, itm_newa, time
	
	Example MDT: print sum ( aact_ali01 ) using rule_geo ;
	
	Recent changes: 
		max_computation_level not used anymore
		new option for compute operator: return all/updated cells
 */

/*
 * Copy hierarchical ruleset
 */
static void copyRuleset ( VTLObject obj, String dblink, int new_object_id ) throws VTLError
{
	UserFunction.copySyntaxTree(obj, dblink, new_object_id);
	/*
	String 		mdt_sys ;
	
	mdt_sys = ( dblink != null ? obj.remoteOwner + ".mdt_equations" + dblink : Db.mdt_equations ) ;
	Db.sqlExec( "INSERT INTO " + Db.mdt_user_equations 
			+ "(object_id,left_part,equation_type,constant_part,computation_level,num_items,equation_number,cond_lowerbound, cond_upperbound, equation_comment)"
			+ "SELECT " + new_object_id + ",left_part,equation_type,constant_part,computation_level,num_items,equation_number,cond_lowerbound, cond_upperbound, equation_comment"	
			+ " FROM " + mdt_sys + " WHERE object_id=" + obj.objectId ) ;

	mdt_sys = ( dblink != null ? obj.remoteOwner + ".mdt_equations_items" + dblink : Db.mdt_equations_items ) ;
	Db.sqlExec( "INSERT INTO " + Db.mdt_user_equations_items 
	    + "(object_id,pos_code,factor,item_number,equation_number)"
	    + " SELECT " + new_object_id + ",pos_code,factor,item_number,equation_number"	
	    + " FROM " + mdt_sys + " WHERE object_id=" + obj.objectId ) ;
		
	mdt_sys = ( dblink != null ? obj.remoteOwner + ".mdt_equations_tree" + dblink : Db.mdt_equations_tree ) ;
	Db.sqlExec( "INSERT INTO " + Db.mdt_user_equations_tree 
	    + "(object_id,pos_index,pos_code,pos_level,pos_class,equation_number)"	
	    + " SELECT " + new_object_id + ",pos_index,pos_code,pos_level,pos_class,equation_number"		
	    + " FROM " + mdt_sys + " WHERE object_id=" + obj.objectId ) ;		

	mdt_sys = ( dblink != null ? obj.remoteOwner + ".mdt_dimensions" + dblink : Db.mdt_dimensions ) ;
	Db.sqlExec( "INSERT INTO " + Db.mdt_user_dimensions 
	      + "(object_id,dim_name,dim_index,dim_type,dim_const,dim_null,dim_width,column_type)"
	      + "SELECT " + new_object_id + ",dim_name,dim_index,dim_type,dim_const,dim_null,dim_width,column_type"	
	      + " FROM " + mdt_sys + " WHERE object_id=" + obj.objectId) ;

	mdt_sys = ( dblink != null ? obj.remoteOwner + ".mdt_syntax_trees" + dblink : Db.mdt_syntax_trees ) ;
	Db.sqlExec( "INSERT INTO " + Db.mdt_user_syntax_trees 
			+ "(object_id,node_index,node_num_children,node_name,node_info,node_value_1,node_value_2)" 
			+ " SELECT " + new_object_id + ",node_index,node_num_children,node_name,node_info,node_value_1,node_value_2"	
			+ " FROM " + mdt_sys + " WHERE object_id=" + obj.objectId ) ;
	*/
}

// end of class
}

