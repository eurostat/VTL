
/*
 * Datapoint (horizontal) ruleset.
 */

import java.util.Vector;

class HorizontalRule {
	String	ruleid ;
	Node	p_precondition ;
	Node	p_condition ;
	String	errorcode	;
	String	errorlevel ;
}

public class DatapointRuleset {
	int						objectId ;
	String					rulesetName ;			
	ListString				variables ; 			// variable (or alias) used in the condition
	ListString				valuedomains ; 			// value domain of variable (or alias) used in the condition
	Vector <HorizontalRule> rules ; 
	Node					syntaxTree ;			// syntax tree of the ruleset
	boolean					definedOnVariable ;		// defined on variable or valuedomain

/*
 * Get descriptor of datapoint ruleset.
 */
static public DatapointRuleset getRuleset ( String dprName ) throws VTLError
{
	DatapointRuleset	dpr ;
	VTLObject			obj ;
	Node				hd ;
	obj = VTLObject.getObjectDescSpecific( dprName, VTLObject.O_DATAPOINT_RULESET ) ;

	hd = UserFunction.getSyntaxTree( Db.mdt_syntax_trees, obj.objectId ) ;
	if ( hd == null )
		VTLError.InternalError( "no internal representation found for datapoint ruleset: " + dprName );
	dpr = Command.evalDatapointRuleset ( hd ) ;
	//UIConsole.showInfo("test", hd.printSyntaxTree( ) ) ;
	dpr.objectId = obj.objectId ;
	return( dpr );
	/*
	HorizontalRule		hr ;
	Statement 			st ;
	ResultSet 			rs ;
	String				sql_query, str_variables ;
	dpr = new DatapointRuleset ( ) ;
	dpr.rulesetName = dprName ;
	str_variables = Db.sqlGetValue ( "SELECT data_object_name FROM " + Db.mdt_validation_rules + " WHERE object_id=" + obj.objectId ) ;	
	dpr.variables = new ListString ( str_variables.split(",") ) ;
	dpr.object_id = obj .objectId ;
	dpr.rules = new Vector <HorizontalRule> ( ) ;
	
	sql_query = "SELECT precondition,condition,error_message,severity_code,sql_precondition,sql_condition"
			+ " FROM " + Db.mdt_validation_conditions 
			+ " WHERE object_id=" + obj.objectId 
			+ " ORDER BY line_number" ;
	try {
		st = Db . DbConnection . createStatement () ;
		rs = st . executeQuery ( sql_query ) ;
		while ( rs . next () ) {
			hr = new HorizontalRule () ;
			hr.precondition = rs.getString( 1 ) ;
			if ( hr.precondition.equals ( "*" ) )		// * because Oracle field is not null
				hr.precondition = null ;
			hr.condition = rs.getString( 2 ) ;
			hr.errorcode = rs.getString( 3 ) ;
			hr.errorlevel = rs.getString( 4 ) ;
			hr.sql_precondition = rs.getString( 5 ) ;			
			hr.sql_condition = rs.getString( 6 ) ;			
			dpr.rules.add ( hr ) ;
		}
		rs . close () ;
		st . close () ;
	}
	catch ( SQLException e ) {
		VTLError . SqlError  ( e . toString () + "\nSQL query:\n" + sql_query ) ;
	}
	return ( dpr ) ;
	*/
}

/*
 * Save descriptor of datapoint ruleset.
 * Example:
	VTL: data_object_name contains the list of variables
	create or replace datapoint ruleset aval_2 ( aact_ali01 ) is
  	when geo  = "IT" then obs_value  < 700 message "value too big" severity "Warning" ;
  	when geo  = "IT" then obs_status <> "e" message "bad flag" severity "Error" ;
	end datapoint ruleset ;
define datapoint ruleset ruleset_bop (stk_flow, obs_value) is 
when stk_flow = "CREDIT" or stk_flow = "DEBIT" then obs_value > 0 ;
end datapoint ruleset ;
TBD: precondition column cannot be null in MDT
 */
public void saveRuleset ( boolean create_or_replace ) throws VTLError
{
	boolean		is_new_object ;
	
	this.objectId = 0 ;
	if ( create_or_replace )
		this.objectId = VTLObject.createReplaceObjectId ( this.rulesetName, VTLObject.O_DATAPOINT_RULESET ) ;
	is_new_object = ( this.objectId == 0 ) ;
	if ( is_new_object )
		this.objectId = VTLObject.createObjectId ( this.rulesetName, VTLObject.O_DATAPOINT_RULESET ) ;
	
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
}

/*
 * Build definition of the ruleset in VTL syntax
 */
String getDefinition() throws VTLError
{
	StringBuffer 	syntax = new StringBuffer () ;
	DatapointRuleset 	vr = this ;
	HorizontalRule		vc ;

	syntax.append( "define datapoint ruleset " ).append( vr.rulesetName ).append( " ( variable " )
			.append( vr.variables.toString( ',' ) ).append( " ) is\n" ) ;		
	for ( int idx = 0; idx < vr.rules.size(); idx ++ ) {
		if ( idx > 0 )
			syntax.append ( " ;\n" ) ;
		vc = vr.rules.get(idx) ;
		syntax.append ( "/* " ).append( idx + 1 ).append( " */\t" ) ;
		if ( vc.ruleid != null )
			syntax.append ( vc.ruleid ).append( " : " ) ;
		if ( vc.p_precondition != null && vc.p_precondition.name != Nodes.N_EMPTY ) {
			syntax.append ("when " ) ;
			vc.p_precondition.unparseOp( syntax ) ;
			syntax.append ( " then " ) ;			
		}
		if ( vc.p_condition == null || vc.p_condition.name == Nodes.N_EMPTY )
			VTLError.InternalError( "Datapoint ruleset: condition is empty");
		
		vc.p_condition.unparseOp( syntax ) ;

		if ( vc.errorcode != null )
			syntax.append ( " errorcode \"" ).append( vc.errorcode ).append( "\"" ) ;
		if ( vc.errorlevel != null )
			syntax.append ( " errorlevel \"" ).append( vc.errorlevel ).append( "\"" ) ;
	}
	syntax.append( "\nend datapoint ruleset" ) ;
	return ( syntax.toString() ) ;
}
/*

sql" 
SELECT a2.currency AS currency,a2.bop_item AS bop_item,a2.s_adj AS s_adj,a2.sectpart AS sectpart,a2.stk_flow AS stk_flow,a2.obs_value AS obs_value,a2.obs_decimals AS obs_decimals,a2.obs_status AS obs_status, a2.ruleid , a2.errorlevel,case a2.ruleid when 'R1' then 'kfsdkfsdfl' when 'R2' then 'eee' end as errorcode FROM ( 
SELECT * FROM (
SELECT a1.currency AS currency,a1.bop_item AS bop_item,a1.s_adj AS s_adj,a1.sectpart AS sectpart,a1.stk_flow AS stk_flow,a1.obs_value AS obs_value,a1.obs_decimals AS obs_decimals,a1.obs_status AS obs_status,CASE WHEN (stk_flow = 'CRE' or stk_flow = 'DEB') AND NOT(obs_value > 10000) THEN '--' END AS R1 FROM refin.bop_eu6_m a1 WHERE a1.geo='EU28' AND a1.partner='EXT_EU28' AND a1.sector10='S1' AND a1.time='2010M01'
) 
UNPIVOT ( errorlevel FOR ruleid IN ( R1 )) 
)a2
" ;
 */
/*
	check_datapoint ( op , dpr { components listComp } { output output } )
	output ::= invalid | all | all_measures
	invalid			return only the data points with bool_var = false (default) and the measures
	all				return all data points and the only measure bool_var
	all_measures	return all data points, the measures and boolvar
	
 	define datapoint ruleset test_dpr ( variable time, obs_value ) is
		obs_value > 10000 	errorcode "Greater than 10000" ;
		when time = 2015 then obs_value > 100		errorcode "Greater than 100" errorlevel "Warning" ;
		obs_value > 10 		errorlevel "Error" ;
	end datapoint ruleset
	check_datapoint ( na_main, test_dpr )

	TBD: ruleid is always in uppercase
 */
static Query checkDataPoint ( Query q, String rsName, ListString components, String output ) throws VTLError
{
	int					origNumMeasures = q.measures.size() ;		// original number of measures
	DatapointRuleset	dpr ;
	ListString			ruleids = new ListString();
	String				sqlExpr, m, ruleidPrefix = rsName.toUpperCase() + "_" ;
	StringBuffer		errorcode = new StringBuffer( "CASE ruleid " ) ;
	StringBuffer		errorlevel = new StringBuffer( "CASE ruleid " ) ;
	
	dpr = getRuleset( rsName ) ;
	
  	// add variables to the environment
  	Env.addfunctionCall( "check_datapoint");
  	if ( ( ! dpr.definedOnVariable ) ) {
  	  	if ( components == null )
  	  		VTLError.TypeError( "check_datapoint: components must be specified for a ruleset defined on valuedomain");
	  	if ( components.size() < dpr.variables.size() )
			VTLError.TypeError( "check_datapoint: not enough components specified");
  	}
	for ( int idx = 0; idx < dpr.variables.size(); idx ++ ) {
		String	varName = dpr.variables.get(idx) ;
		String	vdName =  dpr.valuedomains.get(idx) ;
 	  	if ( components == null ) {
	 		q.checkVariable( varName );
			Env.addVar ( varName, vdName, varName ) ; 	
			// variable or alias, valuedomain, variable 	  		
 	  	}
 	  	else {
	 	  	q.checkComponentType( components.get(idx), vdName ) ;	 	  		
			Env.addVar ( varName, vdName, components.get(idx) ) ; 	
			// alias, valuedomain, component
 	  	}
	}

	int	ruleNumber = 1 ;
	for ( HorizontalRule hr : dpr.rules ) {
		// add a new measure for each rule
		if ( hr.p_precondition == null )
			sqlExpr = "CASE WHEN " + hr.p_condition.sqlWhereCondition() + " THEN 'true' ELSE 'false' END" ;
		else
			sqlExpr = "CASE WHEN NOT (" + hr.p_precondition.sqlWhereCondition() + ") OR " 
							+ hr.p_condition.sqlWhereCondition() + " THEN 'true' ELSE 'false' END" ;
		m = hr.ruleid == null ? ruleidPrefix + ( ruleNumber++ ) : hr.ruleid ;
		ruleids.add( m ) ;
		q.addMeasure( m, "string", sqlExpr ) ;	
		// errorcode and errorlevel
		if ( hr.errorcode != null )
			errorcode.append( " WHEN '" + m + "' THEN '" + hr.errorcode + "'" ) ;
		if ( hr.errorlevel != null )
			errorlevel.append( " WHEN '" + m + "' THEN '" + hr.errorlevel + "'" ) ;
	}
	errorcode.append( " END " ) ;
	errorlevel.append( " END " ) ;
	q = q.flatQueryUnpivot ( " UNPIVOT ( bool_var FOR ruleid IN (" + ruleids.toString( ',' ) + "))" ) ;//;  unpivot.toString() ) ; // " UNPIVOT ( errorcode FOR ruleid IN ( R1 )) " ) ;
	q.measures.setSize(origNumMeasures ) ;		// remove the inserted measures
	Env.removefunctionCall();
	
	// ruleid
	q.addDimension("ruleid", "string", 0, "ruleid", ruleids ) ;
	
	switch ( output ) {
		case "invalid" :
			q.sqlWhere = "bool_var='false'" ;
			q.setDoFilter();
			break ;
		case "all_measures" :
			q.addMeasure( "bool_var", "boolean", "bool_var") ;
			break ;
		case "all" :
			q.removeMeasures();
			q.addMeasure( "bool_var", "boolean", "bool_var") ;
	}

	// inserted in the opposite order: first measure will be errorcode
	q.insertMeasure( "errorlevel", "string", ( errorlevel.indexOf( " THEN " ) < 0 ? "NULL" 
							: errorlevel.toString() ) ) ;		// no error levels
	q.insertMeasure( "errorcode", "string", ( errorcode.indexOf( " THEN " ) < 0 ? "NULL" 
							: errorcode.toString() ) ) ;			// no error codes

	q.removeAttributes();
	
	return ( q ) ;
}

/*
 * Copy datapoint ruleset.
 */
public static void copyRuleset ( VTLObject obj, String dblink, int new_object_id ) throws VTLError
{
	UserFunction.copySyntaxTree(obj, dblink, new_object_id);
	
	/*
	String		mdt_sys ;
	
	mdt_sys = ( dblink != null ? obj.remoteOwner + ".mdt_validation_rules" + dblink : Db.mdt_validation_rules ) ;
	
	Db.sqlExec( "INSERT INTO " + Db.mdt_user_validation_rules 
			+ "(object_id,data_object_name,severity_dictionary)"	
			+ "SELECT " + new_object_id + ",data_object_name,severity_dictionary"	
			+ " FROM " + mdt_sys + " WHERE object_id=" + obj.objectId ) ;
	
	mdt_sys = ( dblink != null ? obj.remoteOwner + ".mdt_validation_conditions" + dblink : Db.mdt_validation_conditions ) ;
	Db.sqlExec( "INSERT INTO " + Db.mdt_user_validation_conditions 
	    + "(object_id,line_number,precondition,condition,error_message,severity_code,sql_precondition,sql_condition)"
	    + " SELECT " + new_object_id + ",line_number,precondition,condition,error_message,severity_code,sql_precondition,sql_condition"	
	    + " FROM " + mdt_sys + " WHERE object_id=" + obj.objectId ) ;		
	*/
}

// end class
}
