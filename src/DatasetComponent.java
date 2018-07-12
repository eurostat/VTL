

/*
 * DatasetComponent. Used in Dataset and Query descriptors.
 */
public class DatasetComponent { 
	String		compName ;
	String		compType ;
	int    		compWidth ;
	boolean		canBeNull ;
	boolean		isViralAttribute ;
	ListString	dim_values ;
	String		sql_expr ;

DatasetComponent ( )
{
	super () ;
}
	
DatasetComponent ( DatasetComponent m )
{
	compName = m.compName;
	compType = m.compType ;
	compWidth = m.compWidth ;
	sql_expr = m.sql_expr;
	canBeNull = m.canBeNull;
	dim_values = m.dim_values ;
	isViralAttribute = false ;
}

DatasetComponent ( String my_col_name, String my_col_type, String my_sql_expr, int my_col_width, boolean my_col_null )
{
	compName = my_col_name;
	compType = my_col_type ;
	sql_expr = my_sql_expr;
	compWidth = my_col_width ;
	canBeNull = my_col_null;
	dim_values = null ;
	isViralAttribute = false ;
}

DatasetComponent ( String my_col_name, String my_col_type, String my_sql_expr )
{
	compName = my_col_name;
	compType = my_col_type ;
	sql_expr = my_sql_expr;
	compWidth = 0 ;
	canBeNull = false;
	dim_values = null ;
	isViralAttribute = false ;
}

DatasetComponent(String dim_name, String dim_type, int dim_width, String sql_expr2, ListString dim_values2) {
	compName = dim_name;
	compType = dim_type ;
	sql_expr = sql_expr2;
	compWidth = dim_width ;
	canBeNull = false;
	dim_values = dim_values2 ;
	isViralAttribute = false ;
}

boolean isViralAttribute ()
{
	return ( this.isViralAttribute ) ;
}

final void setViralAttribute ()
{
	this.isViralAttribute = true ;
}

}
