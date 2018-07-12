
/*
   RangeDimension class. A range or filter on a dimension is used to restrict operations to a given 
   set of values of that dimension.
*/

import java.util.Vector;

class RangeVariable {

String		dimName ;
ListString	dimValues ;

static Vector <RangeVariable> filteredDimensions = new Vector <RangeVariable> () ;

RangeVariable ( String myDimName, ListString myDimValues ) {
	dimName = myDimName ;
	dimValues = myDimValues ;
}

/*
 * Reset all filters.
 */
static void resetAllRanges ( )
{
	filteredDimensions.clear() ;
}

/*
 * Get current filter on dim_name.
 */
public static RangeVariable getFilter ( String dim_name )
{
  	for ( RangeVariable item : filteredDimensions ) {
  		if ( item.dimName.equals ( dim_name ) )
  		   return ( item ) ;
  	}
  	
    return ( null ) ;
}

/*
 * Set filter on dim_name.
*/
static Vector <RangeVariable> saveFilters ( )
{
	Vector <RangeVariable> my_filters = new Vector <RangeVariable> () ;
	
	for ( RangeVariable item : filteredDimensions ) {
		my_filters.add ( new RangeVariable ( item.dimName, item.dimValues ) ) ;		
	}
	return ( my_filters ) ;
}

/*
 * Set filter on dim_name.
*/
static void restoreFilters ( Vector <RangeVariable> myFilters )
{
	filteredDimensions = myFilters ;
}

/*
 * Set filter on dim_name.
 * if dim_values is empty then the filter is removed (both for in and not in)
 */
static void setFilter ( String dimName, ListString dimValues )
{
	for ( RangeVariable item : filteredDimensions ) {
		if ( item.dimName.equals ( dimName ) ) {			// found existing filter
			if ( dimValues.size () > 0 )
				item.dimValues = dimValues ;
			else
				filteredDimensions.remove ( item ) ;
			return ;
	  	}
	}

	// existing filter not found
	if ( dimValues.size() > 0 ) {
		filteredDimensions.add ( new RangeVariable ( dimName, dimValues ) ) ;
	}
}

// end class
}
