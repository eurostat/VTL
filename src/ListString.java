
/*
   A ListString contains a list of string implemented by a Vector.
*/

import java.util.* ;

public class ListString extends Vector <String> { // Vector <String> {

static final long	serialVersionUID = 1;		// required by Java serializable class

/*
  New list.
*/

public ListString ( )
{
  super () ; // new Vector ( ) ; 
}

/*
  New list (int size).
*/

public ListString ( int ls_size )
{
  super ( ls_size ) ;
}

/*
 * New list (a Vector).
 */
public ListString ( ListString my_vector )
{
  super ( my_vector ) ;
  // super ( ( Collection <String> ) my_vector ) ;
  // my_vector . clone () ;
}

/*
 * New list (a Vector).
 */
public ListString ( String [] arr )
{
	super ( arr.length ) ;
	for ( String str : arr )
		this.add ( str ) ;
	/*
	int 		idx ;	
	for ( idx = 0; idx < arr.length; idx ++ )
		this.add ( arr [ idx ] ) ;
	*/
}

/*
 * Convert to string array.
 */
public String[] toArray ( )
{
	String[] arr = new String[this.size()] ;
	for ( int x = 0; x < this.size(); x ++ )
		arr[x] = this.get(x) ;
	return ( arr ) ;
}

/*
 * Merge this list with a list (union operation).
 */
public ListString merge ( ListString ls )
{
  ListString	new_list = new ListString ( this ) ;

  for ( String str : ls )
	  if ( this.indexOf ( str ) < 0 )
		  new_list.add( str ) ;
  return ( new_list ) ;  
}

/*
 * Join this list with a list (intersection operation).
 */
public ListString join ( ListString ls )
{
  ListString	new_list = new ListString ( ) ;

  for ( String str : this )
	  if ( ls.indexOf ( str ) >= 0 )
		  new_list.add( str ) ;
  return ( new_list ) ;
}

/*
 * Minus this list with a list (minus operation).
 */
public ListString minus ( ListString ls )
{
  ListString	new_list = new ListString ( ) ;

  for ( String str : this )
      if ( ls.indexOf ( str) < 0 )
   	   new_list.add ( str ) ;
  return ( new_list ) ;

}

/*
 * Returns array of values of ls that are [not] in this.
 */
public ListString intersect ( ListString ls, boolean filter_in )
{
	return ( filter_in ? ls.join( this ) : ls.minus( this ) ) ;
}

/*
 * Returns sublist of this list.
 * Index start from 1.
 */
public ListString sublist ( int index_from, int index_to )
{
  int	idx, my_size ; 
  ListString	new_list = new ListString ( ) ;
  
  my_size =  this . size () ;

  if ( index_from < 0 )
     index_from = 1 + my_size + index_from ;

  if ( index_to < 0 )
     index_to = 1 + my_size + index_to ;

  index_from -- ;
  index_to -- ;

  if ( index_from < 0 )
     index_from = 0 ;

  if ( index_to >= my_size )
     index_to = my_size - 1 ;

  for ( idx = index_from; idx <= index_to; idx ++ )
	  new_list . add( this.get ( idx ) ) ;

  return ( new_list ) ;
}

/*
  Sort this list by order existing in another list.
*/

public ListString sort_by_list ( ListString ls )
{
  int			idx, idx_found ;
  ListString	ordered_list = new ListString ( this ) ; // ( ListString ) this . clone ( ) ;
  ListString	new_list = new ListString ( ) ;
  boolean		item_found [] ;
  
  item_found = new boolean [ this . size () ] ;
  Arrays.fill ( item_found , false ) ;
  
  Collections . sort ( ordered_list ) ;

  for ( String my_pos : ls ) {
	  	if ( ( idx_found = Collections . binarySearch ( ordered_list, my_pos ) ) >= 0 ) {
	  		new_list.add ( my_pos ) ;
	  		item_found [ idx_found ] = true ;
	  	}
  }
  
  for ( idx = 0; idx < item_found . length ; idx ++ ) {
	  if ( ! item_found [ idx ] )
		  new_list.add ( ordered_list . get ( idx ) ) ;
  }

  return ( new_list ) ;
}

/*
  Sort this list in ascending order.
*/

public ListString sort_asc ( )
{
  ListString	new_list = new ListString ( this ) ;

  Collections . sort ( new_list ) ;

  return ( new_list ) ;
}

/*
  Sort this list in descending order.
*/

public ListString sort_desc ( )
{
  ListString	new_list = new ListString ( this ) ;

  Collections . sort ( new_list ) ;

  Collections . reverse ( new_list ) ;

  return ( new_list ) ;
}

/*
 * Returns true if intersection is empty.
 */
public boolean isEmptyIntersection ( ListString ls )
{
	for ( String s : this  ) {
        if ( ls.contains ( s ) )
        	return ( false ) ;
	}
	return ( true ) ;
}

/*
 * Returns true if this is a subset of ls.
 */
public boolean isSubset ( ListString ls )
{
	for ( String s : this  ) {
        if ( ls.indexOf ( s ) < 0 )
        	return ( false ) ;
	}
	return ( true ) ;
}

/* 
 * Build SQL syntax:
 * 
 * 		IN('item1', ... )
 * or
 * 		NOT IN('item1', ... )
 * 
 * if array is empty, returns a condition that is always false
 * 
 * if array has one item, returns:
 * 	='item1'
 * 	<>'item1'
 * 
 * NB: items must not contain character '
*/
String sqlSyntaxInList( String column_name, String sqlOp ) 
{
	StringBuffer str = new StringBuffer(100);
	
	switch ( this.size() ) {
	    case 0:
	    	str.append("0=1");
	    	break;
	    case 1:
	    	str = str.append("(" + column_name ).append( sqlOp.equals("IN") ? "=" : "<>" )
	      				.append("'").append(this.get(0)).append("')");
	    	break;
	    default:
	    	String	logicalOp = sqlOp.equals("IN") ? "OR" : "AND" ;
	    	str.append( "(" ).append( column_name ).append (" " + sqlOp + " ").append("('").append(this.get(0)).append("'");
	    	Iterator <String> iter = this.iterator();
	    	iter.next();
	    	int	idx ;
	    	while (iter.hasNext()) {
		        idx = 1;
		        while (idx < 1000 && iter.hasNext()) {
		      	  str.append(",'").append( iter.next() ).append("'");
		      	  idx++;
		        }
		        if (iter.hasNext()) {
		      	  str = str.append(")" + logicalOp + " " + column_name + " " + sqlOp + "( '");
		      	  str.append( iter.next() ).append("'");
		        }
	    	}
		    str.append("))") ;
	}
	
	return (str.toString());
}

/*
 * Returns a string containing all values separated by , and enclosed in ""
 */
public String toString ( )
{
	StringBuffer	buff = new StringBuffer () ;
	int	idx ; 
	
	for ( idx = 0; idx < this . size ( ); idx ++ ) {
		if ( idx > 0 )
			buff .append( "," ) ;
		buff.append('"') .append(this.get(idx)).append('"') ;
	}
	return ( buff . toString() ) ;
}

/*
 * Returns a string containing all values separated by separator.
 */
public String toString ( char separator )
{
	StringBuffer	buff = new StringBuffer () ;
	for ( String s : this ) {
		if ( buff.length() > 0 )
			buff . append ( separator ) ;
		buff . append ( s ) ;
	}
	return ( buff . toString() ) ;
}

/*
 * Returns the max string in the list.
 */
public String findMax ( )
{
	String	maxElement = null ;
	
	for ( String str : this ) {
		if ( maxElement == null || str.compareTo( maxElement ) > 0 )
			maxElement = str ;		
	}
	return ( maxElement ) ;
}

/*
 * Returns the min string in the list.
 */
public String findMin ( )
{
	String	minElement = null ;
	
	for ( String str : this ) {
		if ( minElement == null || str.compareTo( minElement ) < 0 )
			minElement = str ;		
	}
	return ( minElement ) ;
}

/*
 * Add item to list and check uniqueness.
 */
public void addUnique ( String item, String errorMessage ) throws VTLError
{
	if ( this.indexOf( item ) >= 0 )
		VTLError.TypeError( errorMessage + ", element is not unique: " + item + "(all elements: " + this.toString(',') + ')' );

	this.add( item ) ;
}

// END class
}
