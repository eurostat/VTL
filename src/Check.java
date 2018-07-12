

/*
 * Type checking functions.
 */

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;

class Check {

static final String predefinedTypes [ ] = { "string", "integer", "number", "date", 
											"boolean", "time", "time_period", "duration", "scalar", "null" } ;

/*
 * Get sql type to be used in a SQL CAST ( )
 */
static String getSqlCastType ( String dataType ) throws VTLError
{
	String	sqlType ;
	
	switch ( dataType ) {
		case "integer" : 
		case "number" : 
		case "date" :
			sqlType = dataType ;
			break ;
		case "string" :
		case "boolean" :
		case "time" :
		case "time_period" :
		case "scalar" :
		case "null" :
			sqlType = "varchar" ;
			break ;
		default :
			Dataset dict = Dataset.getValueDomain ( dataType ) ;
		    sqlType = getSqlCastType ( dict.dims.firstElement().compType ) ;
	}

	return ( sqlType ) ;
}

static final boolean isPeriodDataType ( String dataType )
{ 
	return ( dataType.equals( "time_period" ) ) ; 
}

/*
 * Is data_type a predefined type ?
 */
public static boolean isPredefinedType ( String dataType ) 
{
	return ( Sys.getPosition ( predefinedTypes , dataType ) >= 0 ) ;
}

/*
 * Is user-defined datatype.
 */
public static boolean isValueDomainType ( String data_type ) throws VTLError
{
	VTLObject obj = VTLObject.getObjectDesc ( data_type ) ;

	return ( obj != null && obj.objectType == VTLObject.O_VALUEDOMAIN ) ;
}

/*
 * Is data_type a multidimensional type ?
 */
public static boolean isQueryType ( Object data_type )
{
	return ( data_type instanceof Query ) ;
}

/*
 * Is data_type a set scalar type ?
 * "set string", "set integer", "set number", "set date", "set date" ... 
 */
public static boolean isSetScalarType ( Object dataType )
{
	return ( dataType instanceof String && ( ( String) dataType ).startsWith ( "set " ) ) ;
}

/*
 * Is data_type a scalar type ?
 * string, integer, number, date, time or user-defined type
*/
public static boolean isScalarType ( Object data_type )
{
	return ( data_type instanceof String && ! ( ( String) data_type ).startsWith ( "set " ) ) ;
}

/*
 * Get base data_type of set scalar type.
 */
static String baseTypeOfSetType ( Object dataType ) throws VTLError 
{
	if ( ! ((String)dataType).startsWith ( "set " ) )
		VTLError.InternalError( "Bad scalar set type: " + ((String) dataType)) ;
		
	return ( ((String) dataType).substring ( 4 ) ) ;
}

/*
 * Is data_type a numeric type (integer, number) ?
 */
static boolean isNumericType ( String data_type )
{
	return ( data_type.equals( "number" ) || data_type.equals( "integer" ) ) ;
}

/* 
 * Lexical check function on integers, with optional sign.
 */
static boolean isInteger ( String str )
{
	int  	i = 0 ;
	int		len = str.length () ;
	if ( str . charAt ( i ) == '-' ) 
		i++ ;
	while ( i < len && str.charAt ( i ) >= '0' && str.charAt ( i ) <= '9' )
		i ++;

	return( i == len ) ;
}

/* 
 * Lexical check function on integers, without optional sign.
 */
static boolean isPositiveInteger ( String str )
{
	for ( int i = 0; i < str.length ()  ; i++ )
		if ( ! Character.isDigit( str.charAt ( i ) ) )
			return ( false ) ;

	return( true ) ;
}

/* 
 * Lexical check function on numbers.
 * var x number; x = "1.0E-6" ; print x ; 
 */
static boolean isNumber ( String str )
{
	/*
	/try { Double.parseDouble(str) ; }
	catch ( NumberFormatException e ) { return ( false ) ; }
	return ( true ) ;
	*/
	int  	i = 0 ;
	int	len = str . length () ;
	
	if ( str . charAt ( i ) == '-' ) 
		i++ ;

	while ( i < len && str . charAt ( i ) >= '0' && str . charAt ( i ) <= '9' )
		i ++ ;

	if ( i < len && str . charAt ( i ) == '.') {
		i ++;
		while ( i < len && str . charAt ( i ) >= '0' && str . charAt ( i ) <= '9' )
			i ++ ;		
	}
	
	if ( i < len && Character.toUpperCase( str.charAt ( i ) ) == 'E') {	
		i ++ ;
		if ( i < len && str.charAt ( i ) == '-' || str.charAt ( i ) == '+' )  
			  i++ ;
		while ( i < len && str . charAt ( i ) >= '0' && str . charAt ( i ) <= '9' )
			i ++;
	}

	return( i == len ) ;

}

/* 
 * If str is not an integer then throws exception.
 */
static void checkInteger ( String str ) throws VTLError
{
	if ( ! isInteger ( str ) )
		VTLError.RunTimeError( "Not an integer number: " + str );
}

/* 
 * If str is not a positive integer then throws exception.
 */
static void checkPositiveInteger ( String str ) throws VTLError
{
	if ( ! isPositiveInteger ( str ) )
		VTLError.RunTimeError( "Not a positive integer number: " + str );
}
/* 
 * If str is not an integer then throws exception.
 * N:day number
 */
static boolean isDuration ( String str ) throws VTLError
{
	String frequencies = "ASQMWD" ;
	return ( str != null && str.length() == 1 && frequencies.indexOf( str ) >= 0 ) ;
}

/*
 * Get frequency of time value
 */
public static char getFrequency (String timeValue) throws VTLError {
    int len = timeValue.length();

    if ( len < 4 )
    	VTLError.RunTimeError( "Bad time_period: " + timeValue );

    if (len == 10)
        return 'D' ;
    
    if (len == 4)
        return 'A';
 
    switch (timeValue.charAt(4)) {
        case 'M':
        case 'Q':
        case 'S':
        case 'A':
        case 'W':
            return ( timeValue.charAt(4) ) ;
        default:
            VTLError.InternalError("Unknown time dimension value: " + timeValue);
        	return ' ' ;
    }
}

/*
 * TBD: check the time portion
 * if value contains T then the format is fixed
 * 2000-01-01 
 * 2000-01-01T12:00:00
 * 20000101
 * 20000101000000
 * mask used by load data file: YYYYMMDDHH24MISS
 */
static boolean isDate ( String value ) 
{
	String	strDay ;
	boolean	isDateTime ;
	int		month, year ;
	
	switch ( value.length() ) {
		case 10 : isDateTime = false ; break;
		case 19 : isDateTime = true  ; break ;
		default:
			return ( false ) ;
	}
	
	if ( value.charAt(4) != '-' || value.charAt(7) != '-' )
		return ( false ) ;
	
	month = Integer.parseInt( value.substring ( 5,7 ) ) ;
	strDay = value.substring ( 8, 10 ) ;
	
	if ( value.charAt(4) != '-' || value.charAt(7) != '-' || month < 1 || month > 12 || strDay.compareTo( "01" ) < 0 )
		return ( false ) ;
	
	if ( month == 2 ) {
		year = Integer.parseInt( value.substring ( 0, 4 ) ) ;
		if ( strDay.compareTo( (year % 4 == 0 && ( year % 100 != 0 || year % 1000 == 0) ) ? "29" : "28" ) > 0 ) 
			return ( false );
	}
	else
		if ( strDay.compareTo( (month == 4 || month == 6 || month == 9 || month == 11) ? "30" : "31" ) > 0 ) 
			return ( false );
	
	if ( isDateTime == false )		// only date, no time portion
		return ( true ) ;
	
	if ( value.charAt(10) != 'T' || value.charAt(13) != '-' || value.charAt(16) != '-' )
		return ( false ) ;

	String hour = value.substring( 11, 13 ) ;
	String min = value.substring( 14, 16 ) ;
	String sec = value.substring( 17, 20 ) ;

	return ( ( hour.compareTo( "00") >= 0 && hour.compareTo( "23") <= 0)
				&& ( min.compareTo( "00") >= 0 && min.compareTo( "59") <= 0)
				&& ( sec.compareTo( "00") >= 0 && sec.compareTo( "59") <= 0) ) ;
}

/*
 * Formats accepted:
	20000101
	20000101000000
	2000-01-01
	2000-01-01T00:00:00
 * the string returned is compatible with the format used by load data file:
		YYYYMMDDHH24MISS
 */
static String toDate ( String value ) 
{
	String	strDay ;
	boolean	withTime ;
	int		month, year ;
	
	switch ( value.length() ) {
		case 8 : 	
			value = value + "000000" ;
			withTime = false ; 
			break;
		case 14 : 	
			withTime = true ; 
			break;
		case 10 :
			// 2000-01-01
			if ( ! ( value.charAt(4) == '-' && value.charAt(7) == '-' ) )
				return ( null ) ;
			value = value.substring ( 0, 4 ) + value.substring ( 5, 7 ) + value.substring ( 8, 10 ) ;
			value = value + "000000" ;
			withTime = false ; 
			break;
		case 19 : 
			// 2000-01-01T00:00:00
			if ( ! ( value.charAt(4) == '-' && value.charAt(7) == '-' ) )
				return ( null ) ;
			if ( ! ( value.charAt(10) == 'T' && value.charAt(13) == ':' && value.charAt(16) == ':' ) )
				return ( null ) ;
			value = value.substring ( 0, 4 ) + value.substring ( 5, 7 ) + value.substring ( 8, 10 ) +
					value.substring ( 11, 13 ) + value.substring ( 14, 16 ) + value.substring ( 17, 19 ) ;
			withTime = true  ;
			break ;
		default: 
			return ( null ) ;
	}
	
	// 20000101000000
	month = Integer.parseInt( value.substring ( 4, 6 ) ) ;
	strDay = value.substring ( 6, 8 ) ;
	if ( month < 1 || month > 12 || strDay.compareTo( "01" ) < 0 )
		return ( null ) ;
	
	if ( month == 2 ) {
		year = Integer.parseInt( value.substring ( 0, 4 ) ) ;
		if ( strDay.compareTo( (year % 4 == 0 && ( year % 100 != 0 || year % 1000 == 0) ) ? "29" : "28" ) > 0 ) 
			return ( null );
	}
	else
		if ( strDay.compareTo( (month == 4 || month == 6 || month == 9 || month == 11) ? "30" : "31" ) > 0 ) 
			return ( null );
	
	if ( withTime == false )		// only date, no time portion
		return ( value ) ;
	
	// 20000101000000
	String hour = value.substring( 8, 10 ) ;
	String min = value.substring( 10, 12 ) ;
	String sec = value.substring( 12, 14 ) ;

	if ( ( hour.compareTo( "00") >= 0 && hour.compareTo( "23") <= 0)
				&& ( min.compareTo( "00") >= 0 && min.compareTo( "59") <= 0)
				&& ( sec.compareTo( "00") >= 0 && sec.compareTo( "59") <= 0) ) 
		return ( value );
	
	return (null );
}

/*
 * Formats :
	20000101
	20000101000000
	2000-01-01
	2000-01-01T00:00:00
 * the string returned is compatible with the format used by load data file:
		YYYY-MM-DD\THH24:MI:SS
static String toDateOld ( String value ) 
{
	String	strDay ;
	boolean	withTime ;
	int		month, year ;
	
	switch ( value.length() ) {
		case 8 : 
			value = value.substring ( 0, 4 ) + '-' + value.substring ( 4, 6 ) + '-' + value.substring ( 6, 8 ) ;
			withTime = false ; 
			break;
		case 14 : 
			value = value.substring ( 0, 4 ) + '-' + value.substring ( 4, 6 ) + '-' + value.substring ( 6, 8 )
				+ 'T' + value.substring ( 8, 10 ) + ':' + value.substring ( 10, 12 ) + ':' + value.substring ( 12, 14 ) ;
			withTime = true ; 
			break;
		case 10 : withTime = false ; break;
		case 19 : withTime = true  ; break ;
		default: return ( null ) ;
	}
	
	if ( ! ( ( value.charAt(4) == '-' ) && ( value.charAt(7) == '-' ) ) )
		return ( null ) ;
	
	if ( withTime && ! ( value.charAt(10) == 'T' && value.charAt(13) == ':' && value.charAt(16) == ':' ) )
		return ( null ) ;
	
	month = Integer.parseInt( value.substring ( 5, 7 ) ) ;
	strDay = value.substring ( 8, 10 ) ;
	
	if ( month < 1 || month > 12 || strDay.compareTo( "01" ) < 0 )
		return ( null ) ;
	
	if ( month == 2 ) {
		year = Integer.parseInt( value.substring ( 0, 4 ) ) ;
		if ( strDay.compareTo( (year % 4 == 0 && ( year % 100 != 0 || year % 1000 == 0) ) ? "29" : "28" ) > 0 ) 
			return ( null );
	}
	else
		if ( strDay.compareTo( (month == 4 || month == 6 || month == 9 || month == 11) ? "30" : "31" ) > 0 ) 
			return ( null );
	
	if ( withTime == false )		// only date, no time portion
		return ( value ) ;
	
	String hour = value.substring( 11, 13 ) ;
	String min = value.substring( 14, 16 ) ;
	String sec = value.substring( 17, 19 ) ;

	if ( ( hour.compareTo( "00") >= 0 && hour.compareTo( "23") <= 0)
				&& ( min.compareTo( "00") >= 0 && min.compareTo( "59") <= 0)
				&& ( sec.compareTo( "00") >= 0 && sec.compareTo( "59") <= 0) ) 
		return ( value );
	
	return (null );
}
*/
/*
 * Checks a value of predefined dimension time_period.
	2000
	2000-S1
	2000-S01
	2000-Q1
	2000-Q01
	2000-M01
	2000-W01
	20000101
	20000101000000
	2000-01-01
	2000-01-01T00:00:00
 */
static String toTimePeriod ( String value ) {
	String	year, subPeriod ;
	int		len ;
	char	frequency ;
	boolean	sdmx ;
  
	if ( value.length () < 4 )
		return ( null ) ;
  
	year = value.substring ( 0, 4 ) ;
	
	if ( ! ( year.compareTo( "1900" ) >= 0 && year.compareTo( "2999" ) <= 0 ) )
		return ( null ) ;
  
	if ( value.length () == 4 )
		return ( value ) ;

	if ( value.length () == 5 && value.charAt(4) == 'A' )
		return ( value ) ;

	if ( value.charAt ( 4 ) == '-' ) {
		frequency = value.charAt ( 5 ) ;
		subPeriod = value.substring ( 6 ) ;
		sdmx = true ;
	}
	else {
		frequency = value.charAt ( 4 ) ;
		subPeriod = value.substring ( 5 ) ;		
		sdmx = false ;
	}

	len = subPeriod.length() ;
	switch ( frequency ) {
	    case 'S' :
	    	if (len == 1 && subPeriod.compareTo( "1" ) >= 0 && subPeriod.compareTo( "2" ) <= 0 )
	    		return ( sdmx ? year + 'S' + subPeriod : value ) ;
	    	if (len == 2 && subPeriod.compareTo( "01" ) >= 0 && subPeriod.compareTo( "02" ) <= 0 )
	    		return ( year + 'S' + subPeriod.substring(1) ) ;
	    	break ;
	    case 'Q' :
	    	if (len == 1 && subPeriod.compareTo( "1" ) >= 0 && subPeriod.compareTo( "4" ) <= 0 )
	    		return ( sdmx ? year + 'Q' + subPeriod : value ) ;
	    	if (len == 2 && subPeriod.compareTo( "01" ) >= 0 && subPeriod.compareTo( "04" ) <= 0 )
	    		return ( year + 'Q' + subPeriod.substring(1) ) ;
	    	break ;
	    case 'W' :
	    	if (len == 2 && subPeriod.compareTo( "01" ) >= 0 && subPeriod.compareTo( "52" ) <= 0 )
	    		return ( sdmx ? year + 'W' + subPeriod : value ) ;
	    	break ;
	    case 'M' :
	    	if (len == 2 && subPeriod.compareTo( "01" ) >= 0 && subPeriod.compareTo( "12" ) <= 0 )
	    		return ( sdmx ? year + 'M' + subPeriod :value ) ;
	    	if ( len == 5 && value.charAt ( 2 ) == 'D' ) {		// 01M01
	    		return ( toDate (year + subPeriod.substring( 0, 2) + subPeriod.substring( 3, 5) ) ) ;
	    	}
	    	break ;
	    default :	
	    	return ( toDate (value ) ) ;
	}
	return ( null ) ;
}

/*
 * Checks a value of predefined dimension time_period.
 * SDMX style:
	2000
	2000-S1
	2000-Q1
	2000-M01
	2000-W01
	2000-01-01
	2000-01-01T00:00:00
 */
static boolean isTimePeriodSDMX ( String value ) {
	String	subPeriod ;
	int		len ;
	char	frequency ;

	len = value.length () ;
  
	if ( len < 4 )
		return ( false ) ;
  
	if ( ! Check.isPositiveInteger ( value.substring ( 0, 4 ) ) )
		return ( false ) ;
  
	if ( len == 4 )
		return ( true ) ;

	if ( value.charAt ( 4 ) != '-' )
		return ( false ) ;

	frequency = value.charAt ( 5 ) ;

	subPeriod = value.substring ( 6 ) ;

	switch ( frequency ) {
	    case 'S' :
	    	return (len == 7 && subPeriod.compareTo( "1" ) >= 0 && subPeriod.compareTo( "2" ) <= 0 );
	    case 'Q' :
	    	return (len == 7 && subPeriod.compareTo( "1" ) >= 0 && subPeriod.compareTo( "4" ) <= 0 ) ;
	    case 'M' :
	    	return (len == 8 && subPeriod.compareTo( "01" ) >= 0 && subPeriod.compareTo( "12" ) <= 0 ) ;
	    case 'W' :
	    	return (len == 8 && subPeriod.compareTo( "01" ) >= 0 && subPeriod.compareTo( "52" ) <= 0 ) ;
	    default :
	    	return ( isDate ( value ) ) ;
	}
}

/*
 * Checks a value of predefined dimension time_period.
 */
static boolean isTimePeriod ( String dim_value ) {
	String	subPeriod, day, month ;
	int		len, num_month, num_year ;
	char	frequency ;

	len = dim_value.length () ;
  
	if ( len < 4 )
		return ( false ) ;
  
	if ( ! Check.isPositiveInteger ( dim_value.substring ( 0, 4 ) ) )
		return ( false ) ;
  
	if ( len == 4 )
		return ( true ) ;

	frequency = dim_value.charAt ( 4 ) ;

	subPeriod = dim_value.substring ( 5 ) ;

	switch ( frequency ) {
	    case 'S' :
	    	return (len == 6 && subPeriod.compareTo( "1" ) >= 0 && subPeriod.compareTo( "2" ) <= 0 );
	
	    case 'Q' :
	    	return (len == 6 && subPeriod.compareTo( "1" ) >= 0 && subPeriod.compareTo( "4" ) <= 0 ) ;
	
	    case 'M' :
	    	if (len == 7 )
	    		return ( subPeriod.compareTo( "01" ) >= 0 && subPeriod.compareTo( "12" ) <= 0 ) ;
	    	else if ( len == 10 ) {
	    	  if ( dim_value . charAt ( 7 ) != 'D' )
	    		  return ( false ) ;
	    	  month = dim_value.substring ( 5,7 ) ;
	    	  if ( ! ( month . compareTo( "01" ) >= 0 && month . compareTo( "12" ) <= 0 ) )
	    		  return ( false ) ;
	    	  num_month = Integer.parseInt( month ) ;
	    	  day = dim_value.substring ( 8 ) ;
	    	  if ( day . compareTo( "01" ) < 0 )
	    		  return ( false ) ;
	    	  if ( num_month == 2 ) {
	    		  num_year = Integer.parseInt( dim_value . substring ( 0, 4 ) ) ;
	    		  if ( num_year % 4 == 0 && ( num_year % 100 != 0 || num_year % 1000 == 0) )
	    			  return ( day.compareTo( "29" ) <= 0 ) ;
	    		  else
	    			  return ( day.compareTo( "28" ) <= 0 ) ;
	    	  }
	    	  else if ( num_month == 4 || num_month == 6 || num_month == 9 || num_month == 11 )
	    		  return ( day.compareTo( "30" ) <= 0 ) ;
	    	  else
	    		  return ( day.compareTo( "31" ) <= 0 ) ;
	    	}
	    	else
	    		return ( false );
	    case 'W' :
	    	return (len == 8 && subPeriod.compareTo( "01" ) >= 0 && subPeriod.compareTo( "52" ) <= 0 ) ;
	}
  
	return ( false ) ;
}

/*
 * Build list of values (interval) for time period.
 */
static ListString buildTimePeriodInterval ( String v1, String v2 ) throws VTLError
{
	int			year, year1, year2, period, period1, period2, max_period ;
	String		time1, time2 ;
	ListString	ls ;
	char		frequency ;
	boolean		sort_desc ;
	String		frequencies = "ASQM" ;
	final int	max_periods[] = { 1, 2, 4, 12 } ;
	
	if ( v1.compareTo(v2) < 0 ) {
		time1 = v1 ;
		time2 = v2 ;
		sort_desc = false ;
	}
	else {
		time1 = v2 ;
		time2 = v1 ;
		sort_desc = true ;		
	}
	
	year1 = Integer.parseInt ( time1 .substring(0, 4) ) ;
	year2 = Integer.parseInt ( time2 .substring(0, 4) ) ;

	frequency = getFrequency ( time1) ; 
	
	if ( frequency != getFrequency ( time2) )
		VTLError . RunTimeError ( "time interval: " + v1 + " and " + v2 + " do not have the same frequency" ) ;
	
	if ( frequency == 'A' ) {
		period1 = 1 ;
		period2 = 1	;	// changed from 2 to 1
	}
	else {
		period1 = Integer.parseInt ( time1.substring( 5 ) ) ;
		period2 = Integer.parseInt ( time2.substring( 5 ) ) ;
	}
	
	max_period = max_periods[ frequencies.indexOf(frequency) ] ;
	
	ls = new ListString ( ) ;

	for ( year = year1; year <= year2; year ++ ) {
		if ( year == year2 )
			max_period = period2 ;
		for ( period = period1; period <= max_period; period ++ ) {
			if ( frequency == 'A' )
				ls . add ( year + "" ) ;
			else {
				if ( frequency == 'M' && period < 10 )
					ls . add ( Integer.toString( year ) + frequency + "0" + Integer.toString(period) ) ;
				else
					ls . add ( Integer.toString( year ) + frequency + Integer.toString(period) ) ;								
			}
		}
		period1 = 1 ;
	}
	
	if ( sort_desc )
		Collections . reverse ( ls ) ;
	
	return ( ls ) ;
}


/*
 * Compute time_period offset.
	"CASE length(" + t + ")"
	+ "WHEN 4 THEN TO_CHAR(SUBSTR(" + t + ",1,4)+" + num + ")"
		+ "WHEN 10 THEN TO_CHAR(TO_DATE(SUBSTR(" + t + ",1,4)||SUBSTR(" + t + ",6,2)||SUBSTR(" + t + ",9,2)"+ ",'YYYYMMDD')+" + num +",'YYYY-MM-DD')" 
			+ "ELSE TRUNC((SUBSTR(" + t + ",1,4)*" + ns + "+SUBSTR(" + t + ",6)+" + num + "-1)/" + ns + ")"
				+ " || SUBSTR(" + t + ",5,1) "
				+ " || TO_CHAR(mod ( ((SUBSTR(" + t + ",1,4)*12)+SUBSTR(" + t + ",6)+" + num + "-1)," + ns + ")+1,CASE WHEN SUBSTR(" + t + ",5,1)='M' THEN 'FM09' ELSE 'FM9' END)"
	+ " END " ;
 */
static String buildTimePeriodOffset( String t, int offset ) throws VTLError
{
	int			year, period ;
	String		res ;
	
	if ( ! Check.isTimePeriod(t) )
		VTLError.RunTimeError( "time_offset: " + t + " is not a valid time period" ) ;
	
	year = Integer.parseInt ( t.substring(0, 4) ) ;
	
	switch ( t.length() ) {
		case 4 :
			res = ( year + offset ) + "" ;
			break;
		case 10 :
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			Calendar c = Calendar.getInstance();
			c.setTime(new Date()); // Now use today date.
			c.add(Calendar.DATE, 5); // Adding 5 days
			res = sdf.format(c.getTime());
			break;
		default :
			period = Integer.parseInt ( t.substring( 5 ) ) ;
			int		subPeriods = 0 ;
			switch ( t.charAt(4)) {
				case 'M' : subPeriods = 12 ;
				case 'Q' : subPeriods = 4 ;
				case 'S' : subPeriods = 2 ;
				case 'W' : subPeriods = 52 ;
			}
			year = ( ( year * subPeriods ) + period + offset - 1 ) / subPeriods ;
			period = ( ( ( year * subPeriods ) + period + offset - 1 ) % subPeriods ) + 1 ;
			res = year + t.substring( 5 ) + period ;
	}

	return ( res ) ;
}

/*
 * Check whether data_value is a legal value of data_type. If not, generate exception.
 */
static void checkLegalValue ( String dataType, String value ) throws VTLError
{
	boolean		badValue = false ;
	ListString	dim_values ;

	if ( value == null )
		return ;
	
	// data_value = data_value.trim() ;
	
	switch ( dataType ) {
		case "string" :			// nothing
			break ;
		case "integer" :
			if ( value.length()==0 )
				badValue = true ;
			else if ( ! isInteger ( value )  )
				badValue = true ;
			break ;
		case "number" :
			if( value.length()==0 ) 
				badValue = true ;
			else if ( ! isNumber ( value ) )
				badValue = true ;
			break ;
		case "date" : 			// example: "2000-01-01" "2000-01-01T12:00:00"
			badValue = value.length() != 10 || ! isTimePeriod ( value ) ;  
			break ;
		case "boolean" : 			
			badValue = ! ( value.equals( "true" ) || value.equals( "false" ) ) ;
			break ;
		case "time_period" : 
		case "time" :				// time is used as a synonym of type_period 
			badValue = ! isTimePeriod ( value )  ;
			break ;
		case "duration" :				// time is used as a synonym of type_period 
			badValue = ! isDuration ( value )  ;
			break ;
		default :
			// user-defined (valuedomain)
			dim_values = Dataset.getValuedomainCodeList ( dataType ) ; 
			if ( dim_values.size () > 0 )
				badValue = ! dim_values.contains ( value ) ;
	}

	if ( badValue )
		VTLError.RunTimeError ( value + " is not a valid value for type " + dataType ) ;
}

/*
 * Check a set scalar.
 * data_type can be null (no check)
*/
static void checkLegalValues ( String dataType, ListString ls ) throws VTLError
{
	  String		bad_value = null ;

	  if ( dataType == null )
		  return ;	// no data type specified for this set

	  if ( dataType.equals ( "string" ) )
		  return ;	// nothing
	  
	  else if ( dataType . equals ( "integer" ) ) {
		  for ( String value : ls ) {
			  if ( ! isInteger ( value )  ) {
				  bad_value = value ;
				  break ;				  
			  }
		  }
	  }
	  else if ( dataType . equals ( "number" ) ) {
		  for ( String data_value : ls ) {
			  if ( ! isNumber ( data_value )  ){
				  bad_value = data_value ;
				  break ;				  
			  }		  
		  }
	  }
	  else if ( dataType . equals ( "date" ) ) {
	     ;	// to be done
	  }
	  else if ( Check.isPeriodDataType ( dataType ) ) {
		  for ( String data_value : ls ) {
			  if ( ! isTimePeriod ( data_value )  ){
				  bad_value = data_value ;
				  break ;				  
			  }
		  }
	  }
	  else {
		  ListString	dimValues = Dataset.getValuedomainCodeList ( dataType ) ; 
		  if ( dimValues.size () > 0 ) {
	    	 for ( String data_value : ls ) {
		    	 if ( dimValues.indexOf ( data_value ) < 0 ){
					  bad_value = data_value ;
					  break ;				  
				  }	
	    	 }
	     }
	  }

    if ( bad_value != null )
         VTLError . RunTimeError ( bad_value + " is not a valid value for data type " + dataType ) ;
}

/*
 * Is the current user the owner of object?
*/
static boolean isOwnerCurrentUser ( String object_name ) throws VTLError
{
	int		idx ;

	if ( object_name . indexOf ( "@" ) >= 0 )
		return ( false ) ;

	if ( ( idx = object_name.indexOf ( Parser.ownershipSymbol ) ) >= 0 ) {
		// return ( Db . db_username.toLowerCase ().compareTo ( object_name . substring ( 0, idx ) )  == 0 ) ;
		return ( Db . db_username.equalsIgnoreCase ( object_name . substring ( 0, idx ) ) ) ;

	}

	return ( true ) ;
}

/*
 * Check owner of object. Syntax:
 * 		[ owner. ] object_name [ @ dblink ]
 * used to check commands create/alter/drop/rename/grant/revoke
 * check that connection is not read-only.
 * Return object_name with no owner.
 */
static String checkObjectOwner ( String object_name ) throws VTLError
{
	int		idx ;

	if ( object_name.indexOf ( "@" ) >= 0 )
		VTLError.TypeError ("Remote objects cannot be created, modified, dropped or renamed") ;

	Db.checkConnectionNotReadOnly ( );

	if ( ( idx = object_name.indexOf ( Parser.ownershipSymbol ) ) >= 0 ) {
		if ( ! Db.db_username.toLowerCase ().equals ( object_name . substring ( 0, idx ) ) )
			VTLError.TypeError ( "You are not the owner of object " + object_name );
		return ( object_name.substring( idx + 1 )) ;		
	}
	
	return ( object_name ) ;
}

/*
 * Check if new object_name is a legal name.
 */
public static void checkNewObjectName ( String object_name ) throws VTLError
{
	if ( object_name.length() > 30 )
	    VTLError.RunTimeError ( "Create, rename or copy: name " + object_name + " is too long (30 characters max.)" ) ;
	
	if ( object_name.indexOf ( Parser.ownershipSymbol ) > 0 )
		VTLError.RunTimeError ( "Create, rename or copy: name " + object_name + " cannot contain the ownership symbol (\\)" ) ;
		
	if ( object_name.indexOf ( "@" ) >= 0 )
		VTLError.RunTimeError ( "Create, rename or copy: name " + object_name + " cannot contain a @" ) ;

	if ( VTLObject.object_exists ( object_name ) )
		VTLError.RunTimeError ( "Create, rename or copy: name " + object_name + " is already used by an existing object" ) ;
}

}
