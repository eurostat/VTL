
/*
 * List of tokens returned by the lexical scanner (keywords, identifiers, numbers, strings, etc.).
 */

interface Tokens {
static final short  
Y_AND 			=  0 , // and
Y_OR 			=  1 , // or
Y_XOR			=  2 , // xor
Y_NOT 			=  3 , // not
Y_IN 			=  4 , // in
Y_NOT_IN 		=  5 , // not_in
Y_NULL	 		=  6 , // null
Y_PLUS	   		=  7 , // +
Y_SUBTRACT		=  8 , // -
Y_MULTIPLY		=  9 , // *
Y_DIVIDE		= 10 , // /
Y_EQUAL 		= 11 , // =
Y_GT			= 12 , // >
Y_LT			= 13 , // <
Y_SHARP			= 14 , // #
Y_N_EQUAL		= 15 , // <>
Y_GT_EQUAL 		= 16 , // >=
Y_LT_EQUAL		= 17 , // <=
Y_CONCAT		= 18 , // ||
Y_BRACKET_OPEN	= 19 , // [
Y_BRACKET_CLOSE	= 20 , // ]
Y_BRACE_OPEN	= 21 , // {
Y_BRACE_CLOSE	= 22 , // }
Y_SEMICOLON 	= 23 , // ;
Y_TEMP_ASSIGNMENT	= 24 , // :=
Y_PAR_OPEN		= 25 , // (
Y_PAR_CLOSE 	= 26 , // )
Y_COMMA	    	= 27 , // ,
Y_POINT	   		= 28 , // .
Y_COLON			= 29 , // :
Y_PERS_ASSIGNMENT	= 30 , // <-
Y_BACKSLASH		= 31 , // \ (schema selection
Y_UNDERSCORE	= 32 , // _ (
Y_NUMBER    	= 33 ,
Y_STRING    	= 34 , 
Y_IDENTIFIER	= 35 , 
Y_QUOTEDNAME	= 36 , // ' ... '
Y_POSITION  	= 37 ;
}
