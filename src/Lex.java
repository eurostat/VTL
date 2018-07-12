
class Lex {

/* 
 * Lexical analizer (scanner): split input string in tokens. The returned tokens are in the interface Tokens.
 */
static final String KeywordArray [] = 
{
	"and", 
	"or", 
	"xor", 
	"not", 
	"in", 
	"not_in", 
	"null" ,
	"+","-","*","/","=",">","<","#","<>",">=","<=","||","[","]","{","}",";",":=","(",")",",",".",":","<-", "\\", "_"
} ;

static final ListString Keywords = new ListString ( KeywordArray ) ;
static final String 	LegalSymbols = "+-*/=><.|(){};:=[],@:#\\_" ;
static final boolean 	isLegalSymbol ( char c ) { return LegalSymbols.indexOf ( c ) >= 0 ; } 

static String         	Buffer;                   	// input buffer: contains text to be interpreted
static String         	TokenString;  
static boolean        	Unscanned;
static int            	TokenCode;             		// token code
static int            	TokenStart;
static int            	TokenEnd;
static short 			LineNumber;

/*
 * Mark starting point of create user function, create view etc.
 */
static final int inputTextMarkStart ( )	
{ 
	return ( TokenEnd ) ; 
}

/*
 * Get input text, the starting point has been previously marked.
 */
static String inputTextGet ( int markStartText )	 
{
	if ( markStartText > TokenEnd )
		return ( "" ) ;
	return ( Buffer.substring ( markStartText, TokenEnd ).trim() ) ;
}

/*
 * Get input line number (displayed for error messages)
 */
static short getInputLineNumber ( ) 
{
	return ( LineNumber ) ;
}

/*
 * in case of syntax error, the position where token starts
 */
static int getInputTokenStart ( ) 
{
	return ( TokenStart ) ;
}

/*
 * in case of syntax error, the position where the token ends
 */
static int getInputTokenEnd ( ) 
{
	return ( TokenStart ) ;
}

/*
 * Initialize Scanner.
 */
static void initScan ( String inp ) 
{
	Unscanned = false;
	Buffer = inp;
	TokenString = "";
	TokenCode = -1 ;
	TokenStart = 0;
	TokenEnd = 0;
	LineNumber = 1 ;
}
	
/*
 * Read current character.
 */
static final char currChar() 
{
  	return ( Buffer.charAt ( TokenEnd ) ) ;
}

/*
 * Advance pointer to current character.
 */
static final boolean nextChar () 
{
	if ( TokenEnd < Buffer.length () )
		TokenEnd ++ ;
	return ( TokenEnd < Buffer.length () ) ;
}

/*
 * Skip input to the next new line (comment //).
 */
static final void skipInput ( ) 
{
	while ( currChar ( ) != '\n' && TokenEnd < ( Buffer.length () - 1 ) )
		TokenEnd ++ ;
}

/*
 *  // while ((c_char == ' ') || (c_char == '\t') || (c_char == '\r' ) || (c_char == '\n' ) || (c_char == '/' ) )
 */
static boolean endOfInput ( ) throws VTLError 
{
	char  c_char, next_char ;
	
	if ( Unscanned )
		return ( false ) ;

	if ( TokenEnd >= Buffer.length () )
		return ( true ) ;

	c_char = currChar ( ) ;
	
	while ( Character . isWhitespace ( c_char ) || (c_char == '/' ) ) {
		if ( c_char == '/' ) {
			if ( nextChar () ) {
				switch ( c_char = currChar () ) {
		            case '/' :
		            	while ( currChar ( ) != '\n' && TokenEnd < ( Buffer.length () - 1 ) )
		            		TokenEnd ++ ;
		            	break ;
	     
		            case '*' :
		            	c_char = ' ' ;
		            	while ( nextChar () ) {
		            		next_char = currChar () ;
		            		if ( c_char ==  '*' && next_char == '/' )
		            			break ;
		            		c_char = next_char ;
		            	}
		            	break ;

		            default :
		            	TokenEnd -- ;
	                    return ( false );
		         }
	        }
	        else {
	        	TokenEnd -- ;
	            return ( false );
	        }
		}
	    else {
	    	if ( c_char == '\n' )
	    		LineNumber ++ ;
		}
	    if ( nextChar () )
	        c_char = currChar ();
	    else
	   	  	return ( true ) ;
	    }

	return ( false ) ;
}

/*
 * Unget last token.
 */
static final void unscan () 
{
	Unscanned = true;
}

/*
 * Get token from input stream. Should return short instead of int.
 */
static int scan () throws VTLError 
{
	char  c_char ;
  
	if (Unscanned){
		Unscanned = false ;
		return ( TokenCode ) ;
	}

	if ( endOfInput ( ) )
		VTLError.SyntaxError ( "Found end of input" ) ;

	c_char = currChar ( ) ;

	TokenStart = TokenEnd ;
  
	if ( isLegalSymbol ( c_char ) ) {
		if ( nextChar () ) {
            char n_char = currChar () ;
            if (  ( c_char == '<' && n_char == '=' ) || ( c_char == '<' && n_char == '>' )
               || ( c_char == '>' && n_char == '=' ) || ( c_char == '|' && n_char == '|' ) 
               || ( c_char == ':' && n_char == '=' ) || ( c_char == '<' && n_char == '-' ) )
            	nextChar () ;
		}
		TokenString = Buffer.substring ( TokenStart, TokenEnd ) ;
		TokenCode = Keywords.indexOf ( TokenString ) ;
	    if ( TokenCode == -1 )
	    	VTLError.InternalError ( "Scan: token: " + TokenString ) ;
	}
	else if (c_char == '"' ) {
       TokenStart ++ ;
       nextChar ();
       c_char = currChar() ;

       while ( c_char != '"' ) {
           if ( nextChar ( ) ) {
               c_char = currChar();
               if ( c_char == '"' ) {
               		if ( nextChar ( ) && ( c_char = currChar() ) == '"' ) {
               			if ( ! nextChar ( ) )
               				break ;
               			c_char = currChar () ;
               		}
               		else
                     	 break ;
               }
               else if ( c_char == '\n' )
               			LineNumber ++ ;
               }
           else
        	   break; 
       }

       if ( TokenEnd >= Buffer.length () )
    	   VTLError . SyntaxError ( "Found end of input" ) ;
       if ( TokenStart == TokenEnd ) {
    	   TokenString = "" ;
    	   TokenEnd ++ ;
       }
       else
    	   TokenString = Buffer . substring ( TokenStart, TokenEnd - 1 ).replace( "\"\"", "\"" ) ;
       
       TokenCode = Tokens . Y_STRING ;
	}
	else if (c_char == '\'' ) {
      TokenStart ++ ;
      nextChar ();
      c_char = currChar() ;

      while ( c_char != '\'' ) {
          if ( nextChar ( ) ) {
              c_char = currChar();
              if ( c_char == '\'' ) {
              		if ( nextChar ( ) && ( c_char = currChar() ) == '\'' ) {
              			if ( ! nextChar ( ) )
              				break ;
              			c_char = currChar () ;
              		}
              		else
                    	 break ;
              }
              else if ( c_char == '\n' )
              			LineNumber ++ ;
              }
          else
       	   break; 
      }

      if ( TokenEnd >= Buffer.length () )
    	  VTLError . SyntaxError ( "Found end of input" ) ;
      if ( TokenStart == TokenEnd ) {
    	  TokenString = "" ;
    	  TokenEnd ++ ;
      }
      else
    	  TokenString = Buffer.substring ( TokenStart, TokenEnd - 1 ).replace( "\'\'", "\'" ) ;
      
      TokenCode = Tokens . Y_QUOTEDNAME ;
  	}
  	else if ( (c_char >= '0') && (c_char <= '9') ) {
  		while ( (c_char >= '0') && (c_char <= '9') )
    	 if ( nextChar () )
    		 c_char = currChar () ;
    	 else
    		 break ;

      if (c_char == '.') {
    	  if ( nextChar () ) {
             c_char = currChar () ;
             while ( (c_char >= '0') && (c_char <= '9') )
	           if ( nextChar () )
                      c_char = currChar () ;
	           else
	              break ;
	      }
          TokenCode = Tokens . Y_NUMBER ;
	  }
      else if ( ( c_char >= 'A' && c_char <= 'Z') || ((c_char >= 'a') && (c_char <= 'z')) || ( c_char == '_' ) ) {
            while ( ((c_char >= 'A') && (c_char <= 'Z'))
                    || ((c_char >= 'a') && (c_char <= 'z'))
                    || ((c_char >= '0') && (c_char <= '9'))
                    || ( c_char == '_' ) )
		      if ( nextChar () )
		         c_char = currChar () ;
		      else
		         break ;
	        TokenCode = Tokens.Y_POSITION ;
	  }
      else
    	  TokenCode = Tokens.Y_NUMBER ;
      TokenString = Buffer.substring ( TokenStart, TokenEnd ) ;
  	}
  	else if (((c_char >= 'A') && (c_char <= 'Z')) || ((c_char >= 'a') && (c_char <= 'z'))) {
       while ( ((c_char >= 'A') && (c_char <= 'Z')) 
	       || ((c_char >= 'a') && (c_char <= 'z'))
	       || ((c_char >= '0') && (c_char <= '9'))
	       || ( c_char == '_' ) ) {
           if ( ! nextChar () )
        	   break ;
           // session . debug ( "Scan loop: " + TokenStart + " " + TokenEnd + " " + BufferLength ) ;
           c_char = currChar();
       }
       
       // session . debug ( "TokenStart - Token End: " + TokenStart + " " + TokenEnd ) ;
       TokenString = Buffer . substring ( TokenStart, TokenEnd ) ;
       TokenCode = Keywords.indexOf( TokenString );
       if (TokenCode == -1)
    	   TokenCode = Tokens.Y_IDENTIFIER;
  	}
  	else {
  		TokenString = Buffer . substring ( TokenStart, TokenEnd ) ;
  		TokenCode = -1 ;
  		VTLError . SyntaxError ( "Illegal symbol: " + Buffer . substring ( TokenStart, TokenStart + 1 ) ) ;
  	}

	if ( TokenCode != Tokens.Y_STRING && TokenString.equals ( "" ) )
		VTLError . InternalError ( "Scan: tok: " + TokenCode + ", TokenStart: " + TokenStart + ", Buffer: " + Buffer ) ;

	return ( TokenCode ) ;
}

static String nextIdeKeyword() throws VTLError 
{
	Lex.scan() ;
	if ( TokenCode != Tokens.Y_IDENTIFIER )
		VTLError.SyntaxError( "Expected identifier");
	return ( Lex.TokenString ) ;	
}

static String nextTokenString() throws VTLError 
{
	Lex.scan() ;
	return ( Lex.TokenString ) ;	
}

static void syntaxError ( int tok_expected ) throws VTLError 
{
	String msg_found, msg_expected ;
		
	msg_found =  TokenString ; // + " (" + TokenCode + ")" ; 
	
	switch ( tok_expected ) {
	  	case Tokens . Y_IDENTIFIER :
	  		msg_expected = "Expected: identifier" ;
	  		break;
	  	case Tokens . Y_NUMBER :
	  		msg_expected = "Expected: number" ;
	  		break;
	  	case Tokens . Y_POSITION :
	  		msg_expected = "Expected: position" ;
	  		break;
	  	case Tokens . Y_STRING :
	  		msg_expected = "Expected: string" ;
	  		break;
	  	default :
	  	  if ( tok_expected >= 0 && tok_expected < Keywords.size() )
	          msg_expected = "Expected: " + Keywords.get( tok_expected ) ; // + " (" + tok_expected + ")" ; 
	  	  else
        	  msg_expected = "Internal error - token not found" ;  		
	}
	  
	VTLError.SyntaxError ( msg_expected.length() == 0 
					? "Found: " + msg_found 
					: msg_expected + "\nFound: " + msg_found ) ;
}

static void readToken ( int tok ) throws VTLError 
{
	if ( scan() != tok ) 
		syntaxError ( tok ) ;
}

static boolean readOptionalToken ( int tok ) throws VTLError 
{
	if (  scan() == tok ) 
		return ( true ) ;
  
	unscan () ;

	return ( false ) ;
}

static void readIdeKeyword ( String ide ) throws VTLError 
{ 
	scan() ;

	if ( ! ide . equals ( TokenString ) )
		VTLError . SyntaxError ( "Expected: 	" + ide + "\nFound: 	" + TokenString ) ;
}

static boolean readOptionalIdeKeyword ( String ide ) throws VTLError 
{
	scan () ;
	if ( TokenString.equals ( ide ) )
		return ( true ) ;
	
	unscan () ;
	return ( false ) ;
}

static String readIdentifier ( ) throws VTLError 
{
  if ( scan() != Tokens.Y_IDENTIFIER ) 
     syntaxError ( Tokens.Y_IDENTIFIER ) ;

  return ( TokenString ) ;
}

static String readNumber ( ) throws VTLError 
{
  if ( scan() != Tokens.Y_NUMBER ) 
     syntaxError ( Tokens.Y_IDENTIFIER ) ;

  return ( TokenString ) ;
}

static String readObjectName ( ) throws VTLError 
{
	String 	ide ;

	if ( scan () == Tokens.Y_QUOTEDNAME )
		ide = TokenString ;
	else {
		unscan () ;
		ide = readIdentifier ( ) ;
		if ( readOptionalToken ( Tokens.Y_BACKSLASH ) )  		// . in MDT
			ide = ide + "." + readIdentifier ( ) ;		
	}
  
	// if ( readOptionalToken ( Tokens . Y_POINT ) )  ide = ide + "." + readIdentifier ( ) ;	// VTL 
  
	/* if ( readOptionalToken ( Tokens . Y_AT ) ) {
	  ide = ide + "@" + readIdentifier () ;
	  while ( readOptionalToken ( Tokens . Y_POINT ) )		
	      ide = ide + "." + readIdentifier ( ) ;
  	} */
	  
	return ( ide ) ;
}

}
