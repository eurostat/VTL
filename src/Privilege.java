
public class Privilege {

static final	String	privileges [] = { "read", "update", "add_positions" };
static final	String	privCodes [] = { "R", "U", "P" };

/*
 * grant priv on obj to user
 */
static void grant ( String privilege, String object_name, ListString users_roles ) throws VTLError
{
	int			key_index ;
	String		priv ;
	String		with_grant_option ;
	Dataset		tab ;
	
	tab = Dataset.getDatasetDesc(object_name) ;
	Check . checkObjectOwner ( object_name ) ;

	key_index = Sys.getPosition(privileges, privilege) ;
	if ( key_index < 0 )
		VTLError.RunTimeError("Invalid privilege: " + privilege) ;
	priv = privCodes[key_index] ;
		
	for ( String user_name : users_roles ) {
		user_name = user_name.toUpperCase() ;
		
		if ( priv.equals( "R" ) ) {
			if (Db.sqlGetValue( "SELECT username FROM all_users WHERE username='" + user_name + "'" ) != null )
				with_grant_option = " WITH GRANT OPTION" ;
			else
				with_grant_option = "" ;
			Db.sqlExec ( "GRANT SELECT ON " + object_name + " TO " + user_name + with_grant_option ) ;		
		}

		if ( priv.equals( "U" )) {			
			Db.sqlExec ( "GRANT UPDATE,INSERT,DELETE ON " + object_name + " TO " + user_name ) ;		
		}
			
		if ( priv.equals( "P" ) ) {
			// P: add positions and labels therefore grant insert only - this is used in create new note in browse edit
			Db.sqlExec ( "GRANT INSERT ON " + object_name + " TO " + user_name ) ;	
		}

		Db.sqlExec ( "DELETE FROM " + Db.mdt_user_privileges + " WHERE object_id=" + tab.objectId 
			+ " AND grantee='" + user_name + "' AND privilege='" + priv + "'" ) ;
		
		Db.sqlExec ( "INSERT INTO " + Db.mdt_user_privileges + " (object_id,grantee,privilege) VALUES(" 
				+ tab.objectId + ",'" + user_name + "','" + priv + "')" ) ;
		
		Db.sqlCommit();
	}	
}

/*
 * revoke priv on obj from user
 */
static void revoke ( String privilege, String object_name, ListString users_roles ) throws VTLError
{
	int			key_index ;
	String		priv ;
	Dataset		tab ;
	
	tab = Dataset.getDatasetDesc(object_name) ;
	Check . checkObjectOwner ( object_name ) ;

	key_index = Sys.getPosition(privileges, privilege) ;
	if ( key_index < 0 )
		VTLError.RunTimeError("Invalid privilege: " + privilege) ;
	priv = privCodes[key_index] ;
	
	for ( String user_name : users_roles ) {
		user_name = user_name.toUpperCase() ;

		if ( priv.equals( "R" ) ) {
			Db.sqlExec ( "REVOKE SELECT ON " + object_name + " FROM " + user_name ) ;		
		}
		else if ( priv.equals( "U" ) ) {	
			if ( Db.sqlGetValue ( "SELECT privilege FROM " + Db.mdt_user_privileges + " WHERE object_id=" + tab.objectId
					+ " AND grantee='" + user_name + "' AND privilege='P'" ) != null ) {
				Db.sqlExec ( "REVOKE UPDATE,DELETE ON " + object_name + " FROM " + user_name ) ;		
			}
			else  {
				Db.sqlExec ( "REVOKE UPDATE,INSERT,DELETE ON " + object_name + " FROM " + user_name ) ;		
			}
		}
		else if ( priv.equals( "P" ) ) {
			// P: add positions and labels therefore grant insert only - this is used in create new note in browse edit
			Db.sqlExec ( "REVOKE INSERT ON " + object_name + " FROM " + user_name ) ;	
		}

		Db.sqlExec ( "DELETE FROM " + Db.mdt_user_privileges + " WHERE object_id=" + tab.objectId 
				+ " AND grantee='" + user_name + "' AND privilege='" + priv + "'" ) ;
		Db.sqlCommit() ;
	}
}

/*
 * Check privilege: update
 */
static void checkPrivilegeUpdate ( VTLObject obj ) throws VTLError
{
	String		tmp ;

	if ( ! Check.isOwnerCurrentUser ( obj.realObjectName ) ) {
		tmp = Db.sqlGetValue( "SELECT count(*) FROM " + Db.mdt_privileges 
				+ " WHERE object_id=" + obj.objectId 
				+ " AND privilege='U' AND ( grantee = USER OR grantee in ( SELECT role FROM session_roles ) ) ") ;
		if ( tmp.equals( "0") )
			VTLError.RunTimeError( "Privilege \"update\" not granted on " + obj.realObjectName ) ;						
	}
}

/*
 * Check privilege: add positions
 */
static void checkPrivilegeAddPositions( String object_name, int object_id) throws VTLError
{
	String		tmp ;

	if ( ! Check.isOwnerCurrentUser ( object_name ) ) {
		tmp = Db.sqlGetValue( "SELECT count(*) FROM " + Db.mdt_privileges 
				+ " WHERE object_id=" + object_id 
				+ " AND privilege='P' AND ( grantee = USER OR grantee in ( SELECT role FROM session_roles ) ) ") ;
		if ( tmp.equals( "0" ) )
			VTLError.RunTimeError( "Privilege \"add positions\" not granted on " + object_name ) ;						
	}
}

/*
 * Get users/roles to which privilege on object has been granted 
 */
static ListString getGrantedPrivileges ( String object_name, String privilege ) throws VTLError
{
	VTLObject	obj ;
	String		priv, owner ;
	int			key_index, idx ;
	ListString 	granted_privs ;

	key_index = Sys.getPosition(privileges, privilege) ;
	if ( key_index < 0 )
		VTLError.RunTimeError("Invalid privilege: " + privilege) ;
	priv = privCodes[key_index] ;

	if ( ( obj = VTLObject.getObjectDesc( object_name ) ) == null )
		VTLError . RunTimeError ( "Object " + object_name + " does not exist" ) ;

	if ( priv.equals( "R" ) ) {
		if ( ( idx = object_name.indexOf( Parser.ownershipSymbol ) ) > 0 ) {
			owner = object_name.substring( 0, idx ).toUpperCase() ;
			object_name = object_name.substring( idx + 1 ).toUpperCase() ;
		}
		else {
			owner = Db.db_username.toUpperCase() ;
			object_name = object_name.toUpperCase() ;
		}

		granted_privs = Db.sqlFillArray( "SELECT grantee FROM all_tab_privs WHERE table_schema='" + owner
							+ "' and table_name='" + object_name + "' AND privilege='SELECT'" ) ;

	}
	else if ( priv.equals( "U" ) || priv.equals( "P" ) ) {
		granted_privs = Db.sqlFillArray( "SELECT grantee FROM " + Db.mdt_privileges 
				+ " WHERE object_id=" + obj.objectId + " AND privilege='" + priv + "'" ) ;		
	}
	else
		granted_privs = null ;
	
	//Sys.println("Privilege: " + privilege + " " + object_name + " " + granted_privs.toString()) ;
	
	return ( granted_privs ) ;
}

/*
Set default select privilege to newly created table/view.
ORACLE rules:

	- you cannot grant access on something to a role "with grant option"
	- to create a view, you need a "grant option" on the base tables granted 
	  directly (not through a role)
	- if user bopfdi wants to create a view bop1 on table bop of user1:
	
		table		bop1 created by user1
		view		bop2 created by user2
		
	user1:
	
		create table bop1 ;
		grant select on bop1 to user2 with grant option ;
		grant select on bop1 to role_x ;
		
	user2:
		create view bop2 as select * from user1 . bop1 ;
		grant select on bop2 to role_x ;
		
	
	result: all users in role_x can view the table and the view.
*/
static void grant_select ( String sqlTableName ) throws VTLError
{  
	Db.sqlGrantSelect ( sqlTableName, UserProfile.defaultSelectPrivUser, UserProfile.defaultSelectPrivRole ) ;
}

// end of class

}
