import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Component;
import java.awt.Font;
import java.awt.Rectangle;
import java.awt.Toolkit;
import java.awt.datatransfer.Clipboard;
import java.awt.datatransfer.StringSelection;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.ComponentAdapter;
import java.awt.event.ComponentEvent;
import java.awt.event.KeyEvent;
import java.awt.event.KeyListener;
import java.awt.event.MouseEvent;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.text.DecimalFormat;

import javax.swing.AbstractAction;
import javax.swing.ActionMap;
import javax.swing.InputMap;
import javax.swing.JButton;
import javax.swing.JComponent;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.JTextArea;
import javax.swing.JTextPane;
import javax.swing.JToolBar;
import javax.swing.KeyStroke;
import javax.swing.UIManager;
import javax.swing.event.DocumentEvent;
import javax.swing.event.UndoableEditEvent;
import javax.swing.event.UndoableEditListener;
import javax.swing.plaf.FontUIResource;
import javax.swing.table.DefaultTableCellRenderer;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.TableCellRenderer;
import javax.swing.table.TableColumn;
import javax.swing.text.AbstractDocument;
import javax.swing.text.BadLocationException;
import javax.swing.text.DefaultHighlighter;
import javax.swing.text.Highlighter;
import javax.swing.text.SimpleAttributeSet;
import javax.swing.text.StyleConstants;
import javax.swing.text.StyledDocument;
import javax.swing.undo.UndoManager;
import javax.swing.undo.UndoableEdit;

public class UIConsole extends JFrame  
{
private static final long serialVersionUID = 1L;

static JTextPane 	prompt;
static JScrollPane	promptScrollPane ;
static JTextArea 	statusLine ;
static JPanel 		thisPanel ;
static JTable 		dataTable ;
static JScrollPane 	dataTableScrollPane ;
static JFrame 		frameInfo ;				// info window

/*
 * Show textual info using a multiline dialog with scrollbar.
 */
static public void showInfo ( String title, String text ) 
{
    JTextArea textArea = new JTextArea( );
    textArea.setLineWrap(true);
    textArea.setWrapStyleWord(true);
    textArea.setFont(textArea.getFont().deriveFont(20f));
    textArea.setText( text );
    textArea.setCaretPosition(0);
    
    frameInfo = new JFrame();
	frameInfo.setTitle( title );
    frameInfo.add( new JScrollPane(textArea) );
    frameInfo.setSize(900, 600);
    frameInfo.setLocationRelativeTo(null);
    frameInfo.setVisible(true);      
    // bind escape keystroke to close the window
	InputMap im = frameInfo.getRootPane().getInputMap(JComponent.WHEN_IN_FOCUSED_WINDOW);
	ActionMap am = frameInfo.getRootPane().getActionMap();
	im.put(KeyStroke.getKeyStroke(KeyEvent.VK_ESCAPE, 0), "cancel");
	am.put("cancel", new AbstractAction() {
		private static final long serialVersionUID = 1L;
		@Override
	    public void actionPerformed(ActionEvent e) {
			frameInfo.setVisible(false); 
	    }
	}) ;
}

/*
 * Show messagebox
 */
static void messageBox(String titleBar,String infoMessage )
{
    JOptionPane.showMessageDialog(null, infoMessage, titleBar, JOptionPane.INFORMATION_MESSAGE);
    prompt.requestFocus();
}

/*
 * Show messagebox
 */
static int confirmBox(String titleBar, String infoMessage )
{
	int res = JOptionPane.showConfirmDialog(null, titleBar, infoMessage, JOptionPane.YES_NO_CANCEL_OPTION);
	return ( res == JOptionPane.CANCEL_OPTION ? -1 : res == JOptionPane.YES_OPTION ? 1 : 0 ) ;
}

static int DecimalPlaces = 1 ;
static DecimalFormat DecFormat = new DecimalFormat(".0") ;  
static final DecimalFormat IntFormat = new DecimalFormat("0") ;

/*
 * 	Set n. of decimals
*/
static void executeDecimals ( )
{
	final String decimal_places [] = { "0", "1", "2", "3", "4", "5", "6", "7", "8", "9" } ;
    String var_value = chooseFromList( "Choose option", "Decimal places", decimal_places ) ;
    if ( var_value != null ) {
    	DecimalPlaces = Integer.parseInt( var_value ) ;	
    	if ( DecimalPlaces == 0 )
    		DecFormat = IntFormat ;
    	else 
    		DecFormat = new DecimalFormat(".000000000000000".substring(0, 1 + DecimalPlaces));  
    }
}

static String lastSqlQuery = null ;

static DefaultTableModel model ;

static void resetTable ()
{
	model.setRowCount(0);
	model.setColumnCount(0);
}

static class DecimalFormatRenderer extends DefaultTableCellRenderer {
    
	private static final long serialVersionUID = 1L;
	// private static final DecimalFormat formatter = new DecimalFormat( "#.00" );

    public Component getTableCellRendererComponent( JTable table, Object value, boolean isSelected, 
    												boolean hasFocus, int row, int column) {
    	// format the cell value as required
    	if ( ( sqlTypes [ column ] == java.sql.Types.NUMERIC || sqlTypes [ column ] == java.sql.Types.INTEGER ) && value instanceof Number ) {
        	if ( Command.getLastQueryColumnIsInteger(column))
        		value = IntFormat.format( (Number)value ) ;
        	else
        		value = DecFormat.format( (Number)value ) ;    		
    	}

    	/*if ( sqlTypes [ column ] == java.sql.Types.NUMERIC && value instanceof Number )
    		value = DecFormat.format( (Number)value ) ;
    	else if ( sqlTypes [ column ] == java.sql.Types.INTEGER && value instanceof Number )
    		value = IntFormat.format( (Number)value ) ; // (Number)value);*/
       // pass it on to parent class
       return super.getTableCellRendererComponent( table, value, isSelected, hasFocus, row, column );
    }
}

static 	int			sqlTypes [] ;

static void executeQuery ( String sqlQuery ) throws VTLError
{
	Object[] 	row ;
	int			n_columns ;
	String		headers [] ;
	// messageBox ( "Execute query", "Open" ) ;
	// statusLine.setText( "************************" ) ;
	// statusLine.paintImmediately(statusLine.getX(),statusLine.getY(),statusLine.getWidth(),statusLine.getHeight() );

	resetTable();
	SqlDataWindow	dw = SqlDataWindow.open( sqlQuery ) ;
	// messageBox ( "Execute query", "After open" ) ;

	DefaultTableCellRenderer rightRenderer = new DecimalFormatRenderer();	// new DefaultTableCellRenderer
	rightRenderer.setHorizontalAlignment(JLabel.RIGHT);
	rightRenderer.setToolTipText( "No info" );
	headers = dw.getHeaders() ;
	sqlTypes = dw.getSqlTypes() ;
	n_columns = headers.length ;
	for ( int colIndex = 0; colIndex < headers.length; colIndex ++ )  {
		if ( Command.getLastQueryCompName ( colIndex ).length() > 0 )
			model.addColumn( Command.getLastQueryCompName ( colIndex ) ) ;
		else 
			model.addColumn( headers [ colIndex ].toLowerCase() ) ;
	}
		
	// str.substring (0, 1) + str.substring (1).toLowerCase() );	
	
	// setBackgroundColor ( n_columns ) ;
	
	for ( int idx = 0; idx < n_columns; idx ++ ) {
		if ( sqlTypes [ idx ] == java.sql.Types.INTEGER || sqlTypes [ idx ] == java.sql.Types.NUMERIC ) {
			dataTable.getColumnModel().getColumn(idx).setCellRenderer(rightRenderer);
		}
	}

	// messageBox ( "Execute query", "Start retrieving data" ) ;
	while ( ( row = dw.retrieveRow () ) != null ) {
		model.addRow ( row ) ;
	}
	// messageBox ( "Execute query", "End" ) ;

	// if ( dw.getRowsRetrieved() % 1000 == 0 ) statusLine.setText( dw.getRowsRetrieved() + " rows" ) ;
	// does not work: repaint
}

/*
 * Implements right trim
 */
static String rtrim ( String s) {
    int i = s.length()-1;
    while (i >= 0 && Character.isWhitespace(s.charAt(i))) {
        i--;
    }
    return s.substring( 0, i+1 );
}

/*
 * 
 */
static void executeRun ( String cmd )
{	
	long	elapsed_time_start ;
	try {		
		elapsed_time_start = System.currentTimeMillis ( ) ;
    	lastSqlQuery = InterpreterInterface.eval ( cmd ) ;
		if ( lastSqlQuery == null ) {
			statusLine.setText( "" ) ;
			resetTable() ;
			return ;
		}
		executeQuery ( lastSqlQuery ) ;
		statusLine.setText( "Elapsed time: " + ( System.currentTimeMillis ( ) - elapsed_time_start ) + " ms (" + model.getRowCount() + " rows)" );
	} catch ( VTLError e ) {
		String 	functionName = e.getErrorFunctionName() ;
		String 	title, msg ;
		short	errorLineNumber = e.getErrorLineNumber() ;
		if ( functionName == null || functionName.equals( "Top level" ) ) {
			title =  e.getErrorType() + ( errorLineNumber > 0 ? " at line " + errorLineNumber : "" );
			msg = e.getErrorMessage () ;
			if ( e.getErrorTokenStart () >= 0 && e.getErrorTokenStart () < prompt.getText().length() ) {
				try {
					prompt.setCaretPosition(e.getErrorTokenStart ());
					prompt.moveCaretPosition(e.getErrorTokenEnd ());
					//prompt.getHighlighter().addHighlight(tokenStart, e.getErrorTokenEnd (), DefaultHighlighter.DefaultPainter);
				}
				catch (Exception exc ) { 
				}
			}
		}
		else {
			title = e.getErrorType() ; // + " in function " + functionName + " at line " + e.getErrorLineNumber() ;
			msg = e.getErrorMessage () ;		
		}
		model.setRowCount(0);
		model.setColumnCount(0);
		if ( msg.length() < 100 )
			messageBox ( title,msg) ;
		else
			showInfo( title, msg ); 
	}
}

// static AttributeSet aset = StyleContext.getDefaultStyleContext().
// addAttribute(SimpleAttributeSet.EMPTY, StyleConstants.Foreground, Color.BLUE) ;
static void colorText ( StyledDocument 	d, String text, String start, String end, SimpleAttributeSet at )
{
	int			strIndex, fromIndex, endIndex ;
	fromIndex = 0 ;
	do {
		if ( ( strIndex = text.indexOf( start, fromIndex) ) >= 0 ) {
			if ( (endIndex = text.indexOf( end, strIndex + 1 ) ) >= 0 ) {
				d.setCharacterAttributes(strIndex, 1 + endIndex - strIndex, at, true);
				fromIndex = endIndex + 1 ;				
			}
			else 
				strIndex = -1 ;
		}
	} 
	while ( strIndex >= 0 ) ;
}

static SimpleAttributeSet attrBoldRed ;
static SimpleAttributeSet attrBoldBlue ;
static SimpleAttributeSet attrBoldGray ;
static SimpleAttributeSet attrNormal ;

static final String[] keywords = { "(", ")", "[", "]", "{", "}", "+", "-", "*", "/", "#", 
								",", ";", "||", "=", "<>", ">", ">=", "<", "<=", ":=", "<-", ":", "\\",
								"and", "or", "xor", "not", "in", "not_in", "if", "then", "else",
								"filter", "calc", "keep", "drop", "rename", "pivot", "unpivot", "sub", "aggr" 
								};

static void setKeywordColor( ) {
	StyledDocument 	d = prompt.getStyledDocument();
	int				strIndex, fromIndex ;
    String 			text = prompt.getText() ;
    d.setCharacterAttributes(0, text.length(), attrNormal, true);
    text = text.replaceAll( "\r\n", "\n" ) ;	// apparently text contains double end-of-line character
    d.setCharacterAttributes(0, text.length(), attrNormal, true);
	for ( String keyw : keywords ) {
		fromIndex = 0 ;
    	do {
    		if ( ( strIndex = text.indexOf(keyw, fromIndex) ) >= 0 ) {
    			if ( Character.isLetter( keyw.charAt(0) ) ) {
    				if ( strIndex == 0 || text.substring(strIndex -1, strIndex).equals( " " ) 
        					&& (strIndex + keyw.length() + 1) <= text.length() && text.substring(strIndex + keyw.length(), strIndex + keyw.length() + 1).equals( " " ))
        			d.setCharacterAttributes(strIndex, keyw.length(), attrBoldRed, true);
    			}
    			else
        			d.setCharacterAttributes(strIndex, keyw.length(), attrBoldRed, true);
    			fromIndex = strIndex + keyw.length() ;
    		}
    	} while ( strIndex >= 0 ) ;
    }

	colorText ( d, text, "\"", "\"", attrBoldBlue ) ;
	colorText ( d, text, "//", "\n", attrBoldGray ) ;
	colorText ( d, text, "/*", "*/", attrBoldGray ) ;

	// colorText ( d, text, "'", "'", attrBoldBlue ) ;
}

static void executeClipboard( ) 
{
    int numRows = dataTable.getRowCount();
    int numCols = dataTable.getColumnCount();
    
    StringBuffer str=new StringBuffer(); 
    
    for ( int x = 0; x < numCols; x ++ ) {
    	if ( x > 0 )
    		str.append('	');
    	str.append( dataTable.getColumnName(x) ) ;
    }
    str.append("\r\n"); 
    for (int i=0; i<numRows; i++) { 
            for (int j=0; j<numCols; j++) { 
            	if ( dataTable.getValueAt( i , j ) != null )
                    str.append(dataTable.getValueAt( i , j )); 
                if (j<numCols-1) { 
                	str.append('	'); 
                } 
            } 
            str.append("\r\n"); 
    } 
    final Clipboard clipboard = Toolkit.getDefaultToolkit().getSystemClipboard(); 

    StringSelection sel  = new StringSelection(str.toString()); 
    clipboard.setContents(sel, sel); 	
}

static void executeHistory()
{
	final String ch [] = { "Current session", "Past sessions", "Last statement" } ;
    String 	choice = chooseFromList( "Choose history", "Objects", ch ) ;
    String	cmd ;
    if (choice == null)
    	return;
    if ( choice.equals("Current session")) {
        StringBuffer	withQuery = new StringBuffer() ;
        int		idx = 1 ;

        if ( History.getHistoryOfCommands ( ).size() == 0 )
        	return ;

        for ( History.LocalHistory h : History.getHistoryOfCommands ( ) ) {
        	withQuery.append( ( withQuery.length() == 0 ? "WITH TMP$1 AS (" : " UNION " ) 
						+ "SELECT " + idx++ + " AS History_id," 
						+ Db.sqlQuoteString( h.statement ) + " AS Statement," 
						+ h.elapsedTime + " AS Elapsed_time FROM DUAL" );
        }	
        withQuery.append( ") SELECT s.session_id, s.time_logon, t.history_id, t.statement,t.elapsed_time FROM TMP$1 t CROSS JOIN " + Db.mdt_user_sessions 
						+ " s WHERE s.session_id=(SELECT max (session_id) FROM " + Db.mdt_user_sessions + ")" ) ;
        cmd = withQuery.toString() ;
    }
    else if ( choice.equals("Last statement")){
        try {
			cmd = Db.sqlGetValue("SELECT statement FROM " + Db.mdt_user_history + " h JOIN " + Db.mdt_user_sessions 
					+ " s ON ( h.session_id=s.session_id ) ORDER BY s.time_logon DESC" ) ;
			prompt.setText(cmd) ;
			UIConsole.setKeywordColor();
		} catch (VTLError e) {
			messageBox ( "Error", e.getErrorMessage()) ;
		}    	
        return ;
    }
    else {
        cmd = "SELECT s.session_id, s.time_logon, history_id, statement,elapsed_time FROM " 
    			+ Db.mdt_user_history + " h JOIN " + Db.mdt_user_sessions 
    			+ " s ON ( h.session_id=s.session_id ) ORDER BY s.time_logon DESC" ;    	
    }
    
    try {
    	Command.setLastQueryToNull();
    	executeQuery( cmd ) ; 
    }
    catch ( VTLError e ) {
    	messageBox ( "Error", e.getErrorMessage()) ;
    }
}

static void executeFindObject () 
{
	final String oTypes [] = 
		{ "Datapoint ruleset", "Hierarchical ruleset", "User defined operator or function", 
				"Valuedomain", "Dataset", "View" } ;
    String 	choice = chooseFromList( "Choose type of object", "", oTypes ) ;
    String	tmp, syntax ;
    char	oType = ' ' ;
    
    if ( choice == null )
    	return ;
    
    switch ( choice ) {
		case "Datapoint ruleset" : oType = VTLObject.O_DATAPOINT_RULESET ;break ;
		case "Hierarchical ruleset" : oType =VTLObject.O_HIERARCHICAL_RULESET ;break ;
		case "User defined operator or function" : oType = VTLObject.O_OPERATOR ; break ;
		case "Valuedomain" : oType = VTLObject.O_VALUEDOMAIN ; break ;
		case "Dataset" : oType = VTLObject.O_DATASET ; break ;
		case "View" : oType = VTLObject.O_VIEW ; break ;
	}
    try {
		if ( ( tmp = chooseObject( choice, oType ) ) == null )
			return ;
		syntax = VTLObject.objectDefinition ( tmp, oType, true ) ;
		showInfo( "Show definition", syntax ); 
    } catch ( VTLError e ) {
    	messageBox( "Error", e.getErrorMessage () );
    }
}

/*
 * Resize event - resize the window.
 */
class resizeListener extends ComponentAdapter {
    public void componentResized(ComponentEvent e) {
    	int h ;
    	Rectangle r = thisPanel.getBounds() ;
    	h = (r.height / 2) - 50 ;
    	// messageBox ( "ee",e.paramString());
    	prompt.setBounds( 10, 10, r.width - 20, h ) ;
    	promptScrollPane.setBounds( 10, 10, r.width - 20, h ) ;
    	dataTable.setBounds(10, h + 20, r.width - 20, r.height - ( h + 40 ) );
    	dataTableScrollPane.setBounds(10, h+20, r.width - 20, r.height - ( h + 40 ) ) ; //r.height - 250 );
    	statusLine.setBounds(10, r.height - 20, r.width - 20, 100 );
    }
}

/*
static String OutputForm = "Flat table" ;
static void changeOutputForm ( ) {
	final String outputForms [] = { "Flat table", "Time series", "Multidimensional" } ;
    String 	choice = chooseFromList( "Choose type of output form", "", outputForms ) ;
    if ( choice != null )
    	OutputForm = choice ;
}*/

/*
 * Create button
 */
static JButton createButton ( JToolBar toolbar, String title, String toolTipText, ActionListener actionL )
{
	JButton button ;
	button = new JButton(title);
	button.setToolTipText( toolTipText );
	button.setFont(button.getFont().deriveFont(16f));
	button.setBackground(Color.WHITE);
	button.setBorderPainted(false);
	button.addActionListener( actionL ) ;
    toolbar.add(button);
    // toolbar.addSeparator() ;
	return ( button );
}

/*
 * Build console.
 */
public UIConsole(String title)
{	
    super(title);
    
    UIManager.put("ToolTip.font", new FontUIResource("SansSerif", Font.PLAIN, 18));

	thisPanel = new JPanel();
    thisPanel.setLayout(null);

    // prompt field and scrollbars
    prompt = new JTextPane() ; 									// JTextField();
    prompt.setFont(prompt.getFont().deriveFont(22f));    
    thisPanel.add(prompt);
    promptScrollPane = new JScrollPane(prompt);
    promptScrollPane.setVerticalScrollBarPolicy(JScrollPane.VERTICAL_SCROLLBAR_AS_NEEDED);
    thisPanel.add( promptScrollPane) ;
    
    final UndoManager undo = new UndoManager();
    prompt.getDocument().addUndoableEditListener( new UndoableEditListener() {  
	   public void undoableEditHappened(UndoableEditEvent evt) {
		   //  Check for an attribute change
		   AbstractDocument.DefaultDocumentEvent event = (AbstractDocument.DefaultDocumentEvent)evt.getEdit();
		   if (! ( event.getType().equals(DocumentEvent.EventType.CHANGE)) )
			   undo.addEdit(event); // super.undoableEditHappened(evt);
	   }
    } ) ;
    prompt.getActionMap().put( "Undo", new AbstractAction("Undo") {
	   private static final long serialVersionUID = 1L;
	   public void actionPerformed(ActionEvent evt) {
		   if (undo.canUndo())
			   undo.undo();
	   }
    } ) ;
    prompt.getInputMap().put(KeyStroke.getKeyStroke("control Z"), "Undo");

    // keyword highlighting
    prompt.addKeyListener( new KeyListener() {
    	@Override 
    	public void keyPressed(KeyEvent e) {
        	UIConsole.setKeywordColor();
    	}
    	@Override
    	public void keyReleased(KeyEvent arg0) { UIConsole.setKeywordColor() ; }
    	@Override
    	public void keyTyped(KeyEvent arg0) { UIConsole.setKeywordColor() ; }
    } ) ;
    	   
    // output table
	model = new DefaultTableModel(); 
	dataTable = new JTable(model) {
		private static final long serialVersionUID = 1L;
		@Override
      	public Component prepareRenderer(TableCellRenderer renderer, int row, int column) {
         	Component component = super.prepareRenderer(renderer, row, column);
         	int rendererWidth = component.getPreferredSize().width;
         	TableColumn tableColumn = getColumnModel().getColumn(column);
         	tableColumn.setPreferredWidth(Math.max(rendererWidth + getIntercellSpacing().width + 120, 
        		   							tableColumn.getPreferredWidth()));
         	// if ( column == 1 ) // <= Command.getLastQueryNumDims () ) component.setBackground(Color.lightGray);
         	
         	return component;
    	}
		public String getToolTipText(MouseEvent e) {
            String tip = null;
            java.awt.Point p = e.getPoint();
            int rowIndex = rowAtPoint(p);
            int colIndex = columnAtPoint(p);

            try {
                // tip = getValueAt(rowIndex, colIndex).toString();
                switch ( Command.getLastQueryCompRole ( colIndex ) ) {
	    			case Command.ComponentType.COMP_DIMENSION : tip = "Dimension: " ; break ;
	    			case Command.ComponentType.COMP_MEASURE : tip = "Measure: " ; break ;
	    			case Command.ComponentType.COMP_ATTRIBUTE : tip = "Attribute: " ; break ;
	    			case Command.ComponentType.COMP_UNKNOWN : tip = "" ; break ;
                	}
            	tip = tip + dataTable.getColumnName(colIndex) ;
            	if ( true ) {
            		try {
            			if ( getValueAt(rowIndex, colIndex) != null )
            				tip = "<html>" + tip + "<br>"  + getLabel ( dataTable.getColumnName(colIndex).toLowerCase(), 
            				getValueAt(rowIndex, colIndex).toString() ) + "</html>" ;
            		} catch ( VTLError a ) {
            			// tip = "ERROR" ;
            		}
            	}
            } catch (RuntimeException e1) {
            	tip = rowIndex  + " " + colIndex ; // (//catch null pointer exception if mouse is over an empty line
            }

            return ( tip ) ;
        }

	} ;
	dataTable.setAutoResizeMode(JTable.AUTO_RESIZE_OFF);

    dataTable.getTableHeader().setFont(prompt.getFont().deriveFont(20f));
    dataTable.setFont(prompt.getFont().deriveFont(20f)); 
    dataTable.setRowHeight( 30 );
    thisPanel.add( dataTable ) ;

    // scrollbars
    dataTableScrollPane = new JScrollPane(dataTable);
    dataTableScrollPane.setVerticalScrollBarPolicy(JScrollPane.VERTICAL_SCROLLBAR_AS_NEEDED);
    dataTableScrollPane.setHorizontalScrollBarPolicy(JScrollPane.HORIZONTAL_SCROLLBAR_ALWAYS);  
    thisPanel.add( dataTableScrollPane ) ;
	
    // toolbar
    JToolBar toolbar = new JToolBar();
    toolbar.setBackground(Color.WHITE);
    add(toolbar, BorderLayout.NORTH);

    // button: Run
    createButton ( toolbar, "Run", "Execute the VTL program",
    	new ActionListener() {
        public void actionPerformed(ActionEvent buttonEvent) {
        	// resetTable() ;
        	String text = rtrim( prompt.getText() );
    		if ( text.length() > 0 ) {
    			//if ( UIConsole.OutputForm.equals("Time series" ) ) {text = "(" + text + ") [pivot time,obs_value]" ;}
    			if ( ! text.endsWith(";"))
    				text = text + "\n;" ;	// \n added to avoid end of input in "expr // ;"
    			executeRun ( text ) ;
    		}
    		else 
    			resetTable() ;
    		
        } 
    } ) ;

    // button: Find object, show definition
    createButton ( toolbar, "Find", "Find object",
    	new ActionListener() {
        public void actionPerformed(ActionEvent buttonEvent) {
        	executeFindObject () ;
        } 
    } ) ;
    
    // history of commands
    createButton ( toolbar, "History", "Show history of commands",
    	new ActionListener() {
        public void actionPerformed(ActionEvent buttonEvent) {
        	executeHistory() ;
        } 
    } ) ;

    // Copy data to clipboard
    createButton ( toolbar, "Clipboard", "Copy data to cliboard",
    	new ActionListener() {
        public void actionPerformed(ActionEvent buttonEvent) {
        executeClipboard( ) ;
        } 
    } ) ;

    // button: Info on last query
    createButton ( toolbar, "Info", "Info on last query: SQL query, Syntax tree, Data structure",
    	new ActionListener() {
        public void actionPerformed(ActionEvent buttonEvent) {
        	showInfo( "Last SQL query", 
        			"SQL query:\n" + ( lastSqlQuery == null ? "" : lastSqlQuery ) 
        				+ "\n\n\nSyntax tree:\n" + Command.lastSyntaxTree
        				+ "\n\nData structure:\n" + Command.getLastQueryStructure () 
        				+ "\n\nReferenced datasets:\n" + Command.getLastQueryReferencedTables() );
        } 
    } ) ;

    // button: Decimal places
    createButton ( toolbar, ".00", "Change number of decimals digits displayed",
    	new ActionListener() {
        public void actionPerformed(ActionEvent buttonEvent) {
        	executeDecimals ( ) ;
        } 
    } ) ;
    
    // button: Export all
    createButton ( toolbar, "Export all", "Export all objects",
    	new ActionListener() {
        public void actionPerformed(ActionEvent buttonEvent) {
        	try { 
        		String tmp = JOptionPane.showInputDialog( "Enter file name: " );
        		int choice = confirmBox ( "Export data?", "Export all objects" ) ;
        		if ( choice < 0 )
        			return ;
        		InterpreterInterface.exportAll ( tmp, choice > 0 ) ; 
        	}
        	catch ( Exception e ) { 
        		messageBox ( "Error", e.toString() ) ; 
        	} 
        } 
    } ) ;

    // button: Choose label
    createButton ( toolbar, "Label", "Choose label (description of a valuedomain item)" ,
    new ActionListener() {
        public void actionPerformed(ActionEvent buttonEvent) {
        	executeChooseLabel ( ) ;
        } 
    } ) ;

    // button: Wizard Control
    createButton ( toolbar, "Wizard-Statement", "Syntax: Statements" ,
    new ActionListener() {
        public void actionPerformed(ActionEvent buttonEvent) {
        	executeWizard ( "Statement" ) ;
        } 
    } ) ;

    // button: Wizard Definition Language
    createButton ( toolbar, "Wizard-DL", "Syntax: Definition Language" ,
    new ActionListener() {
        public void actionPerformed(ActionEvent buttonEvent) {
        	executeWizard ( "DL") ;
        } 
    } ) ;

    // button: Wizard DML
    createButton ( toolbar, "Wizard-ML", "Syntax: Manipulation Language" ,
    new ActionListener() {
        public void actionPerformed(ActionEvent buttonEvent) {
        	executeWizard ( "Expression") ;
        } 
    } ) ;

    /*// button: Wizard DML
    createButton ( toolbar, "Output", "Change output form" ,
    new ActionListener() {
        public void actionPerformed(ActionEvent buttonEvent) {
        	changeOutputForm ( ) ;
        } 
    } ) ;*/

    // status line
    statusLine = new JTextArea() ; // JTextField();
    statusLine.setEditable( false );
    statusLine.setBackground(thisPanel.getBackground());
    thisPanel.add(statusLine);

    thisPanel.addComponentListener(new resizeListener());
    
    getContentPane().add(thisPanel);

    // setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE) ; // EXIT_ON_CLOSE);
    setDefaultCloseOperation(JFrame.DO_NOTHING_ON_CLOSE);
    
    addWindowListener( new WindowAdapter()
    {
        public void windowClosing(WindowEvent we)
        {
            JFrame frame = (JFrame)we.getSource();
     
            int result = JOptionPane.YES_OPTION ;
            // int result = JOptionPane.showConfirmDialog( frame, "Exit?", "Exit Application", JOptionPane.YES_NO_OPTION);
     
            if (result == JOptionPane.YES_OPTION) {
                frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
                try {
                	Db.disconnectDb() ;
                }
                catch ( VTLError e ){
                	messageBox ( "Error disconnecting from database", e.getErrorMessage() ) ;
                }
            }
        }
    });

    toolbar.setVisible(true);
    toolbar.setMargin(null);

    setVisible(true);
    setSize(1120, 900 ) ; // 392, 400);
    setLocationRelativeTo(null);
    prompt.requestFocus();
    
    attrBoldRed = new SimpleAttributeSet() ;
    //attrBoldRed.addAttribute(StyleConstants.CharacterConstants.Bold, true);
    attrBoldRed.addAttribute(StyleConstants.CharacterConstants.Foreground, new Color(153,0,0)); //Color.RED);
    
    attrBoldBlue = new SimpleAttributeSet();
    attrBoldBlue.addAttribute(StyleConstants.CharacterConstants.Foreground, Color.BLUE);

    attrBoldGray = new SimpleAttributeSet();
    attrBoldGray.addAttribute(StyleConstants.CharacterConstants.Foreground, Color.GRAY );

    attrNormal = new SimpleAttributeSet() ;
    /*
    JMenuItem menuItemDebug;
    JMenu jMenu1;
    JMenuBar jMenuBar1;
    jMenuBar1 = new javax.swing.JMenuBar();
    jMenu1 = new javax.swing.JMenu();
    menuItemDebug = new javax.swing.JMenuItem();
    setDefaultCloseOperation(javax.swing.WindowConstants.EXIT_ON_CLOSE);
    jMenu1.setText("File");
    menuItemDebug.setText("Debugging");
    menuItemDebug.setToolTipText("Show SQL query (debugging)");
    menuItemDebug.addActionListener(new java.awt.event.ActionListener() {
        @Override
        public void actionPerformed(java.awt.event.ActionEvent evt) {
        	UIMultilineDialog.showMultilineDialog( "Debugging" , "Last SQL query", 
        	"SQL query: \r\n" + sqlQuery + "\r\n\r\nSyntax tree:\r\n" + Command.lastSyntaxTree );
        }
    });
    jMenu1.add(menuItemDebug);
    jMenuBar1.add(jMenu1);
    setJMenuBar(jMenuBar1);
    ImageIcon icon = new ImageIcon("exit.png");
    JButton exitButton = new JButton(icon);
    toolbar.add( new JButton ( "load", UIManager.getIcon("FileView.directoryIcon") )) ;
   	*/
}

// this is the method called from the original application
public static void setConsole(final String title) 
{
	new UIConsole(title);
}

static String chooseFromList ( String title, ListString ls )
{
	if ( ls.size() == 0 )
		return ( null ) ;
	return ( UIListDialog.showDialog( title, "Wizard", ls.toArray() ) ) ;	
}

static String chooseFromList ( String title, String subTitle, String arr[] )
{
	if ( arr.length == 0 )
		return ( null ) ;
	return ( UIListDialog.showDialog( subTitle, title, arr ) ) ;	
}

static String[] chooseFromListMulti ( String title, ListString ls )
{
	if ( ls.size() == 0 )
		return ( null ) ;
	int[] indexes = UIListDialog.showDialogMultiSelection ( title, "Multiple choice", ls.toArray() ) ;
	String[] values = new String[indexes.length] ;
	
	for ( int idx = 0; idx < indexes.length; idx ++ )
		values [idx] = ls.get( indexes[idx]) ;
	return ( values ) ;	
}

static ListString chooseFromListMultiLs ( String title, ListString ls )
{
	if ( ls.size() == 0 )
		return ( null ) ;
	int[] indexes = UIListDialog.showDialogMultiSelection( title, title, ls.toArray() ) ;
	ListString values = new ListString () ;
	
	for ( int idx = 0; idx < indexes.length; idx ++ )
		values.add( ls.get( indexes[idx] ) ) ;
	return ( values ) ;	
}

static String lastDataset, lastDimension, lastVariable  ;

/*
 * Start wizard with "DataDefinitionLanguage" or "Expression"
 */
static void executeWizard ( String choice )
{
	prompt.setText(choice + " ");		// keep the space at the end
	prompt.select( 0, choice.length() );  

	try {
		lastDataset = lastDimension = lastVariable = null ;
		wizard ( ) ;	
		prompt.requestFocus();
	}
	catch ( VTLError e ) {
		messageBox( "Error", e.getErrorMessage () );
	}
}

/*
 * Ignore objects dropped (in the recycle bin).
 */
static String chooseObject ( String stringType, String owner, char oType )
{
	ListString	ls ;
	String		tmp = null ;
	try {
		if ( owner == null )
			ls = Db.sqlFillArray( "SELECT object_name FROM " + Db.mdt_user_objects 
					+ " WHERE object_type='" + oType +"' AND drop_time IS NULL ORDER BY object_name" ) ;		
		else
			ls = Db.sqlFillArray( "SELECT object_name FROM " + Db.mdt_objects 
					+ " WHERE object_type='" + oType +"' AND user_name='" + owner.toUpperCase() 
					+ "' AND drop_time IS NULL ORDER BY object_name" ) ;		
			
		ls.add( "(Change schema)") ;
		tmp = chooseFromList( "Choose " + stringType + ":", ls) ;
		if ( tmp != null && tmp.equals("(Change schema)")) {
			ls = Db.sqlFillArray( "SELECT DISTINCT LOWER(user_name) FROM " + Db.mdt_objects + " ORDER BY 1" ) ;
			tmp = chooseFromList( "Choose schema", ls) ;
			if ( tmp != null ) {
				return ( chooseObject ( stringType, tmp, oType )) ;		
			}
		}
	}
	catch  (VTLError e ){
		messageBox ("Error", e.getErrorMessage()) ;
	}
	if ( owner != null && tmp != null )
		tmp = owner + "\\" + tmp ;
	
	return ( tmp ) ;
}

static String chooseObject ( String stringType, char oType )
{
	return ( chooseObject ( stringType, null, oType )) ;
}

static String DisplayLabel = "label_en" ;

//this is the method called from the original application
static void executeChooseLabel( ) 
{
	ListString 	ls = InterpreterInterface.executeSelectLabels ( ) ;
	if ( ls.size() == 0 )
		messageBox ( "Label", "Cannot find any measure of valuedomains") ;
	else {
		String tmp = chooseFromList ( "Choose label", ls ) ;
		if ( tmp != null )
			DisplayLabel = tmp ;
	}
}

/*
 * get label (description) of code from valuedomain measure label_en (should be parameterised)
 */
static String	currentVariable = "" ;
static ListString	currentVariableCodes = null ;
static ListString	currentVariableLabels = null ;
static String getLabel ( String variable, String item ) throws VTLError
{
	int		idx ;
	
	if ( ! variable.equals( currentVariable ) ) {
		currentVariable = variable ;
		currentVariableCodes = Db.sqlFillArray("SELECT " + currentVariable + " FROM " 
							+ currentVariable + " ORDER BY " + currentVariable );
		currentVariableLabels = Db.sqlFillArray("SELECT " + DisplayLabel
							+ " FROM " + currentVariable + " ORDER BY " + currentVariable );
	}
	
	if ( (idx = currentVariableCodes.indexOf( item ) ) >= 0 )
		return ( item + "    " + currentVariableLabels.get( idx ) ) ;
	// label not found
	return ( item ) ;
}

/*
 * Choose value of dimension
 * sort asc ? or leave it in the given order
 */
static String chooseDimensionValue ( String dim_name, ListString dim_values ) throws VTLError
{
	String	str ;
	if ( dim_values.size() > 0 ) {
		ListString	ls = new ListString() ;
		for ( String s : dim_values.sort_asc() )
			ls.add( getLabel ( dim_name, s ) ) ;
		str = chooseFromList ( "Choose value of " + dim_name, ls ) ; // ds.get_dimension(0).dim_values.sort_asc() ) ;	
		int idx ;
		if ( ( str != null ) && (idx = str.indexOf( " " )) > 0 )
			str = str.substring(0, idx) ;					
	}
	else
		str = JOptionPane.showInputDialog( "Value of " + dim_name + ":");
	
	return ( '"' + str + '"' ) ;
}

static String syntaxPredefinedElements ( String op ) throws VTLError
{
	String	str = null ;

	switch ( op ) {
		case "DatasetName" :
			lastDataset = str = chooseObject( op, VTLObject.O_DATASET );
			break ;
		case "Dimension" :
			if ( lastDataset != null ) {
				Dataset ds = Dataset.getDatasetDesc( lastDataset ) ;
			    str = chooseFromList( "Choose dimension", ds.listDimensions() );
			    lastDimension = str ;
			}
			break;
		case "DimensionList" :
			if ( lastDataset != null ) {
				Dataset ds = Dataset.getDatasetDesc( lastDataset ) ;
				ListString ls = chooseFromListMultiLs ( "Choose dimension list", ds.listDimensions() ) ;
				str = ls.toString(',') ;
			}
			break;
		case "OrderByList" :
			if ( lastDataset != null ) {
				Dataset ds = Dataset.getDatasetDesc( lastDataset ) ;
				ListString ls = chooseFromListMultiLs ( "Choose dimension list", ds.listDimensions() ) ;
				str = ls.toString(',') ;
			}
			break;
		case "DimensionValue" :
			if ( lastDataset != null && lastDimension != null ) {
				Dataset ds = Dataset.getDatasetDesc( lastDataset ) ;
				DatasetComponent dim = ds.getDimension(lastDimension) ;
				str = chooseDimensionValue ( dim.compName, dim.dim_values ) ;
			}
			break ;
		case "IdentifierMeasureAttribute" :
			if ( lastDataset != null ) {
				Dataset ds = Dataset.getDatasetDesc( lastDataset ) ;
				str = chooseFromList( "Choose identifier, measure or attribute", ds.listAllComponents() );
			}
			break ;
		case "MeasureAttribute" :
			if ( lastDataset != null ) {
				Dataset ds = Dataset.getDatasetDesc( lastDataset ) ;
				String arr[] = new String[ ds.getMeasures().size() + ds.getAttributes().size() ] ; 
				for ( int x = 0; x < ds.getMeasures().size(); x++ )
					arr[x] = ds.getMeasures().get(x).compName ;
				for ( int x = 0; x < ds.getAttributes().size(); x++ )
					arr[x+ds.getMeasures().size()] = ds.getAttributes().get(x).compName ;
			    str = chooseFromList( "Choose measure or attribute", "Wizard" + op, arr );
			}
			break;

		case "Variable" :
		case "ValueDomain" :
			lastVariable = str = chooseObject( op, VTLObject.O_VALUEDOMAIN );
			break ;
			
		case "DatapointRuleset" :
			str = chooseObject( op, VTLObject.O_DATAPOINT_RULESET) ;
			break ;
		case "HierarchicalRuleset" :
			str = chooseObject( op, VTLObject.O_HIERARCHICAL_RULESET ) ;		
			break ;
		case "UserDefinedOperator" :
			str = chooseObject( op, VTLObject.O_OPERATOR ) ;		
			break ;
		case "HLeftSide" :
			str = null ;
			if ( lastVariable != null ) {
				Dataset ds = Dataset.getDatasetDesc( lastVariable ) ;
				DatasetComponent dim = ds.get_dimension (0) ;
				str = chooseDimensionValue ( dim.compName, dim.dim_values ) ;
			}
			break ;
		case "HRightSide" :
			if ( lastVariable != null ) {
				String[] vals ;
				StringBuffer b = new StringBuffer();
				Dataset ds = Dataset.getDatasetDesc( lastVariable ) ;
				if ( ds.get_dimension(0).dim_values.size() > 0 ) {
					// sort asc ? or leave it in the given order
					ListString		ls = new ListString() ;
					DatasetComponent	dim = ds.get_dimension(0) ;
					for ( String s : dim.dim_values.sort_asc() )
						ls.add( getLabel ( dim.compName, s ) ) ;
					vals = chooseFromListMulti ( "Choose values of dimension", ls ) ; // ds.get_dimension(0).dim_values.sort_asc() ) ;	

					int idx ;
					for ( String v : vals ) {
						if ( (idx = v.indexOf( " " )) > 0 )
							v = v.substring(0, idx) ;
						if (b.length() > 0)
							b.append( " + " );
						b.append( v ) ; // ( '"' ).append( v ).append( '"' ) ;
					}
					str = b.toString() ;
				}
			}
			break ;

		default :
			str = JOptionPane.showInputDialog( op + ":");
			if ( str != null && op.equals("StringConstant") )
				str = '"' + str + '"' ;
	}
	return ( str ) ;
}
	
static void wizard (  ) throws VTLError
{
	ListString	ls = new ListString() ;
	String		op = prompt.getSelectedText() ;
	String		tmp ;
	int 		start = -1, end ;
	Highlighter highLighter  = prompt.getHighlighter();
	
	while ( op != null && op.length() > 0 ) {
		ls.setSize(0);
		for ( int x = 0; x < syntax.length; x ++ ) {
			if ( syntax[x][0].equals(op) )
				ls.add( syntax[x][1]) ;
		}
		if ( ls.size() == 0 )
			break ;
		
		if ( ls.size() == 1 ) {
			tmp = ls.get(0) ;
			if ( tmp.startsWith( "?_ " ) ) {	// expression can be _ or empty (last token)
				ls.removeAllElements() ;
				ls.add( tmp.substring( 3 ) ) ;	// replaceFirst works on regular expressions
				ls.add( "Empty" ) ;
				ls.add( ", _" ) ;
			}
			else if ( tmp.startsWith( "_ " ) ) {	// expression can be _ (when not last token)
				ls.removeAllElements() ;
				ls.add( tmp.substring( 2 ) ) ;	// replaceFirst works on regular expressions
				ls.add( "_" ) ;
			}
			else if ( tmp.startsWith( "? " ) ) {		// expression can be empty
				ls.removeAllElements() ;
				ls.add( tmp.substring( 2 ) ) ;	// replaceFirst works on regular expressions
				ls.add( "Empty" ) ;
			}
			else if ( tmp.equals ("*") )
				op = syntaxPredefinedElements ( op ) ;
			else 
				op = tmp ;
		}
		
		if ( ls.size() > 1 ) {
			if ( op.equals ("Statement") || op.equals ("DL") ) {
				if ( VTLMain.vtlOnly ) {
					ListString lsVtlOnly = new ListString ( ) ;
					for ( String s : ls ) {
						if ( ! s.startsWith("(+)" ) )
							lsVtlOnly.add( s ) ;
					}
					ls = lsVtlOnly ;
				}
				else
					op = op + " (NB: (+) indicates a VTL extension)" ;				
			}
			op = chooseFromList( "Choose: " + op, ls );
		    if ( op == null )
				break;			
		}
		if ( op == null )
			break ;
		
		if ( op.startsWith( "(+) " ) )
			op = op.substring( 4 ) ;			// delete "(+) " - used to mark a non-VTL operator
		
		if ( op.equals("Empty") )
			op = "" ;

		prompt.replaceSelection(op);
		UIConsole.setKeywordColor();

		int minIdx = 10000, idx ;
		for ( int x = 0; x < syntax.length; x ++ ) {
			if ( ( ( idx = prompt.getText().indexOf( syntax[x][0], start ) ) >= 0 ) && idx < minIdx )
					minIdx = idx ; 
		}
			
		start = minIdx ; // prompt.getText().indexOf( "", start + 1 ) ;
		end = prompt.getText().indexOf( " ", start + 1 ) ;
		if ( start < 10000 ) { // >= 0 ) {
			prompt.select(start, end );		// before the space
			try {
				highLighter.addHighlight(start, end, DefaultHighlighter.DefaultPainter);
			} 
			catch (BadLocationException ble) {
            }
		}
		op = prompt.getSelectedText() ;
	}
	
	highLighter.removeAllHighlights( );
}

/*
 * Syntax specifications.
 */
static final String[] syntax [] = {

	{ "Expression", "DatasetName" },
	{ "Expression", "TemporaryDatasetName" },
	{ "Expression", "ParameterName" },
	{ "Expression", "Expression # IdentifierMeasureAttribute" },
	{ "Expression", "Expression [ ClauseOperator ]" } ,		
	{ "Expression", "JoinExpression" } ,
	{ "Expression", "Number" } ,		
	{ "Expression", "String" },
	{ "Expression", "Boolean" } ,
	{ "Expression", "ValidationOperator" } ,
	{ "Expression", "HierarchicalOperator" } ,
	{ "Expression", "AggregateOperator" } ,
	{ "Expression", "AnalyticOperator" } ,
	{ "Expression", "SetOperator" } ,
	{ "Expression", "TimeOperator" } ,
	{ "Expression", "UserDefinedOperator ( ExpressionList )" },
	{ "Expression", "current_date()" },
	{ "Expression", "null" },
	{ "Expression", "nvl ( Expression , Expression )" } ,
	{ "Expression", "if Boolean then Expression else Expression" } ,
	{ "Expression", "cast ( Expression , BasicScalarType , Format )" } ,
	{ "Expression", "eval ( ExpressionList ) language StringConstant returns DataType )" } ,
	{ "Expression", "( Expression )" },

	{ "JoinExpression", "inner_join ( JoinDatasets UsingDimList JoinClauses )" } ,
	{ "JoinExpression", "left_join ( JoinDatasets UsingDimList JoinClauses )" } ,
	{ "JoinExpression", "full_join ( JoinDatasets UsingDimList JoinClauses )" } ,
	{ "JoinExpression", "cross_join ( JoinDatasets JoinClauses )" } ,
	{ "JoinClauses", "FilterClause CalcClause AggrClause ApplyClause KeepClause DropClause RenameClause" } ,
	{ "FilterClause", "? filter Boolean" } ,
	{ "CalcClause", "? calc Calculations" } ,
	{ "AggrClause", "? aggr Aggregations GroupingClause HavingClause" } ,
	{ "ApplyClause", "? apply Expression" } ,	
	{ "KeepClause", "? keep MeasureAttributeList" } ,
	{ "DropClause", "? drop MeasureAttributeList" } ,		
	{ "RenameClause", "? rename RenameList" } ,				

	{ "MeasureAttributeList", "[ MeasureAttribute MeasureAttributeListOptional ]" } ,
	{ "MeasureAttributeListOptional", "? , MeasureAttributeList )" } ,
	{ "RenameList", "MeasureAttribute to MeasureAttribute RenameListOptional" } ,
	{ "RenameListOptional", "? , RenameList )" } ,

	{ "ClauseOperator", "sub Subspace" },
	{ "ClauseOperator", "calc Calculations" } ,
	{ "ClauseOperator", "aggr Aggregations GroupingClause HavingClause" } ,
	{ "ClauseOperator", "filter ComponentBoolean" } ,
	{ "ClauseOperator", "keep MeasureAttributeList" } ,
	{ "ClauseOperator", "drop MeasureAttributeList" } ,		
	{ "ClauseOperator", "rename RenameList" } ,		
	{ "ClauseOperator", "pivot Dimension , MeasureAttribute" } ,		
	{ "ClauseOperator", "unpivot Dimension , MeasureAttribute" } ,		
	{ "Subspace", "Dimension = DimensionValue SubscriptList" },
	{ "SubscriptList", "? , Subspace" },
	
	{ "Calculations", "ComponentRoleCalc ComponentName := ComponentExpression CalculationsOptional" } ,
	{ "CalculationsOptional", "? , Calculations )" } ,

	{ "ComponentExpression", "ParameterName" },
	{ "ComponentExpression", "IdentifierMeasureAttribute" },
	{ "ComponentExpression", "Expression # IdentifierMeasureAttribute" },
	{ "ComponentExpression", "ComponentNumber" },
	{ "ComponentExpression", "ComponentString" },
	{ "ComponentExpression", "ComponentBoolean" },
	{ "ComponentExpression", "ComponentAnalyticOperator" } ,
	{ "ComponentExpression", "UserDefinedOperator ( ComponentExpressionList )" },
	{ "ComponentExpression", "current_date()" },
	{ "ComponentExpression", "null" },
	{ "ComponentExpression", "nvl ( ComponentExpression , ComponentExpression )" } ,
	{ "ComponentExpression", "if ComponentBoolean then ComponentExpression else ComponentExpression" } ,
	{ "ComponentExpression", "cast ( ComponentExpression , BasicScalarType , Format )" } ,
	{ "ComponentExpression", "eval ( ComponentExpressionList ) language StringConstant returns DataType )" } ,
	{ "ComponentExpression", "( ComponentExpression )" },
	
	{ "ComponentExpressionList", "? Expression ComponentExpressionListOptional" } ,
	{ "ComponentExpressionListOptional", "? , ComponentExpressionList" } ,

	{ "ComponentRoleCalc", "measure" },
	{ "ComponentRoleCalc", "attribute" },
	{ "ComponentRoleCalc", "viral attribute" },
	{ "ComponentRoleCalc", "identifer" },
	{ "ComponentRoleCalc", "Empty" },

	{ "Aggregations", "ComponentRoleAggr ComponentName := AggrOperator AggregationsOptional" } ,
	{ "AggregationsOptional", "? , Aggregations )" } ,

	{ "ComponentRoleAggr", "measure" },
	{ "ComponentRoleAggr", "attribute" },
	{ "ComponentRoleAggr", "viral attribute" },
	{ "ComponentRoleAggr", "Empty" },
	
	// aggregate functions
	{ "AggrOperator" , "avg ( Expression ) " 		} ,
	{ "AggrOperator" , "count ( Expression )" 		} ,
	{ "AggrOperator" , "count ( )" 					} , // in a having clause
	{ "AggrOperator" , "max ( Expression )"			} ,
	{ "AggrOperator" , "median ( Expression )"		} ,
	{ "AggrOperator" , "min ( Expression )" 		} ,
	{ "AggrOperator" , "stddev ( Expression )"		} ,
	{ "AggrOperator" , "stddev_pop ( Expression )"	} ,
	{ "AggrOperator" , "stddev_samp ( Expression )"	} ,
	{ "AggrOperator" , "sum ( Expression )" 		} ,
	{ "AggrOperator" , "variance ( Expression )"	} ,
	{ "AggrOperator" , "var_pop ( Expression )"		} ,
	{ "AggrOperator" , "var_samp ( Expression )" 	} ,

	{ "JoinDatasets", "DatasetName DatasetAlias DatasetNameOptional" } ,
	{ "DatasetNameOptional", "? , JoinDatasets" } ,
	{ "DatasetAlias", "? as AliasName"} ,

	{ "UsingDimList", "? using DimensionList" } ,

	{ "Format", "String" },
	
	{ "ValidationOperator", "check ( Expression , Boolean CheckErrorcode CheckErrorlevel CheckImbalance CheckOutput )" } ,
	{ "CheckErrorcode", "? Expression" },
	{ "CheckErrorlevel", "? Expression" },
	{ "CheckImbalance", "? Expression" },
	{ "CheckOutput", "all" },
	{ "CheckOutput", "invalid" },
	{ "CheckOutput", "Empty" },

	{ "Validation", "check_datapoint ( Expression , DatapointRuleset CheckDatapointComponents CheckDatapointOutput )" } ,
	{ "CheckDatapointComponents", "condition ListComponents" },
	{ "CheckDatapointComponents", "Empty" },
	{ "ListComponents", "? Variable ListComponents" },
	{ "CheckDatapointOutput", "invalid" },
	{ "CheckDatapointOutput", "all" },
	{ "CheckDatapointOutput", "all_measures" },
	{ "CheckDatapointOutput", "Empty" },

	{ "Validation", "check_hierarchy ( Expression , HierarchicalRuleset CheckHierarchyComponents RuleDimension HierarchyMode CheckHierarchyInput CheckHierarchyOutput )" } ,
	{ "CheckHierarchyComponents", "condition ListComponents" },
	{ "CheckHierarchyComponents", "Empty" },
	{ "ListComponents", "? Variable ListComponents" },
	{ "RuleDimension", "? rule Dimension" },		
	{ "CheckHierarchyInput", "dataset" },
	{ "CheckHierarchyInput", "dataset_priority" },
	{ "CheckHierarchyInput", "Empty" },
	{ "CheckHierarchyOutput", "invalid" },
	{ "CheckHierarchyOutput", "all" },
	{ "CheckHierarchyOutput", "all_measures" },
	{ "CheckHierarchyOutput", "Empty" },

	{ "HierarchicalOperator", "hierarchy ( Expression , HierarchicalRuleset HierarchyComponents RuleDimension HierarchyMode HierarchyInput HierarchyOutput )" } ,
	{ "HierarchyComponents", "condition ListComponents" },
	{ "HierarchyComponents", "Empty" },
	{ "ListComponents", "? Variable ListComponents" },
	{ "HierarchyMode", "non_null" },
	{ "HierarchyMode", "non_zero" },
	{ "HierarchyMode", "partial_null" },
	{ "HierarchyMode", "partial_zero" },
	{ "HierarchyMode", "always_null" },
	{ "HierarchyMode", "always_zero" },
	{ "HierarchyMode", "Empty" },
	{ "HierarchyInput", "rule" },
	{ "HierarchyInput", "rule_priority" },
	{ "HierarchyInput", "dataset" },
	{ "HierarchyInput", "Empty" },
	{ "HierarchyOutput", "computed" },
	{ "HierarchyOutput", "all" },
	{ "HierarchyOutput", "Empty" },

	{ "PartitionBy", "? partition by DimensionList" } ,
	{ "OrderBy", "? order by OrderByList" } ,
	{ "WindowingClause", "rows between LimitClause and LimitClause" } ,
	{ "WindowingClause", "range between LimitClause and LimitClause" } ,
	{ "WindowingClause", "Empty" } ,
	{ "LimitClause", "IntegerConstant preceding" } ,
	{ "LimitClause", "IntegerConstant following" } ,
	{ "LimitClause", "current row" } ,
	{ "LimitClause", "unbounded preceding" } ,
	{ "LimitClause", "unbounded following" } ,
	
	{ "SetOperator", "union ( SetExpressionList )" } ,
	{ "SetOperator", "intersect ( SetExpressionList )" } ,
	{ "SetOperator", "setdiff ( Expression , Expression )" } ,
	{ "SetOperator", "symdiff ( Expression , Expression )" } ,
	
	{ "SetExpressionList", "Expression ExpressionListOptional" } ,
	{ "ExpressionListOptional", "? , SetExpressionList" } ,

	{ "OptionalComponentName", "? ComponentName" },
	
	{ "OptionalLimits", "_ Limits" },
	{ "Limits", "_ all" },
	{ "Limits", "_ single" },

	{ "PeriodIndValue" , "\"A\"" },
	{ "PeriodIndValue" , "\"S\"" },
	{ "PeriodIndValue" , "\"Q\"" },
	{ "PeriodIndValue" , "\"M\"" },
	{ "PeriodIndValue" , "\"W\"" },
	{ "PeriodIndValue" , "\"D\"" },
			
	{ "RetainOptional" , "? , Retain" },
	{ "Retain", "all" } ,
	{ "Retain", "true" } ,
	{ "Retain", "false" } ,
	
	{ "Number", "NumberConstant" },
	{ "Number", "- Expression" },
	{ "Number", "Expression + Expression" },
	{ "Number", "Expression - Expression" },
	{ "Number", "Expression / Expression" },
	{ "Number", "Expression * Expression" },
	{ "Number", "abs ( Expression )" } ,
	{ "Number", "mod ( Expression , NumberConstant )" } ,
	{ "Number", "sqrt ( Expression )" } ,
	{ "Number", "power ( Expression , NumberConstant )" } ,
	{ "Number", "ceil ( Expression )" } ,
	{ "Number", "floor ( Expression )" } ,
	{ "Number", "round ( Expression NumDigits )" } ,
	{ "Number", "trunc ( Expression NumDigits )" } ,
	{ "Number", "exp ( Expression NumberConstant )" } ,
	{ "Number", "log ( Expression NumberConstant )" } ,
	{ "Number", "ln ( Expression )" } ,
	{ "Number", "length ( String )" } ,

	{ "ComponentNumber", "NumberConstant" },
	{ "ComponentNumber", "- ComponentExpression" },
	{ "ComponentNumber", "ComponentExpression + ComponentExpression" },
	{ "ComponentNumber", "ComponentExpression - ComponentExpression" },
	{ "ComponentNumber", "ComponentExpression / ComponentExpression" },
	{ "ComponentNumber", "ComponentExpression * ComponentExpression" },
	{ "ComponentNumber", "abs ( ComponentExpression )" } ,
	{ "ComponentNumber", "mod ( ComponentExpression , NumberConstant )" } ,
	{ "ComponentNumber", "sqrt ( ComponentExpression )" } ,
	{ "ComponentNumber", "power ( ComponentExpression , NumberConstant )" } ,
	{ "ComponentNumber", "ceil ( ComponentExpression )" } ,
	{ "ComponentNumber", "floor ( ComponentExpression )" } ,
	{ "ComponentNumber", "round ( ComponentExpression NumDigits )" } ,
	{ "ComponentNumber", "trunc ( ComponentExpression NumDigits )" } ,
	{ "ComponentNumber", "exp ( ComponentExpression NumberConstant )" } ,
	{ "ComponentNumber", "log ( ComponentExpression NumberConstant )" } ,
	{ "ComponentNumber", "ln ( ComponentExpression )" } ,
	{ "ComponentNumber", "length ( ComponentString )" } ,

	{ "NumDigits" , "?_ , NumberConstant" },

	{ "String", "StringConstant" } ,
	{ "String", "Expression || Expression" } ,
	{ "String", "lower ( Expression )" } ,
	{ "String", "upper ( Expression )" } ,
	{ "String", "trim ( Expression )" } ,
	{ "String", "ltrim ( Expression )" } ,
	{ "String", "rtrim ( Expression )" } ,
	{ "String", "replace ( Expression , StringPattern StringReplacement )" } ,
	{ "String", "instr ( Expression , Expression , StartPositionOptional OccurrenceOptional )" } ,
	{ "String", "substr ( Expression , StartPositionOptional Length )" } ,
	
	{ "ComponentString", "StringConstant" } ,
	{ "ComponentString", "ComponentString || ComponentString" } ,
	{ "ComponentString", "lower ( ComponentString )" } ,
	{ "ComponentString", "upper ( ComponentString )" } ,
	{ "ComponentString", "trim ( ComponentString )" } ,
	{ "ComponentString", "ltrim ( ComponentString )" } ,
	{ "ComponentString", "rtrim ( ComponentString )" } ,
	{ "ComponentString", "replace ( ComponentString , StringPattern StringReplacement )" } ,
	{ "ComponentString", "instr ( ComponentString , ComponentString , StartPositionOptional OccurrenceOptional )" } ,
	{ "ComponentString", "substr ( ComponentString , StartPositionOptional Length )" } ,

	{ "StringPattern" , "StringConstant" } ,
	{ "StringReplacement", "?_ , StringConstant" } ,
	{ "StartPositionOptional", "_ NumberConstant" },
	{ "OccurrenceOptional", "?_ NumberConstant" },
	{ "Length", "?_ , NumberConstant" },
	
	{ "TimeOperator", "fill_time_series ( Expression , OptionalLimits )" } ,
	{ "TimeOperator", "period_indicator ( OptionalComponentName )" } ,
	{ "TimeOperator", "timeshift ( Expression , IntegerConstant )" } ,
	{ "TimeOperator", "flow_to_stock ( Expression )" } ,
	{ "TimeOperator", "stock_to_flow ( Expression )" } ,
	
	{ "ExpressionOptional" , "? , Expression" },

	{ "ExpressionList", "? Expression ExpressionListOptional" } ,
	{ "ExpressionListOptional", "? , ExpressionList" } ,
	
	{ "Boolean", "Expression = Expression" } ,
	{ "Boolean", "Expression >= Expression" } ,
	{ "Boolean", "Expression <= Expression" } ,
	{ "Boolean", "Expression > Expression" } ,
	{ "Boolean", "Expression < Expression" } ,
	{ "Boolean", "Expression <> Expression" } ,
	{ "Boolean", "between ( Expression , Expression , Expression )" } ,
	{ "Boolean", "match_characters ( String , String )" } ,
	{ "Boolean", "isnull ( Expression )" } ,
	{ "Boolean", "Expression in Collection" } ,
	{ "Boolean", "Expression not_in Collection" } ,
	{ "Boolean", "Expression and Expression" } ,
	{ "Boolean", "Expression or Expression" } ,
	{ "Boolean", "Expression xor Expression" } ,
	{ "Boolean", "not Expression" } ,
	{ "Boolean", "exists_in ( Expression , Expression RetainOptional )" } ,
	{ "Boolean", "true" },
	{ "Boolean", "false" },
	
	{ "ComponentBoolean", "ComponentExpression = ComponentExpression" } ,
	{ "ComponentBoolean", "ComponentExpression >= ComponentExpression" } ,
	{ "ComponentBoolean", "ComponentExpression <= ComponentExpression" } ,
	{ "ComponentBoolean", "ComponentExpression > ComponentExpression" } ,
	{ "ComponentBoolean", "ComponentExpression < ComponentExpression" } ,
	{ "ComponentBoolean", "ComponentExpression <> ComponentExpression" } ,
	{ "ComponentBoolean", "between ( ComponentExpression , ComponentExpression , ComponentExpression )" } ,
	{ "ComponentBoolean", "match_characters ( ComponentString , ComponentString )" } ,
	{ "ComponentBoolean", "isnull ( ComponentExpression )" } ,
	{ "ComponentBoolean", "ComponentExpression in Collection" } ,
	{ "ComponentBoolean", "ComponentExpression not_in Collection" } ,
	{ "ComponentBoolean", "ComponentExpression and ComponentExpression" } ,
	{ "ComponentBoolean", "ComponentExpression or ComponentExpression" } ,
	{ "ComponentBoolean", "ComponentExpression xor ComponentExpression" } ,
	{ "ComponentBoolean", "not ComponentExpression" } ,
	{ "ComponentBoolean", "true" },
	{ "ComponentBoolean", "false" },

	{ "Collection", "{ Values }" } ,
	{ "Collection", "ValueDomainName" } ,			// TBD: set or valuedomain
	{ "Values", "Expression ValueOptional" } ,
	{ "ValueOptional", "? , Values" } ,

	{ "GroupingClause", "group by DimensionList" } ,
	{ "GroupingClause", "group except DimensionList" } ,
	{ "GroupingClause", "group all time_agg ( PeriodIndTo , PeriodIndFrom OptionalComponentName )" } ,
	{ "GroupingClause", "Empty" } ,

	{ "HavingClause", "? having Boolean" } ,
	{ "PeriodIndTo", "PeriodIndValue" } ,
	{ "PeriodIndFrom", "_ PeriodIndValue" } ,
	
	// aggregate functions
	{ "AggregateOperator" , "avg ( Expression GroupingClause HavingClause ) " 		} ,
	{ "AggregateOperator" , "count ( Expression GroupingClause HavingClause )" 		} ,
	{ "AggregateOperator" , "count ( )" 								} , // in a having clause
	{ "AggregateOperator" , "max ( Expression GroupingClause HavingClause )"		} ,
	{ "AggregateOperator" , "median ( Expression GroupingClause HavingClause )"		} ,
	{ "AggregateOperator" , "min ( Expression GroupingClause HavingClause )" 		} ,
	{ "AggregateOperator" , "stddev_pop ( Expression GroupingClause HavingClause )"	} ,
	{ "AggregateOperator" , "stddev_samp ( Expression GroupingClause HavingClause )"} ,
	{ "AggregateOperator" , "sum ( Expression GroupingClause HavingClause )" 		} ,
	{ "AggregateOperator" , "var_pop ( Expression GroupingClause HavingClause )"	} ,
	{ "AggregateOperator" , "var_samp ( Expression GroupingClause HavingClause )" 	} ,
	
	{ "AnalyticClause", " over ( PartitionBy OrderBy WindowingClause )" } ,

	// analytic functions
	{ "AnalyticOperator" , "avg ( Expression AnalyticClause )" 			} ,
	{ "AnalyticOperator" , "count ( Expression AnalyticClause )" 		} ,
	{ "AnalyticOperator" , "max ( Expression AnalyticClause )"			} ,
	{ "AnalyticOperator" , "median ( Expression PartitionBy )"			} ,
	{ "AnalyticOperator" , "min ( Expression AnalyticClause )" 			} ,
	{ "AnalyticOperator" , "stddev ( Expression AnalyticClause )"		} ,
	{ "AnalyticOperator" , "stddev_pop ( Expression AnalyticClause )"	} ,
	{ "AnalyticOperator" , "stddev_samp ( Expression AnalyticClause )"	} ,
	{ "AnalyticOperator" , "sum ( Expression AnalyticClause )" 			} ,
	{ "AnalyticOperator" , "variance ( Expression AnalyticClause )"		} ,
	{ "AnalyticOperator" , "var_pop ( Expression AnalyticClause )"		} ,
	{ "AnalyticOperator" , "var_samp ( Expression AnalyticClause )" 	} ,
	{ "AnalyticOperator" , "first_value ( Expression AnalyticClause )"	} ,
	{ "AnalyticOperator" , "last_value ( Expression AnalyticClause )"	} ,
	{ "AnalyticOperator" , "lag ( Expression, Expression ExpressionOptional over ( PartitionBy OrderBy )"	} ,
	{ "AnalyticOperator" , "lead ( Expression, Expression ExpressionOptional over ( PartitionBy OrderBy )"	} ,
	{ "AnalyticOperator" , "ratio_to_report ( Expression AnalyticClause )"	} ,
	{ "AnalyticOperator" , "rank ( AnalyticClause )" 						} ,
	
	{ "ComponentAnalyticOperator" , "avg ( ComponentExpression AnalyticClause )" 			} ,
	{ "ComponentAnalyticOperator" , "count ( ComponentExpression AnalyticClause )" 		} ,
	{ "ComponentAnalyticOperator" , "max ( ComponentExpression AnalyticClause )"			} ,
	{ "ComponentAnalyticOperator" , "median ( ComponentExpression PartitionBy )"			} ,
	{ "ComponentAnalyticOperator" , "min ( ComponentExpression AnalyticClause )" 			} ,
	{ "ComponentAnalyticOperator" , "stddev ( ComponentExpression AnalyticClause )"		} ,
	{ "ComponentAnalyticOperator" , "stddev_pop ( ComponentExpression AnalyticClause )"	} ,
	{ "ComponentAnalyticOperator" , "stddev_samp ( ComponentExpression AnalyticClause )"	} ,
	{ "ComponentAnalyticOperator" , "sum ( ComponentExpression AnalyticClause )" 			} ,
	{ "ComponentAnalyticOperator" , "variance ( ComponentExpression AnalyticClause )"		} ,
	{ "ComponentAnalyticOperator" , "var_pop ( ComponentExpression AnalyticClause )"		} ,
	{ "ComponentAnalyticOperator" , "var_samp ( ComponentExpression AnalyticClause )" 	} ,
	{ "ComponentAnalyticOperator" , "first_value ( ComponentExpression AnalyticClause )"	} ,
	{ "ComponentAnalyticOperator" , "last_value ( ComponentExpression AnalyticClause )"	} ,
	{ "ComponentAnalyticOperator" , "lag ( ComponentExpression, ComponentExpression ExpressionOptional over ( PartitionBy OrderBy )"	} ,
	{ "ComponentAnalyticOperator" , "lead ( ComponentExpression, ComponentExpression ExpressionOptional over ( PartitionBy OrderBy )"	} ,
	{ "ComponentAnalyticOperator" , "ratio_to_report ( ComponentExpression AnalyticClause )"	} ,
	{ "ComponentAnalyticOperator" , "rank ( AnalyticClause )" 						} ,

	{ "BasicScalarType" , "number" 					} ,
	{ "BasicScalarType" , "integer" 				} ,
	{ "BasicScalarType" , "string"					} ,
	{ "BasicScalarType" , "time_period"				} ,
	{ "BasicScalarType" , "date"					} ,
	{ "BasicScalarType" , "time"					} ,
	{ "BasicScalarType" , "duration"				} ,
	{ "BasicScalarType" , "boolean"					} ,
	{ "BasicScalarType" , "scalar"					} ,
	{ "BasicScalarType" , "ValueDomainName"			} ,

	{ "ScalarType" , "BasicScalarType OptionalScalarConstraint CanBeNull" 	} ,
	{ "OptionalScalarConstraint" , "[ Boolean ]" 	} ,
	{ "OptionalScalarConstraint" , "{ ExpressionList } " 	} ,
	{ "OptionalScalarConstraint" , "Empty" 	} ,
	{ "CanBeNull" , "null" 	} ,
	{ "CanBeNull" , "not null" 	} ,
	{ "CanBeNull" , "Empty" 	} ,
	// missing: operator type and product type,and component with no name
	{ "DataType" , "ScalarType" 	} ,
	{ "DataType" , "SetType" 	} ,
	{ "DataType" , "DatasetType" 	} ,
	{ "DataType" , "RulesetType" 	} ,
	{ "DataType" , "ComponentType" 	} ,
	{ "DatasetType" , "dataset {\r\n ComponentTypeList \r\n}" 	} ,
	{ "ComponentTypeList" , "ComponentType ComponentNameMod OptionalComponentTypeList" 	} ,
	{ "OptionalComponentTypeList" , "? , \r\n ComponentType ComponentNameMod ComponentTypeList" 	} ,
	{ "ComponentType" , "ComponentRole < ScalarType >" 	} ,
	{ "ComponentNameMod", "ComponentName" },
	{ "ComponentNameMod", "_" },
	{ "ComponentNameMod", "_ +" },
	{ "ComponentNameMod", "_ *" },
	{ "ComponentRole", "identifier" },
	{ "ComponentRole", "measure" },
	{ "ComponentRole", "attribute" },
	{ "ComponentRole", "viral attribute" },
	{ "SetType" , "set < DataType >" 	} ,
	{ "RulesetType" , "ruleset" 	} ,

	{ "ParameterType" , "DataType" 	} ,
	
	{ "DL", "DefineDatapointRuleset" },
	{ "DL", "DefineHierarchicalRuleset" },
	{ "DL", "DefineOperator" },
	{ "DL", "(+) DefineValueDomain" },
	{ "DL", "(+) DefineDataset" },
	{ "DL", "(+) DefineView" },
	{ "DL", "(+) DefineFunction" },
	{ "DL", "(+) DefineSynonym" },
	{ "DL", "(+) description of ObjectName is [ Properties ]" } ,
	{ "DL", "(+) alter DatasetName AlterClause" },
	{ "DL", "(+) rename ObjectName to NewObjectName" } ,
	{ "DL", "(+) copy ObjectName to NewObjectName" } ,
	{ "DL", "(+) drop ObjectName PurgeOptional" } ,
	{ "DL", "(+) restore ObjectName RenameOptional" } ,
	{ "DL", "(+) purge recyclebin" } ,
	{ "DL", "(+) grant Privilege on DatasetName to UserOrRole" } ,
	{ "DL", "(+) revoke Privilege on DatasetName from UserOrRole" } ,
	{ "Properties" , "PropertyName = StringConstant OptionalProperties" 	} ,
	{ "OptionalProperties" , "? , Properties" 	} ,
	{ "RenameOptional", "? rename to NewObjectName" } ,
	{ "PurgeOptional", "? purge" } ,
	{ "AlterClause", "add DatasetComponents" } ,
	{ "AlterClause", "modify DatasetComponents" } ,
	{ "AlterClause", "drop ComponentName" } ,
	{ "AlterClause", "rename ComponentName to VariableName" } ,
	{ "AlterClause", "move ComponentName BeforeComponentName" } ,
	{ "AlterClause", "storage StorageOptions" } ,
	{ "BeforeComponentName", "? before ComponentName" } ,
	
	{ "Privilege", "read" } ,
	{ "Privilege", "update" } ,

	{ "DefineDataset", "define dataset NewObjectName is\r\n DatasetComponents \r\nend dataset" },
	{ "DefineDataset", "define dataset NewObjectName like DatasetName \r\nend dataset" },
	{ "DefineValueDomain", "define valuedomain NewObjectName is\r\n DatasetComponentList \r\nend valuedomain" } ,
	{ "DefineValueDomain", "define valuedomain NewObjectName subset of ValueDomainName OptionalScalarConstraint \r\nend valuedomain" } ,
	{ "DatasetComponents", "ComponentRole ComponentName ScalarType OptionalDataLength" } ,
	{ "DatasetComponentList", "? ; \r\n DatasetComponents" } ,
	{ "OptionalDataLength", "? ( IntegerConstant )" } ,

	{ "DefineView", "define view NewObjectName is\r\n Expression \r\nend view" },

	{ "DefineDatapointRuleset", "define datapoint ruleset NewObjectName ( Variables ) is\r\nDatapointRules \r\nend datapoint ruleset" },
	{ "Variables", "? Variable VariableList" } ,
	{ "VariableList", "? , Variables" } ,
	{ "DatapointRules", "AntecedentCondition ConsequentCondition ErrorLevel ErrorCode DatapointRuleList" } ,
	{ "DatapointRuleList", "? ; \r\n DatapointRules" } ,
	{ "AntecedentCondition", "? \r\n  when Condition then" } ,
	{ "ConsequentCondition", "Boolean" } ,
	{ "ErrorLevel", "? errorlevel ( String )" } ,
	{ "ErrorCode", "? errorcode ( String )" } ,
	
	{ "DefineHierarchicalRuleset", "define hierarchical ruleset NewObjectName ( AntecedentVariables variable = Variable ) is\r\n HierarchicalRules \r\nend hierarchical ruleset" },
	{ "AntecedentVariables", "? antecedent variables = ( Variables ), " } ,
	{ "HierarchicalRules", "AntecedentCondition HConsequentCondition ErrorCode ErrorLevel HierarchicalRuleList" } ,
	{ "HierarchicalRuleList", "? ; \r\n HierarchicalRules" } ,
	{ "HConsequentCondition", "HLeftSide HOperator HRightSide" } ,
	{ "HOperator", " = " 	} ,
	{ "HOperator", " >= " 	} ,
	{ "HOperator", " <= " 	} ,
	{ "HOperator", " > " 	} ,
	{ "HOperator", " < " 	} ,

	{ "DefineOperator", "define operator NewObjectName ( Parameters ) OptionalReturnType is\r\n  Expression \r\nend operator" },
	{ "Parameters", "? ParameterName ParameterType ParameterDefaultValue OptionalParameters" },
	{ "OptionalParameters", "? , Parameters" },
	{ "OptionalReturnType", "? , returns DataType" },
	{ "ParameterDefaultValue", "? default Expression" } ,

	{ "DefineFunction", "define function NewObjectName ( Parameters ) OptionalReturnType is\r\n  StatementList \r\nend function" },

	{ "Statements", "Statement StatementList" } ,
	{ "StatementList", "? ;  \r\nStatements" } ,
	{ "Statement", "TemporaryDatasetName := Expression" } ,
	{ "Statement", "LeftSideAssignment <- Expression" } ,
	{ "Statement", "(+) print Expression OrderBy ToFile" } ,
	{ "Statement", "(+) sql String" } ,
	{ "Statement", "(+) load FileName into DatasetName" } ,
	{ "Statement", "(+) range Dimension Collection" } ,
	{ "Statement", "(+) for Variable in Collection do Statements \nend for" } ,
	{ "Statement", "(+) case WhenThen OptionalElse \nend case" } ,
	{ "Statement", "(+) try \r\n  Statements \r\ncatch ( ParameterName string )\r\n  Statements \r\nend try" } ,
	{ "Statement", "(+) throw ( StringConstant )" } ,
	{ "Statement", "(+) ObjectName ( ExpressionList )" } ,
	{ "Statement", "(+) return Expression" } ,
	{ "LeftSideAssignment", "DatasetName OptionalSubspace OptionalFilter" } ,
	{ "OptionalSubspace", "? [ sub Subspace ]" } ,
	{ "OptionalFilter", "? [ filter Boolean ]" } ,
	{ "ToFile",	   "? to FileName" } ,
	{ "OptionalElse", "? else Statements" } ,
	{ "WhenThen",  "when Boolean then Statements OptionalWhenThen" },
	{ "OptionalWhenThen",  "?  \nWhenThen " },
	
	{ "StringConstant", "*" },
	{ "NumberConstant", "*" },
	{ "IntegerConstant", "*" },
	{ "ParameterName", "*" },
	{ "TemporaryDatasetName", "*" },
	{ "ObjectName", "*" },
	{ "NewObjectName", "*" },
	{ "AliasName", "*" },
	{ "HLeftSide", "*" },
	{ "HRightSide", "*" },
	{ "DatasetName", "*" },
	{ "Dimension", "*" },
	{ "DimensionValue", "*" },
	{ "MeasureAttribute", "*" },
	{ "DatapointRuleset", "*" },
	{ "HierarchicalRuleset", "*" },
	{ "UserFunctionName", "*" },
	{ "Variable", "*" },
	{ "VariableName", "*" },
	{ "ComponentName", "*" },
	{ "ValueDomainName", "*" },
	{ "UserDefinedOperator", "*" },
	{ "DimensionList", "*" },
	{ "OrderByList", "*" },
	{ "UserOrRole", "*" },
	{ "StorageOptions", "*" },
	{ "FileName", "*" },
	{ "IdentifierMeasureAttribute", "*" },
	{ "PropertyName", "*" },
	
	/* 
	{ "SetOperator", "MetaDataset" } ,
	{ "MetaDataset", "(+) vtl_all_objects" } ,
	{ "MetaDataset", "(+) vtl_user_objects" } ,
	{ "MetaDataset", "(+) vtl_all_datasets" } ,
	{ "MetaDataset", "(+) vtl_user_datasets" } ,
	*/	
} ;

// end class
}
/*

static void setBackgroundColor ( int n_columns ) throws AppError
{
	Border b = BorderFactory.createLineBorder(Color.BLACK);
	
	// header: background color
	
	DefaultTableCellRenderer headerRendererDimension = new DefaultTableCellRenderer();
	headerRendererDimension.setBackground( new Color (224,224,224) );
	headerRendererDimension.setHorizontalAlignment(JLabel.CENTER);
	headerRendererDimension.setBorder(b);
	DefaultTableCellRenderer headerRendererMeasure = new DefaultTableCellRenderer();
	headerRendererMeasure.setBackground(new Color (204,255,255) );
	headerRendererMeasure.setHorizontalAlignment(JLabel.CENTER);
	headerRendererDimension.setBorder(b);
	DefaultTableCellRenderer headerRendererAttribute = new DefaultTableCellRenderer();
	headerRendererAttribute.setBackground(new Color (255,229,204));
	headerRendererAttribute.setHorizontalAlignment(JLabel.CENTER);
	headerRendererDimension.setBorder(b);

	for (int i = 0; i < n_columns; i++) {
		// System.out.println ( Command.getLastQueryCompType ( i ) ) ;
		switch ( Command.getLastQueryCompType ( i ) ) {
			case Command.ComponentType.COMP_DIMENSION : 
				dataTable.getColumnModel().getColumn(i).setHeaderRenderer(headerRendererDimension);
			    break ;
			case Command.ComponentType.COMP_MEASURE : 
			    dataTable.getColumnModel().getColumn(i).setHeaderRenderer(headerRendererMeasure);
			    break ;
			case Command.ComponentType.COMP_ATTRIBUTE : 
			    dataTable.getColumnModel().getColumn(i).setHeaderRenderer(headerRendererAttribute);
			    break ;
			case Command.ComponentType.COMP_UNKNOWN : 
				break ;
		}
	}
}
*/
// start
/*
final class UpdateTrigger implements DocumentListener, ActionListener {
	final Timer timer=new Timer(150, this);
	boolean enabled=true;
	//boolean enabled=true;
	UpdateTrigger() {
	    timer.setRepeats(false);
	}
	public void insertUpdate(DocumentEvent e) {
		if(enabled) timer.restart();
	}
	public void removeUpdate(DocumentEvent e) {
	    if(enabled) timer.restart();
	}
	public void changedUpdate(DocumentEvent e) {
	    if(enabled) timer.restart();
	}
	public void actionPerformed(ActionEvent e) {
	    enabled=false;
	    try { setKeywordColor ( ); }
	    finally { enabled=true; }
	}
}
d.addDocumentListener(new UpdateTrigger());
// end*/
