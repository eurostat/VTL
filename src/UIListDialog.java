
/*
 * List dialog.
 */

import javax.swing.*;
import java.awt.*;
import java.awt.event.*;

/*
 * Create list dialog. 
 */
class UIListDialog extends JDialog {

private static final long 		serialVersionUID = 1L;
private static UIListDialog 	dialog = null ;
private static String 			value ;
private JLabel 					labelMain ;
private JList<String> 			listMain;				// main list
private JScrollPane 			scrollerMain ;
private JButton 				cancelButton ;
private JButton 				chooseButton ;

public static String showDialog ( String labelText, String title, String[] items ) 
{
	value = null ;
	dialog = new UIListDialog(labelText, title, items, false);
	/*if ( dialog == null )
	else {
		dialog.listMain.removeAll();
		dialog.listMain = new JList<String>(items) ;
	}*/
	dialog.setVisible(true);
	return ( value ) ;
}

public static int[] showDialogMultiSelection ( String labelText, String title, String[] items ) 
{
	value = null ;
	dialog = new UIListDialog(labelText, title, items, true);
	dialog.setVisible(true);
	return ( dialog.listMain.getSelectedIndices() ) ;
}

/*
 * List dialog. choose single or multiple selection.
 */
private UIListDialog( String labelText, String title, String[] items, boolean multiSelection ) 
{
    super(JOptionPane.getRootFrame(), title, true);
    
    // List object
    listMain = new JList<String>(items) ;
    
    listMain.setSelectionMode( multiSelection ? ListSelectionModel.MULTIPLE_INTERVAL_SELECTION : 
    														ListSelectionModel.SINGLE_SELECTION ) ;
    
    // Cancel button
    cancelButton = new JButton("Cancel");
    cancelButton.addActionListener( new ActionListener() {
        public void actionPerformed(ActionEvent buttonEvent) {
        	UIListDialog.value = null ;
        	UIListDialog.dialog.setVisible(false);
        }
    } ) ;
    
    // Choose button
    chooseButton = new JButton("Choose");
    chooseButton.addActionListener( new ActionListener() {
        public void actionPerformed(ActionEvent buttonEvent) {
        	UIListDialog.value = listMain.getSelectedValue ( ) ;
        	UIListDialog.dialog.setVisible(false);
        }
    } ) ;
    
    // bind escape keystroke to close the window
	InputMap im = getRootPane().getInputMap(JComponent.WHEN_IN_FOCUSED_WINDOW);
	ActionMap am = getRootPane().getActionMap();
	im.put(KeyStroke.getKeyStroke(KeyEvent.VK_ESCAPE, 0), "cancel");
	am.put("cancel", new AbstractAction() {
		private static final long serialVersionUID = 1L;
		@Override
	    public void actionPerformed(ActionEvent e) {
			UIListDialog.value = null ;
			UIListDialog.dialog.setVisible(false); 
	    }
	}) ;
    
    getRootPane().setDefaultButton(chooseButton);
    
    listMain.setLayoutOrientation(JList.VERTICAL ) ;
    listMain.setVisibleRowCount(-1);
    listMain.setFont(listMain.getFont().deriveFont(18f)) ;
    // listMain.setFont(listMain.getFont().deriveFont(listMain.getFont().getStyle() & ~ Font.BOLD));
    listMain.addMouseListener(new MouseAdapter() {
        public void mouseClicked(MouseEvent e) {
            if (e.getClickCount() == 2) {
                chooseButton.doClick();
            }
        }
    });
    scrollerMain = new JScrollPane(listMain);

    // Labels
    labelMain = new JLabel(labelText);
    labelMain.setFont(labelMain.getFont().deriveFont(16f) ) ;
    labelMain.setLabelFor(listMain);

    Container contentPane = getContentPane();
    contentPane.add( labelMain );
    contentPane.add( scrollerMain );
    contentPane.add( cancelButton );
    contentPane.add( chooseButton );
    
    this.setSize ( 440, 580 ) ;
    this.addComponentListener(new resizeListener());
    this.setLocationRelativeTo(null);
}

/*
 * Resize event - resize the window and components.
 * setBounds(x, y, width, height)
 */
class resizeListener extends ComponentAdapter {
    public void componentResized(ComponentEvent e) {
    	Rectangle r = dialog.getBounds() ;
    	int hei = r.height ;
    	int wid = r.width ;
        labelMain.setBounds( 10, 10, wid - 40, 20 ) ;
        scrollerMain.setBounds( 10, 30, wid - 40, hei - 130 ) ;
        cancelButton.setBounds( 70, hei - 95, 80, 30 ) ;
        chooseButton.setBounds( 280, hei - 95, 80, 30 ) ;
    }
}

// end class
}

