package de.x28hd.tool;

import java.awt.Color;
import java.awt.Point;
import java.io.File;
import java.io.IOException;
import java.sql.Connection; 
import java.sql.DriverManager; 
import java.sql.ResultSet; 
import java.sql.SQLException; 
import java.sql.Statement; 
import java.util.Hashtable;

import javax.xml.transform.TransformerConfigurationException;

import org.sqlite.SQLiteConfig;
import org.xml.sax.SAXException;

public class CtvImport {
	
	//	Major fields
	String dataString = "";
	Hashtable<Integer,GraphNode> nodes = new Hashtable<Integer,GraphNode>();
	Hashtable<Integer,GraphEdge> edges = new Hashtable<Integer,GraphEdge>();
	
	//	DB related (from sample program)
    private static Connection connection; 
    static { 
        try { 
            Class.forName("org.sqlite.JDBC"); 
        } catch (ClassNotFoundException e) { 
        	System.out.println("Error CVI101 " + e);
        } 
    } 
    String[][] table = {{"ReferenceAuthor", "ReferenceCategory", "ReferenceKeyword",
    	"KnowledgeItemCategory", "KnowledgeItemKeyword"},
    	{"Person", "Category", "Keyword", "Category", "Keyword", "Reference", "KnowledgeItem"}};
    String[][] field = {{"PersonID", "CategoryID", "KeywordID", "CategoryID", "KeywordID"},
		{"LastName", "Name", "Name", "Name", "Name", "ShortTitle", "CoreStatement"}};
    
    String[] colors = {"#eeeeee", "#ffbbbb", "#eeeeee", "#ffbbbb", "#eeeeee", "#bbbbff", "#ffff99"};
    
	//	Keys for nodes and edges, incremented in addNode and addEdge
	Hashtable<String,Integer> inputID2num = new  Hashtable<String,Integer>();
	int j = 0;
	int edgesNum = 0;
	
	//	Constants
	int maxVert = 10;
	GraphPanelControler controler;
	String filename;
	boolean ctv6 = false;

	
    public CtvImport(File file, GraphPanelControler controler){ 
    	this.controler = controler;
        try {
        	filename = file.getCanonicalPath();
            } catch (IOException e) {
            	System.out.println("Error CVI102 " + e);
		} 
        if (filename.endsWith("ctv6")) ctv6 = true;
        initDBConnection(); 
        handleCitaviDB(); 
    } 
     
//
//	Initialize
    
    private void initDBConnection() { 
        SQLiteConfig config = new SQLiteConfig();
        config.setReadOnly(true); 
        try { 
            if (connection != null) 
                return; 
            connection = DriverManager.getConnection("jdbc:sqlite:" + filename, config.toProperties()); 
            if (!connection.isClosed()) 
                System.out.println("...Connection established " + connection.toString()); 
        } catch (SQLException e) { 
        	throw new RuntimeException(e); 
        } 

        Runtime.getRuntime().addShutdownHook(new Thread() { 
            public void run() { 
                try { 
                    if (!connection.isClosed() && connection != null) { 
                    	connection.close(); 
                        if (connection.isClosed()) 
                            System.out.println("Connection to Database closed"); 
                    } 
                } catch (SQLException e) { 
                	System.out.println("Error CVI103 " + e);
                } 
            } 
        }); 
    } 
    
//
//	Main work
    
    private void handleCitaviDB() { 
        try { 
            Statement stmt = connection.createStatement(); 
            
            //	Collect multiples
            
            for (int w = 0; w < 3; w++) { 
            	String t = table[0][w];
            	String f = field[0][w];
            	String c = t + "." + f;
           	String sqlString = 
            		"SELECT QUOTE(" + c + 
            		") AS QuotedID, Count(\"\") AS Ausdr1" +
            		" FROM " + t +
            		" GROUP BY QuotedID " +
            		" HAVING (((Count(\"\"))>1))"; 
            		// (Authors, Categories or Keywords that occur with 
            		//	multiple publications)
            	ResultSet rs = stmt.executeQuery(sqlString);
            	while (rs.next()) { 
            		String id = rs.getString("QuotedID"); 
            		addNode(id, w);
            	}
            	rs.close(); 
            
            	//	Add publications and connect them
            	sqlString = "SELECT QUOTE(" + c + ") as q1, QUOTE(" +
            		t + ".ReferenceID) as q2 FROM " + t + ";";
            		//	(All Authors, Categories, Keywords of all Publications)
            	rs = stmt.executeQuery(sqlString);
            	while (rs.next()) { 
            		String multID = rs.getString("q1"); 
            		if (!inputID2num.containsKey(multID)) continue;
            		String pubID = rs.getString("q2");
            		if (!inputID2num.containsKey(pubID)) {
            			addNode(pubID, 5);
            		}
            		addEdge(multID, pubID);
            	}
            	rs.close(); 
            }
            
            //	Process publications that are linked
            Statement stmt2 = connection.createStatement(); 
            String sqlString2 = "SELECT QUOTE(ActiveEntityID) as q1, QUOTE(PassiveEntityID) as q2 FROM EntityLInk;";
            if (ctv6) sqlString2 = "SELECT EntityLink.Indication, QUOTE(SourceID) as q1, QUOTE(TargetID) as q2 FROM EntityLInk;";
        	ResultSet rs2 = stmt2.executeQuery(sqlString2);
        	while (rs2.next()) { 
        		if (ctv6 && !rs2.getString("Indication").equals("ReferenceLink")) continue; 
        		String actID = rs2.getString("q1"); 
        		if (!inputID2num.containsKey(actID)) {
        			addNode(actID, 5);
        		}
        		String passID = rs2.getString("q2"); 
        		if (!inputID2num.containsKey(passID)) {
        			addNode(passID, 5);
        		}
        		addEdge(actID, passID);
        	}
            rs2.close();
            
            //	Process ideas that have categories or keywords
            
            for (int w = 3; w < 5; w++) { 
            	String t = table[0][w];
            	String f = field[0][w];
            	String c = t + "." + f;
            	String sqlString = 
            		"SELECT QUOTE(" + c +
            		") as q1, QUOTE(" + t + ".KnowledgeItemID) AS q2 FROM " + t + ";";
            		//	(All Categories and Keywords of all Ideas)
            	ResultSet rs = stmt.executeQuery(sqlString);
            	while (rs.next()) { 
            		String multID = rs.getString("q1"); 
            		if (!inputID2num.containsKey(multID)) {
            			addNode(multID, w);
            		}
            		String ideaID = rs.getString("q2"); 
            		if (!inputID2num.containsKey(ideaID)) {
            			addNode(ideaID, 6);
            		}
            		addEdge(multID, ideaID);
            	}
            	rs.close(); 
            }
            
            //	Process genuine ideas (with a CoreStatement) linking to publications, and all stand-alone ideas 
            String sqlString = "SELECT ID, QUOTE(ID) as q1, CoreStatement, ReferenceID, QUOTE(ReferenceID) as q2 FROM KnowledgeItem";
        	ResultSet rs = stmt.executeQuery(sqlString);
        	while (rs.next()) { 
        		String coreStmt = rs.getString("CoreStatement");
        		if (coreStmt == null) continue;	// Don't know how else to exclude the odd items from the demo
        		String ideaID = rs.getString("q1");
        		if (!inputID2num.containsKey(ideaID)) {
        			addNode(ideaID, 6);
        		}
        		String rawRefID = rs.getString("ReferenceID");
        		if (rawRefID == null) continue;
        		String pubID = rs.getString("q2");
        		if (!inputID2num.containsKey(pubID)) {
        			addNode(pubID, 5);
        		}
        		addEdge(ideaID, pubID);
        	}
            
            connection.close(); 
            
        } catch (SQLException e) { 
        	System.out.println("Error CVI104 " + e);
        } 

        
        //	pass on
        try {
        	dataString = new TopicMapStorer(nodes, edges).createTopicmapString();
        } catch (TransformerConfigurationException e1) {
        	System.out.println("Error CVI108 " + e1);
        } catch (IOException e1) {
        	System.out.println("Error CVI109 " + e1);
        } catch (SAXException e1) {
        	System.out.println("Error CVI110 " + e1);
        }
        controler.getNSInstance().setInput(dataString, 2);
    } 
    
//
//	Detail methods
    
	public void addNode(String nodeRef, int tables) { 
		String newNodeColor = "";
		String newLine = "\r";
		String topicName = ""; 
		String verbal = "";
		
        Statement stmt;
		String more = "";
		if (tables == 5) more = ", Reference.Title";
		if (tables == 6) more = ", KnowledgeItem.Text";
		try {
			stmt = connection.createStatement();
			int w = tables;	// which table
        	String t = table[1][w];
        	String f = field[1][w];
        	String c = t + "." + f;
			String sqlString = "SELECT " + c + more +
	        		" FROM " + t + " WHERE (" 
					+ t + ".ID = " + nodeRef + ");"; 
			
//			String sqlString = "SELECT " + t + ".ID as q, QUOTE(" + t + ".ID) as q2" + more +
//    		" WHERE (CAST (" + t + ".ID as blob) "
//    		+ "= CAST('" + nodeRef + "' as blob));"; 
//			// Thanks to https://stackoverflow.com/a/42293740 -- did not always work 
			
			ResultSet rs = stmt.executeQuery(sqlString);
			while (rs.next()) { 
				try {
					String shorty = rs.getString(f); 
					if (tables == 5) {
						verbal = rs.getString("Title");
						topicName = shorty;
						if (shorty == null) topicName = verbal;
					} else if (tables == 6) {
						verbal = rs.getString("Text");
						if (verbal == null) verbal = "";
						verbal = shorty + "<br/><br/>" + verbal;
						topicName = "";
					} else {
						topicName = shorty;
						verbal = shorty;
					}
				} catch (SQLException e) {
					System.out.println("Error CVI106 " + e);
				} 
			}
		} catch (SQLException e) {
			System.out.println("Error CVI107 " + e);
		} 
        
		newNodeColor = colors[tables];
		
		if (topicName == null) topicName = "";
		if (verbal == "null") verbal = "";
		int len = topicName.length();
		if (len > 39) {
			len = 30;
			if (!verbal.isEmpty() && !verbal.contains(topicName)) verbal = topicName + "<br/><br/>" + verbal;
			topicName = topicName.substring(0, len);
		} 

		topicName = topicName.replace("\r"," ");
		if (topicName.equals(newLine)) topicName = "";
		if (verbal == null || verbal.equals(newLine)) verbal = "";
		if (topicName.isEmpty() && verbal.isEmpty()) System.out.println("Error CVI111");
		int id = 100 + j;

		int y = 40 + (j % maxVert) * 50 + (j/maxVert)*5;
		int x = 40 + (j/maxVert) * 150;
		Point p = new Point(x, y);
		GraphNode topic = new GraphNode (id, p, Color.decode(newNodeColor), topicName, verbal);	

		nodes.put(id, topic);
		inputID2num.put(nodeRef, id);
		j++;
	}
	
	public void addEdge(String fromRef, String toRef) {
		GraphEdge edge = null;
		String newEdgeColor = "#d8d8d8";
		edgesNum++;
		GraphNode sourceNode = nodes.get(inputID2num.get(fromRef));
		GraphNode targetNode = nodes.get(inputID2num.get(toRef));
		edge = new GraphEdge(edgesNum, sourceNode, targetNode, Color.decode(newEdgeColor), "");
		edges.put(edgesNum, edge);
	}

}
