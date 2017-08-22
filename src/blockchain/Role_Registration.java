package blockchain;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;

public class Role_Registration {
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";

	public static void main(String[] args) throws SQLException, NoSuchAlgorithmException {
		try {
		      Class.forName(driverName);
		    } catch (ClassNotFoundException e) {
		      e.printStackTrace();
		      System.exit(1);
		    }
		    Connection con = DriverManager.getConnection("jdbc:hive2://localhost:10000/metastore_db","hiveuser","111");
		    Statement stmt = con.createStatement();
		    //stmt.execute("create database blockchain");
		    stmt.execute("use blockchain");
		    Scanner input = new Scanner(System.in);
		    System.out.println("First Name: ");
		    String firstName = input.nextLine();
		    System.out.println("Last Name: ");
		    String lastName = input.nextLine();
		    System.out.println("User Name: ");
		    String userName = input.nextLine();
		    System.out.println("Password: ");
		    String password = input.nextLine();
		    password = hashPassword(password);
		    System.out.println("Role:");
		    String role = input.nextLine();
		    String key = "NA";
		    int id = 1;
		    if(role.equalsIgnoreCase("admin")){
		    	System.out.println("Secret Key:");
			     key = input.nextLine();
		    }
		    CreateBlockchain cbc = new CreateBlockchain(); 
		    //stmt.execute("drop table REGISTRATION"); 
		   //stmt.execute("create table if not exists REGISTRATION (ID INT, FIRST_NAME STRING, LAST_NAME STRING, USERNAME STRING, PASSWORD STRING, ROLE STRING, KEY STRING)");
		   /*if(cbc.checkTableExists("REGISTRATION", stmt)){ 
		   id =  nextId(stmt);
		   }*/
		   stmt.execute("insert into table REGISTRATION values('"+id + "', '" + firstName + "','" + lastName +"','" + userName +"','" + password +"','" + role +"','" + key +"')");
		   /*if(cbc.checkTableExists("REGISTRATION", stmt)){ 
		   ResultSet res = providePreviousData("REGISTRATION", stmt);
		    while (res.next()) {
		    	System.out.println(res.getInt(1) + " " + res.getString(2));
		      }
		   }*/
		   con.close();
	}
	
	public static String hashPassword(String password) throws NoSuchAlgorithmException{
	    	MessageDigest md = MessageDigest.getInstance("MD5");
	        byte[] toBytes = password.getBytes();
	        md.reset();
	        byte[] digested = md.digest(toBytes);
	        StringBuffer sb = new StringBuffer();
	        for(int i=0;i<digested.length;i++){
	            sb.append(Integer.toHexString(0xff & digested[i]));
	        }
	        return sb.toString();
	   }
	public static ResultSet providePreviousData(String tableName, Statement stmt) throws SQLException{
		 ResultSet res = stmt.executeQuery("SELECT * FROM " + tableName);
		 return res;
	 }
	public static int nextId(Statement stmt) throws SQLException{
		int id = 0; 
		ResultSet res = stmt.executeQuery("SELECT ID FROM REGISTRATION ORDER BY ID DESC");
		 while (res.next()) {
		    id = res.getInt(1);	
		         break;
		      }
		 return id+1;
	}
}
