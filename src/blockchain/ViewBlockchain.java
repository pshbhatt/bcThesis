package blockchain;

import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;

public class ViewBlockchain {
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
			    Scanner input = new Scanner(System.in);
			    System.out.println("Enter UserName: ");
			    String userName = input.nextLine();
			    System.out.println("Enter Password: ");
			    String password = input.nextLine();
			    password = Role_Registration.hashPassword(password);
			    String role = "";
			    String key = "";
			    ResultSet credentials = stmt.executeQuery("SELECT PASSWORD,ROLE,KEY FROM REGISTRATION WHERE USERNAME = '"+ userName + "'");
			    while(credentials.next()){
			    if(!password.equals(credentials.getString(1))){
			    	System.out.println("Invalid Password");
			    } else {
			    	role = credentials.getString(2);
			    	key = credentials.getString(3);
			    }
			    }
			    if(role.equals("admin")){
			    	Scanner dataset = new Scanner(System.in);
			    	System.out.println("Please enter the dataset name to view it's blockchain");
			    	String dataset_name = dataset.nextLine();
			    	//use the dataset to fetch the blockchain
			    }
	}
}
