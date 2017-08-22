package blockchain;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

public class ValidateBlockchains implements Job{
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	
	public ArrayList<String> createList() throws IOException{
		ArrayList<String> blockList = new ArrayList<String>();
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://localhost:9000");
		conf.set("fs.hdfs.impl", 
		        org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
		    );
		    conf.set("fs.file.impl",
		        org.apache.hadoop.fs.LocalFileSystem.class.getName()
		    );
		FileSystem fs = FileSystem.get(conf);
		File file = new File("/usr/local/hadoop/tempList");
		Path path = new Path("/blockchain/list");
		fs.copyToLocalFile(false, path, new Path("/usr/local/hadoop/tempList"), true);
		FileReader fr = new FileReader("/usr/local/hadoop/tempList");
			BufferedReader br = new BufferedReader(fr);
			String sCurrentLine;
			br = new BufferedReader(new FileReader("/usr/local/hadoop/tempList"));
		    while ((sCurrentLine = br.readLine()) != null) 
		    {
		        Path newPath = new Path("/blockchain/" + sCurrentLine);
		        if(fs.exists(newPath)){
		    	blockList.add(sCurrentLine);
		        }
		    }
		    br.close();
		  file.delete();  
		return blockList;
	}
	
	public void execute(JobExecutionContext context) throws JobExecutionException{
		String key = "";
		ArrayList<String> blockList = new ArrayList<String>();
		try {
			blockList = this.createList();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		//Convert all the files to hive tables(either direct;y or through CSV conversion)
		ArrayList<Integer> listOfIds = new ArrayList<Integer>();
		String blockheader = "";
		CreateBlockchain cbc = new CreateBlockchain();
		try {
		      Class.forName(driverName);
		    } catch (ClassNotFoundException e) {
		      e.printStackTrace();
		      System.exit(1);
		    }
		try{
			Connection con = DriverManager.getConnection("jdbc:hive2://localhost:10000/metastore_db","hiveuser","111");
		    Statement stmt = con.createStatement();
		    ResultSet secret = stmt.executeQuery("SELECT KEY FROM REGISTRATION WHERE ROLE = 'admin'");
		    while(secret.next()){
		    	key = secret.getString(1);
		    }
		    stmt.execute("use blockchain");
		    for(int i = 0;i<blockList.size();i++){
		    	if(!blockList.get(i).contains("local")){
				stmt.execute("CREATE EXTERNAL TABLE IF NOT EXISTS"+ blockList.get(i) + 
						"(Version_Number STRING,TIME STRING, BITS STRING, MERKLE_ROOT STRING, BLOCK_HEADER STRING, encUserName STRING, encDataSet STRING, encDataItems STRING, encMethod STRING) COMMENT 'Blockchain' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION '/blockchain/"+blockList.get(i) + "'");
		    	}
		    	}
		    /*ResultSet res = stmt.executeQuery("SHOW TABLES");
		    while(res.next()){
		    	listofTables.add(res.getString(1));
		    }*/
		    for(int i=0;i<blockList.size();i++){
		    	ResultSet res = stmt.executeQuery("SELECT * FROM " + blockList.get(i));
		    	while(res.next()){
		    		listOfIds.add(res.getInt(1));
		    	}
		    	for(int j=0;j<listOfIds.size();j++){
		    		String first_VN = "";
		    		String first_TS = "";
		    		String first_Bits = "";
		    		String first_BH = "";
		    		String first_MR = "";
		    		String second_VN = "";
		    		String second_TS = "";
		    		String second_Bits = "";
		    		String second_BH = "";
		    		String second_MR = "";
		    		ResultSet resFirst = stmt.executeQuery("SELECT * FROM " + blockList.get(i) + " where ID=" + listOfIds.get(j));
		    		while(resFirst.next()){
		    			first_VN = resFirst.getString(2);
		    			first_TS = resFirst.getString(3);
		    			first_Bits = resFirst.getString(4);
		    			first_BH = resFirst.getString(6);
		    			first_MR = resFirst.getString(5);
		    		}
		    		ResultSet resNext = stmt.executeQuery("SELECT * FROM " + blockList.get(i) + " where ID=" + listOfIds.get(j+1));
		    		while(resNext.next()){
		    			second_VN = resNext.getString(2);
		    			second_TS = resNext.getString(3);
		    			second_Bits = resNext.getString(4);
		    			second_BH = resNext.getString(6);
		    			second_MR = resNext.getString(5);
		    		}
		    		if(j==0){
		    			blockheader = cbc.provideHashValue(first_VN + first_TS + first_Bits + first_MR + "");
		    			if(!blockheader.equals(first_BH)){
		    				System.out.println("Same Row Anomaly");
		    			}
		    			blockheader = cbc.provideHashValue(second_VN + second_TS + second_Bits + second_MR + first_BH);
		    			if(!blockheader.equals(second_BH)){
		    				System.out.println("Rows mismatch");
		    			}
		    		} else {
		    			blockheader = cbc.provideHashValue(second_VN + second_TS + second_Bits + second_MR + first_BH);
		    			if(!blockheader.equals(second_BH)){
		    				System.out.println("Rows mismatch");
		    			}
		    		}
		    		j++;
		    	}
		    }
		    for(int j = 0;j<blockList.size();j++){
				stmt.executeQuery("Drop table " + blockList.get(j));
			}
		    } catch (SQLException|NoSuchAlgorithmException e){
		    	
		    }
		
	}
}
