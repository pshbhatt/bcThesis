package blockchain;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

import org.apache.derby.impl.sql.catalog.SYSCOLUMNSRowFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import com.google.common.hash.Hashing;

public class CreateBlockchain {
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";

	public static void main(String args[]) throws Exception {
		Properties blockchain = new Properties();
	    FileInputStream propFile;
	    String filePath = "./blockchain.properties";
	    propFile = new FileInputStream(filePath);
	    blockchain.load(propFile);
	    propFile.close();
		CreateBlockchain cbc = new CreateBlockchain();
		String key = "";
		try {
		      Class.forName(driverName);
		    } catch (ClassNotFoundException e) {
		      e.printStackTrace();
		      System.exit(1);
		    }
		String hiveUsername = blockchain.getProperty("hive.username");
		String hivePassword = blockchain.getProperty("hive.password");
		String database = blockchain.getProperty("hive.database");
		String adminRole = blockchain.getProperty("main.role");
		Connection con = DriverManager.getConnection("jdbc:hive2://localhost:10000/metastore_db",hiveUsername,hivePassword);
		    Statement stmt = con.createStatement();
		    stmt.execute("use " + database);
		    ResultSet secret = stmt.executeQuery("SELECT KEY FROM REGISTRATION WHERE ROLE ='"+ adminRole +"'");
		    while(secret.next()){
		    	key = secret.getString(1);
		    }
		String previousHeader = "";
		String id = "1";
		String username = args[0];
		String datasetName = args[1];
		String method = args[2];
		String selectFlag  = args[3];
		if(selectFlag.equals("delete")){
			Runtime.getRuntime().exec("hdfs dfs -rm -r /blockchain/"+ datasetName + "_blockc");
		} else {
		String blockHeader = "";
		String dataItems = "field1,field2,field3";
		String simpleUsername = EncryptionClass.encryptValues(key, username);
		String simpleDataset = EncryptionClass.encryptValues(key, datasetName);
		String simpleDataItems = EncryptionClass.encryptValues(key, dataItems);
		String simpleMethod = EncryptionClass.encryptValues(key, method);
		String usrHash = cbc.provideHashValue(username);
		String dsNameHash = cbc.provideHashValue(datasetName);
		String dItemsHash = cbc.provideHashValue(dataItems);
		String methodHash = cbc.provideHashValue(method);
		String merkleRoot = cbc.calculateMRoot(usrHash, dsNameHash, dItemsHash, methodHash);
		merkleRoot = EncryptionClass.encryptValues(key, merkleRoot);
		String timeStamp = new SimpleDateFormat("MMddyyyyHHmmss").format(new Date());
		long num_of_bits = cbc.calculateBits(username + datasetName + dataItems + method);
		Random rand = new Random();
		int versionNumber = rand.nextInt((((Integer.parseInt(timeStamp.substring(3,8))) - (int)num_of_bits)-0)+1)-0;
		long versionNumberHex = cbc.decToHex((long)versionNumber);
		long num_of_bits_hex = cbc.decToHex(num_of_bits);
		long timeStampHex = cbc.decToHex(Long.parseLong(timeStamp));
		String littleEndianVN = cbc.littleEndian(String.valueOf(versionNumberHex));
		String littleEndianBits = cbc.littleEndian(String.valueOf(num_of_bits_hex));
		littleEndianBits = EncryptionClass.encryptValues(key, littleEndianBits);
		littleEndianVN = EncryptionClass.encryptValues(key, littleEndianVN);
		String littleEndianTime = cbc.littleEndian(String.valueOf(timeStampHex));
		littleEndianTime = EncryptionClass.encryptValues(key, littleEndianTime);
		String tableName = datasetName + "_blockc";
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://localhost:9000");
		conf.set("fs.hdfs.impl", 
		        org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
		    );
		    conf.set("fs.file.impl",
		        org.apache.hadoop.fs.LocalFileSystem.class.getName()
		    );
		    System.out.println(2);
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path("/blockchain/"+tableName + "/" + tableName);
		Path listPath = new Path("/blockchain/list");
		if(!fs.exists(listPath)){
			FSDataOutputStream fin = fs.create(listPath);
			fin.close();
		}
		if(fs.exists(path)){
			//Runtime.getRuntime().exec("hdfs dfs -get " + path + " " + "/usr/local/hadoop/temporary");
			String tempFile = blockchain.getProperty("cbc.tempFile");  
			File file = new File(tempFile);
			fs.copyToLocalFile(false, path, new Path(tempFile), true);
			System.out.println(5);
			FileReader fr = new FileReader(tempFile);
				BufferedReader br = new BufferedReader(fr);
				String sCurrentLine;
				br = new BufferedReader(new FileReader(tempFile));
				String lastLine = "";
			    while ((sCurrentLine = br.readLine()) != null) 
			    {
			        lastLine = sCurrentLine;
			    }
			    String[] elements = lastLine.split(",");
			    previousHeader = elements[4];
			    id = String.valueOf((Integer.parseInt(elements[0])+1));
			file.delete();
		}
		if (!fs.exists(path)){
			fs.setReplication(listPath, (short)1);
			FSDataOutputStream fin = fs.create(path);
			FSDataOutputStream appFin = fs.append(listPath);
			PrintWriter writer = new PrintWriter(appFin);
			   writer.append(tableName + "\n");
			   writer.close();
			   Thread th = new Thread();
			   th.sleep(60000);
			fin.close();
		}
		/*if(selectFlag.equals("notnew")&&cbc.checkTableExists(tableName, stmt)){
		ResultSet res = cbc.providePreviousData(tableName, stmt);
	    while (res.next()) {
	        id = res.getInt(1)+1;
	        previousHeader = res.getString(2);
	    	System.out.println(res.getInt(1) + " " + res.getString(2));
	         break;
	      }
		}*/
		//fs.close();
	    blockHeader = cbc.provideHashValue(littleEndianBits + littleEndianTime+ littleEndianVN + previousHeader + merkleRoot);
	    blockHeader = EncryptionClass.encryptValues(key, blockHeader);
	    //if(selectFlag.equals("notnew")){
	    cbc.addBlocks(blockchain,id, tableName, merkleRoot, blockHeader, stmt, simpleUsername, simpleDataItems, simpleDataset, simpleMethod, littleEndianBits, littleEndianTime, littleEndianVN);
		//}
	    fs.close();
	    
		}
		con.close();
	}

	public String provideHashValue(String text) throws NoSuchAlgorithmException {
		final String hashed = Hashing.sha256().hashString(text, StandardCharsets.UTF_8).toString();
		return hashed;
	}

	public String calculateMRoot(String usrHash, String dsNameHash, String dItemsHash, String methodHash)
			throws NoSuchAlgorithmException {
		return provideHashValue(provideHashValue(usrHash + dsNameHash) + provideHashValue(dItemsHash + methodHash));
	}

	public long calculateBits(String text) throws UnsupportedEncodingException {
		byte[] byteArray = text.getBytes("UTF-16BE");
		long sizeInBits = byteArray.length * 8;
		return sizeInBits;
	}
	public Long decToHex(long n) {
		  return Long.valueOf(String.valueOf(n), 16);
		}
	
	public String littleEndian(String hexString) {
	    int length = hexString.length() / 2;
	    char[] chars = new char[length * 2];
	    for (int index = 0; index < length; index++) {
	        int reversedIndex = length - 1 - index;
	        chars[reversedIndex * 2] = hexString.charAt(index * 2);
	        chars[reversedIndex * 2 + 1] = hexString.charAt(index * 2 + 1);
	    }
	    return new String(chars);
	}
	
	public void createBlockchain(String tableName) throws IOException, SQLException{
		Runtime.getRuntime().exec("hdfs dfs -touchz /blockchain/"+tableName + "/" + tableName);
		//stmt.execute("create table if not exists " + tableName + " (ID INT, VERSION_NUMBER STRING, TIME_STAMP STRING, NUMBER_OF_BITS STRING, MERKLE_ROOT STRING, BLOCK_HEADER STRING, ENC_USER_NAME STRING, ENC_DATASET STRING, ENC_DATA_ITEMS STRING, ENC_METHOD_OF_ACCESS STRING)");
	}
	
	 public  void addBlocks(Properties blockchain, String id, String tableName, String merkleRoot, String blockHeader, Statement stmt, String encUsername, String encDataitems, String encDataset, String encMethod, String bits, String time, String VN) throws Exception {
		 //System.out.println("insert into table " + tableName + " values('" + id + "', '" + VN + "','" + time +"','" + bits +"','" + merkleRoot +"','" + blockHeader +"','" + encUsername +"','" + encDataset + "','" + encDataitems +"','" + encMethod+ "')");
		 //stmt.execute("insert into table " + tableName + " values('" + id + "', '" + VN + "','" + time +"','" + bits +"','" + merkleRoot +"','" + blockHeader +"','" + encUsername +"','" + encDataset + "','" + encDataitems +"','" + encMethod+ "')");
		 Configuration conf = new Configuration();
			conf.set("fs.defaultFS", "hdfs://localhost:9000");
			conf.set("fs.hdfs.impl", 
			        org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
			    );
			    conf.set("fs.file.impl",
			        org.apache.hadoop.fs.LocalFileSystem.class.getName()
			    );
			    System.out.println(8);
			FileSystem fs = FileSystem.get(conf);
			Path path = new Path("/blockchain/"+tableName + "/" + tableName);
			String hdfsSitePath = blockchain.getProperty("hdfs.sitePath");
			conf.addResource(new File(hdfsSitePath).getAbsoluteFile().toURI().toURL());
			conf.reloadConfiguration();
			String f=conf.get("temporary.file.location");
			fs.setReplication(path, (short)1);
				/*FileWriter fw = new FileWriter(f);
				BufferedWriter bw = new BufferedWriter(fw);
				bw.write(VN + "," + time + "," + bits + "," + merkleRoot + "," + blockHeader + "," + encUsername + "," + encDataset + "," + encDataitems + "," + encMethod + "\n");
				bw.close();
				Runtime.getRuntime().exec("hdfs dfs -appendToFile /usr/local/hadoop/temp /blockchain/"+tableName);*/
				FSDataOutputStream fin = fs.append(path);
				PrintWriter writer = new PrintWriter(fin);
				   writer.append(id + "," + VN + "," + time + "," + bits + "," + merkleRoot + "," + blockHeader + "," + encUsername + "," + encDataset + "," + encDataitems + "," + encMethod + "\n");
				   writer.close();
				//fs.close();
				
				
	    }
	/* public ResultSet providePreviousData(String tableName, Statement stmt) throws SQLException{
		 ResultSet res = stmt.executeQuery("SELECT ID,BLOCK_HEADER FROM " + tableName + " ORDER BY ID DESC");
		 return res;
	 }*/
	 /*public boolean checkTableExists(String tablename, Statement stmt) throws SQLException{
		 String sql = "show tables in blockchain like " + tablename;
		 ResultSet res = stmt.executeQuery(sql);
		    if (res.next()) {
		      System.out.println(res.getString(1));
		    	return true;
		    } else {
		    	return false;
		    }
		 
	 }*/
	 
}