package hdfs;

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.OutputStream;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;




public class app1 {
	public static final String HDFS_PATH="hdfs://192.168.1.200:9000/eclipse";
	public static void main(String[] args) throws Exception {
      
		
		 HdfsOperation hdfs= new HdfsOperation(HDFS_PATH);
		
		 
		   FileStatus fileList[] = hdfs.getDirectoryFromHdfs("/111.txt");
		     int size = fileList.length;
		     for(int i = 0; i < size; i++){
		     System.out.println("name:" + fileList[i].getPath().getName() + "     tsize:" + fileList[i].getLen());
		     }
		   
		   
		
//		 }else{
//			 System.out.println("fail");
//		 }

//		Date day=new Date();  
//		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); 
//		System.out.println(df.format(day));
//		
//		hdfs.CopyloadToHdfs("d:/11.txt", "/father.rmvb");
//      hdfs.uploadToHdfs("d:/father.rmvb", "/mater.rmvb");
//		 hdfs.copyFromHdfstoLocal("/mater.rmvb", "d:/1234565432.txt");
		// OutputStream out = new OutputStream("tmp");
		 
		 //File outFile = new File("tmp"); 
		// OutputStream outStream = new FileOutputStream(outFile); 

	//	  hdfs.copyFromHdfsToLocal("/111.txt","c:/mather.txt");
	//byte[] arry=hdfs.LoadHdsfToMemory("/test.txt");
	//System.out.printf(String.valueOf(arry.length));
	//System.out.printf(new String(arry));
		
//		// OutputStream os = new FileOutputStream(fileName);
//		 ByteArrayOutputStream baos=new ByteArrayOutputStream(); 
//		 out.write(baos.toByteArray()); 
//		String str = baos.toString();
//		System.out.println(str);                 
//		 out.flush();
//		 out.close(); 


		
		
		
	}
}
