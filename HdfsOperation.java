package hdfs;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

public  class HdfsOperation {

	private static String Hadoop_Path = new String("");

	public  HdfsOperation(String hdfs_path) {
		Hadoop_Path = hdfs_path;
	}
	
	/**
	 * 上传文件到HDFS
	 * localFile:需要上传的本地文件
	 * hdfsFile：需要上传的目标目录及文件名
	 * */
	public  Boolean upLocalToHdfs(String localFile, String hdfsFile)
			throws FileNotFoundException, IOException {

		String dst = Hadoop_Path + hdfsFile;
		InputStream in = new BufferedInputStream(new FileInputStream(localFile));
		Configuration conf = new Configuration();

		FileSystem fs = FileSystem.get(URI.create(dst), conf);
		OutputStream out = fs.create(new Path(dst), new Progressable() {
			public void progress() {
				//System.out.print(".");
			}
		});

		IOUtils.copyBytes(in, out, 4096, true);
		      
		
		return true;
	}

	/**
	 * 上传文件到HDFS
	 * localFile:需要上传的本地文件
	 * hdfsFile：需要上传的目标目录及文件名
	 * hadoop需要把集群上的core-site.xml和hdfs-site.xml放到当前工程下。eclipse工作目录的bin文件夹下面
	 * */
	public  Boolean copyLocalToHdfs(String localFile, String hdfsFile) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path srcPath = new Path(localFile); //原路径
		Path dstPath = new Path(Hadoop_Path + hdfsFile); //目标路径
		//调用文件系统的文件复制函数,前面参数是指是否删除原文件，true为删除，默认为false
		  
	    fs.copyFromLocalFile(false,srcPath, dstPath);
	     
		fs.close();
		return true;
	}
	
	 /**
	  *将HDFS上读取文件,并保存到byte[]数组，注意：大文件可能造成本地内存大量占用，一般小于64M的文件可以使用此方法。
	  *System.out.printf(new String(arry));
	  * 从HDFS上读取文件,并保存到本地*/
	 public  byte[] loadHdsfToMemory(String hdfsFile) throws FileNotFoundException,IOException {
		 byte [] destArry=null;
		 
	  String src = Hadoop_Path + hdfsFile;
	  Configuration conf = new Configuration();  
	  FileSystem fs = FileSystem.get(URI.create(src), conf);
	  FSDataInputStream hdfsInStream = fs.open(new Path(src));
	  
	  
	  //OutputStream out = new FileOutputStream(dstfilename); 
	  
	  ByteArrayOutputStream bos= new ByteArrayOutputStream();
	  
	  byte[] ioBuffer = new byte[1024];
	  int readLen = hdfsInStream.read(ioBuffer);

	  while(-1 != readLen){
	  bos.write(ioBuffer, 0, readLen);  
	  
	  readLen = hdfsInStream.read(ioBuffer);
	  }
	  bos.close();
	  hdfsInStream.close();
	  fs.close();
	  destArry=bos.toByteArray();
	  return destArry;
	 }
	
	   /**
	    * 将文件保存到本地；
	    * @param hdfsFile:hdfs file
	    * @param dstfilename:local file
	    * @throws IOException
	    */
	 public  void copyFromHdfsToLocal(String hdfsFile,String dstfilename) throws IOException{
	    //	修改system.out：对于小文件可以直接读取；
	        Configuration conf = new Configuration();
	        FileSystem fs = FileSystem.get(conf);
	        Path srcPath = new Path(Hadoop_Path+hdfsFile);
	        InputStream in = null;
	        OutputStream out = new FileOutputStream(dstfilename);
	        try {
	            in = fs.open(srcPath);
	            IOUtils.copyBytes(in, out, 4096, false); //复制到标准输出流

	        } finally {
	            IOUtils.closeStream(in);
	        }
	       // IOUtils.copyBytes(in, out, buffSize, close)
	    }
	 
	 /**
	  * 删除hdfs文件
	  * @param filePath
	  * @throws IOException
	  */
	 public  Boolean  deleteHdfsFile(String hdfsFile) throws IOException{
	        Configuration conf = new Configuration();
	        FileSystem fs = FileSystem.get(conf);
	        Path path = new Path(Hadoop_Path+hdfsFile);
	        boolean isok = fs.deleteOnExit(path);
	        fs.close();
	        return isok;
	    }
	 
	  /**
	   * 创建目录
	   * @param pathDir:创建的目录的名称及路径；
	   */
	    public  Boolean  mkdir(String pathDir) throws IOException{
	        Configuration conf = new Configuration();
	        FileSystem fs = FileSystem.get(conf);
	        Path srcPath = new Path(Hadoop_Path+pathDir);
	        boolean isok = fs.mkdirs(srcPath);
	        fs.close();
	        return isok;
	    }
	    
	   
	    /**
	     * 文件重命名
	     */
	    public Boolean renameHdfsFile(String oldName,String newName) throws IOException{
	        Configuration conf = new Configuration();
	        FileSystem fs = FileSystem.get(conf);
	        Path oldPath = new Path(Hadoop_Path+oldName);
	        Path newPath = new Path(Hadoop_Path+newName);
	        boolean isok = fs.rename(oldPath, newPath);
	        fs.close();
	        return isok;
	    }

	    
	    /*
	     * 创建新文件
	     */
	    public Boolean createFile(String dst , byte[] contents) throws IOException{
	        Configuration conf = new Configuration();
	        FileSystem fs = FileSystem.get(conf);
	        Path dstPath = new Path(Hadoop_Path+dst); //目标路径
	        //打开一个输出流
	        FSDataOutputStream outputStream = fs.create(dstPath);
	        outputStream.write(contents);
	        outputStream.close();
	        fs.close();
	        return true;
	    }

	    /*
	     * 遍历HDFS上的文件和目录,获取某一文件或某目录下的全部文件信息。
	     * dstdir:目标目录  或  目标文件
	     * 
	     * 
	     */
	    public FileStatus [] getDirectoryFromHdfs(String dstdirOrdstfile) throws FileNotFoundException,IOException {
	     String dst = Hadoop_Path+dstdirOrdstfile;
	     Configuration conf = new Configuration();  
	     FileSystem fs = FileSystem.get(URI.create(dst), conf);
	     FileStatus fileList[] = fs.listStatus(new Path(dst));
//	     int size = fileList.length;
//	     for(int i = 0; i < size; i++){
//	     System.out.println("name:" + fileList[i].getPath().getName() + "/t/tsize:" + fileList[i].getLen());
//	     }
	     fs.close();
	     return fileList;
	    } 
	    
	    /*
	     * 以append方式将内容添加到HDFS上文件的末尾;注意：文件更新，需要在hdfs-site.xml中添<property><name>dfs.append.support</name><value>true</value></property>
	     *对于append操作，从hadoop-0.21版本开始就不支持了，关于Append的操作可以参考Javaeye上的一篇文档。
	     * */
	    
	    public void appendToHdfs(String dstFile,String contentstr) throws FileNotFoundException,IOException {
	    	String dst = Hadoop_Path+dstFile;
	     Configuration conf = new Configuration();  
	     FileSystem fs = FileSystem.get(URI.create(dst), conf);  
	     FSDataOutputStream out = fs.append(new Path(dst));


	     int readLen = contentstr.getBytes().length;

	     while(-1 != readLen){
	     out.write(contentstr.getBytes(), 0, readLen);
	     }
	     out.close();
	     fs.close();
	     
	    }

}
