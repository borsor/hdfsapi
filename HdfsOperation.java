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
	 * �ϴ��ļ���HDFS
	 * localFile:��Ҫ�ϴ��ı����ļ�
	 * hdfsFile����Ҫ�ϴ���Ŀ��Ŀ¼���ļ���
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
	 * �ϴ��ļ���HDFS
	 * localFile:��Ҫ�ϴ��ı����ļ�
	 * hdfsFile����Ҫ�ϴ���Ŀ��Ŀ¼���ļ���
	 * hadoop��Ҫ�Ѽ�Ⱥ�ϵ�core-site.xml��hdfs-site.xml�ŵ���ǰ�����¡�eclipse����Ŀ¼��bin�ļ�������
	 * */
	public  Boolean copyLocalToHdfs(String localFile, String hdfsFile) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path srcPath = new Path(localFile); //ԭ·��
		Path dstPath = new Path(Hadoop_Path + hdfsFile); //Ŀ��·��
		//�����ļ�ϵͳ���ļ����ƺ���,ǰ�������ָ�Ƿ�ɾ��ԭ�ļ���trueΪɾ����Ĭ��Ϊfalse
		  
	    fs.copyFromLocalFile(false,srcPath, dstPath);
	     
		fs.close();
		return true;
	}
	
	 /**
	  *��HDFS�϶�ȡ�ļ�,�����浽byte[]���飬ע�⣺���ļ�������ɱ����ڴ����ռ�ã�һ��С��64M���ļ�����ʹ�ô˷�����
	  *System.out.printf(new String(arry));
	  * ��HDFS�϶�ȡ�ļ�,�����浽����*/
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
	    * ���ļ����浽���أ�
	    * @param hdfsFile:hdfs file
	    * @param dstfilename:local file
	    * @throws IOException
	    */
	 public  void copyFromHdfsToLocal(String hdfsFile,String dstfilename) throws IOException{
	    //	�޸�system.out������С�ļ�����ֱ�Ӷ�ȡ��
	        Configuration conf = new Configuration();
	        FileSystem fs = FileSystem.get(conf);
	        Path srcPath = new Path(Hadoop_Path+hdfsFile);
	        InputStream in = null;
	        OutputStream out = new FileOutputStream(dstfilename);
	        try {
	            in = fs.open(srcPath);
	            IOUtils.copyBytes(in, out, 4096, false); //���Ƶ���׼�����

	        } finally {
	            IOUtils.closeStream(in);
	        }
	       // IOUtils.copyBytes(in, out, buffSize, close)
	    }
	 
	 /**
	  * ɾ��hdfs�ļ�
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
	   * ����Ŀ¼
	   * @param pathDir:������Ŀ¼�����Ƽ�·����
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
	     * �ļ�������
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
	     * �������ļ�
	     */
	    public Boolean createFile(String dst , byte[] contents) throws IOException{
	        Configuration conf = new Configuration();
	        FileSystem fs = FileSystem.get(conf);
	        Path dstPath = new Path(Hadoop_Path+dst); //Ŀ��·��
	        //��һ�������
	        FSDataOutputStream outputStream = fs.create(dstPath);
	        outputStream.write(contents);
	        outputStream.close();
	        fs.close();
	        return true;
	    }

	    /*
	     * ����HDFS�ϵ��ļ���Ŀ¼,��ȡĳһ�ļ���ĳĿ¼�µ�ȫ���ļ���Ϣ��
	     * dstdir:Ŀ��Ŀ¼  ��  Ŀ���ļ�
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
	     * ��append��ʽ��������ӵ�HDFS���ļ���ĩβ;ע�⣺�ļ����£���Ҫ��hdfs-site.xml����<property><name>dfs.append.support</name><value>true</value></property>
	     *����append��������hadoop-0.21�汾��ʼ�Ͳ�֧���ˣ�����Append�Ĳ������Բο�Javaeye�ϵ�һƪ�ĵ���
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
