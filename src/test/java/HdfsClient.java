import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

public class HdfsClient {

    private FileSystem fs = null;

    @Before
    public void setUp() throws Exception {

        // 构造一个配置参数对象，设置一个参数:我们要访问的 hdfs 的 URI
        // 从而 FileSystem.get()方法就知道应该是去构造一个访问 hdfs 文件系统的客户端，以及 hdfs 的 访问地址
        // new Configuration();的时候，它就会去加载 jar 包中的 hdfs-default.xml
        // 然后再加载 classpath 下的 hdfs-site.xml

        Configuration conf = new Configuration();

        conf.set("fs.defaultFS", "hdfs://hdp-node01:9000");

        /*
         参数优先级:
             1、客户端代码中设置的值
             2、classpath 下的用户自定义配置文件
             3、服务器的默认配置
         */
        conf.set("dfs.replication", "3");

        // 获取一个 hdfs 的访问客户端，根据参数，这个实例应该是 DistributedFileSystem 的实例
        // fs = FileSystem.get(conf);
        // 如果这样去获取，那 conf 里面就可以不要配"fs.defaultFS"参数，而且，这个客户端的身份标识已经是 hadoop 用户
        fs = FileSystem.get(new URI("hdfs://localhost:9000"), conf, "hungrated");
    }

    @After
    public void tearDown() {

    }

    /**
     * 往 hdfs 上传文件
     *
     * @throws Exception 文件操作失败时抛出该异常
     */
    @Test
    public void testAddFileToHdfs() throws Exception {

        // 要上传的文件所在的本地路径
        Path src = new Path("g:/redis-recommend.zip");

        // 要上传到 hdfs 的目标路径
        Path dst = new Path("/aaa");
        fs.copyFromLocalFile(src, dst);

        fs.close();
    }

    /**
     * 从 hdfs 中复制文件到本地文件系统
     *
     * @throws IOException              文件操作失败时抛出该异常
     * @throws IllegalArgumentException 参数错误时抛出该异常
     */

    @Test
    public void testDownloadFileToLocal() throws IllegalArgumentException, IOException {
        fs.copyToLocalFile(new Path("/jdk-7u65-linux-i586.tar.gz"), new Path("d:/"));
        fs.close();
    }

    /**
     * 在 hdfs 中进行目录增删改操作
     *
     * @throws IOException              目录操作失败时抛出该异常
     * @throws IllegalArgumentException 参数错误时抛出该异常
     */

    @Test
    public void testMkdirAndDeleteAndRename() throws IllegalArgumentException, IOException {

        // 创建目录
        fs.mkdirs(new Path("/a1/b1/c1"));

        // 删除文件夹 ，如果是非空文件夹，参数 2 必须给值 true fs.delete(new Path("/aaa"), true);
        // 重命名文件或文件夹
        fs.rename(new Path("/a1"), new Path("/a2"));
    }


    /**
     * 查看文件信息
     *
     * @throws FileNotFoundException    文件未找到时抛出该异常
     * @throws IOException              目录操作失败时抛出该异常
     * @throws IllegalArgumentException 参数错误时抛出该异常
     */
    @Test
    public void testListFiles() throws FileNotFoundException, IllegalArgumentException, IOException {

        // 思考：为什么返回迭代器，而不是 List 之类的容器
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);

        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();
            System.out.println(fileStatus.getPath().getName());
            System.out.println(fileStatus.getBlockSize());
            System.out.println(fileStatus.getPermission());
            System.out.println(fileStatus.getLen());
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            for (BlockLocation bl : blockLocations) {
                System.out.println("block-length:" + bl.getLength() + "--" + "block-offset:" + bl.getOffset());
                String[] hosts = bl.getHosts();
                for (String host : hosts) {
                    System.out.println(host);
                }
            }
            System.out.println("----------------------------");
        }
    }

    /**
     * 查看文件及文件夹信息
     *
     * @throws FileNotFoundException    文件未找到时抛出该异常
     * @throws IOException              目录操作失败时抛出该异常
     * @throws IllegalArgumentException 参数错误时抛出该异常
     */
    @Test
    public void testListAll() throws FileNotFoundException, IllegalArgumentException, IOException {
        FileStatus[] listStatus = fs.listStatus(new Path("/"));
        String flag = "d-- ";
        for (FileStatus fstatus : listStatus) {
            if (fstatus.isFile()) flag = "f-- ";
            System.out.println(flag + fstatus.getPath().getName());
        }
    }

}
