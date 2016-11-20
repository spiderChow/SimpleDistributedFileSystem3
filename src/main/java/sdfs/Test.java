package sdfs;

import sdfs.client.SDFSClient;
import sdfs.client.SDFSFileChannel;
import sdfs.datanode.DataNodeServer;
import sdfs.namenode.NameNodeServer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.rmi.NotBoundException;

/**
 * Created by shiyuhong on 16/11/19.
 */
public class Test {
    public static void main(String args[]) throws IOException, NotBoundException {
        System.out.println("Hello World!");
        NameNodeServer nameNodeServer=new NameNodeServer(1000000);
        DataNodeServer dataNodeServer=new DataNodeServer(64*1024);
        dataNodeServer.run();
        nameNodeServer.run();


        SDFSClient sdfsClient1=new SDFSClient();
        SDFSClient sdfsClient2=new SDFSClient();
        SDFSFileChannel sdfsFileChannel1=sdfsClient1.create("A.txt");
        SDFSFileChannel sdfsFileChannel2=sdfsClient1.create("A.txt");
        byte[] bytes={1,2,3};
        ByteBuffer byteBuffer=ByteBuffer.wrap(bytes);
        sdfsFileChannel1.write(byteBuffer);
        ByteBuffer byteBuffer2=ByteBuffer.wrap(bytes);
        sdfsFileChannel2.write(byteBuffer2);


        //SDFSClient sdfsClient2=new SDFSClient();

    }
}
