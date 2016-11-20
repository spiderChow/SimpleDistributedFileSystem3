package sdfs.client;

import sdfs.exception.SDFSFileAlreadyExistException;
import sdfs.namenode.NameNodeServer;
import sdfs.namenode.SDFSFileChannelData;
import sdfs.protocol.INameNodeProtocol;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;

/**
 * Created by shiyuhong on 16/11/18.
 */
public class SDFSClient implements ISDFSClient {


    @Override
    public SDFSFileChannel openReadonly(String fileUri) throws IOException, NotBoundException {
        INameNodeProtocol nameNodeServer=(INameNodeProtocol) Naming.lookup("rmi://127.0.0.1:6600/NameNodeServer");
        SDFSFileChannelData sdfsFileChannelData=nameNodeServer.openReadonly(fileUri);
        SDFSFileChannel sdfsFileChannel=new SDFSFileChannel(sdfsFileChannelData,true);
        return sdfsFileChannel;
    }

    @Override
    public SDFSFileChannel openReadWrite(String fileUri) throws IOException, NotBoundException {
        INameNodeProtocol nameNodeServer=(INameNodeProtocol) Naming.lookup("rmi://127.0.0.1:6600/NameNodeServer");
        SDFSFileChannelData sdfsFileChannelData=nameNodeServer.openReadwrite(fileUri);
        SDFSFileChannel sdfsFileChannel=new SDFSFileChannel(sdfsFileChannelData,false);
        return sdfsFileChannel;
    }

    @Override
    public SDFSFileChannel create(String fileUri) throws SDFSFileAlreadyExistException, RemoteException, NotBoundException, MalformedURLException, FileNotFoundException {
            INameNodeProtocol nameNodeServer=(INameNodeProtocol) Naming.lookup("rmi://127.0.0.1:6600/NameNodeServer");
            SDFSFileChannelData sdfsFileChannelData=nameNodeServer.create(fileUri);
            SDFSFileChannel sdfsFileChannel=new SDFSFileChannel(sdfsFileChannelData,false);
            return sdfsFileChannel;
    }

    @Override
    public void mkdir(String fileUri) throws SDFSFileAlreadyExistException, RemoteException, MalformedURLException, NotBoundException {
            INameNodeProtocol nameNodeServer=(INameNodeProtocol) Naming.lookup("rmi://127.0.0.1:6600/NameNodeServer");
            nameNodeServer.mkdir(fileUri);
    }
}
