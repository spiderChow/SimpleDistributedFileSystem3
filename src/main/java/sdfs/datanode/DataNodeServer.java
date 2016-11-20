package sdfs.datanode;

import sdfs.client.SDFSFileChannel;
import sdfs.exception.IllegalAccessTokenException;
import sdfs.namenode.AccessTokenPermission;
import sdfs.namenode.LocatedBlock;
import sdfs.namenode.NameNodeServer;
import sdfs.namenode.SDFSFileChannelData;
import sdfs.protocol.IDataNodeProtocol;
import sdfs.protocol.INameNodeDataNodeProtocol;
import sdfs.protocol.INameNodeProtocol;

import java.io.*;
import java.net.MalformedURLException;
import java.nio.file.Files;
import java.rmi.AlreadyBoundException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;

/**
 * Created by pengcheng on 2016/11/15.
 */
public class DataNodeServer extends UnicastRemoteObject
        implements IDataNodeProtocol, INameNodeDataNodeProtocol, Runnable, Serializable {
    // private INameNodeDataNodeProtocol data2name;are u kidding?!

    //cache from NameNode
    HashMap<AccessTokenPermission, Set<Integer>> blockInfosCache = new HashMap<>();
    HashMap<AccessTokenPermission, Set<Integer>> newblockInfosCache = new HashMap<>();

    public int blockSize;
    public static int BLOCK_SIZE = 64 * 1024;


    public DataNodeServer(int blockSize) throws RemoteException {
        super();
        this.blockSize = blockSize;
        File file = new File("Blocks");
        if (!file.exists()) {
            file.mkdir();
        }
    }

    //所有对这个文件的操作都结束之后才可以merge
    //加锁! 加锁!
    private void mergeCache() {
        Iterator<Map.Entry<AccessTokenPermission, Set<Integer>>> entryIterator = newblockInfosCache.entrySet().iterator();
        while (entryIterator.hasNext()) {
            Map.Entry<AccessTokenPermission, Set<Integer>> entry = entryIterator.next();
            AccessTokenPermission token = entry.getKey();
            blockInfosCache.remove(token);
            blockInfosCache.put(token, entry.getValue());
            newblockInfosCache.remove(token);
        }
    }

    @Override
    public byte[] read(UUID fileAccessToken, int blockNumber, long position, int size) throws IllegalAccessTokenException, IllegalArgumentException, IOException {
        AccessTokenPermission accessTokenPermission = getAccessTokenOriginalPermission(fileAccessToken);
        if (accessTokenPermission == null) {
            throw new IllegalAccessTokenException();
        }
        Set<Integer> allowBlocks = accessTokenPermission.getAllowBlocks();
        if (!allowBlocks.contains(blockNumber)) {
            throw new IllegalAccessTokenException();
        }


        if (position + size > BLOCK_SIZE) {
            throw new IllegalArgumentException();
        }
        File block = new File("Blocks/" + blockNumber + ".block");
        RandomAccessFile randomAccessFile = new RandomAccessFile(block, "rw");
        randomAccessFile.seek(position);
        byte[] bytes = new byte[size];
        randomAccessFile.read(bytes, 0, size);
        randomAccessFile.close();
        //return success
        return bytes;

    }

    @Override
    public void write(UUID fileAccessToken, int blockNumber, long position, byte[] buffer) throws IllegalAccessTokenException, IllegalArgumentException, IOException {
        AccessTokenPermission accessTokenPermission = getAccessTokenOriginalPermission(fileAccessToken);
        if (accessTokenPermission == null) {
            throw new IllegalAccessTokenException();
        }
        if(!accessTokenPermission.isWriteable()){
            throw new IllegalAccessTokenException();
        }
        Set<Integer> allowBlocks = accessTokenPermission.getAllowBlocks();
        if (!allowBlocks.contains(blockNumber)) {
            throw new IllegalAccessTokenException();
        }

        //write
        if (position + buffer.length > BLOCK_SIZE) {
            throw new IllegalArgumentException();
        }
        File block = new File("Blocks/" + blockNumber + ".block");
        RandomAccessFile randomAccessFile = new RandomAccessFile(block, "rw");
        randomAccessFile.seek(position);

        //  System.out.println(fileAccessToken + " write " + blockNumber + " " + Arrays.toString(buffer));
        randomAccessFile.write(buffer);


        randomAccessFile.close();
        //return success

    }

    @Override
    public AccessTokenPermission getAccessTokenOriginalPermission(UUID fileAccessToken) throws RemoteException, IllegalAccessTokenException {
        try {
            INameNodeDataNodeProtocol nameNodeServer = (INameNodeDataNodeProtocol) Naming.lookup("rmi://127.0.0.1:6600/NameNodeServer");
            AccessTokenPermission accessTokenPermission = nameNodeServer.getAccessTokenOriginalPermission(fileAccessToken);
            return accessTokenPermission;
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }

    @Override
    public Set<Integer> getAccessTokenNewBlocks(UUID fileAccessToken) throws RemoteException {
        try {
            INameNodeDataNodeProtocol nameNodeServer = (INameNodeDataNodeProtocol) Naming.lookup("rmi://127.0.0.1:6600/NameNodeServer");
            Set<Integer> allowBlocks = nameNodeServer.getAccessTokenNewBlocks(fileAccessToken);
            return allowBlocks;
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }

    @Override
    public void run() {
        try {
            LocateRegistry.createRegistry(6611);
            Naming.bind("rmi://127.0.0.1:6611/DataNodeServer", new DataNodeServer(1024 * 64));
        } catch (AlreadyBoundException e) {
            e.printStackTrace();
        } catch (RemoteException e) {
            e.printStackTrace();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
    }

    public int getBlockSize() {
        return blockSize;
    }


    public void copyOnWrite(UUID accessToken, int blockNumber, int newblockNumber) throws IllegalAccessTokenException, IOException {
        AccessTokenPermission accessTokenPermission = getAccessTokenOriginalPermission(accessToken);
        Set<Integer> allowBlocks = accessTokenPermission.getAllowBlocks();
        boolean isValid = allowBlocks.contains(blockNumber);
        if (isValid) {
            //copy on write
            File oldblock = new File("Blocks/" + blockNumber + ".block");
            File newblock = new File("Blocks/" + newblockNumber + ".block");
            newblock.createNewFile();
            Files.copy(oldblock.toPath(), newblock.toPath());
        }
    }
}
