package sdfs.namenode;

import sdfs.exception.IllegalAccessTokenException;
import sdfs.exception.SDFSFileAlreadyExistException;
import sdfs.filetree.DirNode;
import sdfs.filetree.FileNode;
import sdfs.protocol.INameNodeDataNodeProtocol;
import sdfs.protocol.INameNodeProtocol;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.nio.channels.OverlappingFileLockException;
import java.rmi.AlreadyBoundException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;

/**
 * Created by pengcheng on 2016/11/15.
 */
public class NameNodeServer extends UnicastRemoteObject implements INameNodeProtocol, INameNodeDataNodeProtocol, Runnable, Serializable {
    private long flushDiskInternalSeconds;
    public static long FLUSH_DISK_INTERNAL_SECONDS = 1000;
    int blockSize=64*1024;
    int blockNum = 0;
    private final HashMap<UUID, AccessTokenPermission> accessTokenPermissions = new HashMap<>();
    //private final ArrayList<Integer> readWriteRecord=new ArrayList<>();//inodeNum
    private final HashMap<Integer, UUID> readWriteRecord = new HashMap<>();
    //以上三者注意原子性操作!
    //mapping
    HashMap<UUID, FileNode> inMemmoryFileNodes = new HashMap<UUID, FileNode>();//inodeNum,FileNode
    //private final Map<> readWriteFile=new HashMap<>();

    public NameNodeServer(long flushDiskInternalSeconds) throws IOException {

        this.flushDiskInternalSeconds = flushDiskInternalSeconds;
        //建立root节点
        File file = new File("Nodes");
        if (!file.exists()) {
            file.mkdir();

            File root = new File("Nodes/0.node");
            if (!root.exists()) {
                DirNode dirNode = new DirNode(0);
                dirNode.dump();
            }
        }

    }

    @Override
    public void run() {
        try {
            LocateRegistry.createRegistry(6600);
            Naming.bind("rmi://127.0.0.1:6600/NameNodeServer", this);
        } catch (AlreadyBoundException e) {
            e.printStackTrace();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (RemoteException e) {
            e.printStackTrace();
        }
    }

    @Override
    public AccessTokenPermission getAccessTokenOriginalPermission(UUID fileAccessToken) throws RemoteException, IllegalAccessTokenException {
        if (!accessTokenPermissions.containsKey(fileAccessToken)) {
            throw new IllegalAccessTokenException();
        }
        Iterator<Map.Entry<UUID, AccessTokenPermission>> entryIterator = accessTokenPermissions.entrySet().iterator();
        while (entryIterator.hasNext()) {
            Map.Entry<UUID, AccessTokenPermission> entry = entryIterator.next();
            UUID uuid = entry.getKey();
            if (uuid.equals(fileAccessToken)) {
                return entry.getValue();
            }
        }
        return null;

    }

    @Override
    public Set<Integer> getAccessTokenNewBlocks(UUID fileAccessToken) throws RemoteException {
        return null;
    }


    @Override
    public SDFSFileChannelData openReadonly(String fileUri) throws IOException {
        UUID uuid = UUID.randomUUID();
        FileNode fileNode = parseFileURI_open(fileUri);
        SDFSFileChannelData sdfsFileChannelData = new SDFSFileChannelData(uuid, fileNode);
        //加一个记录
        accessTokenPermissions.put(uuid, new AccessTokenPermission(false, fileNode.allowBlocks()));
        inMemmoryFileNodes.put(uuid, fileNode);
      //  System.out.println(uuid + " " + fileNode.getFileNumber() + "readOnly " + fileNode.allowBlocks().size());
        return sdfsFileChannelData;
    }

    @Override
    public SDFSFileChannelData openReadwrite(String fileUri) throws OverlappingFileLockException, IOException {
        UUID uuid = UUID.randomUUID();
        FileNode fileNode = parseFileURI_open(fileUri);
        SDFSFileChannelData sdfsFileChannelData = new SDFSFileChannelData(uuid, fileNode);
        if (readWriteRecord.containsKey(fileNode.getFileNumber())) {
            //违反一个writer
            throw new OverlappingFileLockException();
        } else {
            readWriteRecord.put(fileNode.getFileNumber(), uuid);
            accessTokenPermissions.put(uuid, new AccessTokenPermission(true, fileNode.allowBlocks()));
            inMemmoryFileNodes.put(uuid, fileNode);
          //  System.out.println(uuid + " " + fileNode.getFileNumber() + "readWrite " + fileNode.allowBlocks().size());
        }
        return sdfsFileChannelData;
    }

    @Override
    public SDFSFileChannelData create(String fileUri) throws SDFSFileAlreadyExistException, RemoteException, FileNotFoundException {
        UUID uuid = UUID.randomUUID();
        FileNode fileNode = parseFileURI_create(fileUri);
        SDFSFileChannelData sdfsFileChannelData = new SDFSFileChannelData(uuid, fileNode);
        if (readWriteRecord.containsKey(fileNode.getFileNumber())) {
            //违反一个writer
            throw new OverlappingFileLockException();
        } else {
            readWriteRecord.put(fileNode.getFileNumber(), uuid);
            inMemmoryFileNodes.put(uuid, fileNode);
            accessTokenPermissions.put(uuid, new AccessTokenPermission(true, fileNode.allowBlocks()));
          //  System.out.println(uuid + " " + fileNode.getFileNumber() + "create " + fileNode.allowBlocks().size());

        }
        return sdfsFileChannelData;
    }

    @Override
    public void closeReadonlyFile(UUID fileAccessToken) throws IllegalAccessTokenException, IOException {
        if (!accessTokenPermissions.containsKey(fileAccessToken)) {
            throw new IllegalAccessTokenException();
        }
        if (accessTokenPermissions.get(fileAccessToken).isWriteable()) {
            throw new IllegalAccessTokenException();
        }
        accessTokenPermissions.remove(fileAccessToken);
        inMemmoryFileNodes.remove(fileAccessToken);
    }

    @Override
    public void closeReadwriteFile(UUID fileAccessToken, long newFileSize) throws IllegalAccessTokenException, IllegalArgumentException, IOException {
        if (!accessTokenPermissions.containsKey(fileAccessToken)) {
            throw new IllegalAccessTokenException();
        }
        if (!accessTokenPermissions.get(fileAccessToken).isWriteable()) {
            throw new IllegalAccessTokenException();
        }
        accessTokenPermissions.remove(fileAccessToken);

        int inodeNum = inodeFromUuid(fileAccessToken);
        readWriteRecord.remove(inodeNum);
        FileNode fileNode = inMemmoryFileNodes.get(fileAccessToken);
        inMemmoryFileNodes.remove(fileAccessToken);
        if (newFileSize > 0 && fileNode.allowBlocks().size() == 0) {
            throw new IllegalArgumentException();
        }
        if (newFileSize < 0) {
            throw new IllegalArgumentException();

        }
        if (newFileSize == 0 && fileNode.allowBlocks().size() > 0) {
            throw new IllegalArgumentException();
        }
        if(newFileSize>fileNode.allowBlocks().size()*blockSize){
            throw new IllegalArgumentException();
        }
        if(newFileSize<=(fileNode.allowBlocks().size()-1)*blockSize){
            throw new IllegalArgumentException();
        }
        //将所有的信息更新
       // System.out.println(fileAccessToken + " " + fileNode.getFileNumber() + " close " + fileNode.allowBlocks().size());
        fileNode.setSize(newFileSize);
        fileNode.dump();
    }

    @Override
    public void mkdir(String fileUri) throws SDFSFileAlreadyExistException, RemoteException {
        DirNode dirNode = parseDirURI_create(fileUri);
    }

    @Override
    public List<LocatedBlock> addBlocks(UUID fileAccessToken, int blockAmount) throws IllegalAccessTokenException, RemoteException {
        if (!accessTokenPermissions.containsKey(fileAccessToken)) {
            throw new IllegalAccessTokenException();
        }
        if (!accessTokenPermissions.get(fileAccessToken).isWriteable()) {
            throw new IllegalAccessTokenException();
        }
        if (blockAmount < 0) {
            throw new IllegalArgumentException();

        }
        List<LocatedBlock> ret = new ArrayList<LocatedBlock>();
        for (int i = 0; i < blockAmount; i++) {
            LocatedBlock locatedBlock = addBlock(fileAccessToken);
            ret.add(locatedBlock);
        }


        return ret;
    }

    public LocatedBlock addBlock(UUID fileAccessToken) throws IllegalAccessTokenException, RemoteException {
        try {
            InetAddress inetAddress = InetAddress.getLocalHost();
            int blockNum = newBlockNumber();
            FileNode fileNode = inMemmoryFileNodes.get(fileAccessToken);
            LocatedBlock locatedBlock = new LocatedBlock(inetAddress, blockNum);
            fileNode.addBlock(locatedBlock);
            inMemmoryFileNodes.replace(fileAccessToken, fileNode);
            Set<Integer> ints = accessTokenPermissions.get(fileAccessToken).getAllowBlocks();
            ints.add(blockNum);
            accessTokenPermissions.replace(fileAccessToken, new AccessTokenPermission(true, ints));
            return locatedBlock;
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void removeLastBlocks(UUID fileAccessToken, int blockAmount) throws IllegalAccessTokenException, IndexOutOfBoundsException, RemoteException {
        if (!accessTokenPermissions.containsKey(fileAccessToken)) {
            throw new IllegalAccessTokenException();
        }
        if (!accessTokenPermissions.get(fileAccessToken).isWriteable()) {
            throw new IllegalAccessTokenException();
        }
        if (blockAmount < 0) {
            throw new IllegalArgumentException();

        }
        for (int i = 0; i < blockAmount; i++) {
            removeLastBlock(fileAccessToken);
        }
    }

    public void removeLastBlock(UUID fileAccessToken) throws IllegalAccessTokenException, RemoteException {
        FileNode fileNode = inMemmoryFileNodes.get(fileAccessToken);
        int oldBlockNum = fileNode.removlastBlock();
        inMemmoryFileNodes.replace(fileAccessToken, fileNode);
        Set<Integer> ints = accessTokenPermissions.get(fileAccessToken).getAllowBlocks();
        ints.remove(oldBlockNum);
        accessTokenPermissions.replace(fileAccessToken, new AccessTokenPermission(true, ints));
    }

    @Override
    public LocatedBlock newCopyOnWriteBlock(UUID fileAccessToken, int fileBlockNumber) throws IllegalStateException, RemoteException {
        if (!accessTokenPermissions.containsKey(fileAccessToken)) {
            throw new IllegalAccessTokenException();
        }
        if (!accessTokenPermissions.get(fileAccessToken).isWriteable()) {
            throw new IllegalAccessTokenException();
        }
        try {
            InetAddress inetAddress = InetAddress.getLocalHost();
            int blockNum = newBlockNumber();
            //make mapping
            FileNode fileNode = inMemmoryFileNodes.get(fileAccessToken);
            int oldBlockNum = fileNode.blockNumberAt(fileBlockNumber);
            LocatedBlock locatedBlock = new LocatedBlock(inetAddress, blockNum);
            fileNode.updateBlockInfo(fileBlockNumber, locatedBlock);
            inMemmoryFileNodes.replace(fileAccessToken, fileNode);
            //update accessPermission
            Set<Integer> ints = accessTokenPermissions.get(fileAccessToken).getAllowBlocks();
            ints.remove(oldBlockNum);
            ints.add(blockNum);
            accessTokenPermissions.replace(fileAccessToken, new AccessTokenPermission(true, ints));
            return locatedBlock;
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }


        return null;
    }

    /**
     * make sure all dir ahead of the file/dir name to create exists
     * and the dir right ahead the file/dir Name does not have the same-name file/dir
     * and if success, create the entry for the name in the dir right ahead the file/dir name
     * return the file/dir Node
     *
     * @param fileUri
     * @return last name of the uri if success
     */
    private FileNode parseFileURI_create(String fileUri) throws SDFSFileAlreadyExistException, FileNotFoundException {
        //System.out.println(fileUri);
        FileNode ret = null;
        if (fileUri != null && !fileUri.equals("")) {
            if (fileUri.charAt(0) == '/') {

            } else {
                fileUri = "/" + fileUri;
            }

            String[] seg = fileUri.split("/");
            DirNode dirNode = new DirNode(0);
            dirNode.load();


            //the dir ahead is exit?
            for (int i = 1; i < seg.length - 1; i++) {
                int isfound = dirNode.hasFile(seg[i]);
                if (isfound > -1) {
                    dirNode = new DirNode(isfound);
                    dirNode.load();
                } else {
                    //not found 抛异常
                    throw new FileNotFoundException();

                }
            }
            //test if the DirNode at length -2 has the same fileNode
            String fileName = seg[seg.length - 1];
            // System.out.println(fileName);
            int sameFile = dirNode.hasFile(fileName);
            if (sameFile > -1) {
                //throw exception same-name file exist
                throw new SDFSFileAlreadyExistException();
            } else {
                //create new entry for the dirNode at Length-2
                int newInodeNum = newInodeNum();
                dirNode.createEntry(newInodeNum, fileName);
                dirNode.dump();
                ret = new FileNode(newInodeNum);
                ret.dump();
            }

        }
        return ret;
    }

    private FileNode parseFileURI_open(String fileUri) throws SDFSFileAlreadyExistException, FileNotFoundException {
        FileNode ret = null;
        if (fileUri != null && !fileUri.equals("")) {
            if (fileUri.charAt(0) == '/') {

            } else {
                fileUri = "/" + fileUri;
            }

            String[] seg = fileUri.split("/");
            DirNode dirNode = new DirNode(0);
            dirNode.load();

            //the dir ahead is exit?
            for (int i = 1; i < seg.length - 1; i++) {
                int isfound = dirNode.hasFile(seg[i]);
                if (isfound > -1) {
                    dirNode = new DirNode(isfound);
                    dirNode.load();
                } else {
                    //not found 抛异常
                }
            }
            //test if the DirNode at length -2 has the same fileNode
            String fileName = seg[seg.length - 1];
            int fileInode = dirNode.hasFile(fileName);
            if (fileInode > -1) {
                //存在
                ret = new FileNode(fileInode);
                ret.load();
            } else {
                //throw exception same-name file exist
                throw new FileNotFoundException();

            }

        }
        return ret;
    }

    private DirNode parseDirURI_create(String fileUri) throws SDFSFileAlreadyExistException {
        DirNode ret = null;
        if (fileUri != null && !fileUri.equals("")) {
            if (fileUri.charAt(0) == '/') {

            } else {
                fileUri = "/" + fileUri;
            }

            String[] seg = fileUri.split("/");
            DirNode dirNode = new DirNode(0);
            dirNode.load();

            //the dir ahead is exit?
            for (int i = 1; i < seg.length - 1; i++) {
                int isfound = dirNode.hasFile(seg[i]);
                if (isfound > -1) {
                    dirNode = new DirNode(isfound);
                    dirNode.load();
                } else {
                    //not found 抛异常
                }
            }
            //test if the DirNode at length -2 has the same dirNode
            String fileName = seg[seg.length - 1];
            int fileInode = dirNode.hasFile(fileName);
            //System.out.println(fileInode);
            if (fileInode > -1) {
                //存在 throw
                System.out.print("重复");
                throw new SDFSFileAlreadyExistException();

            } else {
                //create new entry for the dirNode at Length-2

                int newInodeNum = newInodeNum();
                dirNode.createEntry(newInodeNum, fileName);
                dirNode.dump();
                ret = new DirNode(newInodeNum);
                ret.dump();
                //throw new SDFSFileAlreadyExistException();

            }

        }
        return ret;
    }

    private int newInodeNum() {
        File file = new File("Nodes");
        String[] indexes = file.list();
        if (indexes.length == 0) {
            return 0;
        }
        Integer[] ints = new Integer[indexes.length];
        for (int i = 0; i < indexes.length; i++) {
            String index = indexes[i];
            index = index.substring(0, index.length() - 5);
            int num = Integer.parseInt(index);
            ints[i] = num;

        }

        int max = (int) Collections.max(Arrays.asList(ints));
        return (max + 1);

    }

    private int newBlockNumber() {
        blockNum++;
        return blockNum;
    }

    public void printState() {
        System.out.println("accessTokenPermissions:");
        Iterator<Map.Entry<UUID, AccessTokenPermission>> entryIterator = accessTokenPermissions.entrySet().iterator();
        while (entryIterator.hasNext()) {
            Map.Entry<UUID, AccessTokenPermission> entry = entryIterator.next();
            System.out.println(entry.getKey());
        }
        System.out.println("readWriteRecord:");
        Iterator<Map.Entry<Integer, UUID>> entryIterator2 = readWriteRecord.entrySet().iterator();
        while (entryIterator2.hasNext()) {
            Map.Entry<Integer, UUID> entry = entryIterator2.next();
            System.out.println(entry.getKey() + " " + entry.getValue());
        }

    }

    private int inodeFromUuid(UUID fileAccessToken) {

        int inodeNum = -1;
        Iterator<Map.Entry<Integer, UUID>> entryIterator = readWriteRecord.entrySet().iterator();
        while (entryIterator.hasNext()) {
            Map.Entry<Integer, UUID> entry = entryIterator.next();
            UUID uuid = entry.getValue();
            if (uuid.equals(fileAccessToken)) {
                inodeNum = entry.getKey();
                return inodeNum;
            }
        }
        return inodeNum;
    }
}
