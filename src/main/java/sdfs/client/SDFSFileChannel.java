/*
 * Copyright (c) Jipzingking 2016.
 */

package sdfs.client;

import sdfs.filetree.FileNode;
import sdfs.namenode.LocatedBlock;
import sdfs.namenode.SDFSFileChannelData;
import sdfs.protocol.IDataNodeProtocol;
import sdfs.protocol.INameNodeProtocol;

import java.io.Flushable;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.SeekableByteChannel;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.util.UUID;

public class SDFSFileChannel implements SeekableByteChannel, Flushable, Serializable {
    private static final long serialVersionUID = 6892411224902751501L;
    private UUID accessToken;
    private FileNode fileNode;
    private long fileSize;
    private long position;
    private final boolean isReadOnly;
    private int Block_size = 64 * 1024;
    private boolean isOpen = true;


    public SDFSFileChannel(SDFSFileChannelData sdfsFileChannelData, boolean isReadOnly) {
        this.accessToken = sdfsFileChannelData.getAccessToken();
        this.fileNode = sdfsFileChannelData.getFileNode();
        this.isReadOnly = isReadOnly;
        this.fileSize = fileNode.getFileSize();

    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        System.out.println("read 之后 " + fileNode.allowBlocks().size());
        if (!isOpen)
            throw new ClosedChannelException();
        //要和DataNode通讯
        try {
            //调用远
            // 程对象，注意RMI路径与接口必须与服务器配置一致
            int ret = 0;
            IDataNodeProtocol dataNodeServer = (IDataNodeProtocol) Naming.lookup("rmi://127.0.0.1:6611/DataNodeServer");
            Block_size = dataNodeServer.getBlockSize();
            long bytesCanRead = 0;
            if (fileSize < dst.remaining() + position) {
                bytesCanRead = fileSize - position;
            } else {
                bytesCanRead = dst.remaining();
            }

            while (bytesCanRead > 0) {
                int blockIndex = (int) (position / Block_size);
                int offset = (int) (position - blockIndex * Block_size);
                int left = Block_size - offset;
                if (bytesCanRead < left) {
                    left = (int) bytesCanRead;
                }
                int blockNum = fileNode.blockNumberAt(blockIndex);
                byte[] bytes = dataNodeServer.read(accessToken, blockNum, offset, left);
                position += left;
                ret += left;
                bytesCanRead = bytesCanRead - left;
                dst.put(bytes);
            }
            return ret;
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return -1;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        System.out.println("write 之前 " + fileNode.allowBlocks().size());
        if (!isOpen)
            throw new ClosedChannelException();
        if (isReadOnly)
            throw new NonWritableChannelException();
        //调用datanode 然后
        try {
            //先和nameNode通讯,得到新的blockNum
            //调用远程对象，注意RMI路径与接口必须与服务器配置一致

            IDataNodeProtocol dataNodeServer = (IDataNodeProtocol) Naming.lookup("rmi://127.0.0.1:6611/DataNodeServer");

            //write(UUID fileAccessToken, int blockNumber, long position, byte[] buffer)
            Block_size = dataNodeServer.getBlockSize();

            int ret = 0;
            truncate((int) (src.remaining() + position));
            if (src.remaining() + position > fileSize) {
                fileSize = position + src.remaining();
            } else {

            }

            while (src.hasRemaining()) {
                System.out.println("还剩" + src.remaining());
                BlockWirteInfo blockWirteInfo = blockAtPosition(position, src);
                ret += blockWirteInfo.blockBuffer.length;
                //fileSize+=blockWirteInfo.blockBuffer.length;
                dataNodeServer.write(accessToken, blockWirteInfo.getBlockNumber(), blockWirteInfo.blockPosition, blockWirteInfo.getBlockBuffer());
            }
            System.out.println("write 之后 " + fileNode.allowBlocks().size());
            return (int) (ret);


        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return -1;
    }

    private BlockWirteInfo blockAtPosition(long position, ByteBuffer src) {
        int blockNumberIndex = (int) (position / Block_size);//应该写在第几个block
        int byteLeft = (int) (position - blockNumberIndex * Block_size);//那个block的偏移量
        long high = Block_size * (blockNumberIndex + 1);
        int remaining = src.remaining();
        if (remaining + position < high)
            high = remaining + position;

        this.position = high;
        byte[] blockbuffer = new byte[(int) (high - position)];//要写进去的字符们
        src.get(blockbuffer, 0, blockbuffer.length);
        LocatedBlock locatedBlock = null;
        int blockNumber = -1;
        try {
            blockNumber = fileNode.blockNumberAt(blockNumberIndex);
        } catch (IndexOutOfBoundsException e) {
            //还未分配这个block,所以需要和NameNode通讯
            try {
                //调用远程对象，注意RMI路径与接口必须与服务器配置一致
                INameNodeProtocol nameNodeServer = (INameNodeProtocol) Naming.lookup("rmi://127.0.0.1:6600/NameNodeServer");
                System.out.println(accessToken + "here");
                locatedBlock = nameNodeServer.addBlock(accessToken);
                fileNode.addBlock(locatedBlock);

            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }


        //已经分配了这个block,也要通讯得到新的Blocknum
        try {
            //调用远程对象，注意RMI路径与接口必须与服务器配置一致
            INameNodeProtocol nameNodeServer = (INameNodeProtocol) Naming.lookup("rmi://127.0.0.1:6600/NameNodeServer");
            IDataNodeProtocol dataNodeServer = (IDataNodeProtocol) Naming.lookup("rmi://127.0.0.1:6611/DataNodeServer");

            locatedBlock = nameNodeServer.newCopyOnWriteBlock(accessToken, blockNumberIndex);
            dataNodeServer.copyOnWrite(accessToken, blockNumber, locatedBlock.getBlockNumber());
            //更新自己这里的信息
            fileNode.updateBlockInfo(blockNumberIndex, locatedBlock);
        } catch (Exception ex) {
            ex.printStackTrace();
        }


        BlockWirteInfo blockWirteInfo = new BlockWirteInfo(locatedBlock.getBlockNumber(), byteLeft, blockbuffer);
        return blockWirteInfo;

    }

    @Override
    public long position() throws IOException {
        if (!isOpen)
            throw new ClosedChannelException();
        return position;
    }

    @Override
    public SeekableByteChannel position(long newPosition) throws IOException {
        if (!isOpen)
            throw new ClosedChannelException();
        if (newPosition < 0) {
            throw new IllegalArgumentException();
        }
        position = newPosition;
        return this;

    }

    @Override
    public long size() throws IOException {
        if (!isOpen)
            throw new ClosedChannelException();

        return fileSize;
    }

    @Override
    public SeekableByteChannel truncate(long size) throws IOException {
        if (!isOpen)
            throw new ClosedChannelException();
        if (isReadOnly)
            throw new NonWritableChannelException();
        //this.fileSize=size;
        if (this.fileSize > size) {
            //缩小
            INameNodeProtocol nameNodeServer = null;
            try {
                nameNodeServer = (INameNodeProtocol) Naming.lookup("rmi://127.0.0.1:6600/NameNodeServer");

                this.fileSize = size;
                int newBlockAmount = (int) (fileSize / Block_size);
                if (newBlockAmount * Block_size != fileSize) {
                    newBlockAmount++;
                }
                int n = fileNode.allowBlocks().size();
                for (int i = newBlockAmount; i < n; i++) {
                    fileNode.removlastBlock();

                }
                nameNodeServer.removeLastBlocks(accessToken, n - newBlockAmount);
                if (position >= fileSize) {
                    position = fileSize;
                }
            } catch (NotBoundException e) {
                e.printStackTrace();
            }
        }
        //实际读进来的才是file的大小
        return null;
    }

    @Override
    public boolean isOpen() {

        return isOpen;

    }

    @Override
    public void close() throws IOException {
        if (isOpen) {
            try {
                //调用远程对象，注意RMI路径与接口必须与服务器配置一致
                INameNodeProtocol nameNodeServer = (INameNodeProtocol) Naming.lookup("rmi://127.0.0.1:6600/NameNodeServer");
                if (isReadOnly) {
                    nameNodeServer.closeReadonlyFile(accessToken);
                } else {
                    nameNodeServer.closeReadwriteFile(accessToken, fileSize);
                }

                isOpen = false;

            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

    @Override
    public void flush() throws IOException {
        if (!isOpen)
            throw new ClosedChannelException();
        //todo your code here
    }


    class BlockWirteInfo implements Serializable {
        private int blockNumber;
        private long blockPosition;
        private byte[] blockBuffer;

        public BlockWirteInfo(int blockNumber, long blockPosition, byte[] blockBuffer) {
            this.setBlockNumber(blockNumber);
            this.setBlockPosition(blockPosition);
            this.setBlockBuffer(blockBuffer);
        }


        public int getBlockNumber() {
            return blockNumber;
        }

        public void setBlockNumber(int blockNumber) {
            this.blockNumber = blockNumber;
        }

        public long getBlockPosition() {
            return blockPosition;
        }

        public void setBlockPosition(long blockPosition) {
            this.blockPosition = blockPosition;
        }

        public byte[] getBlockBuffer() {
            return blockBuffer;
        }

        public void setBlockBuffer(byte[] blockBuffer) {
            this.blockBuffer = blockBuffer;
        }
    }
}
