package sdfs.filetree;

import sdfs.namenode.LocatedBlock;

import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by pengcheng on 2016/11/15.
 */
public class FileNode implements Node {
    int inodeNum = -1;
    public ArrayList<LocatedBlock> locatedBlocks = new ArrayList<>();//有顺序
    private long fileSize=0;


    public FileNode(int inodeNum) {
        this.inodeNum = inodeNum;
    }


    @Override
    public void load() {
        if (inodeNum > -1) {
            ObjectInputStream input = null;
            File file = new File("Nodes/" + inodeNum + ".node");
            try {
                input = new ObjectInputStream(new FileInputStream(file));
                FileNode fileNode = (FileNode) input.readObject();
                input.close();
                this.inodeNum = fileNode.inodeNum;
                this.locatedBlocks = fileNode.locatedBlocks;
                this.fileSize=fileNode.fileSize;

            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }


        }
    }

    @Override
    public void dump() {
        if (inodeNum > -1) {

            File file = new File("Nodes/" + inodeNum + ".node");
            ObjectOutputStream output = null;
            try {
                output = new ObjectOutputStream(new FileOutputStream(file));
                output.writeObject(this);
                output.flush();
                output.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    public int blockNumberAt(int blockNumberIndex) {
        if (blockNumberIndex >= locatedBlocks.size()) {
            throw new IndexOutOfBoundsException();
        }
        LocatedBlock locatedBlock = locatedBlocks.get(blockNumberIndex);
        return locatedBlock.getBlockNumber();
    }

    public int getFileNumber() {
        return inodeNum;
    }

    public Set<Integer> allowBlocks() {
        Set<Integer> set = new HashSet<>();
        for (LocatedBlock l : locatedBlocks) {
            set.add(l.getBlockNumber());
        }
        return set;
    }

    public void updateBlockInfo(int blockNumberIndex, LocatedBlock locatedBlock) {
        if (locatedBlocks.size() == blockNumberIndex) {
            locatedBlocks.add(locatedBlock);
        } else
            locatedBlocks.set(blockNumberIndex, locatedBlock);
    }

    public void addBlock(LocatedBlock locatedBlock) {
        locatedBlocks.add(locatedBlock);
    }

    public void setSize(long size) {
        this.setFileSize(size);
    }

    public int removlastBlock() {
        locatedBlocks.remove(locatedBlocks.size()-1);
        return locatedBlocks.size();
    }

    public long getFileSize() {
        return fileSize;
    }

    public void setFileSize(long fileSize) {
        this.fileSize = fileSize;
    }
}
