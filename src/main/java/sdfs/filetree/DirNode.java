package sdfs.filetree;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by pengcheng on 2016/11/15.
 */
public class DirNode implements Node {

    HashMap<Integer, String> children = new HashMap<Integer, String>();//inodeNum fileName
    int inodeNum = -1;

    public DirNode(int newInodeNum) {
        super();
        this.inodeNum=newInodeNum;
    }


    @Override
    public void load() {
        if (inodeNum > -1) {
            ObjectInputStream input=null;
            File file = new File("Nodes/" + inodeNum + ".node");
            try{
                input=new ObjectInputStream(new FileInputStream(file));
                DirNode dirNode=(DirNode)input.readObject();
                input.close();
                this.inodeNum=dirNode.inodeNum;
                this.children=dirNode.children;

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
            ObjectOutputStream output=null;
            try{
                output=new ObjectOutputStream(new FileOutputStream(file));
                output.writeObject(this);
                output.close();
            }catch (IOException e){
                e.printStackTrace();
            }
        }
    }

    public int  hasFile(String file) {
        int ret=-1;
        if (!children.isEmpty())
            for (Map.Entry<Integer, String> entry : children.entrySet()) {
                String name = entry.getValue();
                if (name.equals(file)) {
                    return entry.getKey();
                }
            }
        return ret;

    }


    public void createEntry(int num, String fileName) {
        children.put(num, fileName);
    }
}
