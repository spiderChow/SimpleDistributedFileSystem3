package sdfs.namenode;

import sdfs.filetree.FileNode;

import java.io.Serializable;
import java.util.UUID;

/**
 * Created by pengcheng on 2016/11/15.
 */
public class SDFSFileChannelData implements Serializable {
    private static final long serialVersionUID = 5725498307666004432L;

    private UUID accessToken;
    private FileNode fileNode;

    public SDFSFileChannelData(UUID accessToken, FileNode fileNode) {
        this.setAccessToken(accessToken);
        this.setFileNode(fileNode);
    }

    public UUID getAccessToken() {
        return accessToken;
    }

    public void setAccessToken(UUID accessToken) {
        this.accessToken = accessToken;
    }

    public FileNode getFileNode() {
        return fileNode;
    }

    public void setFileNode(FileNode fileNode) {
        this.fileNode = fileNode;
    }
}
