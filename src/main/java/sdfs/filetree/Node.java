package sdfs.filetree;

import java.io.IOException;
import java.io.Serializable;

/**
 * Created by pengcheng on 2016/11/15.
 */
public interface Node extends Serializable {

    void load();


    void dump() ;
}
