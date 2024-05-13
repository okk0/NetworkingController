import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class FileInfo {
    String status; 
    Set<String> dstores; 
    long fileSize;  

    public FileInfo(String status, long fileSize) {
        this.status = status;
        this.fileSize = fileSize;
        this.dstores = ConcurrentHashMap.newKeySet(); 
    }

    @Override
    public String toString() {
        return "FileInfo{status='" + status + "', fileSize=" + fileSize + ", dstores=" + dstores + "}";
    }

    public long getFileSize() {
        return fileSize;
    }

    public void setFileSize(long fileSize) {
        this.fileSize = fileSize;
    }
}