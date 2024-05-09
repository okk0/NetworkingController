import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class FileInfo {
    String status; // The status of the file, e.g., "store complete"
    Set<String> dstores; // A set of Dstores that contain this file

    // Constructor that initializes status and creates an empty set of Dstores
    public FileInfo(String status) {
        this.status = status;
        this.dstores = ConcurrentHashMap.newKeySet(); // Initialize the Dstores set
    }

    // Optional: Override toString for better logging/debugging
    @Override
    public String toString() {
        return "FileInfo{status='" + status + "', dstores=" + dstores + "}";
    }
}
