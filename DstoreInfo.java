import java.net.Socket;

public class DstoreInfo {
    private Socket socket;
    private int port;

    public DstoreInfo(Socket socket, int port) {
        this.socket = socket;
        this.port = port;
    }

    public Socket getSocket() {
        return socket;
    }

    public int getPort() {
        return port;
    }
}
