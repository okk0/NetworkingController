import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.util.Comparator;

public class Dstore {
    private ServerSocket serverSocket;
    private int port;
    private Socket controllerSocket;
    private PrintWriter controllerOut;
    private BufferedReader controllerIn;
    private String controllerHost;
    private int controllerPort;
    private boolean running = true;
    private String fileFolder;
    private int timeout; // Timeout in milliseconds

    public Dstore(int port, String controllerHost, int controllerPort, int timeout, String fileFolder) {
        this.port = port;
        this.controllerHost = controllerHost;
        this.controllerPort = controllerPort;
        this.timeout = timeout;
        this.fileFolder = fileFolder;
    }

    public void start() throws IOException {
        clearLocalData();
        connectToController();
        serverSocket = new ServerSocket(port);  // Listen for client connections
        System.out.println("Dstore listening on port: " + port);

        new Thread(this::acceptClientConnections).start();  // Handle client connections
    }

    private void acceptClientConnections() {
        while (running) {
            try {
                Socket clientSocket = serverSocket.accept();
                handleClientConnection(clientSocket);
            } catch (IOException e) {
                System.out.println("Error accepting client connection: " + e.getMessage());
            }
        }
    }

    private void handleClientConnection(Socket clientSocket) {
        try {
            // Stream for reading text data such as commands
            BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            PrintWriter writer = new PrintWriter(clientSocket.getOutputStream(), true);
            
            // Read the STORE command header
            String header = reader.readLine();
            if (header == null) return;
    
            String[] parts = header.split(" ");
            if (parts.length != 3 || !parts[0].equals("STORE")) {
                writer.println("ERROR_MALFORMED_COMMAND");
                return;
            }
    
            String filename = parts[1];
            int filesize = Integer.parseInt(parts[2]);
            File file = new File(fileFolder, filename);
    
            // Send ACK to Client to start sending the file content
            writer.println("ACK");
    
            // Switch to a different stream to read binary data directly from the socket's InputStream
            InputStream fileStream = clientSocket.getInputStream();
            try (FileOutputStream fileOut = new FileOutputStream(file)) {
                byte[] buffer = new byte[1024];
                int count;
                int remaining = filesize;
                while (remaining > 0 && (count = fileStream.read(buffer, 0, Math.min(buffer.length, remaining))) != -1) {
                    fileOut.write(buffer, 0, count);
                    remaining -= count;
                }
            }
    
            // Notify Controller that the file has been stored
            notifyControllerStoreAck(filename);
    
        } catch (IOException e) {
            System.out.println("Error handling client store operation: " + e.getMessage());
        }
    }
    

    private void notifyControllerStoreAck(String filename) {
        if (controllerOut != null) {
            
            controllerOut.println("STORE_ACK " + filename);
        }
    }

    private void clearLocalData() {
        try {
            Files.walk(Paths.get(fileFolder))
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);
            new File(fileFolder).mkdirs();
        } catch (IOException e) {
            System.out.println("Failed to clear local data: " + e.getMessage());
        }
    }

    private void connectToController() throws IOException {
        controllerSocket = new Socket();
        controllerSocket.connect(new InetSocketAddress(controllerHost, controllerPort), timeout);
        controllerOut = new PrintWriter(controllerSocket.getOutputStream(), true);
        controllerIn = new BufferedReader(new InputStreamReader(controllerSocket.getInputStream()));

        System.out.println("Connected to Controller at " + controllerHost + ":" + controllerPort);
        controllerOut.println("JOIN " + port);
    }

    public void stop() {
        running = false;
        try {
            if (serverSocket != null) {
                serverSocket.close();
            }
            if (controllerSocket != null) {
                controllerSocket.close();
            }
        } catch (IOException e) {
            System.out.println("Error while closing the connections: " + e.getMessage());
        }
    }

    public static void main(String[] args) {
        if (args.length != 4) {
            System.out.println("Usage: java Dstore <port> <controllerPort> <timeout> <fileFolder>");
            return;
        }
        int port = Integer.parseInt(args[0]);
        String controllerHost = "localhost";
        int controllerPort = Integer.parseInt(args[1]);
        int timeout = Integer.parseInt(args[2]);
        String fileFolder = args[3];

        try {
            Dstore dstore = new Dstore(port, controllerHost, controllerPort, timeout, fileFolder);
            dstore.start();
        } catch (IOException e) {
            System.out.println("Failed to start Dstore: " + e.getMessage());
        }
    }
}
