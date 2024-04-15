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
    
        serverSocket = new ServerSocket(port); // Ensure this comes before accepting connections
        System.out.println("Dstore listening on port: " + port);
    
        new Thread(this::acceptClientConnections).start(); // Listen for client connections
    }
    

    private void acceptClientConnections() {
        while (running) {
            try {
                Socket clientSocket = serverSocket.accept();
                System.out.println("Accepted client connection from: " + clientSocket.getRemoteSocketAddress());
                new Thread(() -> handleClientConnection(clientSocket)).start(); // Handle each client in a new thread
            } catch (IOException e) {
                if (!running) break; // Exit if the server is stopped
                System.out.println("Error accepting client connection: " + e.getMessage());
            }
        }
    }
    
    private void handleClientConnection(Socket clientSocket) {
        try {
            System.out.println("Connection established with client: " + clientSocket.getRemoteSocketAddress());
    
            BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            PrintWriter writer = new PrintWriter(clientSocket.getOutputStream(), true);
            
            String header = reader.readLine();
            if (header == null) {
                System.out.println("No data received. Client might have closed the connection.");
                return;
            }
    
            System.out.println("Received command: " + header);
            String[] parts = header.split(" ");
            if (parts.length != 3 || !parts[0].equals("STORE")) {
                writer.println("ERROR_MALFORMED_COMMAND");
                System.out.println("Malformed STORE command: " + header);
                return;
            }
    
            String filename = parts[1];
            int filesize = Integer.parseInt(parts[2]);
            System.out.println("Preparing to store file: " + filename + " with size: " + filesize);
    
            File file = new File(fileFolder, filename);
            writer.println("ACK");  // Send ACK to client to start sending the file content
            System.out.println("Sent ACK to client.");
            System.out.println(fileFolder);
            // Prepare to receive the file data
            byte[] buffer = new byte[4096];
            int bytesRead;
            int totalRead = 0;
    
            // Open a file output stream to write the incoming file data
            try (FileOutputStream fileOut = new FileOutputStream(file)) {
                InputStream fileStream = clientSocket.getInputStream();
                while (totalRead < filesize && (bytesRead = fileStream.read(buffer)) != -1) {
                    fileOut.write(buffer, 0, bytesRead);
                    totalRead += bytesRead;
                    System.out.println("Received " + totalRead + " bytes so far.");
                }
            }
    
            if (totalRead == filesize) {
                notifyControllerStoreAck(filename);  // Notify the controller of successful storage
                System.out.println("File stored successfully: " + filename);
            } else {
                System.out.println("File transfer incomplete. Expected: " + filesize + ", received: " + totalRead);
            }
            
        } catch (IOException e) {
            System.out.println("Error handling client store operation: " + e.getMessage());
        } finally {
            try {
                clientSocket.close();
                System.out.println("Closed client connection.");
            } catch (IOException e) {
                System.out.println("Failed to close client connection: " + e.getMessage());
            }
        }
    }
    
    

    private void notifyControllerStoreAck(String filename) {
        if (controllerOut != null) {
            System.out.println("STORE_ACK");
            controllerOut.println("STORE_ACK " + filename);
        }
    }

    private void clearLocalData() {
        try {
            Path directory = Paths.get(fileFolder);
            if (!Files.exists(directory)) {
                System.out.println("Directory does not exist, expected to exist: " + directory);
                return; // Optionally throw an exception or handle this case as needed
            }
    
            // Use Files.walk to iterate through all files and subdirectories, but skip the top-level directory
            Files.walk(directory)
                 .sorted(Comparator.reverseOrder()) // Important for deleting directories after their contents
                 .map(Path::toFile)
                 .filter(file -> !file.equals(directory.toFile())) // Skip the root directory itself
                 .forEach(File::delete);
    
            System.out.println("Cleared local data in existing directory successfully.");
        } catch (IOException e) {
            System.out.println("Failed to clear local data: " + e.getMessage());
        }
    }
    
    

    private void connectToController() throws IOException {
        controllerSocket = new Socket(controllerHost, controllerPort);
        controllerOut = new PrintWriter(controllerSocket.getOutputStream(), true);
        controllerIn = new BufferedReader(new InputStreamReader(controllerSocket.getInputStream()));
    
        // Send JOIN message along with the Dstore's listening port
        controllerOut.println("JOIN " + port);  // Here, 'port' is the port Dstore listens on for client connections
        System.out.println("Sent JOIN message with port: " + port);
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
