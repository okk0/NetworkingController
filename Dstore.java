import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.util.Comparator;


public class Dstore {
    private int port;
    private Socket socket;
    private PrintWriter out;
    private BufferedReader in;
    private int controllerPort;
    private String controllerHost;
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
    }

    private void clearLocalData() {
        try {
            Files.walk(Paths.get(fileFolder))
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);
            new File(fileFolder).mkdirs(); // Recreate directory structure if deleted
        } catch (IOException e) {
            System.out.println("Failed to clear local data: " + e.getMessage());
        }
    }

    private void connectToController() throws IOException {
        socket = new Socket();
        socket.connect(new InetSocketAddress(controllerHost, controllerPort), timeout);
        out = new PrintWriter(socket.getOutputStream(), true);
        in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
    
        System.out.println("Connected to Controller at " + controllerHost + ":" + controllerPort);
    
        // Send JOIN message immediately after connection
        out.println("JOIN " + port);
        
        // Start listening to the controller
        new Thread(this::listenToController).start();
    }
    

    private void listenToController() {
        try {
            // Initially set a timeout if necessary
            socket.setSoTimeout(timeout);
            String message;
            while ((message = in.readLine()) != null && running) {
                System.out.println("Message from Controller: " + message);
                processMessage(message);
                // Reset timeout after JOIN_ACK or change state to reduce sensitivity to immediate messages
                if (message.equals("JOIN_ACK")) {
                    // Option: Remove or increase the timeout after successful join
                    socket.setSoTimeout(0); // This sets the timeout to infinity
                }
            }
        } catch (SocketTimeoutException e) {
            System.out.println("Timeout while waiting for a message from the Controller.");
        } catch (IOException e) {
            System.out.println("Lost connection to the Controller: " + e.getMessage());
        } finally {
            disconnect();
        }
    }
    

    private void processMessage(String message) {
        // Handle different types of messages here
        switch (message.split(" ")[0]) {
            case "STORE":
                handleStore(message);
                break;
            case "REMOVE":
                handleRemove(message);
                break;
            // Add more cases as needed
        }
    }

    private void handleClientStore(String[] commandParts) {
        String filename = commandParts[1];
        int filesize = Integer.parseInt(commandParts[2]);
        // Implement file receiving logic here
        // After receiving, send ACK to client
        clientWriter.println("ACK");
        // After storing the file content
        notifyControllerStoreAck(filename);
    }
    
    private void notifyControllerStoreAck(String filename) {
        // Notify the controller of successful storage
        // Assuming there is a way to communicate back to the controller
        controllerOut.println("STORE_ACK " + filename);
    }
    

    private void handleRemove(String message) {
        // Implementation for removing a file
    }

    public void disconnect() {
        try {
            running = false;
            if (socket != null) {
                socket.close();
            }
            System.out.println("Disconnected from the Controller.");
        } catch (IOException e) {
            System.out.println("Error while closing the connection: " + e.getMessage());
        }
    }

    public static void main(String[] args) {
        if (args.length != 4) {
            System.out.println("Usage: java Dstore <port> <controllerPort> <timeout> <fileFolder>");
            return;
        }
        int port = Integer.parseInt(args[0]); // Port for this Dstore
        String controllerPort = args[1]; // Controller's port
        int timeout = Integer.parseInt(args[2]); // Timeout in milliseconds
        String fileFolder = args[3]; // Local folder for storing data
    
        try {
            // Parse the controllerPort as integer
            int cport = Integer.parseInt(controllerPort);
            Dstore dstore = new Dstore(port, "localhost", cport, timeout, fileFolder);
            dstore.start();
        } catch (NumberFormatException e) {
            System.out.println("Invalid port number format.");
        } catch (IOException e) {
            System.out.println("Failed to start Dstore: " + e.getMessage());
        }
    }
    
}
