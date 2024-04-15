import java.io.*;
import java.net.*;
import java.util.concurrent.*;
import java.util.*;
import java.util.stream.Collectors;

public class Controller {
    private int port;
    private int replicationFactor;
    private int timeout; // Timeout in milliseconds
    private int rebalancePeriod; // Rebalance period in seconds
    private ServerSocket serverSocket;
    private volatile boolean running = true;
    private Map<String, DstoreInfo> dstores = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, String> fileIndex; // Track file states
    private ConcurrentHashMap<String, Set<String>> storeAcks = new ConcurrentHashMap<>();
    private Map<String, Socket> clientConnections = new ConcurrentHashMap<>();

    public Controller(int port, int replicationFactor, int timeout, int rebalancePeriod) {
        this.port = port;
        this.replicationFactor = replicationFactor;
        this.timeout = timeout;
        this.rebalancePeriod = rebalancePeriod;
        this.dstores = new ConcurrentHashMap<>();
        this.fileIndex = new ConcurrentHashMap<>();
    }

    public void start() throws IOException {
        serverSocket = new ServerSocket(port);
        System.out.println("Controller started on port " + port + " with replication factor " + replicationFactor + ", timeout " + timeout + " ms, rebalance period " + rebalancePeriod + " s.");

        new Thread(this::acceptConnections).start();
        new Thread(this::rebalance).start();
    }

    private void acceptConnections() {
        while (running) {
            try {
                Socket socket = serverSocket.accept();
                System.out.println("New connection from " + socket.getRemoteSocketAddress());
                // Handle each connection in a new thread
                new Thread(() -> handleConnection(socket)).start();
            } catch (IOException e) {
                System.out.println("Error accepting connection: " + e.getMessage());
            }
        }
    }
    

    private void handleConnection(Socket socket) {
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
    
            String message;
            while (running && !socket.isClosed()) {  // Ensure the server is still running and socket isn't closed
                message = reader.readLine();
                if (message == null) {
                    // If the client closes the connection, stop the loop
                    break;
                }
                String[] parts = message.split(" ");
                if (parts.length == 0) {
                    continue; // Skip empty messages
                }
    
                switch (parts[0]) {
                    case "JOIN":
                        if (parts.length < 2) {
                            System.out.println("Malformed JOIN message");
                        } else {
                            handleJoin(socket, message);
                            writer.println("JOIN_ACK");
                            System.out.println("JOIN_ACK");
                        }
                        break;
                    case "STORE_ACK":
                        System.out.println("STORE_ACK received");
                        if (parts.length < 2) {
                            System.out.println("Malformed STORE_ACK message");
                        } else {
                            handleStoreAck(parts[1], socket);
                        }
                        break;
    
                    default:
                        handleClientRequest(socket, parts, writer);
                        break;
                }
            }
        } catch (IOException e) {
            System.out.println("Error handling connection: " + e.getMessage());
        } finally {
            if (!socket.isClosed()) {
                try {
                    socket.close();  // Only close the socket if it's not already closed
                } catch (IOException e) {
                    System.out.println("Error closing socket: " + e.getMessage());
                }
            }
        }
    }
    
    
    private void handleClientRequest(Socket clientSocket, String[] commandParts, PrintWriter writer) {
        switch (commandParts[0]) {
            case "LIST":
                processListCommand(writer);
                break;
            case "STORE":
                if (commandParts.length < 3) {
                    writer.println("ERROR_MALFORMED_COMMAND");
                } else {
                    // Before handling the store, register the client socket
                    clientConnections.put(commandParts[1], clientSocket); // Save the client connection
                    handleStoreCommand(commandParts, writer, clientSocket); // Pass clientSocket for further use
                }
                break;
            // Add more cases as needed
        }
    }
    
    

    private void handleJoin(Socket dstoreSocket, String joinMessage) {
        String[] parts = joinMessage.split(" ");
        if (parts.length < 2) {
            System.out.println("Invalid JOIN message: " + joinMessage);
            return;
        }
        int listeningPort = Integer.parseInt(parts[1]);  // Convert port string to integer
        String dstoreID = getDstoreID(dstoreSocket);
    
        dstores.put(dstoreID, new DstoreInfo(dstoreSocket, listeningPort));
        System.out.println("Dstore joined from port " + listeningPort + " with ID: " + dstoreID);
    }
    
    
    private void handleStoreAck(String filename, Socket socket) {
        String dstoreId = socket.getRemoteSocketAddress().toString(); // Unique identifier based on socket address
        System.out.println("Received STORE_ACK for " + filename + " from Dstore " + dstoreId);
        storeAcks.putIfAbsent(filename, ConcurrentHashMap.newKeySet());
        Set<String> ackedStores = storeAcks.get(filename);
        ackedStores.add(dstoreId);
        System.out.println(ackedStores);
        System.out.println("rep factor:" + replicationFactor);
        // Check if we have received enough ACKs
        if (ackedStores.size() >= replicationFactor) {
            fileIndex.put(filename, "store complete");
            storeAcks.remove(filename); // Cleanup after completing the store process
            notifyClientStoreComplete(filename);
        }
    }
    
    


    private void notifyClientStoreComplete(String filename) {
        System.out.println("notifyCli");
        Socket clientSocket = clientConnections.get(filename);
        if (clientSocket != null) {
            try {
                PrintWriter writer = new PrintWriter(clientSocket.getOutputStream(), true);
                System.out.println("STORE_COMPLETE");
                writer.println("STORE_COMPLETE");
            } catch (IOException e) {
                System.out.println("Failed to notify client for filename: " + filename);
            }
        }
    }

    private String getDstoreID(Socket socket) {
        return socket.getRemoteSocketAddress().toString();
    }

    private void processListCommand(PrintWriter writer) {
        System.out.println("processing LIST");
        if (dstores.size() < replicationFactor) {
            writer.println("ERROR_NOT_ENOUGH_DSTORES");
            return;
        }
        String fileList = fileIndex.entrySet().stream()
                .filter(entry -> entry.getValue().equals("store complete"))
                .map(Map.Entry::getKey)
                .collect(Collectors.joining(" "));
    
        if (fileList.isEmpty()) {
            writer.println("LIST");
            System.out.println("DEBUG: No files to list, all files are either in progress or none exist."); // Debug message
        } else {
            writer.println("LIST " + fileList);
        }
    }
    

    private void handleStoreCommand(String[] commandParts, PrintWriter clientWriter, Socket clientSocket) {
        System.out.println("STORE");
        String filename = commandParts[1];
        if (fileIndex.containsKey(filename) && fileIndex.get(filename).equals("store complete")) {
            clientWriter.println("ERROR_FILE_ALREADY_EXISTS");
            return;
        }
    
        fileIndex.put(filename, "store in progress");
        List<String> selectedDstorePorts = selectDstoresForStorage();
        if (selectedDstorePorts.size() < replicationFactor) {
            clientWriter.println("ERROR_NOT_ENOUGH_DSTORES");
            return;
        }
    
        String response = "STORE_TO " + String.join(" ", selectedDstorePorts);
        clientWriter.println(response);
        System.out.println("Sending STORE_TO command with ports: " + response);
    }
    
    
    private List<String> selectDstoresForStorage() {
        return dstores.values().stream()
                     .limit(replicationFactor)
                     .map(DstoreInfo::getPort)
                     .map(String::valueOf)  // Convert integer port to string if necessary
                     .collect(Collectors.toList());
    }
    
    private void rebalance() {
        // Implement rebalancing logic here, to be run periodically
    }

    public void stop() throws IOException {
        running = false;
        if (serverSocket != null) {
            serverSocket.close();
        }
    }

    public static void main(String[] args) {
        if (args.length < 4) {
            System.out.println("Usage: java Controller <port> <replicationFactor> <timeout> <rebalancePeriod>");
            return;
        }
        int port = Integer.parseInt(args[0]);
        int replicationFactor = Integer.parseInt(args[1]);
        int timeout = Integer.parseInt(args[2]);
        int rebalancePeriod = Integer.parseInt(args[3]);

        try {
            Controller controller = new Controller(port, replicationFactor, timeout, rebalancePeriod);
            controller.start();
        } catch (IOException e) {
            System.out.println("Failed to start the Controller: " + e.getMessage());
        }
    }
}
