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
    private Map<String, Socket> dstores; // Map to keep track of connected Dstores
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
            while ((message = reader.readLine()) != null) {  // Changed to check for null to keep the connection open
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
                        }
                        break;
                    case "STORE_ACK":
                        if (parts.length < 2) {
                            System.out.println("Malformed STORE_ACK message");
                        } else {
                            handleStoreAck(parts[1]);
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
            try {
                socket.close();  // Close connection when done
            } catch (IOException e) {
                System.out.println("Error closing socket: " + e.getMessage());
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
                    handleStoreCommand(commandParts, writer);
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
        String port = parts[1];
        String dstoreID = getDstoreID(dstoreSocket);
        synchronized (dstores) {
            dstores.put(dstoreID, dstoreSocket);
            System.out.println("Dstore joined from port " + port + " with ID: " + dstoreID);
        }
    }
    
    private void handleStoreAck(String filename) {
        storeAcks.putIfAbsent(filename, ConcurrentHashMap.newKeySet());
        Set<String> ackedStores = storeAcks.get(filename);
        ackedStores.add(filename);

        // Check if we have received enough ACKs
        if (ackedStores.size() >= replicationFactor) {
            fileIndex.put(filename, "store complete");
            storeAcks.remove(filename);  // Cleanup after completing the store process
            notifyClientStoreComplete(filename);
        }
    }


    private void notifyClientStoreComplete(String filename) {
        Socket clientSocket = clientConnections.get(filename);
        if (clientSocket != null) {
            try {
                PrintWriter writer = new PrintWriter(clientSocket.getOutputStream(), true);
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
    

    private void handleStoreCommand(String[] commandParts, PrintWriter clientWriter) {
        String filename = commandParts[1];
        int filesize = Integer.parseInt(commandParts[2]);
        
        if (fileIndex.containsKey(filename) && fileIndex.get(filename).equals("store complete")) {
            clientWriter.println("ERROR_FILE_ALREADY_EXISTS");
            return;
        }
    
        fileIndex.put(filename, "store in progress");
        List<String> selectedDstores = selectDstoresForStorage();
    
        if (selectedDstores.size() < replicationFactor) {
            clientWriter.println("ERROR_NOT_ENOUGH_DSTORES");
            return;
        }
    
        String response = "STORE_TO " + String.join(" ", selectedDstores);
        clientWriter.println(response);
    }
    
    private List<String> selectDstoresForStorage() {
        // Logic to select R available Dstores
        return dstores.keySet().stream().limit(replicationFactor).collect(Collectors.toList());
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
