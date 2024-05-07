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
    
    private ConcurrentHashMap<String, String> fileIndex; // Track file states
    private ConcurrentHashMap<String, Set<String>> storeAcks = new ConcurrentHashMap<>();
    private Map<String, Socket> clientConnections = new ConcurrentHashMap<>();
    private Map<String, DstoreInfo> dstores = new ConcurrentHashMap<>();
    private Map<String, String> fileToClientAddress = new ConcurrentHashMap<>();

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
                String address = socket.getRemoteSocketAddress().toString();
                System.out.println("New connection from " + address);
    
                // Handle each connection in a new thread
                new Thread(() -> handleConnection(socket, address)).start();
            } catch (IOException e) {
                System.out.println("Error accepting connection: " + e.getMessage());
            }
        }
    }
    

    private void handleConnection(Socket socket, String address) {
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
    
            String initialMessage = reader.readLine(); // First message identifies the entity
            if (initialMessage == null) {
                socket.close();
                return;
            }
    
            String[] initialParts = initialMessage.split(" ");
            if (initialParts.length == 0) {
                socket.close();
                return;
            }
    
            // Determine if this is a Dstore or a Client
            if (initialParts[0].equals("JOIN") && initialParts.length >= 2) {
                handleJoin(socket, initialMessage);
                writer.println("JOIN_ACK");
            } else {
                clientConnections.putIfAbsent(address, socket); // Register the client
                System.out.println("Client connection registered: " + address);
                handleClientRequest(socket, initialParts, writer, address); // Process initial client command
            }
    
            // Continue reading further commands from the connection
            while (running && !socket.isClosed()) {
                String message = reader.readLine();
                if (message == null) {
                    break; // Client or Dstore closed the connection
                }
    
                String[] parts = message.split(" ");
                if (parts.length == 0) {
                    continue; // Skip empty messages
                }
    
                // Delegate the appropriate processing based on the type
                if (dstores.containsKey(address)) {
                    // Dstore commands like STORE_ACK
                    if (parts[0].equals("STORE_ACK") && parts.length >= 2) {
                        handleStoreAck(parts[1], socket);
                    }
                } else {
                    // Client commands
                    handleClientRequest(socket, parts, writer, address);
                }
            }
        } catch (IOException e) {
            System.out.println("Error handling connection: " + e.getMessage());
        } finally {
            // Cleanup and remove the connection from relevant maps
            clientConnections.remove(address);
            dstores.remove(address);
            try {
                socket.close();
            } catch (IOException e) {
                System.out.println("Error closing socket: " + e.getMessage());
            }
        }
    }
    
    
    
    private void handleClientRequest(Socket socket, String[] commandParts, PrintWriter writer, String address) {
        // Example handling of client commands
        if (commandParts.length == 0) {
            writer.println("ERROR_EMPTY_COMMAND");
            return;
        }
    
        switch (commandParts[0]) {
            case "LIST":
                System.out.println("List received from client: " + address);
                processListCommand(writer);
                break;
            case "STORE":
                System.out.println("Store received from client: " + address);
                if (commandParts.length < 3) {
                    writer.println("ERROR_MALFORMED_COMMAND");
                } else {
                    handleStoreCommand(commandParts, writer, socket);
                }
                break;
            case "LOAD":
                System.out.println("Load received from client: " + address);
                if (commandParts.length < 2) {
                    writer.println("ERROR_MALFORMED_COMMAND");
                } else {
                    processLoadCommand(commandParts, writer);
                }
                break;
            default:
                writer.println("ERROR_UNKNOWN_COMMAND");
                break;
        }
    }
    
    ///////////////////////////////// JOIN //////////////////////////////////////////////////////////////////////////////////////////////////

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
    ////////////////////////////////// STORE /////////////////////////////////////////////////////////////////////////////////

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
    
        // Retrieve the client's address for this specific filename
        String clientAddress = fileToClientAddress.get(filename);
        if (clientAddress != null) {
            Socket clientSocket = clientConnections.get(clientAddress);
            if (clientSocket != null) {
                try {
                    PrintWriter writer = new PrintWriter(clientSocket.getOutputStream(), true);
                    System.out.println("STORE_COMPLETE");
                    writer.println("STORE_COMPLETE");
                } catch (IOException e) {
                    System.out.println("Failed to notify client for filename: " + filename);
                }
            } else {
                System.out.println("Client socket not found for filename: " + filename);
            }
        } else {
            System.out.println("Client address not found for filename: " + filename);
        }
    }
    
    private void handleStoreCommand(String[] commandParts, PrintWriter clientWriter, Socket clientSocket) {
        System.out.println("STORE");
        String filename = commandParts[1];

        String clientAddress = clientSocket.getRemoteSocketAddress().toString();
        System.out.println("Store command received from client: " + clientAddress + " for file: " + filename);
        fileToClientAddress.put(filename, clientAddress);

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

    /////////////////////////////////////// LOAD ///////////////////////////////////////////////////////
    
    private void processLoadCommand(String[] commandParts, PrintWriter writer) {
        if (commandParts.length < 2) {
            writer.println("ERROR_MALFORMED_COMMAND");
            return;
        }
        
        String filename = commandParts[1];
        if (!fileIndex.containsKey(filename) || !fileIndex.get(filename).equals("store complete")) {
            writer.println("ERROR_FILE_DOES_NOT_EXIST");
            return;
        }
        
        List<String> availableDstores = selectDstoresForFile(filename);
        if (availableDstores.isEmpty() || availableDstores.size() < replicationFactor) {
            writer.println("ERROR_NOT_ENOUGH_DSTORES");
            return;
        }
        
        // Select a random Dstore to load the file from
        String chosenDstore = availableDstores.get(new Random().nextInt(availableDstores.size()));
        DstoreInfo dstoreInfo = dstores.get(chosenDstore);
        
        // Assume we have a method to get file size, pseudo-implementation
        long fileSize = getFileSize(filename);
        writer.println("LOAD_FROM " + dstoreInfo.getPort() + " " + fileSize);
    }
    
    private List<String> selectDstoresForFile(String filename) {
        return storeAcks.getOrDefault(filename, Collections.emptySet()).stream()
            .filter(dstoreId -> dstores.containsKey(dstoreId))
            .collect(Collectors.toList());
    }
    
    private long getFileSize(String filename) {
        // Implement this based on how your system stores and retrieves file size data
        return 1024; // Example placeholder
    }

///////////////////////////////////////// REBALANCE /////////////////////////////////////////////////////////////////////////////

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
