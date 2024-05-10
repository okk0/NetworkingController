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
    
    private ConcurrentHashMap<String, FileInfo> fileIndex = new ConcurrentHashMap<>();
    private Map<String, Socket> clientConnections = new ConcurrentHashMap<>();
    private Map<String, DstoreInfo> dstores = new ConcurrentHashMap<>();
    private Map<String, String> fileToClientAddress = new ConcurrentHashMap<>();
    private Map<String, String> removefileToClientAddress = new ConcurrentHashMap<>();
    private Map<String, Map<String, String>> clientToLastDstoreMap = new ConcurrentHashMap<>();
    private ConcurrentMap<String, ConcurrentSkipListSet<String>> pendingRemoveAcks = new ConcurrentHashMap<>();

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
        boolean shouldClose = true; // Flag to determine if the socket should be closed in finally block
    
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
    
            String initialMessage = reader.readLine();
    
            if (initialMessage == null) {
                shouldClose = true;
                return;
            }
    
            String[] initialParts = initialMessage.split(" ");
            if (initialParts.length == 0) {
                shouldClose = true;
                return;
            }
    
            // Determine if this is a Dstore or a client
            if (initialParts[0].equals("JOIN") && initialParts.length >= 2) {
                handleJoin(socket, initialMessage);
                writer.println("JOIN_ACK");
            } else {
                clientConnections.putIfAbsent(address, socket);
                System.out.println("Client connection registered: " + address);
                handleClientRequest(socket, initialParts, writer, address);
            }
    
            // Start processing messages
            while (running && !socket.isClosed()) {
                String message = reader.readLine();
                if (message == null) {
                    shouldClose = true;
                    break;
                }
    
                String[] parts = message.split(" ");
                if (parts.length == 0) continue;
    
                if (dstores.containsKey(address)) {
                    switch (parts[0]) {
                        case "STORE_ACK":
                            if (parts.length >= 2) handleStoreAck(parts[1], socket);
                            break;
                        case "REMOVE_ACK":
                            if (parts.length >= 2) handleRemoveAck(parts[1], address);
                            break;
                        default:
                            System.out.println("Unknown Dstore command: " + message);
                            break;
                    }
                } else {
                    handleClientRequest(socket, parts, writer, address);
                }
            }
        } catch (SocketException se) {
            System.out.println("Socket exception with " + address + ": " + se.getMessage());
        } catch (IOException e) {
            System.out.println("Error handling connection for " + address + ": " + e.getMessage());
        } finally {
            if (shouldClose) {
                clientConnections.remove(address);
                dstores.remove(address);
                try {
                    socket.close();
                    System.out.println("Socket closed for " + address);
                } catch (IOException e) {
                    System.out.println("Error closing socket for " + address + ": " + e.getMessage());
                }
            } else {
                System.out.println("Socket kept open for " + address);
            }
        }
    }
    
    
    
    
    
    
    private void handleClientRequest(Socket socket, String[] commandParts, PrintWriter writer, String address) {
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
                    processLoadCommand(commandParts, writer, address);
                }
                break;
    
            case "RELOAD":
                System.out.println("Reload received from client: " + address);
                if (commandParts.length < 2) {
                    writer.println("ERROR_MALFORMED_COMMAND");
                } else {
                    processReloadCommand(commandParts, writer, address);
                }
                break;

            case "REMOVE":
                System.out.println("Remove received from client: " + address);
                if (commandParts.length < 2) {
                    writer.println("ERROR_MALFORMED_COMMAND");
                } else {
                    handleRemoveCommand(commandParts, writer, address);
                }
                break;

            default:
                writer.println("ERROR_UNKNOWN_COMMAND");
                System.out.println("Unknown command received from client: " + address);
                break;
        }
    }
    
    /////////////////////////////////REMOVE////////////////////////////////////////////////////////////////



    
    private void handleRemoveCommand(String[] commandParts, PrintWriter writer, String clientAddress) {
        String filename = commandParts[1];
        System.out.println("Initiating remove operation for file: " + filename);
    
        // Check if the file exists in the index
        FileInfo fileInfo = fileIndex.get(filename);
        if (fileInfo == null) {
            writer.println("ERROR_FILE_DOES_NOT_EXIST");
            System.out.println("File not found: " + filename);
            return;
        }
    
        // Mark the file as "remove in progress"
        fileInfo.status = "remove in progress";
    
        // Track the client that requested the removal
        removefileToClientAddress.put(filename, clientAddress);
    
        // Get Dstores that should remove the file
        Set<String> affectedDstores = fileInfo.dstores;
        System.out.println("Dstores expected to remove file: " + affectedDstores);
    
        // Initialize a concurrent skip list set and populate it with the affected Dstores
        ConcurrentSkipListSet<String> pendingAckSet = new ConcurrentSkipListSet<>(affectedDstores);
    
        // Add to the pendingRemoveAcks map
        pendingRemoveAcks.put(filename, pendingAckSet);
    
        // Send the remove command to all affected Dstores
        for (String dstore : affectedDstores) {
            DstoreInfo dstoreInfo = dstores.get(dstore);
    
            if (dstoreInfo == null) {
                System.out.println("Error: Dstore info not found for " + dstore);
                continue;  // Skip this Dstore since it's not available
            }
    
            Socket dstoreSocket = dstoreInfo.getSocket();
            if (dstoreSocket != null) {
                try (PrintWriter dstoreWriter = new PrintWriter(dstoreSocket.getOutputStream(), true)) {
                    dstoreWriter.println("REMOVE " + filename);
                    System.out.println("Sent REMOVE command to Dstore: " + dstore);
                } catch (IOException e) {
                    System.out.println("Error sending REMOVE command to Dstore: " + dstore + ". " + e.getMessage());
                }
            } else {
                System.out.println("Error: Dstore socket is null for " + dstore);
            }
        }
    }
    
    

    
    private void handleRemoveAck(String filename, String dstoreAddress) {
        ConcurrentSkipListSet<String> acks = pendingRemoveAcks.get(filename);
        if (acks != null) {
            acks.remove(dstoreAddress);
            System.out.println("Received REMOVE_ACK for file: " + filename + " from " + dstoreAddress);

            // Check if all acknowledgments have been received
            if (acks.isEmpty()) {
                fileIndex.remove(filename);
                pendingRemoveAcks.remove(filename);

                // Find the client associated with the remove command
                String clientAddress = removefileToClientAddress.get(filename);
                if (clientAddress != null) {
                    Socket clientSocket = clientConnections.get(clientAddress);
                    if (clientSocket != null && !clientSocket.isClosed()) {
                        try {
                            PrintWriter clientWriter = new PrintWriter(clientSocket.getOutputStream(), true);
                            clientWriter.println("REMOVE_COMPLETE");
                            System.out.println("Remove operation completed for file " + filename);
                        } catch (IOException e) {
                            System.out.println("Error sending REMOVE_COMPLETE to client: " + e.getMessage());
                        }
                    } else {
                        System.out.println("Client socket unavailable or already closed: " + clientAddress);
                    }
                }

                // Cleanup the client-to-file mapping after completion
                removefileToClientAddress.remove(filename);
            }
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
    
        // Retrieve all filenames that are marked as "store complete"
        String fileList = fileIndex.entrySet().stream()
            .filter(entry -> entry.getValue().status.equals("store complete"))
            .map(Map.Entry::getKey)
            .collect(Collectors.joining(" "));
    
        // Respond to the client with the list of files or an empty list
        if (fileList.isEmpty()) {
            writer.println("LIST");
            System.out.println("DEBUG: No files to list, all files are either in progress or none exist.");
        } else {
            writer.println("LIST " + fileList);
        }
    }
    ////////////////////////////////// STORE /////////////////////////////////////////////////////////////////////////////////

    
    private void handleStoreAck(String filename, Socket socket) {
        String dstoreId = socket.getRemoteSocketAddress().toString();
        System.out.println("Received STORE_ACK for " + filename + " from Dstore " + dstoreId);

        // Initialize the file info in fileIndex if not already present
        fileIndex.putIfAbsent(filename, new FileInfo("store in progress"));
        FileInfo fileInfo = fileIndex.get(filename);
        fileInfo.dstores.add(dstoreId);

        System.out.println("DEBUG: Dstores that have acknowledged storing " + filename + ": " + fileInfo.dstores);
        System.out.println("rep factor: " + replicationFactor);

        // Check if we have received enough ACKs
        if (fileInfo.dstores.size() >= replicationFactor) {
            fileInfo.status = "store complete";
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
            fileToClientAddress.remove(filename);
        } else {
            System.out.println("Client address not found for filename: " + filename);
        }
    }
    
    private void handleStoreCommand(String[] commandParts, PrintWriter clientWriter, Socket clientSocket) {
        System.out.println("STORE");
        String filename = commandParts[1];
    
        String clientAddress = clientSocket.getRemoteSocketAddress().toString();
        System.out.println("Store command received from client: " + clientAddress + " for file: " + filename);
    
        // Map the filename to the client's address
        fileToClientAddress.put(filename, clientAddress);
    
        // Check if the file already exists and is marked as "store complete"
        FileInfo fileInfo = fileIndex.get(filename);
        if (fileInfo != null && fileInfo.status.equals("store complete")) {
            clientWriter.println("ERROR_FILE_ALREADY_EXISTS");
            return;
        }
    
        // Initialize file information and set the status to "store in progress"
        fileInfo = new FileInfo("store in progress");
        fileIndex.put(filename, fileInfo);
    
        // Select Dstores for storage
        List<String> selectedDstorePorts = selectDstoresForStorage();
        if (selectedDstorePorts.size() < replicationFactor) {
            clientWriter.println("ERROR_NOT_ENOUGH_DSTORES");
            return;
        }
    
        // Send the store command back to the client
        String response = "STORE_TO " + String.join(" ", selectedDstorePorts);
        clientWriter.println(response);
        System.out.println("Sending STORE_TO command with ports: " + response);
    }
    
    private List<String> selectDstoresForStorage() {
        return dstores.values().stream()
            .limit(replicationFactor)
            .map(DstoreInfo::getPort)
            .map(String::valueOf)
            .collect(Collectors.toList());
    }
    
    

    /////////////////////////////////////// LOAD ///////////////////////////////////////////////////////
    


    private void processReloadCommand(String[] commandParts, PrintWriter writer, String clientAddress) {
    if (commandParts.length < 2) {
        writer.println("ERROR_MALFORMED_COMMAND");
        return;
    }

    String filename = commandParts[1];
    System.out.println("DEBUG: Processing RELOAD command for file: " + filename + " from client: " + clientAddress);

    // Check if the file is marked as stored completely
    FileInfo fileInfo = fileIndex.get(filename);
    if (fileInfo == null || !fileInfo.status.equals("store complete")) {
        writer.println("ERROR_FILE_DOES_NOT_EXIST");
        System.out.println("DEBUG: File does not exist or is not completely stored: " + filename);
        return;
    }

    // Filter only active Dstores that have this file, excluding the previously used one
    String lastDstore = clientToLastDstoreMap.getOrDefault(clientAddress, Collections.emptyMap()).get(filename);
    List<String> availableDstores = fileInfo.dstores.stream()
        .filter(dstores::containsKey)
        .filter(dstore -> !dstore.equals(lastDstore))
        .collect(Collectors.toList());

    System.out.println("DEBUG: Available Dstores for " + filename + " excluding " + lastDstore + ": " + availableDstores);

    // Check if an alternative Dstore is available
    if (availableDstores.isEmpty()) {
        writer.println("ERROR_LOAD");
        System.out.println("DEBUG: No alternative Dstores available for " + filename);
        return;
    }

    // Select a random alternative Dstore to load the file from
    String chosenDstore = availableDstores.get(new Random().nextInt(availableDstores.size()));
    DstoreInfo dstoreInfo = dstores.get(chosenDstore);

    if (dstoreInfo == null) {
        writer.println("ERROR_LOAD");
        System.out.println("DEBUG: Dstore info not found for chosen Dstore: " + chosenDstore);
        return;
    }

    // Update the last used Dstore for this client
    clientToLastDstoreMap.computeIfAbsent(clientAddress, k -> new ConcurrentHashMap<>()).put(filename, chosenDstore);

    // Get file size (example placeholder)
    long fileSize = getFileSize(filename);
    writer.println("LOAD_FROM " + dstoreInfo.getPort() + " " + fileSize);
    System.out.println("DEBUG: LOAD_FROM command sent for " + filename + " from Dstore port " + dstoreInfo.getPort() + " with size " + fileSize);
}

private void processLoadCommand(String[] commandParts, PrintWriter writer, String clientAddress) {
    if (commandParts.length < 2) {
        writer.println("ERROR_MALFORMED_COMMAND");
        return;
    }

    String filename = commandParts[1];
    System.out.println("DEBUG: Processing LOAD command for file: " + filename);

    // Check if the file is marked as stored completely
    FileInfo fileInfo = fileIndex.get(filename);
    if (fileInfo == null || !fileInfo.status.equals("store complete")) {
        writer.println("ERROR_FILE_DOES_NOT_EXIST");
        System.out.println("DEBUG: File does not exist or is not completely stored: " + filename);
        return;
    }

    // Filter only active Dstores that have this file
    List<String> availableDstores = fileInfo.dstores.stream()
        .filter(dstores::containsKey)
        .collect(Collectors.toList());

    System.out.println("DEBUG: Available Dstores for " + filename + ": " + availableDstores);

    // Check if we have enough Dstores available
    if (availableDstores.isEmpty() || availableDstores.size() < replicationFactor) {
        writer.println("ERROR_NOT_ENOUGH_DSTORES");
        System.out.println("DEBUG: Not enough Dstores available for " + filename + ", found: " + availableDstores.size());
        return;
    }

    // Select a random Dstore to load the file from
    String chosenDstore = availableDstores.get(new Random().nextInt(availableDstores.size()));
    DstoreInfo dstoreInfo = dstores.get(chosenDstore);

    if (dstoreInfo == null) {
        writer.println("ERROR_NOT_ENOUGH_DSTORES");
        System.out.println("DEBUG: Dstore info not found for chosen Dstore: " + chosenDstore);
        return;
    }

    // Update the last used Dstore for this client
    clientToLastDstoreMap.computeIfAbsent(clientAddress, k -> new ConcurrentHashMap<>()).put(filename, chosenDstore);

    // Get file size (example placeholder)
    long fileSize = getFileSize(filename);
    writer.println("LOAD_FROM " + dstoreInfo.getPort() + " " + fileSize);
    System.out.println("DEBUG: LOAD_FROM command sent for " + filename + " from Dstore port " + dstoreInfo.getPort() + " with size " + fileSize);
}
    
    private long getFileSize(String filename) {
        // Example placeholder for actual file size logic
        return 1024;
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
