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
                handleConnection(socket);
            } catch (IOException e) {
                System.out.println("Error accepting connection: " + e.getMessage());
            }
        }
    }

    public void handleConnection(Socket socket) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
    
        String initialMessage = reader.readLine();
        if (initialMessage == null) {
            System.out.println("Received null initial message from connection: " + socket.getRemoteSocketAddress());
            socket.close();  // Close the connection if the initial message is null
            return;
        }
    
        if (initialMessage.startsWith("JOIN")) {
            handleJoin(socket, initialMessage);
            // Consider sending an acknowledgment back to the Dstore
            writer.println("JOIN_ACK");
        } else {
            // Handle other types of requests or commands
            handleClientRequest(socket, reader, writer);
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
    
    

    private String getDstoreID(Socket socket) {
        return socket.getRemoteSocketAddress().toString();
    }


    

    private void handleClientRequest(Socket clientSocket, BufferedReader reader, PrintWriter writer) throws IOException {
        String command = reader.readLine();
        if (command == null) {
            return;
        }
        String[] commandParts = command.split(" ");
        switch (commandParts[0]) {
            case "LIST":
                processListCommand(writer);
                break;
            case "STORE":
                if (commandParts.length < 3) {
                    writer.println("ERROR_MALFORMED_COMMAND");
                    return;
                }
                handleStoreCommand(commandParts, writer);
                break;
            // Add more cases as needed
        }
    }
    

    private void processListCommand(PrintWriter writer) {
        if (dstores.size() < replicationFactor) {
            writer.println("ERROR_NOT_ENOUGH_DSTORES");
            return;
        }
        String fileList = fileIndex.entrySet().stream()
                .filter(entry -> entry.getValue().equals("store complete"))
                .map(Map.Entry::getKey)
                .collect(Collectors.joining(" "));
        writer.println(fileList.isEmpty() ? "LIST" : "LIST " + fileList);
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

    private void handleStoreAck(String filename) {
        // Maintain a count of ACKs received and check if it matches the replication factor
        // If yes, update index and notify client
        updateIndexAndNotifyClient(filename);
    }
    
    private void updateIndexAndNotifyClient(String filename) {
        fileIndex.put(filename, "store complete");
        // Logic to notify client
        clientWriter.println("STORE_COMPLETE");
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
