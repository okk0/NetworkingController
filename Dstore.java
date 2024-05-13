import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class Dstore {
    private ServerSocket serverSocket;
    private Socket controllerSocket;
    private PrintWriter controllerOut;
    private BufferedReader controllerIn;
    private final String controllerHost;
    private final int controllerPort;
    private final int port;
    private boolean running = true;
    private final String fileFolder;
    private final int timeout;
    private static final int CLIENT_TIMEOUT_MS = 10000; // Example: 10 seconds

    public Dstore(int port, String controllerHost, int controllerPort, int timeout, String fileFolder) {
        this.port = port;
        this.controllerHost = controllerHost;
        this.controllerPort = controllerPort;
        this.timeout = timeout;
        this.fileFolder = fileFolder;
    }

    public void start() {
        clearLocalData();
        

        try {
            connectToController();
            serverSocket = new ServerSocket(port);
            System.out.println("Dstore listening on port: " + port);

            // Start a new thread for handling controller commands
            new Thread(this::handleControllerCommands).start();

            // Start listening for client connections
            new Thread(this::acceptClientConnections).start();
        } catch (IOException e) {
            System.out.println("Error starting Dstore server: " + e.getMessage());
        }
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
        System.out.println("Connection established with client: " + clientSocket.getRemoteSocketAddress());
    
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
             PrintWriter writer = new PrintWriter(clientSocket.getOutputStream(), true)) {
    
            String header;
    
            // Loop to continuously listen for commands from the client
            while ((header = reader.readLine()) != null) {
                System.out.println("Received command: " + header);
                String[] parts = header.split(" ");
                if (parts.length < 2) {
                    writer.println("ERROR_MALFORMED_COMMAND");
                    System.out.println("Malformed command: " + header);
                    continue; // Continue to the next iteration to process further commands
                }
    
                // Handle STORE, LOAD_DATA, and REMOVE commands
                switch (parts[0]) {
                    case "STORE":
                        handleStoreCommand(parts, writer, clientSocket);
                        System.out.println("STORE");
                        break;
                    case "LOAD_DATA":
                        handleLoadDataCommand(parts[1], clientSocket);
                        System.out.println("LOAD DATA");
                        break;
                    default:
                        writer.println("ERROR_UNKNOWN_COMMAND");
                        System.out.println("Unknown command: " + header);
                        break;
                }
            }
    
            System.out.println("Client disconnected: " + clientSocket.getRemoteSocketAddress());
    
        } catch (IOException e) {
            System.out.println("Error handling client operation: " + e.getMessage());
        }
    }
    
/////////////////////////////////////////STORE AND LOAD ////////////////////////////////////////////////////////////////////////////
    
   

    private void handleLoadDataCommand(String filename, Socket clientSocket) {
        try {
            // Construct the full file path
            File file = new File(fileFolder + File.separator + filename);

            // Check if the file exists
            if (!file.exists()) {
                System.err.println(clientSocket.getPort() + " in dstore " + port + " :File does not exist: " + filename);
                clientSocket.close();
                return;
            }

            // Read all bytes from the file
            byte[] fileContent = Files.readAllBytes(file.toPath());

            // Get the output stream of the client socket
            OutputStream out = clientSocket.getOutputStream();

            // Write the file content to the client's output stream
            out.write(fileContent);
            out.flush();
        } catch (Exception e) {
            System.err.println(clientSocket.getPort() + " in dstore " + port + " :Error loading file: " + e.getMessage());

            // Attempt to close the client socket
            try {
                clientSocket.close();
            } catch (IOException e1) {
                System.err.println(clientSocket.getPort() + " in dstore " + port + " :Error closing client socket: " + e1.getMessage());
            }
        }
    }



    private void handleStoreCommand(String[] commandParts, PrintWriter writer, Socket clientSocket) {
        if (commandParts.length != 3) {
            writer.println("ERROR_MALFORMED_COMMAND");
            System.out.println("Malformed STORE command: " + String.join(" ", commandParts));
            return;
        }

        String filename = commandParts[1];
        int filesize;
        try {
            filesize = Integer.parseInt(commandParts[2]);
        } catch (NumberFormatException e) {
            writer.println("ERROR_MALFORMED_COMMAND");
            System.out.println("Invalid file size in STORE command: " + commandParts[2]);
            return;
        }

        System.out.println("Preparing to store file: " + filename + " with size: " + filesize);

        // Create a new file in the designated storage folder
        File file = new File(fileFolder, filename);

        // Send an ACK to the client indicating readiness to receive the file
        writer.println("ACK");
        System.out.println("Sent ACK to client.");

        // Prepare to receive the file data
        byte[] buffer = new byte[4096];
        int totalRead = 0;

        try (FileOutputStream fileOut = new FileOutputStream(file)) {
            InputStream fileStream = clientSocket.getInputStream();
            while (totalRead < filesize) {
                int bytesRead = fileStream.read(buffer);
                if (bytesRead == -1) {
                    break;
                }
                fileOut.write(buffer, 0, bytesRead);
                totalRead += bytesRead;
                System.out.println("Received " + totalRead + " bytes so far.");
            }
        } catch (IOException e) {
            System.out.println("Error storing file: " + filename + ". " + e.getMessage());
            writer.println("ERROR_STORING_FILE");
            return;
        }

        // Verify that the entire file was received
        if (totalRead == filesize) {
            notifyControllerStoreAck(filename);  // Notify the controller of successful storage
            System.out.println("File stored successfully: " + filename);
        } else {
            System.out.println("File transfer incomplete. Expected: " + filesize + ", received: " + totalRead);
            writer.println("ERROR_STORING_FILE");
        }
    }

    private void notifyControllerStoreAck(String filename) {
        if (controllerOut != null) {
            System.out.println("STORE_ACK");
            controllerOut.println("STORE_ACK " + filename);
        }
    }
///////////////////////////////// REBALANCE /////////////////////////////////////////////////////////////////////////////////////
    
private void handleRebalanceCommand(String[] commandParts) {
        System.out.println("REBALANCE");
        int numFilesToSend = Integer.parseInt(commandParts[1]);
        int currentIndex = 2;

        List<Pair<String, List<String>>> filesToSend = new ArrayList<>();
        for (int i = 0; i < numFilesToSend; i++) {
            String filename = commandParts[currentIndex++];
            int numDstores = Integer.parseInt(commandParts[currentIndex++]);
            List<String> targetDstores = new ArrayList<>();
            for (int j = 0; j < numDstores; j++) {
                targetDstores.add(commandParts[currentIndex++]);
            }
            filesToSend.add(new Pair<>(filename, targetDstores));
        }

        int numFilesToRemove = Integer.parseInt(commandParts[currentIndex++]);
        List<String> filesToRemove = new ArrayList<>();
        for (int i = 0; i < numFilesToRemove; i++) {
            filesToRemove.add(commandParts[currentIndex++]);
        }

        // Send files to target Dstores
        for (Pair<String, List<String>> fileSend : filesToSend) {
            String filename = fileSend.getFirst();
            List<String> targetDstores = fileSend.getSecond();

            for (String dstore : targetDstores) {
                sendFileToDstore(filename, dstore);
            }
        }

        // Remove files as requested
        for (String filename : filesToRemove) {
            File file = new File(fileFolder, filename);
            if (file.exists()) {
                file.delete();
                System.out.println("Removed file: " + filename);
            }
        }

        // Notify controller of rebalance completion
        notifyControllerRebalanceComplete();
    }

    private void sendFileToDstore(String filename, String dstoreAddress) {
        try (Socket dstoreSocket = new Socket(dstoreAddress.split(":")[0], Integer.parseInt(dstoreAddress.split(":")[1]));
            PrintWriter writer = new PrintWriter(dstoreSocket.getOutputStream(), true);
            BufferedReader reader = new BufferedReader(new InputStreamReader(dstoreSocket.getInputStream()));
            InputStream fileInputStream = new FileInputStream(new File(fileFolder, filename));
            OutputStream dstoreOutputStream = dstoreSocket.getOutputStream()) {

            long fileSize = new File(fileFolder, filename).length();
            writer.println("REBALANCE_STORE " + filename + " " + fileSize);

            // Await acknowledgment
            String response = reader.readLine();
            if (response.equals("ACK")) {
                byte[] buffer = new byte[4096];
                int bytesRead;
                while ((bytesRead = fileInputStream.read(buffer)) != -1) {
                    dstoreOutputStream.write(buffer, 0, bytesRead);
                }
                System.out.println("Sent file " + filename + " to Dstore " + dstoreAddress);
            }
        } catch (IOException e) {
            System.out.println("Error sending file to Dstore: " + e.getMessage());
        }
    }

    private void notifyControllerRebalanceComplete() {
        if (controllerOut != null) {
            controllerOut.println("REBALANCE_COMPLETE");
            System.out.println("Rebalance operation completed.");
        }
    }

////////////////////////////// HANDLE COMMANDS //////////////////////////////////////////////////////////////////////////////////////
    
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
    
        controllerOut.println("JOIN " + port); // Register with the controller
        System.out.println("Sent JOIN message with port: " + port);
    
        // Listen for commands from the controller in a separate thread
        new Thread(this::handleControllerCommands).start();
    }

    private void handleControllerCommands() {
        while (running && controllerSocket.isConnected()) {
            try {
                String command = controllerIn.readLine();
                if (command == null) {
                    System.out.println("Lost connection to the controller");
                    running = false;
                    break;
                }
    
                String[] parts = command.split(" ");
                if (parts.length < 1) {
                    System.out.println("Malformed command from controller: " + command);
                    continue;
                }
    
                // Process the command based on the type
                switch (parts[0]) {
                    case "REMOVE":
                        if (parts.length < 2) {
                            System.out.println("REMOVE command missing filename.");
                        } else {
                            handleRemoveCommand(parts[1], controllerOut);
                        }
                        break;
                    case "REBALANCE":
                        handleRebalanceCommand(parts);
                        break;
                    case "LIST":
                        handleListCommand();
                        break;
                    default:
                        System.out.println("Unknown command from controller: " + command);
                        break;
                }
    
            } catch (IOException e) {
                System.out.println("Error in controller communication: " + e.getMessage());
                running = false;
                break;
            }
        }
        System.out.println("Stopped listening to the controller");
    }
    
    private void handleListCommand() {
        File folder = new File(fileFolder);
        File[] listOfFiles = folder.listFiles();
        StringBuilder fileListBuilder = new StringBuilder("LIST");
        if (listOfFiles != null) {
            for (File file : listOfFiles) {
                if (file.isFile()) {
                    fileListBuilder.append(" ").append(file.getName());
                }
            }
        }
        controllerOut.println(fileListBuilder.toString());
        System.out.println("Sent file list to controller.");
    }
    
    
    private void handleRemoveCommand(String filename, PrintWriter controllerWriter) {
        File file = new File(fileFolder, filename);
        System.out.println("Processing REMOVE command for file: " + filename);
    
        if (file.delete()) {
            System.out.println("File successfully removed: " + filename);
            controllerWriter.println("REMOVE_ACK " + filename); // Acknowledge removal to the controller
        } else {
            System.out.println("Failed to remove file: " + filename);
            if (!file.exists()) {
                controllerWriter.println("ERROR_FILE_DOES_NOT_EXIST " + filename);
            } else {
                controllerWriter.println("ERROR_DELETING_FILE " + filename);
            }
        }
    }
    
/////////////////////////////////// MAIN /////////////////////////////////////////////////////////////////////////////////////////////////

    // Other methods remain the same...

    public void stop() {
        running = false;
        try {
            if (serverSocket != null) serverSocket.close();
            if (controllerSocket != null) controllerSocket.close();
        } catch (IOException e) {
            System.out.println("Error closing connections: " + e.getMessage());
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
        } catch (Exception e) {
            System.out.println("Failed to start Dstore: " + e.getMessage());
        }
    }
}
