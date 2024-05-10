import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.util.Comparator;

public class Dstore {
    private ServerSocket serverSocket;
    private Socket controllerSocket;
    private PrintWriter controllerOut;
    private BufferedReader controllerIn;
    private final String controllerHost;
    private final int controllerPort;
    private final int port;
    private final int timeout;
    private boolean running = true;
    private final String fileFolder;
    private static final int CLIENT_TIMEOUT_MS = 10000; // Example: 10 seconds
    private static final int CONTROLLER_RECONNECT_DELAY_MS = 5000; // 5 seconds

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
    
      
    
    // Shared utility method to read and write file data
    private void transferFileData(InputStream inputStream, OutputStream outputStream, int bufferSize) throws IOException {
        byte[] buffer = new byte[bufferSize];
        int bytesRead;
        int totalBytesTransferred = 0;
        while ((bytesRead = inputStream.read(buffer)) != -1) {
            outputStream.write(buffer, 0, bytesRead);
            totalBytesTransferred += bytesRead;
            System.out.println("Transferred " + totalBytesTransferred + " bytes so far.");
        }
        outputStream.flush();
        System.out.println("Total bytes transferred: " + totalBytesTransferred);
    }

    private void handleLoadDataCommand(String filename, Socket clientSocket) {
        System.out.println("Loading file: " + filename);
        File file = new File(fileFolder, filename);

        // Additional debugging information
        System.out.println("File path: " + file.getAbsolutePath());
        System.out.println("File exists: " + file.exists());
        System.out.println("Is a file: " + file.isFile());
        System.out.println("File length: " + file.length());

        if (!file.exists() || !file.isFile()) {
            System.out.println("File not found or is not a file: " + filename);
            try (PrintWriter writer = new PrintWriter(clientSocket.getOutputStream(), true)) {
                writer.println("ERROR_FILE_DOES_NOT_EXIST");
            } catch (IOException e) {
                System.out.println("Error writing error message to client: " + e.getMessage());
            }
            return;
        }

        // Set a socket read timeout
        try {
            clientSocket.setSoTimeout(CLIENT_TIMEOUT_MS); // Replace CLIENT_TIMEOUT_MS with the appropriate timeout value
            System.out.println("Socket timeout set to " + CLIENT_TIMEOUT_MS + " ms");
        } catch (SocketException e) {
            System.out.println("Failed to set socket timeout: " + e.getMessage());
        }

        // Transfer file data using the shared utility function
        try (InputStream fileInputStream = new FileInputStream(file);
            OutputStream clientOutputStream = clientSocket.getOutputStream()) {
            System.out.println("Starting file transfer to client...");
            transferFileData(fileInputStream, clientOutputStream, 1024); // Reduced buffer size to 1024 bytes
            System.out.println("File successfully sent to client: " + filename);
        } catch (IOException e) {
            System.out.println("Error sending file to client: " + e.getMessage());
            try (PrintWriter writer = new PrintWriter(clientSocket.getOutputStream(), true)) {
                writer.println("ERROR_SENDING_FILE");
            } catch (IOException ex) {
                System.out.println("Error writing error message to client: " + ex.getMessage());
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
    
                // Split the command to identify its parts
                String[] parts = command.split(" ");
                if (parts.length < 2) {
                    System.out.println("Malformed command from controller: " + command);
                    continue;
                }
    
                // Process the command based on the type
                switch (parts[0]) {
                    case "REMOVE":
                        handleRemoveCommand(parts[1], controllerOut); // Pass the filename and controller writer
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
