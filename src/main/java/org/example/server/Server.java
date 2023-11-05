package org.example.server;

import net.sf.jsqlparser.JSQLParserException;
import org.example.server.repository.DBMSRepository;
import org.example.server.service.DBMSService;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;

public class Server {
    public static void main(String[] args) {
        final int port = 12345;
        String catalogPath = "catalog.xml";
        DBMSRepository dbmsRepository = new DBMSRepository(catalogPath);
        DBMSService dbmsService = new DBMSService(dbmsRepository);

        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("Serverul a pornit și ascultă pe portul " + port);
            //dbmsService.testMongoDb();
            while (true) {
                Socket clientSocket = serverSocket.accept();
                System.out.println("S-a conectat un client.");


                BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);

                String message;
                while ((message = in.readLine()) != null) {
                    System.out.println("Client: " + message);
                    if (message == "")
                        return;
                    try {
                        dbmsService.executeCommand(message);
                        out.println("Server: Operatie executata cu succes");
                    } catch (JSQLParserException e) {
                        System.out.println(e);
                        out.println("Server: Eroare parsare comanda");
                    } catch (Exception e) {
                        out.println("Server: " + e.getMessage());

                    }

                }

                clientSocket.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
