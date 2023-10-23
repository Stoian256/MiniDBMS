package org.example.client;

import net.sf.jsqlparser.JSQLParserException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Objects;
import java.util.Scanner;

public class Client {
    public static void main(String[] args) {
        final String serverAddress = "localhost"; // Adresa serverului
        final int serverPort = 12345; // Portul pe care serverul ascultă

        try (Socket socket = new Socket(serverAddress, serverPort)) {
            // Creează un flux pentru a trimite mesaje către server
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);

            // Creează un flux pentru a citi mesaje de la server
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

            Scanner scanner = new Scanner(System.in);
            while (true) {
                System.out.print("Introduceți comanda SQL: ");
                String input = scanner.nextLine(); // Citirea unei linii de la intrarea standard
                if (Objects.equals(input, ""))
                    return;

                out.println(input);
                //dbmsService.executeCommand(input);
                String response = in.readLine();
                System.out.println("Server: " + response);

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
