package org.example.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Objects;
import java.util.Scanner;

public class Client {
    public static void main(String[] args) {
        final String serverAddress = "localhost";
        final int serverPort = 12345;

        try (Socket socket = new Socket(serverAddress, serverPort)) {
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

            Scanner scanner = new Scanner(System.in);
            while (true) {
                System.out.print("Introduceți comanda SQL: ");
                String input = scanner.nextLine();
                if (input.equals(""))
                    return;
                out.println(input);
                String response = in.readLine();
                System.out.println(response);

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
