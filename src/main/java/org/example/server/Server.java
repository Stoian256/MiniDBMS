package org.example.server;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.insert.Insert;
import org.example.server.repository.DBMSRepository;
import org.example.server.service.DBMSService;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.Random;

public class Server {
    public static void main(String[] args) {
        final int port = 12345;
        String catalogPath = "catalog.xml";
        DBMSRepository dbmsRepository = new DBMSRepository(catalogPath);
        DBMSService dbmsService = new DBMSService(dbmsRepository);
        try {
            //addDataInCoutry();
            //addDataInCity();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("Serverul a pornit și ascultă pe portul " + port);

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
                        e.printStackTrace();
                        System.out.println(e);
                        out.println("Server: Eroare parsare comanda");
                    } catch (Exception e) {
                        e.printStackTrace();
                        out.println("Server: " + e.getMessage());

                    }

                }

                clientSocket.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void addDataInCity() throws Exception {
        Random random = new Random();
        DBMSService.useDataBase("MyDB");

        for (int i = 1; i <= 10000; i++) {
            int cityID = i;
            int countryID = random.nextInt(100022) + 1; // Generare valoare aleatoare pentru CountryID între 1 și 100022
            String cityName = "Oras" + countryID; // Generare nume de oraș bazat pe ID-ul țării

            // Construirea comenzii SQL sub formă de string
            String sqlCommand = "INSERT INTO City (CityID, CountryID, CityName) VALUES (" +
                    cityID + ", " +
                    countryID + ", " +
                    cityName + ");";

            // Afisarea comenzii SQL generate
            DBMSService.insert((Insert) CCJSqlParserUtil.parse(sqlCommand));
            Thread.sleep(10);
        }
    }

    private static void addDataInCoutry() throws Exception {
        Random random = new Random();
        DBMSService.useDataBase("MyDB");

        for (int i = 90018; i <= 1000000; i++) {
            int countryID = i;
            String countryName = "Country" + i; // Generare un nume de țară bazat pe ID-ul său
            String countryCode = "Code" + i; // Generare un cod de țară aleatoriu

            // Construirea comenzii SQL sub formă de string
            String sqlCommand = "INSERT INTO Country (CountryID, CountryName, CountryCode) VALUES (" +
                    countryID + ", '" +
                    countryName + "', '" +
                    countryCode + "');";

            // Afisarea comenzii SQL generate
            DBMSService.insert((Insert) CCJSqlParserUtil.parse(sqlCommand));
        }
    }
}
