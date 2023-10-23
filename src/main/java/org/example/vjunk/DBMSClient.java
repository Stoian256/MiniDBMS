package org.example.vjunk;

import net.sf.jsqlparser.JSQLParserException;
import org.example.server.repository.DBMSRepository;
import org.example.server.service.DBMSService;

import java.util.Objects;
import java.util.Scanner;

public class DBMSClient {

    public static void main(String[] args) {
        String catalogPath = "catalog.xml";
        DBMSRepository dbmsRepository = new DBMSRepository(catalogPath);
        DBMSService dbmsService = new DBMSService(dbmsRepository);

        Scanner scanner = new Scanner(System.in);
        while(true){
            System.out.print("Introduceți comanda SQL: ");
            String input = scanner.nextLine(); // Citirea unei linii de la intrarea standard
            if(Objects.equals(input, ""))
                return;
            try {
                dbmsService.executeCommand(input);
            } catch (JSQLParserException e) {
                throw new RuntimeException(e);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

    }
}
