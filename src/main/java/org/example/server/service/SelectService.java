package org.example.server.service;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectVisitorAdapter;
import org.example.server.repository.DBMSRepository;
import org.jdom2.Element;
import org.bson.Document;

import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;

import static com.mongodb.client.model.Filters.expr;
import static com.mongodb.client.model.Filters.gt;
import static org.example.server.connectionManager.DbConnectionManager.getMongoClient;

public class SelectService {
    private static DBMSRepository dbmsRepository;
    private static final String DATABASE_NAME = "mini_dbms";

    public SelectService(DBMSRepository dbmsRepository) {
        this.dbmsRepository = dbmsRepository;
    }

    public void processSelect(Select selectStatement) {
        Element rootDataBases = dbmsRepository.getDoc().getRootElement();
        MongoClient client = getMongoClient();
        MongoDatabase database = client.getDatabase(DATABASE_NAME);
        selectStatement.getSelectBody().accept(new SelectVisitorAdapter() {
            @Override
            public void visit(PlainSelect plainSelect) {
                String tableName = plainSelect.getFromItem().toString(); // Obținem numele tabelei
                Expression where = plainSelect.getWhere(); // Obținem clauza WHERE

                if (where instanceof ComparisonOperator) {
                    ComparisonOperator comparisonOperator = (ComparisonOperator) where;
                    Column column = (Column) comparisonOperator.getLeftExpression();
                    String columnName = column.getColumnName();

                    LongValue longValue = (LongValue) comparisonOperator.getRightExpression();
                    int conditionValue = (int) longValue.getValue();

                    String operator = comparisonOperator.getStringExpression();
                    if (existIndexForColumn(rootDataBases, tableName, columnName)) {
                        MongoCollection<Document> studentsCollection = database.getCollection(dbmsRepository.getCurrentDatabase() + tableName);
                        MongoCollection<Document> indexCollection = database.getCollection(dbmsRepository.getCurrentDatabase() + tableName + "Ind" + columnName);
                        List<String> matchingIDs = findMatchingIDs(indexCollection, columnName, conditionValue, operator);
                        List<String> result = retrieveEntities(studentsCollection, matchingIDs);
                        for (String entity : result) {
                            System.out.println(entity);
                        }
                    }
                }
            }
        });

    }

    // Funcție pentru a verifica dacă există un index pentru o anumită coloană
    private static boolean existIndexForColumn(Element rootDataBases, String tableName, String columnName) {
        // Implementează logică pentru a verifica existența indexului pentru coloana specificată
        // Parsează fișierul XML pentru a găsi detaliile despre indexe și verifică dacă există un index pentru coloana specificată
        // Returnează true dacă există, altfel false
        // Exemplu de implementare:
        Element table = rootDataBases.getChild("DataBase").getChild("Table");
        String tableAttrName = table.getAttributeValue("tableName");
        if (tableAttrName.equals(tableName)) {
            Element indexFiles = table.getChild("IndexFiles");
            Element indexFile = indexFiles.getChild("IndexFile");
            String indexAttrName = indexFile.getChild("IndexAttributes").getChildText("IAttribute");
            return indexAttrName.equals(columnName);
        }
        return false;
    }

    // Funcție pentru a găsi ID-urile care satisfac condiția specificată
    private static List<String> findMatchingIDs(
            MongoCollection<Document> indexCollection,
            String columnName,
            Integer conditionValue,
            String operator
    ) {
        List<String> matchingIDs = new ArrayList<>();
        Document query = new Document();

        switch (operator) {
            case ">":
                query.append("$expr", new Document("$gt", Arrays.asList(new Document("$toInt", "$_id"), conditionValue)));
                break;
            case ">=":
                query.append("$expr", new Document("$gte", Arrays.asList(new Document("$toInt", "$_id"), conditionValue)));
                break;
            case "<":
                query.append("$expr", new Document("$lt", Arrays.asList(new Document("$toInt", "$_id"), conditionValue)));
                break;
            case "<=":
                query.append("$expr", new Document("$lte", Arrays.asList(new Document("$toInt", "$_id"), conditionValue)));
                break;
            default:
                // Operatorul nu este recunoscut
                // Poți trata această situație în funcție de necesitățile tale
                break;
        }
        System.out.println(query.toJson());
        for (Document document : indexCollection.find(query)) {
            matchingIDs.add(document.get("values").toString());
        }

        return matchingIDs;
    }

    // Funcție pentru a recupera entitățile complete (înregistrările) pe baza ID-urilor
    private static List<String> retrieveEntities(MongoCollection<Document> studentsCollection, List<String> matchingIDs) {
        List<String> entities = new ArrayList<>();

        // Implementează logica de recuperare a entităților din colecția principală pe baza ID-urilor
        // Exemplu de implementare:
        for (String id : matchingIDs) {
            Document document = studentsCollection.find(new Document("_id", id)).first();
            if (document != null) {
                entities.add(document.toJson()); // sau procesează entitatea în alt mod
            }
        }

        return entities;
    }
}
