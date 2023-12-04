package org.example.server.service;


import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectVisitorAdapter;
import org.bson.BsonTimestamp;
import org.example.server.repository.DBMSRepository;
import org.jdom2.Element;
import org.bson.Document;

import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;

import static org.example.server.connectionManager.DbConnectionManager.getMongoClient;

public class SelectService {
    private static DBMSRepository dbmsRepository;
    private static final String DATABASE_NAME = "mini_dbms";

    public SelectService(DBMSRepository dbmsRepository) {
        this.dbmsRepository = dbmsRepository;
    }

    public void processSelect(Select selectStatement) throws Exception {
        Element rootDataBases = dbmsRepository.getDoc().getRootElement();
        MongoClient client = getMongoClient();
        MongoDatabase database = client.getDatabase(DATABASE_NAME);

        PlainSelect plainSelect = (PlainSelect) selectStatement.getSelectBody();

        String tableName = plainSelect.getFromItem().toString(); // Obținem numele tabelei
        Expression where = plainSelect.getWhere(); // Obținem clauza WHERE

        if (where instanceof ComparisonOperator) {
            processSingleCondition(rootDataBases, database, tableName, where);

        } else if (where instanceof AndExpression) {
            // Tratează cazul în care există mai multe condiții legate prin AND în clauza WHERE
            List<Expression> conditions = extractMultipleConditions((AndExpression) where);
            processMultipleConditions(rootDataBases, database, tableName, conditions);
        }
    }


    private List<Expression> extractMultipleConditions(AndExpression andExpression) {
        List<Expression> conditions = new ArrayList<>();
        Expression left = andExpression.getLeftExpression();
        Expression right = andExpression.getRightExpression();

        if (left instanceof AndExpression) {
            conditions.addAll(extractMultipleConditions((AndExpression) left));
        } else {
            conditions.add(left);
        }

        if (right instanceof AndExpression) {
            conditions.addAll(extractMultipleConditions((AndExpression) right));
        } else {
            conditions.add(right);
        }

        return conditions;
    }

    private void processSingleCondition(Element rootDataBases, MongoDatabase database, String tableName, Expression where) throws Exception {
        ComparisonOperator comparisonOperator = (ComparisonOperator) where;
        Column column = (Column) comparisonOperator.getLeftExpression();
        String columnName = column.getColumnName();

        LongValue longValue = (LongValue) comparisonOperator.getRightExpression();
        int conditionValue = (int) longValue.getValue();

        List<String> matchingIDs = List.of();
        String operator = comparisonOperator.getStringExpression();
        if (existIndexForColumn(rootDataBases, tableName, columnName)) {
            MongoCollection<Document> indexCollection = database.getCollection(dbmsRepository.getCurrentDatabase() + tableName + "Ind" + columnName);
            matchingIDs = findMatchingIDs(indexCollection, columnName, conditionValue, operator);
        }
        if (!matchingIDs.isEmpty()) {
            MongoCollection<Document> studentsCollection = database.getCollection(dbmsRepository.getCurrentDatabase() + tableName);
            List<String> result = retrieveEntities(studentsCollection, matchingIDs);
            result.forEach(System.out::println);
        } else {
            System.out.println("Nu au fost găsite înregistrări care să satisfacă toate condițiile.");
        }
    }

    private void processMultipleConditions(Element rootDataBases, MongoDatabase database, String tableName, List<Expression> conditions) throws Exception {
        List<String> allMatchingIDs = new ArrayList<>();

        for (Expression condition : conditions) {
            if (condition instanceof ComparisonOperator) {
                ComparisonOperator comparisonOperator = (ComparisonOperator) condition;
                Column column = (Column) comparisonOperator.getLeftExpression();
                String columnName = column.getColumnName();

                LongValue longValue = (LongValue) comparisonOperator.getRightExpression();
                int conditionValue = (int) longValue.getValue();

                String operator = comparisonOperator.getStringExpression();
                if (existIndexForColumn(rootDataBases, tableName, columnName)) {
                    MongoCollection<Document> indexCollection = database.getCollection(dbmsRepository.getCurrentDatabase() + tableName + "Ind" + columnName);
                    List<String> matchingIDs = findMatchingIDs(indexCollection, columnName, conditionValue, operator);
                    if (allMatchingIDs.isEmpty())
                        allMatchingIDs.addAll(matchingIDs);
                    else
                        allMatchingIDs.retainAll(matchingIDs);
                }
            }
        }

        if (!allMatchingIDs.isEmpty()) {

            MongoCollection<Document> studentsCollection = database.getCollection(dbmsRepository.getCurrentDatabase() + tableName);
            List<String> result = retrieveEntities(studentsCollection, allMatchingIDs);
            result.forEach(System.out::println);
        } else {
            System.out.println("Nu au fost găsite înregistrări care să satisfacă toate condițiile.");
        }
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
            List<Element> indexFiles = table.getChild("IndexFiles").getChildren();
            for (Element indexFile : indexFiles) {
                String indexAttrName = indexFile.getChild("IndexAttributes").getChildText("IAttribute");
                if(indexAttrName.equals(columnName))
                    return true;
            }
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
        for (Document document : indexCollection.find(query)) {
            matchingIDs.addAll(Arrays.asList(document.get("values").toString().split("\\$")));
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
