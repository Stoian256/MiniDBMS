package org.example.server.service;


import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import org.example.server.repository.DBMSRepository;
import org.jdom2.Element;
import org.bson.Document;

import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.regex.Pattern;

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

        if (where != null) {
            List<Expression> conditions = extractConditions(where);
            processConditions(rootDataBases, database, tableName, conditions);
        }
    }

    private List<Expression> extractConditions(Expression where) {
        List<Expression> conditions = new ArrayList<>();

        if (where instanceof AndExpression) {
            AndExpression andExpression = (AndExpression) where;
            conditions.addAll(extractConditions(andExpression.getLeftExpression()));
            conditions.addAll(extractConditions(andExpression.getRightExpression()));
        } else {
            conditions.add(where);
        }

        return conditions;
    }

    private void processConditions(Element rootDataBases, MongoDatabase database, String tableName, List<Expression> conditions) throws Exception {
        List<String> allMatchingIDs = new ArrayList<>();

        for (Expression condition : conditions) {
            if (condition instanceof ComparisonOperator || condition instanceof LikeExpression) {
                List<String> matchingIDs = processSingleCondition(rootDataBases, database, tableName, condition);
                if (allMatchingIDs.isEmpty()) {
                    allMatchingIDs.addAll(matchingIDs);
                } else {
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

    private List<String> processSingleCondition(Element rootDataBases, MongoDatabase database, String tableName, Expression where) throws Exception {
        List<String> matchingIDs = new ArrayList<>();

        if (where instanceof ComparisonOperator || where instanceof LikeExpression) {
            Column column;
            String conditionValue;
            String operator;

            if (where instanceof ComparisonOperator) {
                ComparisonOperator comparisonOperator = (ComparisonOperator) where;
                column = (Column) comparisonOperator.getLeftExpression();
                conditionValue = comparisonOperator.getRightExpression().toString();
                operator = comparisonOperator.getStringExpression();
            } else {
                LikeExpression likeExpression = (LikeExpression) where;
                column = (Column) likeExpression.getLeftExpression();
                conditionValue = likeExpression.getRightExpression().toString();
                operator = likeExpression.getStringExpression();
            }

            String columnName = column.getColumnName();
            List<String> tempMatchingIDs = new ArrayList<>();

            if (existIndexForColumn(rootDataBases, tableName, columnName)) {
                MongoCollection<Document> indexCollection = database.getCollection(dbmsRepository.getCurrentDatabase() + tableName + "Ind" + columnName);
                tempMatchingIDs = findMatchingIDs(indexCollection, columnName, conditionValue, operator);
            }

            if (!tempMatchingIDs.isEmpty()) {
                if (matchingIDs.isEmpty()) {
                    matchingIDs.addAll(tempMatchingIDs);
                } else {
                    matchingIDs.retainAll(tempMatchingIDs);
                }
            }
        }

        return matchingIDs;
    }

     private static boolean existIndexForColumn(Element rootDataBases, String tableName, String columnName) {
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

     private static List<String> findMatchingIDs(
            MongoCollection<Document> indexCollection,
            String columnName,
            String conditionValue,
            String operator
    ) {
        List<String> matchingIDs = new ArrayList<>();
        Document query = new Document();

        switch (operator) {
            case ">":
                query.append("$expr", new Document("$gt", Arrays.asList(new Document("$toInt", "$_id"), Integer.parseInt(conditionValue))));
                break;
            case ">=":
                query.append("$expr", new Document("$gte", Arrays.asList(new Document("$toInt", "$_id"), Integer.parseInt(conditionValue))));
                break;
            case "<":
                query.append("$expr", new Document("$lt", Arrays.asList(new Document("$toInt", "$_id"), Integer.parseInt(conditionValue))));
                break;
            case "<=":
                query.append("$expr", new Document("$lte", Arrays.asList(new Document("$toInt", "$_id"), Integer.parseInt(conditionValue))));
                break;
            case "=":
                query.append("_id", new Document("$eq", conditionValue));
                break;
            case "LIKE":
                String patternForMongoDB = conditionValue
                        .replace("'","");
                Pattern regexPattern = Pattern.compile(patternForMongoDB);
                query.append("_id", new Document("$regex", regexPattern));
                break;
            default:
                break;
        }
        for (Document document : indexCollection.find(query)) {
            matchingIDs.addAll(Arrays.asList(document.get("values").toString().split("\\$")));
        }

        return matchingIDs;
    }

     private static List<String> retrieveEntities(MongoCollection<Document> studentsCollection, List<String> matchingIDs) {
        List<String> entities = new ArrayList<>();

        for (String id : matchingIDs) {
            Document document = studentsCollection.find(new Document("_id", id)).first();
            if (document != null) {
                entities.add(document.toJson());
            }
        }

        return entities;
    }
}
