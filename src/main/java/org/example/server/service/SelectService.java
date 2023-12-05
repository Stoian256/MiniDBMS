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

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
        Element tableElement = getTableElement(tableName);
        List<String> primaryKeys = getPrimaryKeys(tableElement);
        List<String> attributes = getAttributesForTable(tableElement);
        List<String> selectedColumns = plainSelect.getSelectItems().stream().map(Objects::toString).collect(Collectors.toList());
        attributes.removeAll(primaryKeys);
        Expression where = plainSelect.getWhere(); // Obținem clauza WHERE


        if (where != null) {
            List<Expression> conditions = extractConditions(where);
            processConditions(rootDataBases, database, tableName, conditions, attributes, primaryKeys, selectedColumns);
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

    private void processConditions(Element rootDataBases, MongoDatabase database, String tableName, List<Expression> conditions, List<String> attributes, List<String> primaryKeys, List<String> selectedColumns) throws Exception {
        List<String> allMatchingIDs = new ArrayList<>();
        List<Expression> whereNoIndex = new ArrayList<>();

        for (Expression condition : conditions) {
            if (condition instanceof ComparisonOperator || condition instanceof LikeExpression) {
                List<String> matchingIDs = processSingleCondition(rootDataBases, database, tableName, condition, whereNoIndex);
                if (allMatchingIDs.isEmpty()) {
                    allMatchingIDs.addAll(matchingIDs);
                } else {
                    if (!Objects.equals(matchingIDs.get(0), "-1"))
                        allMatchingIDs.retainAll(matchingIDs);
                }
            }
        }

        if (!allMatchingIDs.isEmpty()) {
            MongoCollection<Document> studentsCollection = database.getCollection(dbmsRepository.getCurrentDatabase() + tableName);
            List<Document> result = retrieveEntities(studentsCollection, allMatchingIDs);
            result = computeInMemoryFilters(result, attributes, whereNoIndex);

            if(selectedColumns.contains("*")) {
                selectedColumns.clear();
                selectedColumns.addAll(primaryKeys);
                selectedColumns.addAll(attributes);
            }

            selectedColumns.forEach(column -> System.out.print(column + " "));
            System.out.println();

            result.forEach(document -> {
                selectedColumns.forEach(
                        selectedColumn -> {
                            Integer keyPosition = primaryKeys.indexOf(selectedColumn);
                            Integer valuePosition = attributes.indexOf(selectedColumn);
                            if (keyPosition!=-1){
                                System.out.print(document.getString("_id").split("$")[keyPosition] +" ");
                            } else if (valuePosition!=-1) {
                                System.out.print(document.getString("values").split("#")[valuePosition] +" ");
                            }
                        }
                );
                System.out.println();
            });
        } else {
            System.out.println("Nu au fost găsite înregistrări care să satisfacă toate condițiile.");
        }
    }

    private List<Document> computeInMemoryFilters(List<Document> result, List<String> attributes, List<Expression> whereNoIndex) {
        for (Expression expression : whereNoIndex) {
            if (expression instanceof LikeExpression) {
                LikeExpression likeExpression = (LikeExpression) expression;
                Column column = (Column) likeExpression.getLeftExpression();
                String conditionValue = likeExpression.getRightExpression().toString().replace("'", "");
                String operator = likeExpression.getStringExpression();


                int columnIndex = attributes.indexOf(column.getColumnName());


                if (columnIndex != -1)
                    return result.stream()
                            .filter(doc -> {
                                Pattern pattern = Pattern.compile(conditionValue);
                                String columnValue = doc.getString("values").split("#")[columnIndex];
                                Matcher matcher = pattern.matcher(columnValue);
                                return matcher.find();
                            }).toList();
            }
            if (expression instanceof ComparisonOperator) {
                ComparisonOperator comparisonOperator = (ComparisonOperator) expression;
                Column column = (Column) comparisonOperator.getLeftExpression();
                Integer conditionValue = Integer.parseInt(comparisonOperator.getRightExpression().toString());
                String operator = comparisonOperator.getStringExpression();

                int columnIndex = attributes.indexOf(column.getColumnName());

                Stream<Document> filteredStream = result.stream();

                switch (operator) {
                    case ">":
                        filteredStream = filteredStream.filter(doc -> {
                            int columnValue = Integer.parseInt(doc.getString("values").split("#")[columnIndex]);
                            return columnValue > conditionValue;
                        });
                        break;
                    case ">=":
                        filteredStream = filteredStream.filter(doc -> {
                            int columnValue = Integer.parseInt(doc.getString("values").split("#")[columnIndex]);
                            return columnValue >= conditionValue;
                        });
                        break;
                    case "<":
                        filteredStream = filteredStream.filter(doc -> {
                            int columnValue = Integer.parseInt(doc.getString("values").split("#")[columnIndex]);
                            return columnValue < conditionValue;
                        });
                        break;
                    case "<=":
                        filteredStream = filteredStream.filter(doc -> {
                            int columnValue = Integer.parseInt(doc.getString("values").split("#")[columnIndex]);
                            return columnValue <= conditionValue;
                        });
                        break;
                    case "=":
                        filteredStream = filteredStream.filter(doc -> {
                            int columnValue = Integer.parseInt(doc.getString("values").split("#")[columnIndex]);
                            return columnValue == conditionValue;
                        });
                        break;
                    default:
                        break;
                }

                return filteredStream.toList();
            }
        }
        return result;
    }

    List<String> getAttributesForTable(Element tableElement) {
        List<String> attributeNames = new ArrayList<>();
        List<Element> attributeElements = tableElement.getChild("Structure").getChildren("Attribute");
        for (Element attribute : attributeElements) {
            String attributeName = attribute.getAttributeValue("attributeName");
            attributeNames.add(attributeName);
        }
        return attributeNames;
    }

    private static List<String> getPrimaryKeys(Element tableElement) {
        return tableElement.getChild("primaryKey").getChildren("pkAttribute").stream()
                .map(pk -> pk.getContent().get(0).getValue())
                .toList();
    }

    private static Element getTableElement(String tableName) {
        Element rootDataBases = dbmsRepository.getDoc().getRootElement().getChild("DataBase");
        List<Element> tables = rootDataBases.getChildren("Table");
        for (Element table : tables) {
            String currentTableName = table.getAttributeValue("tableName");
            if (currentTableName.equals(tableName)) {
                return table;
            }
        }
        return null;
    }

    private List<String> processSingleCondition(Element rootDataBases, MongoDatabase database, String tableName, Expression where, List<Expression> whereNoIndex) throws Exception {
        List<String> matchingIDs = new ArrayList<>();
        matchingIDs.add("-1");

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

                if (!tempMatchingIDs.isEmpty()) {
                    if (Objects.equals(matchingIDs.get(0), "-1")) {
                        matchingIDs.clear();
                        matchingIDs.addAll(tempMatchingIDs);
                    } else {
                        matchingIDs.retainAll(tempMatchingIDs);
                    }
                }
            } else
                whereNoIndex.add(where);


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
                if (indexAttrName.equals(columnName))
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
                        .replace("'", "");
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

    private static List<Document> retrieveEntities(MongoCollection<Document> studentsCollection, List<String> matchingIDs) {
        List<Document> entities = new ArrayList<>();

        for (String id : matchingIDs) {
            Document document = studentsCollection.find(new Document("_id", id)).first();
            if (document != null) {
                entities.add(document);
            }
        }

        return entities;
    }
}
