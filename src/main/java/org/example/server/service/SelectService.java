package org.example.server.service;

import com.mongodb.client.*;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.Distinct;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import org.bson.Document;
import org.example.server.entity.IndexFile;
import org.jdom2.Element;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.example.server.connectionManager.DbConnectionManager.getMongoClient;
import static org.example.server.service.DBMSService.DATABASE_NAME;

public class SelectService {
    public static String selectFromTable(Select select) throws Exception {
        PlainSelect plainSelect = (PlainSelect) select.getSelectBody();
        Distinct distinct = plainSelect.getDistinct();
        boolean isDistinct = distinct != null;

        Table fromTable = (Table) plainSelect.getFromItem();
        String fromTableName = fromTable.getName();

        List<String> selectedColumns = getSelectedColumns(plainSelect);

        List<String> selectedValues = new ArrayList<>();
        selectedValues.add(String.join(";", selectedColumns));

        Element tableElement = DBMSService.getTableByName(fromTableName);
        List<String> columnsForTable = getAttributesForTable(tableElement);
        List<String> primaryKeys = getPrimaryKeys(tableElement);

        Expression whereExpression = plainSelect.getWhere();

        String collectionName = DBMSService.dbmsRepository.getCurrentDatabase() + fromTableName;
        if (whereExpression != null) {
            List<Expression> andExpressionsFromWhereClause = new ArrayList<>(extractAndExpressions(whereExpression));
            List<String> tableColumnsFromWhereClause = new ArrayList<>(getTableColumnsFromExpressions(andExpressionsFromWhereClause));
            Set<String> columns = new HashSet<>(selectedColumns);
            columns.addAll(tableColumnsFromWhereClause);
            validateTableNameAndColumn(columnsForTable, columns);
            List<IndexFile> indexFilesForSelectedColumns = getIndexesForColumnFromWhere(tableElement, tableColumnsFromWhereClause);
            columnsForTable.removeAll(primaryKeys);
            if (indexFilesForSelectedColumns.isEmpty()) {
                selectWithIndex(collectionName, andExpressionsFromWhereClause, indexFilesForSelectedColumns, columnsForTable);
            } else {
                selectWithoutIndex(collectionName, columnsForTable, andExpressionsFromWhereClause);
            }
        } else {
            validateTableNameAndColumn(columnsForTable, new HashSet<>(selectedColumns));
            //selectedValues.addAll(selectWithoutIndex(collectionName, positionsOfSelectedColumn, positionsOfPrimaryKeys));
        }
        if (isDistinct) {
            selectedValues = selectedValues.stream()
                    .distinct()
                    .toList();
        }
        return getStringFromSelectedValues(selectedValues);
    }

    private static String getStringFromSelectedValues(List<String> selectedValues) {
        StringBuilder selectedValuesString = new StringBuilder(selectedValues.get(0));
        selectedValuesString.append("|");
        for (int index = 1; index < selectedValues.size(); index++) {
            selectedValuesString.append(selectedValues.get(index)).append("|");
        }
        return selectedValuesString.toString();
    }
    private static List<Expression> extractAndExpressions(Expression expression) {
        List<Expression> andExpressions = new ArrayList<>();

        if (expression instanceof AndExpression andExpr) {
            andExpressions.addAll(extractAndExpressions(andExpr.getLeftExpression()));
            andExpressions.addAll(extractAndExpressions(andExpr.getRightExpression()));
        } else {
            andExpressions.add(expression);
        }

        return andExpressions;
    }

    private static List<String> getTableColumnsFromExpressions(List<Expression> expressions) {
        return expressions.stream()
                .map(expression -> expression.getASTNode().jjtGetFirstToken().toString())
                .toList();
    }

    private static List<String> getSelectedColumns(PlainSelect plainSelect) {
        return plainSelect.getSelectItems().stream()
                .map(selectItem -> selectItem.getASTNode().jjtGetValue().toString())
                .toList();
    }

    private static void validateTableNameAndColumn(List<String> attributesForTable, Set<String> columns) throws Exception {
        if (!new HashSet<>(attributesForTable).containsAll(columns)) {
            throw new Exception("Invalid attributes!");
        }
    }

    private static List<String> getAttributesForTable(Element tableElement) {
        return tableElement.getChild("Structure").getChildren("Attribute").stream()
                .map(attribute -> attribute.getAttributeValue("attributeName"))
                .collect(Collectors.toList());
    }

    private static List<String> getPrimaryKeys(Element tableElement) {
        return tableElement.getChild("primaryKey").getChildren("pkAttribute").stream()
                .map(pk -> pk.getContent().get(0).getValue())
                .toList();
    }

    private static List<String> selectWithIndex(String collectionName, List<Expression> andExpressionsFromWhereClause, List<IndexFile> indexFiles, List<String> attributes) throws Exception {
        List<String> primaryKeys = new ArrayList<>();
        for (IndexFile indexFile : indexFiles) {
            List<String> primaryKeysFromIndex = selectFromIndex(indexFile.getIndexFileName(), getIdForIndex(indexFile, andExpressionsFromWhereClause));
            if (primaryKeys.isEmpty()) {
                primaryKeys.addAll(primaryKeysFromIndex);
            } else {
                primaryKeys.retainAll(primaryKeysFromIndex);
            }
        }
        List<Expression> whereNoIndex = getWhereNoIndex(andExpressionsFromWhereClause, indexFiles);
        return selectByPrimaryKeys(collectionName, primaryKeys, attributes, whereNoIndex);
    }

    private static List<Expression> getWhereNoIndex(List<Expression> andExpressionsFromWhereClause, List<IndexFile> indexFiles) {
        return andExpressionsFromWhereClause.stream()
                .filter(expression ->
                        indexFiles.stream().noneMatch(index -> index.getAttributes().contains(expression.getASTNode().jjtGetFirstToken().toString())))
                .toList();
    }

    private static List<String> selectByPrimaryKeys(String collectionName, List<String> primaryKeys, List<String> attributes, List<Expression> whereNoIndex) throws Exception {
        List<String> values = new ArrayList<>();
        try (MongoClient client = getMongoClient()) {
            MongoDatabase mongoDatabase = client.getDatabase(DATABASE_NAME);
            MongoCollection<Document> collection = mongoDatabase.getCollection(collectionName);

            Document query = new Document("_id", new Document("$in", primaryKeys));
            MongoCursor<Document> cursor = collection.find(query).iterator();
            List<Document> documents = new ArrayList<>();
            while (cursor.hasNext()) {
                documents.add(cursor.next());
            }
            computeInMemoryFilters(documents, attributes, whereNoIndex);
        } catch (Exception e) {
            throw new Exception(e.getMessage());
        }
        return values;
    }

    private static List<String> selectWithoutIndex(String collectionName, List<String> attributes, List<Expression> andExpressionsFromWhereClause) throws Exception {
        List<String> values = new ArrayList<>();
        try (MongoClient client = getMongoClient()) {
            MongoDatabase mongoDatabase = client.getDatabase(DATABASE_NAME);
            MongoCollection<Document> collection = mongoDatabase.getCollection(collectionName);

            MongoCursor<Document> cursor = collection.find().iterator();
            List<Document> documents = new ArrayList<>();
            while (cursor.hasNext()) {
                documents.add(cursor.next());
            }
            computeInMemoryFilters(documents, attributes, andExpressionsFromWhereClause);
        } catch (Exception e) {
            throw new Exception(e.getMessage());
        }
        return values;
    }

    private static List<Document> computeInMemoryFilters(List<Document> result, List<String> attributes, List<Expression> whereNoIndex) {
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

    private static String getIdForIndex(IndexFile indexFile, List<Expression> andExpressionsFromWhereClause) {
        StringBuilder value = new StringBuilder();
        for (String attribute : indexFile.getAttributes()) {
            Optional<String> valueForAttribute = getRightValueForExpression(attribute, andExpressionsFromWhereClause);
            if (valueForAttribute.isPresent()) {
                if (value.length() == 0) {
                    value.append(valueForAttribute.get());
                } else {
                    value.append("\\$").append(valueForAttribute.get());
                }
            }
        }
        return value.toString();
    }

    private static Optional<String> getRightValueForExpression(String leftExpression, List<Expression> andExpressionsFromWhereClause) {
        for (Expression expression : andExpressionsFromWhereClause) {
            if (expression.getASTNode().jjtGetFirstToken().toString().equals(leftExpression)) {
                return Optional.ofNullable(expression.getASTNode().jjtGetLastToken().toString());
            }
        }
        return Optional.empty();
    }

    private static List<String> selectFromIndex(String indexFileName, String value) throws Exception {
        List<String> primaryKeys = new ArrayList<>();
        try (MongoClient client = getMongoClient()) {
            MongoDatabase mongoDatabase = client.getDatabase(DATABASE_NAME);
            MongoCollection<Document> collection = mongoDatabase.getCollection(indexFileName);

            Pattern regex = Pattern.compile("\\b" + value + "($|\\b)");
            Document query = new Document("_id", regex);
            MongoCursor<Document> cursor = collection.find(query).iterator();

            while (cursor.hasNext()) {
                primaryKeys.addAll(List.of(cursor.next().get("values").toString().split("\\$")));
            }
        } catch (Exception e) {
            throw new Exception(e.getMessage());
        }
        return primaryKeys;
    }

    private static List<IndexFile> getIndexesForColumnFromWhere(Element tableElement, List<String> tableColumnsFromWhereClause) {
        List<IndexFile> indexFiles = DBMSService.getIndexFilesForTable(tableElement);
        return indexFiles.stream()
                .filter(indexFile -> !indexFile.getIndexFileName().contains("FkInd") && tableColumnsFromWhereClause.contains(indexFile.getAttributes().get(0)))
                .toList();
    }

}
