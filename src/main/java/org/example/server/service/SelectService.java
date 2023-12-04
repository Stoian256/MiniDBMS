package org.example.server.service;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.Distinct;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import org.bson.Document;
import org.example.server.entity.IndexFile;
import org.jdom2.Element;

import java.util.*;
import java.util.stream.Collectors;

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
        List<Integer> positionsOfSelectedColumn = getListOfPositionOfTwoLists(columnsForTable, selectedColumns);
        List<Integer> positionsOfPrimaryKeys = getListOfPositionOfTwoLists(columnsForTable, primaryKeys);
        positionsOfPrimaryKeys.retainAll(positionsOfSelectedColumn);

        if (whereExpression != null) {
            List<Expression> andExpressionsFromWhereClause = new ArrayList<>(extractAndExpressions(whereExpression));
            List<String> tableColumnsFromWhereClause = new ArrayList<>(getTableColumnsFromExpressions(andExpressionsFromWhereClause));
            Set<String> columns = new HashSet<>(selectedColumns);
            columns.addAll(tableColumnsFromWhereClause);
            validateTableNameAndColumn(columnsForTable, columns);
            List<IndexFile> indexFilesForSelectedColumns = getIndexesForColumnFromWhere(tableElement, tableColumnsFromWhereClause);
            if (indexFilesForSelectedColumns.isEmpty()) {

            } else {

            }
        } else {
            validateTableNameAndColumn(columnsForTable, new HashSet<>(selectedColumns));
            selectedValues.addAll(selectWithoutIndex(collectionName, positionsOfSelectedColumn, positionsOfPrimaryKeys));
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
                .toList();
    }

    private static List<String> getPrimaryKeys(Element tableElement) {
        return tableElement.getChild("primaryKey").getChildren("pkAttribute").stream()
                .map(pk -> pk.getContent().get(0).getValue())
                .toList();
    }

    private static List<Integer> getListOfPositionOfTwoLists(List<String> firstList, List<String> secondList) {
        return secondList.stream()
                .map(firstList::indexOf)
                .collect(Collectors.toList());
    }

    private static List<String> selectWithoutIndex(String collectionName, List<Expression> andExpressionsFromWhereClause, List<String> selectedColumns, List<Integer> positionsOfSelectedColumn, List<Integer> positionsOfPrimaryKeys) throws Exception {
        List<String> selectedValues = new ArrayList<>();
        try (MongoClient client = getMongoClient()) {
            MongoDatabase mongoDatabase = client.getDatabase(DATABASE_NAME);
            MongoCollection<Document> collection = mongoDatabase.getCollection(collectionName);
            FindIterable<Document> documents = collection.find();

            for (Document document : documents) {
                selectedValues.add(getSelectedValueForDocument(document, andExpressionsFromWhereClause, selectedColumns, positionsOfSelectedColumn, positionsOfPrimaryKeys));
            }
        } catch (Exception e) {
            throw new Exception(e.getMessage());
        }
        return selectedValues;
    }

    private static String getSelectedValueForDocument(Document document, List<Expression> andExpressionsFromWhereClause, List<String> selectedColumns, List<Integer> positionsOfSelectedColumn, List<Integer> positionsOfPrimaryKeys) {
        StringBuilder selectedValue = new StringBuilder();
        int contorForPk = 0;

        String[] values = document.get("values").toString().split("#");
        String[] primaryKeyValues = document.get("_id").toString().split("#");
        for (Integer column : positionsOfSelectedColumn) {
            int primaryKeyIndex = positionsOfPrimaryKeys.indexOf(column);
            if (primaryKeyIndex != -1) {
                selectedValue.append(primaryKeyValues[primaryKeyIndex]).append(";");
                contorForPk ++;
            } else {
                selectedValue.append(values[positionsOfSelectedColumn.indexOf(column) - contorForPk]).append(";");
            }
        }
        return selectedValue.toString();
    }

    private static boolean validateValue(List<Expression> andExpressionsFromWhereClause, String column) {

    }

    private static Optional<Expression> getExpressionByLeftExpression(List<Expression> andExpressionsFromWhereClause, String column) {
        return andExpressionsFromWhereClause.stream()
                .filter(expression -> expression.getASTNode())
    }

    private static List<IndexFile> getIndexesForColumnFromWhere(Element tableElement, List<String> tableColumnsFromWhereClause) {
        List<IndexFile> indexFiles = DBMSService.getIndexFilesForTable(tableElement);
        return indexFiles.stream()
                .filter(indexFile -> new HashSet<>(tableColumnsFromWhereClause).containsAll(indexFile.getAttributes()))
                .toList();
    }

    private static List<String> selectWithIndexes(String collectionName, List<IndexFile> indexFiles, List<Integer> positionsOfSelectedColumn, List<Integer> positionsOfPrimaryKeys) throws Exception {
        List<String> selectedValues = new ArrayList<>();
        try (MongoClient client = getMongoClient()) {
            MongoDatabase mongoDatabase = client.getDatabase(DATABASE_NAME);
            for (IndexFile indexFile : indexFiles) {

            }
        } catch (Exception e) {
            throw new Exception(e.getMessage());
        }
        return selectedValues;
    }

    private static List<String> selectWithoutIndexes(String collectionName, List<Integer> positionsOfSelectedColumn, List<Integer> positionsOfPrimaryKeys) throws Exception {
        List<String> selectedValues = new ArrayList<>();
        try (MongoClient client = getMongoClient()) {
            MongoDatabase mongoDatabase = client.getDatabase(DATABASE_NAME);
            MongoCollection<Document> collection = mongoDatabase.getCollection(collectionName);
            FindIterable<Document> documents = collection.find();

            for (Document document : documents) {
                selectedValues.add(getSelectedValueForDocument(document, positionsOfSelectedColumn, positionsOfPrimaryKeys));
            }
        } catch (Exception e) {
            throw new Exception(e.getMessage());
        }
        return selectedValues;
    }

    private static List<String> getKeysFromIndexFile(String collectionName, String keyValue) throws Exception {
        try (MongoClient client = getMongoClient()) {
            MongoDatabase mongoDatabase = client.getDatabase(DATABASE_NAME);
            MongoCollection<Document> collection = mongoDatabase.getCollection(collectionName);

            Document filter = new Document("_id", keyValue);
            Document value = collection.find(filter).first();
            if (value != null){
                return List.of(value.get("_id").toString().split("#"));
            } else {
                return new ArrayList<>();
            }
        } catch (Exception e) {
            throw new Exception(e.getMessage());
        }
    }
}
