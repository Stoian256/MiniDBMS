package org.example.server.service;

import com.mongodb.client.*;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.Distinct;
import net.sf.jsqlparser.statement.select.Join;
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

import static net.sf.jsqlparser.util.validation.metadata.NamedObject.database;
import static org.example.server.connectionManager.DbConnectionManager.getMongoClient;
import static org.example.server.service.DBMSService.DATABASE_NAME;
import static org.example.server.service.DBMSService.dbmsRepository;

public class SelectService {
    public static String selectFromTable(Select select) throws Exception {
        PlainSelect plainSelect = (PlainSelect) select.getSelectBody();
        Distinct distinct = plainSelect.getDistinct();
        boolean isDistinct = distinct != null;

        Table fromTable = (Table) plainSelect.getFromItem();
        String fromTableName = fromTable.getName();

        List<String> selectedColumns = getSelectedColumns(plainSelect);

        List<String> selectedValues = new ArrayList<>();

        Element tableElement = DBMSService.getTableByName(fromTableName);
        List<String> columnsForTable = getAttributesForTable(tableElement);
        List<String> primaryKeys = getPrimaryKeys(tableElement);

        Expression whereExpression = plainSelect.getWhere();

        String collectionName = dbmsRepository.getCurrentDatabase() + fromTableName;

        if (plainSelect.getJoins() != null && !plainSelect.getJoins().isEmpty()) {
            // Se procesează clauzele JOIN
            List<Join> joins = plainSelect.getJoins();
            // processRecursiveJoin(rootDataBases, database, tableNames, joins, selectedColumns);
            //processRecursiveSortMergeJoin(database,tableNames,attributes,selectedColumns);
        } else if (whereExpression != null) {
            List<Expression> andExpressionsFromWhereClause = new ArrayList<>(extractAndExpressions(whereExpression));
            List<String> tableColumnsFromWhereClause = new ArrayList<>(getTableColumnsFromExpressions(andExpressionsFromWhereClause));
            Set<String> columns = new HashSet<>(selectedColumns);
            columns.addAll(tableColumnsFromWhereClause);
            validateTableNameAndColumn(columnsForTable, columns);
            List<IndexFile> indexFilesForSelectedColumns = getIndexesForColumnFromWhere(tableElement, tableColumnsFromWhereClause);
            columnsForTable.removeAll(primaryKeys);
            selectedValues.addAll(select(collectionName, andExpressionsFromWhereClause, indexFilesForSelectedColumns, columnsForTable, selectedColumns, primaryKeys));
        }

        if (isDistinct) {
            selectedValues = selectedValues.stream()
                    .distinct()
                    .toList();
        }
        return getStringFromSelectedValues(selectedValues);
    }

//    private static void processRecursiveJoin(Element rootDataBases, MongoDatabase database, List<String> tableNames, List<Join> joins, List<String> selectedColumns) {
//        if (tableNames.size() >= 2) {
//            String leftTable = tableNames.get(0);
//            String rightTable = tableNames.get(1);
//
//            MongoCollection<Document> leftCollection = database.getCollection(dbmsRepository.getCurrentDatabase() + leftTable);
//            MongoCollection<Document> rightCollection = database.getCollection(dbmsRepository.getCurrentDatabase() + rightTable);
//
//
//            List<String> leftPrimaryKeys = getPrimaryKeys(getTableElement(leftTable));
//            List<String> rightPrimaryKeys = getPrimaryKeys(getTableElement(rightTable));
//            //System.out.println(leftPrimaryKeys);
//            //System.out.println(rightPrimaryKeys);
//
//            //boolean leftHasKeyIndex = checkIfIndexExists(database, leftTable, leftPrimaryKeys);
//            //boolean rightHasKeyIndex = checkIfIndexExists(database, rightTable, rightPrimaryKeys);
//
//            if (true) {
//                for (Document leftDoc : leftCollection.find()) {
//                    for (Document rightDoc : rightCollection.find()) {
//                        if (leftDoc.get("_id").equals(rightDoc.get("values").toString().split("#")[1])) {
//                            List<String> remainingTables = new ArrayList<>(tableNames.subList(2, tableNames.size()));
//                            if (!remainingTables.isEmpty()) {
//                                List<String> combinedColumns = new ArrayList<>(selectedColumns);
//                                combinedColumns.addAll(getAttributesForTable(getTableElement(rightTable)));
//
//                                processRecursiveJoin(rootDataBases, database, remainingTables, joins.subList(1, joins.size()), combinedColumns);
//                            } else {
//                                // Dacă nu mai sunt tabele de procesat, afișăm rezultatul conform coloanelor selectate
//                                System.out.println(leftDoc);
//                                System.out.println(rightDoc);
//                                /*for (String column : selectedColumns) {
//                                    System.out.print(combinedDoc.get(column) + " ");
//                                }*/
//                                System.out.println();
//                            }
//                        }
//                    }
//                }
//            } else {
//                System.out.println("Nu există index pentru cheile primare într-una dintre tabele.");
//            }
//        } else {
//            System.out.println("Sunt necesare cel puțin două tabele pentru JOIN.");
//        }
//    }

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

    private static List<String> select(String collectionName,
                                       List<Expression> andExpressionsFromWhereClause,
                                       List<IndexFile> indexFiles,
                                       List<String> attributes,
                                       List<String> selectedColumns,
                                       List<String> primaryKeys) throws Exception {
        List<String> primaryKeyValues = new ArrayList<>();
        List<Expression> expressionsForIndex = new ArrayList<>();
        for (IndexFile indexFile : indexFiles) {
            expressionsForIndex.addAll(getExpressionsForIndex(indexFile, andExpressionsFromWhereClause));
            List<String> primaryKeysFromIndex = selectFromIndex(indexFile.getIndexFileName(), getIdForIndex(expressionsForIndex));
            if (primaryKeyValues.isEmpty()) {
                primaryKeyValues.addAll(primaryKeysFromIndex);
            } else {
                primaryKeyValues.retainAll(primaryKeysFromIndex);
            }
        }
        List<Expression> whereNoIndex = getWhereNoIndex(andExpressionsFromWhereClause, expressionsForIndex);
        List<Document> documents = selectByPrimaryKeys(collectionName, primaryKeyValues, attributes, whereNoIndex);
        return projection(documents, selectedColumns, attributes, primaryKeys);
    }

    private static List<String> projection(List<Document> documents, List<String> selectedColumns, List<String> attributes, List<String> primaryKeys) {
        List<String> values = new ArrayList<>();
        if(selectedColumns.contains("*")) {
            selectedColumns.clear();
            selectedColumns.addAll(primaryKeys);
            selectedColumns.addAll(attributes);
        }

        selectedColumns.forEach(column -> System.out.print(column + " "));
        System.out.println();

        documents.forEach(document -> {
            selectedColumns.forEach(
                    selectedColumn -> {
                        Integer keyPosition = primaryKeys.indexOf(selectedColumn);
                        Integer valuePosition = attributes.indexOf(selectedColumn);
                        if (keyPosition != -1) {
                            values.add(document.getString("_id").split("$")[keyPosition] + " ");
                        } else if (valuePosition != -1) {
                            values.add(document.getString("values").split("#")[valuePosition] + " ");
                        }
                    }
            );
        });
        return values;
    }

    private static List<Expression> getWhereNoIndex(List<Expression> andExpressionsFromWhereClause, List<Expression> expressionsForIndex) {
        return andExpressionsFromWhereClause.stream()
                .filter(expression -> !expressionsForIndex.contains(expression))
                .toList();
    }

    private static List<Document> selectByPrimaryKeys(String collectionName, List<String> primaryKeys, List<String> attributes, List<Expression> whereNoIndex) throws Exception {
        try (MongoClient client = getMongoClient()) {
            MongoDatabase mongoDatabase = client.getDatabase(DATABASE_NAME);
            MongoCollection<Document> collection = mongoDatabase.getCollection(collectionName);

            Document query = new Document("_id", new Document("$in", primaryKeys));
            MongoCursor<Document> cursor = collection.find(query).iterator();
            List<Document> documents = new ArrayList<>();
            while (cursor.hasNext()) {
                documents.add(cursor.next());
            }
            return computeInMemoryFilters(documents, attributes, whereNoIndex);
        } catch (Exception e) {
            throw new Exception(e.getMessage());
        }
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

    private static List<Expression> getExpressionsForIndex(IndexFile indexFile, List<Expression> andExpressionsFromWhereClause) {
        List<Expression> expressions = new ArrayList<>();
        for (String attribute : indexFile.getAttributes()) {
            Optional<Expression> expression = getExpressionByLeftExpression(attribute, andExpressionsFromWhereClause);
            expression.ifPresent(expressions::add);
        }
        return expressions;
    }

    private static String getIdForIndex(List<Expression> expressions) {
        StringBuilder value = new StringBuilder();
        for (Expression expression : expressions) {
            String valueForAttribute = expression.getASTNode().jjtGetLastToken().toString();
            if (value.length() == 0) {
                value.append(valueForAttribute);
            } else {
                value.append("\\$").append(valueForAttribute);
            }
        }
        return value.toString();
    }

    private static Optional<Expression> getExpressionByLeftExpression(String leftExpression, List<Expression> andExpressionsFromWhereClause) {
        for (Expression expression : andExpressionsFromWhereClause) {
            if (expression.getASTNode().jjtGetFirstToken().toString().equals(leftExpression)) {
                return Optional.of(expression);
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
