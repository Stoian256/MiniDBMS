package org.example.server.service;


import com.mongodb.client.*;
import com.mongodb.client.model.Sorts;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import org.bson.conversions.Bson;
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

        List<String> tableNames = extractTableNames(plainSelect);
        System.out.println(tableNames);
        String tableName = plainSelect.getFromItem().toString(); // Obținem numele tabelei
        Element tableElement = getTableElement(tableName);
        List<String> primaryKeys = getPrimaryKeys(tableElement);
        List<String> attributes = getAttributesForTable(tableElement);
        List<String> selectedColumns = plainSelect.getSelectItems().stream().map(Objects::toString).collect(Collectors.toList());
        attributes.removeAll(primaryKeys);
        Expression where = plainSelect.getWhere(); // Obținem clauza WHERE

        // Verificăm dacă interogarea are clauze JOIN
        if (plainSelect.getJoins() != null && !plainSelect.getJoins().isEmpty()) {
            // Se procesează clauzele JOIN
            List<Join> joins = plainSelect.getJoins();
            //processIndexedNestedJoin(rootDataBases, database, tableNames, joins, selectedColumns);
            //processRecursiveSortMergeJoin(database, tableNames, attributes, selectedColumns);
            processHashJoin(database, tableNames, attributes, selectedColumns);
        } else {
            if (where != null) {
                List<Expression> conditions = extractConditions(where);
                processConditions(rootDataBases, database, tableName, conditions, attributes, primaryKeys, selectedColumns);
            }
        }
    }

    public static List<String> extractTableNames(PlainSelect plainSelect) {
        List<String> tableNames = new ArrayList<>();


        // Adăugați numele tabelei de bază (primare)
        tableNames.add(plainSelect.getFromItem().toString());

        // Verificați dacă există uniri (joins)
        if (plainSelect.getJoins() != null) {
            // Iterați prin lista de uniri și adăugați numele tabelelor alăturate
            for (Join join : plainSelect.getJoins()) {
                tableNames.add(join.getRightItem().toString());
            }
        }

        return tableNames;
    }

    private void processIndexedNestedJoin(Element rootDataBases, MongoDatabase database, List<String> tableNames, List<Join> joins, List<String> selectedColumns) {
        if (tableNames.size() >= 2) {
            String leftTable = tableNames.get(0);
            tableNames.remove(0);
            //System.out.println(leftTable);
            //System.out.println(rightTable);

            MongoCollection<Document> leftCollection = database.getCollection(dbmsRepository.getCurrentDatabase() + leftTable);
            for (String rightTableName : tableNames) {
                List<String> appendResult = new ArrayList<>();


                MongoCollection<Document> rightCollection = database.getCollection(dbmsRepository.getCurrentDatabase() + rightTableName);
                //System.out.println(leftCollection.find());
                //System.out.println(rightCollection.find());

                List<String> leftPrimaryKeys = getPrimaryKeys(getTableElement(leftTable));
                List<String> rightPrimaryKeys = getPrimaryKeys(getTableElement(rightTableName));
                //System.out.println(leftPrimaryKeys);
                //System.out.println(rightPrimaryKeys);

                //boolean leftHasKeyIndex = checkIfIndexExists(database, leftTable, leftPrimaryKeys);
                //boolean rightHasKeyIndex = checkIfIndexExists(database, rightTable, rightPrimaryKeys);

                if (true) {
                    for (Document leftDoc : leftCollection.find()) {
                        for (Document rightDoc : rightCollection.find()) {
                            if (leftDoc.get("_id").equals(rightDoc.get("values").toString().split("#")[0])) {
                                //List<String> remainingTables = new ArrayList<>(tableNames.subList(2, tableNames.size()));

                                    //List<String> combinedColumns = new ArrayList<>(selectedColumns);
                                    //combinedColumns.addAll(getAttributesForTable(getTableElement(rightTableName)));
                                    appendResult.add(leftDoc.toString()+rightDoc);

                            }
                        }
                    }
                } else {
                    System.out.println("Nu există index pentru cheile primare într-una dintre tabele.");
                }
                appendResult.forEach(System.out::println);
                System.out.println();
                leftCollection = rightCollection;
            }
        } else {
            System.out.println("Sunt necesare cel puțin două tabele pentru JOIN.");
        }
    }

    private void processRecursiveSortMergeJoin(MongoDatabase database, List<String> tableNames, List<String> joinAttributes, List<String> selectedColumns) {
        if (tableNames.size() >= 2) {
            String leftTableName = tableNames.get(0);
            tableNames.remove(0);

            String leftJoinAttribute = "_id";
            String rightJoinAttribute = "values";
            System.out.println(rightJoinAttribute);

            MongoCollection<Document> leftCollection = database.getCollection(dbmsRepository.getCurrentDatabase() + leftTableName);

            List<String> result = new ArrayList<>();
            int resultIndex = 0;
            boolean isFirst = true;
            for (String rightTableName : tableNames) {
                List<String> appendResult = new ArrayList<>();
                MongoCollection<Document> rightCollection = database.getCollection(dbmsRepository.getCurrentDatabase() + rightTableName);

                // Sortăm tabelele după atributul de join
                Bson sortLeft = Sorts.ascending(leftJoinAttribute);
                Bson sortRight = Sorts.ascending(rightJoinAttribute);
                FindIterable<Document> leftSorted = leftCollection.find().sort(sortLeft);
                FindIterable<Document> rightSorted = rightCollection.find().sort(sortRight);

                // Inițializăm iteratorii pentru tabelele sortate
                MongoCursor<Document> leftCursor = leftSorted.iterator();
                MongoCursor<Document> rightCursor = rightSorted.iterator();

                List<Document> leftDocs = new ArrayList<>();
                List<Document> rightDocs = new ArrayList<>();

                // Adăugăm documentele sortate în liste
                while (leftCursor.hasNext()) {
                    leftDocs.add(leftCursor.next());
                }
                while (rightCursor.hasNext()) {
                    rightDocs.add(rightCursor.next());
                }

                // Realizăm Sort Merge Join între tabelele sortate
                List<Document> tuples = new ArrayList<>();
                int leftIndex = 0, rightIndex = 0;

                while (leftIndex < leftDocs.size() && rightIndex < rightDocs.size()) {
                    Document leftDoc = leftDocs.get(leftIndex);
                    Document rightDoc = rightDocs.get(rightIndex);


                    // Obținem valorile atributelor de join din documentele curente
                    Object leftJoinValue = leftDoc.get(leftJoinAttribute);
                    Object rightJoinValue = rightDoc.get(rightJoinAttribute).toString().split("#")[0];


                    // Comparăm valorile pentru a verifica condiția de join
                    int comparisonResult = ((Comparable) leftJoinValue).compareTo(rightJoinValue);

                    if (comparisonResult == 0) {
                        // Dacă valorile sunt egale, facem join și adăugăm rezultatul în lista de tuple
                    /*Document tuple = new Document();
                    for (String column : selectedColumns) {
                        if (leftDoc.containsKey(column)) {
                            tuple.append(column, leftDoc.get(column));
                        } else if (rightDoc.containsKey(column)) {
                            tuple.append(column, rightDoc.get(column));
                        }
                    }
                    tuples.add(tuple);*/
                        if (!isFirst) {
                            appendResult.add(leftDoc.toString() + rightDoc);
                            //result.set(resultIndex, result.get(resultIndex) + rightDoc);
                            resultIndex++;
                        } else {
                            appendResult.add(leftDoc.toString() + rightDoc);
                        }


                        rightIndex++;
                    } else if (comparisonResult < 0) {
                        leftIndex++;
                    } else {
                        rightIndex++;
                    }
                }
                appendResult.forEach(System.out::println);
                System.out.println();
                leftCollection = rightCollection;
                resultIndex = 0;
                isFirst = false;
            }


            // result.forEach(System.out::println);
        } else {
            System.out.println("Sunt necesare cel puțin două tabele pentru JOIN.");
        }
    }

    private void processHashJoin(MongoDatabase database, List<String> tableNames, List<String> joinAttribute, List<String> selectedColumns) {
        String leftTableName = tableNames.get(0);
        tableNames.remove(0);

        String leftJoinAttribute = "_id";
        String rightJoinAttribute = "values";

        MongoCollection<Document> leftCollection = database.getCollection(dbmsRepository.getCurrentDatabase() + leftTableName);

        List<String> result = new ArrayList<>();
        int resultIndex = 0;
        boolean isFirst = true;

        for (String rightTableName : tableNames) {
            List<String> appendResult = new ArrayList<>();
            MongoCollection<Document> rightCollection = database.getCollection(dbmsRepository.getCurrentDatabase() + rightTableName);

            // Pasul 1: Crearea unei structuri de tabelă hash în memorie pentru colecția cu mai puține elemente
            Map<Object, Document> hashTable = new HashMap<>();
            FindIterable<Document> smallerRecords = leftCollection.find();
            for (Document record : smallerRecords) {
                Object key = record.get(leftJoinAttribute);
                hashTable.put(key, record);
            }

            // Pasul 2: Iterarea colecției mai mari și găsirea înregistrărilor corespunzătoare în tabela hash
            FindIterable<Document> largerRecords = rightCollection.find();
            List<Document> tuples = new ArrayList<>();
            for (Document record : largerRecords) {
                Object joinKeyValue = record.get(rightJoinAttribute).toString().split("#")[0];
                Document matchingRecord = hashTable.get(joinKeyValue);

                if (matchingRecord != null) {
                    // Dacă găsim o corespondență, construim un tuple pentru rezultatul JOIN-ului
                    Document tuple = new Document();
                /*for (String column : selectedColumns) {
                    if (matchingRecord.containsKey(column)) {
                        tuple.append(column, matchingRecord.get(column));
                    }
                }
                tuples.add(tuple);*/
                    if (!isFirst) {
                        //System.out.println(resultIndex);
                        appendResult.add(matchingRecord.toString() + record);
                        resultIndex++;
                    } else {
                        appendResult.add(matchingRecord.toString() + record);
                    }
                    //System.out.println(record);
                    //System.out.println(matchingRecord);
                    //System.out.println();
                }
            }
            appendResult.forEach(System.out::println);
            System.out.println();
            result = appendResult;
            //result.forEach(System.out::println);
            leftCollection = rightCollection;
            resultIndex = 0;
            isFirst = false;
        }
        //result.forEach(System.out::println);

        // Afișăm rezultatul Hash Join
        /*for (Document tuple : tuples) {
            System.out.println(tuple.toJson());
        }*/
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

            if (selectedColumns.contains("*")) {
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
                            if (keyPosition != -1) {
                                System.out.print(document.getString("_id").split("$")[keyPosition] + " ");
                            } else if (valuePosition != -1) {
                                System.out.print(document.getString("values").split("#")[valuePosition] + " ");
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
