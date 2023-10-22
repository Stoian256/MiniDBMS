package org.example.service;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.index.CreateIndex;
import net.sf.jsqlparser.statement.create.schema.CreateSchema;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.Index;
import net.sf.jsqlparser.statement.drop.Drop;
import org.example.repository.DBMSRepository;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;
import org.jdom2.output.Format;
import org.jdom2.output.XMLOutputter;

import java.io.*;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Objects;
import java.util.List;
import java.util.SimpleTimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DBMSService {
    public static DBMSRepository dbmsRepository;
    static String regexCreateDatabase = "CREATE\\s+DATABASE\\s+([\\w_]+);";
    static String regexDropDatabase = "DROP\\s+DATABASE\\s+([\\w_]+);";

    public DBMSService(DBMSRepository dbmsRepository) {
        this.dbmsRepository = dbmsRepository;
    }

    public static void executeCommand(String sqlCommand) throws JSQLParserException {
        Pattern pattern = Pattern.compile(regexCreateDatabase);
        Matcher matcher = pattern.matcher(sqlCommand);
        if (matcher.find()) {
            String databaseName = matcher.group(1);
            createDatabase(databaseName);
            return;
        }
        pattern = Pattern.compile(regexDropDatabase);
        matcher = pattern.matcher(sqlCommand);
        if (matcher.find()) {
            String databaseName = matcher.group(1);
            dropDataBase(databaseName);
            return;
        }
        Statement statement = CCJSqlParserUtil.parse(sqlCommand);
        if (statement instanceof CreateTable) {
            CreateTable createTable = (CreateTable) statement;
            createTable(createTable);
        } else if (statement instanceof net.sf.jsqlparser.statement.drop.Drop) {
            Drop dropTable = (Drop) statement;
            dropTable(dropTable);
        }
        else if (statement instanceof CreateIndex) {
            CreateIndex createIndex = (CreateIndex) statement;
            System.out.println(createIndex);
            // Extrageți informațiile relevante din comanda de creare a indexului
            String indexName = createIndex.getIndex().getName();
            String tableName = createIndex.getTable().getName();
            List<String> columnName = createIndex.getIndex().getColumnsNames();


            // Aici puteți utiliza informațiile extrase pentru a efectua operații ulterioare sau pentru a crea fișierul XML

            System.out.println("Index Name: " + indexName);
            System.out.println("Table Name: " + tableName);
            System.out.println("Column Name: " + columnName);
        }
    }


    public static void createDatabase(String databaseName) {
        try {

            Element rootElement = dbmsRepository.doc.getRootElement();
            for (Element element : rootElement.getChildren("DataBase")) {
                System.out.println(element.getAttributeValue("dataBaseName"));
                if (Objects.equals(element.getAttributeValue("dataBaseName"), databaseName)) {
                    return;
                }
            }

            Element databaseElement = new Element("DataBase");
            databaseElement.setAttribute("dataBaseName", databaseName);
            rootElement.addContent(databaseElement);

            Format format = Format.getPrettyFormat();
            format.setEncoding("UTF-8");
            format.setLineSeparator("\n");
            format.setExpandEmptyElements(true);

            // Crearea unui fișier XML
            XMLOutputter xmlOutput = new XMLOutputter(format);
            xmlOutput.output(dbmsRepository.doc, new FileWriter(dbmsRepository.catalogPath));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    public static void dropDataBase(String databaseName) {
        try {

            Element rootElement = dbmsRepository.doc.getRootElement();
            for (Element element : rootElement.getChildren("DataBase")) {
                if (Objects.equals(element.getAttributeValue("dataBaseName"), databaseName)) {
                    rootElement.removeContent(element);
                }
            }

            Format format = Format.getPrettyFormat();
            format.setEncoding("UTF-8");
            format.setLineSeparator("\n");
            format.setExpandEmptyElements(true);

            // Crearea unui fișier XML
            XMLOutputter xmlOutput = new XMLOutputter(format);
            xmlOutput.output(dbmsRepository.doc, new FileWriter(dbmsRepository.catalogPath));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void dropTable(Drop dropTable) {
        try {
            Element rootDataBases = dbmsRepository.doc.getRootElement();
            Element dataBase = rootDataBases.getChild("DataBase");

            for (Element element : dataBase.getChildren("Table")) {
                if (Objects.equals(element.getAttributeValue("tableName"), dropTable.getName().toString())) {
                    dataBase.removeContent(element);
                }
            }

            // Setarea versiunii și codificării XML
            Format format = Format.getPrettyFormat();
            format.setEncoding("UTF-8");
            format.setLineSeparator("\n");
            format.setExpandEmptyElements(true);

            // Crearea unui fișier XML
            XMLOutputter xmlOutput = new XMLOutputter(format);
            xmlOutput.output(dbmsRepository.doc, new FileWriter(dbmsRepository.catalogPath));

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void createTable(CreateTable createTable) {
        try {

            String tableName = createTable.getTable().getName();
            String fileName = dbmsRepository.catalogPath + ".bin"; // Fișierul poate fi hardcodat sau specificat de utilizator
            int rowLength = 0; // Calculați rowLength în funcție de coloanele tabelului

            // Crearea elementului radacina "<Table>"
            Element rootElement = new Element("Table");
            rootElement.setAttribute("tableName", tableName);
            rootElement.setAttribute("fileName", fileName);
            rootElement.setAttribute("rowLength", String.valueOf(rowLength));

            // Crearea elementului "<Structure>"
            Element structureElement = new Element("Structure");
            for (ColumnDefinition column : createTable.getColumnDefinitions()) {
                String attributeName = column.getColumnName();
                String type = column.getColDataType().getDataType();
                int isNull = column.toString().contains("NOT NULL") ? 0 : 1;

                Element attributeElement = new Element("Attribute");
                attributeElement.setAttribute("attributeName", attributeName);
                attributeElement.setAttribute("type", type);
                if (type.equalsIgnoreCase("char") || type.equalsIgnoreCase("varchar")) {
                    int length = column.getColDataType().getArgumentsStringList().size() > 0 ? Integer.parseInt(column.getColDataType().getArgumentsStringList().get(0)) : 30;
                    attributeElement.setAttribute("length", String.valueOf(length));
                }
                attributeElement.setAttribute("isnull", String.valueOf(isNull));
                structureElement.addContent(attributeElement);
            }

            // Crearea elementului "<primaryKey>"
            Element primaryKeyElement = new Element("primaryKey");
            Element uniqueKeysElement = new Element("uniqueKeys");

            Element indexFilesElement = new Element("IndexFiles");
            // Creați elementul XML pentru constrângerea de cheie străină
            Element foreignKeysElement = rootElement.getChild("foreignKeys");
            if (foreignKeysElement == null) {
                foreignKeysElement = new Element("foreignKeys");
            }


            for (Index index : createTable.getIndexes()) {
                if (index.getType().equals("PRIMARY KEY")) {
                    Element indexFileElement = new Element("IndexFile");
                    indexFileElement.setAttribute("indexName", index.getColumnsNames().get(0) + ".ind");
                    indexFileElement.setAttribute("keyLength", "12");
                    indexFileElement.setAttribute("isUnique", "1");
                    indexFileElement.setAttribute("indexType", "BTree");


                    Element indexAttributesElement = new Element("IndexAttributes");
                    for (String columnName : index.getColumnsNames()) {
                        Element pkAttributeElement = new Element("pkAttribute");
                        primaryKeyElement.addContent(pkAttributeElement);
                        pkAttributeElement.addContent(columnName);
                        Element iAttributeElement = new Element("IAttribute");
                        iAttributeElement.setText(columnName); // Aici trebuie să specificați numele atributului indexului

                        indexAttributesElement.addContent(iAttributeElement);
                    }
                    indexFileElement.addContent(indexAttributesElement);
                    indexFilesElement.addContent(indexFileElement);

                }
                if (index.getType().equals("UNIQUE")) {
                    //pentru index
                    // Adăugați elementul IndexFiles pentru index
                    Element indexFileElement = new Element("IndexFile");
                    indexFileElement.setAttribute("indexName", index.getColumnsNames().get(0) + ".ind");
                    indexFileElement.setAttribute("keyLength", "30");
                    indexFileElement.setAttribute("isUnique", "1");
                    indexFileElement.setAttribute("indexType", "BTree");


                    Element indexAttributesElement = new Element("IndexAttributes");
                    for (String columnName : index.getColumnsNames()) {
                        Element uniqueAttributeElement = new Element("UniqueAttribute");
                        uniqueAttributeElement.addContent(columnName);
                        uniqueKeysElement.addContent(uniqueAttributeElement);

                        Element iAttributeElement = new Element("IAttribute");
                        iAttributeElement.setText(columnName); // Aici trebuie să specificați numele atributului indexului

                        indexAttributesElement.addContent(iAttributeElement);

                    }
                    indexFileElement.addContent(indexAttributesElement);
                    indexFilesElement.addContent(indexFileElement);


                }


                if (index.getType().equals("FOREIGN KEY")) {
                    String foreignKeyPattern = "FOREIGN KEY \\(([^)]+)\\) REFERENCES ([^(]+)\\(([^)]+)\\)";
                    Pattern pattern = Pattern.compile(foreignKeyPattern);
                    Matcher matcher = pattern.matcher(index.toString());

                    Element foreignKeyElement = new Element("foreignKey");
                    foreignKeysElement.addContent(foreignKeyElement);

                    if (matcher.find()) {
                        // Extrageți informațiile relevante din constrângere
                        //String foreignKeyColumns = matcher.group(1);
                        String referencedTable = matcher.group(2);
                        String referencedColumns = matcher.group(3);


                        for (String columnName : index.getColumnsNames()) {
                            Element fkAttributeElement = new Element("fkAttribute");
                            fkAttributeElement.setText(columnName);
                            foreignKeyElement.addContent(fkAttributeElement);
                        }

                        Element referencesElement = new Element("references");
                        foreignKeyElement.addContent(referencesElement);

                        Element refTableElement = new Element("refTable");
                        refTableElement.setText(referencedTable);
                        referencesElement.addContent(refTableElement);

                        for (String refColumn : referencedColumns.split(",")) {
                            Element fkAttributeElement = new Element("fkAttribute");
                            fkAttributeElement.setText(refColumn.trim());
                            foreignKeyElement.addContent(fkAttributeElement);
                        }
                    }
                }
            }


            // Adăugați elementele create la elementul radacină "<Table>"
            rootElement.addContent(structureElement);
            rootElement.addContent(primaryKeyElement);
            rootElement.addContent(foreignKeysElement);
            rootElement.addContent(uniqueKeysElement);
            rootElement.addContent(indexFilesElement);

            Element rootDataBases = dbmsRepository.doc.getRootElement();
            Element dataBase = rootDataBases.getChild("DataBase");
            dataBase.addContent(rootElement);
            // Setarea versiunii și codificării XML
            Format format = Format.getPrettyFormat();
            format.setEncoding("UTF-8");
            format.setLineSeparator("\n");
            format.setExpandEmptyElements(true);

            // Crearea unui fișier XML
            XMLOutputter xmlOutput = new XMLOutputter(format);
            xmlOutput.output(dbmsRepository.doc, new FileWriter(dbmsRepository.catalogPath));

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
