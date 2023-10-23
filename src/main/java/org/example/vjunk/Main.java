package org.example.vjunk;

import org.jdom2.*;
import org.jdom2.input.SAXBuilder;
import org.jdom2.output.Format;
import org.jdom2.output.XMLOutputter;

import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.Index;
public class Main {
    public static void main(String[] args) {
        String tableName = "Specialization";
        String indexName = "IX_SpecName";
        String databaseName = "University";
        Element rootElement = new Element("Databases");

// Crearea documentului XML
        Document doc = new Document(rootElement);

// Crearea unui fisier XML initial
        try {
            Format format = Format.getPrettyFormat();
            format.setEncoding("UTF-8"); // Setare codificare
            format.setExpandEmptyElements(true);
            format.setLineSeparator("\n"); // Setare separator de linie (opțional)

            XMLOutputter xmlOutput = new XMLOutputter(format);
            xmlOutput.output(doc, new FileWriter("catalog.xml"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        try{
        // Deschideți și analizați fișierul XML existent
        FileInputStream file = new FileInputStream("catalog.xml");
        SAXBuilder saxBuilder = new SAXBuilder();
        doc = saxBuilder.build(file);
        rootElement = doc.getRootElement();

        // Verificați dacă există deja un element <Tables>, și dacă nu, adăugați-l
            // Adăugați un element <DataBase> cu atributul dataBaseName la elementul radacină
            Element databaseElement = new Element("DataBase");
            databaseElement.setAttribute("dataBaseName", databaseName);
            rootElement.addContent(databaseElement);
        Element tablesElement = rootElement.getChild("Tables");
        if (tablesElement == null) {
            tablesElement = new Element("Tables");
            rootElement.addContent(tablesElement);
        }

            // Adăugați un element <Table> pentru tabel
            Element tableElement = new Element("Table");
            tableElement.setAttribute("tableName", tableName);
            tableElement.setAttribute("fileName", tableName + ".bin");
            tablesElement.addContent(tableElement);

            // Adăugați elementele pentru structura tabelului
            Element structureElement = new Element("Structure");
            // Adăugați atributele aici, similar cu structura tabelului

            tableElement.addContent(structureElement);

            // Adăugați elementul primaryKey pentru cheia primară
            Element primaryKeyElement = new Element("primaryKey");
            Element pkAttributeElement = new Element("pkAttribute");
            pkAttributeElement.setText("Specid"); // Aici trebuie să specificați cheia primară

            primaryKeyElement.addContent(pkAttributeElement);
            tableElement.addContent(primaryKeyElement);

            // Adăugați elementul uniqueKeys pentru indexul unic
            Element uniqueKeysElement = new Element("uniqueKeys");
            Element uniqueAttributeElement = new Element("UniqueAttribute");
            uniqueAttributeElement.setText("SpecName"); // Aici trebuie să specificați numele atributului indexului unic

            uniqueKeysElement.addContent(uniqueAttributeElement);
            tableElement.addContent(uniqueKeysElement);

            // Adăugați elementul IndexFiles pentru index
            Element indexFilesElement = new Element("IndexFiles");
            Element indexFileElement = new Element("IndexFile");
            indexFileElement.setAttribute("indexName", indexName + ".ind");
            indexFileElement.setAttribute("keyLength", "30");
            indexFileElement.setAttribute("isUnique", "1");
            indexFileElement.setAttribute("indexType", "BTree");

            Element indexAttributesElement = new Element("IndexAttributes");
            Element iAttributeElement = new Element("IAttribute");
            iAttributeElement.setText("SpecName"); // Aici trebuie să specificați numele atributului indexului

            indexAttributesElement.addContent(iAttributeElement);
            indexFileElement.addContent(indexAttributesElement);
            indexFilesElement.addContent(indexFileElement);
            tableElement.addContent(indexFilesElement);


            // Salvați structura actualizată în fișierul XML
            XMLOutputter xmlOutput = new XMLOutputter(Format.getPrettyFormat());
            xmlOutput.output(doc, new FileWriter("catalog.xml"));

            // Aici puteți executa comenzile SQL corespunzătoare pentru a crea tabela și indexul în sistemul de gestiune a bazelor de date

            // Închideți fișierul și resursele de conexiune
            file.close();
        } catch (IOException | JDOMException e) {
            e.printStackTrace();
        }
    }

    public void addTable(){
        String createTableSQL = "CREATE TABLE Specialization (" +
                "Specid CHAR(3) NOT NULL," +
                "SpecName VARCHAR(30) NOT NULL," +
                "Language VARCHAR(20) NOT NULL," +
                "PRIMARY KEY (Specid)" +
                ");";

        String createIndexSQL = "-- Create an index on SpecName column\n" +
                "CREATE UNIQUE CLUSTERED INDEX IX_SpecName ON Specialization (SpecName);";

        try {
            Statement createTableStatement = CCJSqlParserUtil.parse(createTableSQL);

            if (createTableStatement instanceof CreateTable) {
                CreateTable createTable = (CreateTable) createTableStatement;
                String tableName = createTable.getTable().getName();

                // Preluarea atributelor
                List<ColumnDefinition> columnDefinitions = createTable.getColumnDefinitions();
                for (ColumnDefinition column : columnDefinitions) {
                    String attributeName = column.getColumnName();
                    String attributeType = column.getColDataType().getDataType();
                    // Aici puteți utiliza attributeName și attributeType pentru a actualiza fișierul XML
                }

                // Preluarea cheii primare
                Index primaryKey = createTable.getIndexes().stream()
                        .filter(index -> index.getType().equals("PRIMARY KEY"))
                        .findFirst()
                        .orElse(null);


            }
        } catch (JSQLParserException e) {
            e.printStackTrace();
        }
    }
}