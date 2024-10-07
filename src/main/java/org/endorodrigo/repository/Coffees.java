package org.endorodrigo.repository;

import java.sql.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Coffees {
    static DatabaseConnection db = DatabaseConnection.getInstance();
    static Connection conn = db.getConnection();
    private String dbms;

    public void createTable() throws SQLException {
        String createString =
                "create table COFFEES " + "(COF_NAME varchar(32) NOT NULL, " +
                        "SUP_ID int NOT NULL, " + "PRICE numeric(10,2) NOT NULL, " +
                        "SALES integer NOT NULL, " + "TOTAL integer NOT NULL, " +
                        "PRIMARY KEY (COF_NAME), " +
                        "FOREIGN KEY (SUP_ID) REFERENCES SUPPLIERS (SUP_ID))";

        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(createString);
        } catch (SQLException e) {
            System.out.print(e.getMessage());
        }
    }

    public void populateTable() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("insert into COFFEES " +
                    "values('Colombian', 00101, 7.99, 0, 0)");
            stmt.executeUpdate("insert into COFFEES " +
                    "values('French_Roast', 00049, 8.99, 0, 0)");
            stmt.executeUpdate("insert into COFFEES " +
                    "values('Espresso', 00150, 9.99, 0, 0)");
            stmt.executeUpdate("insert into COFFEES " +
                    "values('Colombian_Decaf', 00101, 8.99, 0, 0)");
            stmt.executeUpdate("insert into COFFEES " +
                    "values('French_Roast_Decaf', 00049, 9.99, 0, 0)");
        } catch (SQLException e) {
            System.out.print(e.getMessage());
        }
    }


    public static void updateCoffeeSales(HashMap<String, Integer> salesForWeek) throws SQLException {
        String updateString =
                "update COFFEES set SALES = ? where COF_NAME = ?";
        String updateStatement =
                "update COFFEES set TOTAL = TOTAL + ? where COF_NAME = ?";

        try (PreparedStatement updateSales = conn.prepareStatement(updateString);
             PreparedStatement updateTotal = conn.prepareStatement(updateStatement))
        {
            conn.setAutoCommit(false);
            for (Map.Entry<String, Integer> e : salesForWeek.entrySet()) {
                updateSales.setInt(1, e.getValue());
                updateSales.setString(2, e.getKey());
                updateSales.executeUpdate();

                updateTotal.setInt(1, e.getValue());
                updateTotal.setString(2, e.getKey());
                updateTotal.executeUpdate();
                conn.commit();
            }
        } catch (SQLException e) {
            System.out.print(e.getMessage());
            if (conn != null) {
                try {
                    System.err.print("Transaction is being rolled back");
                    conn.rollback();
                } catch (SQLException excep) {
                    System.out.println(excep.getMessage());
                }
            }
        }
    }

    public static void modifyPrices(float percentage) throws SQLException {
        try (Statement stmt =
                     conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE)) {
            ResultSet uprs = stmt.executeQuery("SELECT * FROM COFFEES");
            while (uprs.next()) {
                float f = uprs.getFloat("PRICE");
                uprs.updateFloat("PRICE", f * percentage);
                uprs.updateRow();
            }
        } catch (SQLException e) {
            System.out.print(e.getMessage());
        }
    }

    public static void modifyPricesByPercentage(
            String coffeeName,
            float priceModifier,
            float maximumPrice) throws SQLException {
        conn.setAutoCommit(false);
        ResultSet rs = null;
        String priceQuery = "SELECT COF_NAME, PRICE FROM COFFEES " +
                "WHERE COF_NAME = ?";
        String updateQuery = "UPDATE COFFEES SET PRICE = ? " +
                "WHERE COF_NAME = ?";
        try (PreparedStatement getPrice = conn.prepareStatement(priceQuery, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
             PreparedStatement updatePrice = conn.prepareStatement(updateQuery))
        {
            Savepoint save1 = conn.setSavepoint();
            getPrice.setString(1, coffeeName);
            if (!getPrice.execute()) {
                System.out.println("Could not find entry for coffee named " + coffeeName);
            } else {
                rs = getPrice.getResultSet();
                rs.first();
                float oldPrice = rs.getFloat("PRICE");
                float newPrice = oldPrice + (oldPrice * priceModifier);
                System.out.printf("Old price of %s is $%.2f%n", coffeeName, oldPrice);
                System.out.printf("New price of %s is $%.2f%n", coffeeName, newPrice);
                System.out.println("Performing update...");
                updatePrice.setFloat(1, newPrice);
                updatePrice.setString(2, coffeeName);
                updatePrice.executeUpdate();
                System.out.println("\nCOFFEES table after update:");
                viewTable();
                if (newPrice > maximumPrice) {
                    System.out.printf("The new price, $%.2f, is greater " +
                                    "than the maximum price, $%.2f. " +
                                    "Rolling back the transaction...%n",
                            newPrice, maximumPrice);
                    conn.rollback(save1);
                    System.out.println("\nCOFFEES table after rollback:");
                    viewTable();
                }
                conn.commit();
            }
        } catch (SQLException e) {
            System.out.print(e.getMessage());
        } finally {
            conn.setAutoCommit(true);
        }
    }

    public static void insertRow(String coffeeName, int supplierID, float price,
                                 int sales, int total) throws SQLException {

        try (Statement stmt =
                     conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE))
        {
            ResultSet uprs = stmt.executeQuery("SELECT * FROM COFFEES");
            uprs.moveToInsertRow();
            uprs.updateString("COF_NAME", coffeeName);
            uprs.updateInt("SUP_ID", supplierID);
            uprs.updateFloat("PRICE", price);
            uprs.updateInt("SALES", sales);
            uprs.updateInt("TOTAL", total);

            uprs.insertRow();
            uprs.beforeFirst();

        } catch (SQLException e) {
            System.out.print(e.getMessage());
        }
    }

    public static void batchUpdate() throws SQLException {
        conn.setAutoCommit(false);
        try (Statement stmt = conn.createStatement()) {

            stmt.addBatch("INSERT INTO COFFEES " +
                    "VALUES('Amaretto', 49, 9.99, 0, 0)");
            stmt.addBatch("INSERT INTO COFFEES " +
                    "VALUES('Hazelnut', 49, 9.99, 0, 0)");
            stmt.addBatch("INSERT INTO COFFEES " +
                    "VALUES('Amaretto_decaf', 49, 10.99, 0, 0)");
            stmt.addBatch("INSERT INTO COFFEES " +
                    "VALUES('Hazelnut_decaf', 49, 10.99, 0, 0)");

            int[] updateCounts = stmt.executeBatch();
            conn.commit();
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        } finally {
            conn.setAutoCommit(true);
        }
    }

    public static void viewTable() throws SQLException {
        String query = "select COF_NAME, SUP_ID, PRICE, SALES, TOTAL from COFFEES";
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next()) {
                String coffeeName = rs.getString("COF_NAME");
                int supplierID = rs.getInt("SUP_ID");
                float price = rs.getFloat("PRICE");
                int sales = rs.getInt("SALES");
                int total = rs.getInt("TOTAL");
                System.out.println(coffeeName + ", " + supplierID + ", " + price +
                        ", " + sales + ", " + total);
            }
        } catch (SQLException e) {
            System.out.print(e.getMessage());
        }
    }

    public static void alternateViewTable(Connection conn) throws SQLException {
        String query = "select COF_NAME, SUP_ID, PRICE, SALES, TOTAL from COFFEES";
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next()) {
                String coffeeName = rs.getString(1);
                int supplierID = rs.getInt(2);
                float price = rs.getFloat(3);
                int sales = rs.getInt(4);
                int total = rs.getInt(5);
                System.out.println(coffeeName + ", " + supplierID + ", " + price +
                        ", " + sales + ", " + total);
            }
        } catch (SQLException e) {
            System.out.print(e.getMessage());
        }
    }

    public Set<String> getKeys() throws SQLException {
        HashSet<String> keys = new HashSet<String>();
        String query = "select COF_NAME from COFFEES";
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next()) {
                keys.add(rs.getString(1));
            }
        } catch (SQLException e) {
            System.out.print(e.getMessage());
        }
        return keys;
    }

    public void dropTable() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            if (this.dbms.equals("mysql")) {
                stmt.executeUpdate("DROP TABLE IF EXISTS COFFEES");
            } else if (this.dbms.equals("derby")) {
                stmt.executeUpdate("DROP TABLE COFFEES");
            }
        } catch (SQLException e) {
            System.out.print(e.getMessage());
        }
    }

    public static void main(String[] args) throws SQLException {
        System.out.println("\nContents of COFFEES table:");
        viewTable();

        System.out.println("\nRaising coffee prices by 25%");
        modifyPrices(1.25f);

        System.out.println("\nInserting a new row:");
        insertRow("Kona", 150, 10.99f, 0, 0);
        viewTable();

        System.out.println("\nUpdating sales of coffee per week:");
        HashMap<String, Integer> salesCoffeeWeek =
                new HashMap<String, Integer>();
        salesCoffeeWeek.put("Colombian", 175);
        salesCoffeeWeek.put("French_Roast", 150);
        salesCoffeeWeek.put("Espresso", 60);
        salesCoffeeWeek.put("Colombian_Decaf", 155);
        salesCoffeeWeek.put("French_Roast_Decaf", 90);
        updateCoffeeSales(salesCoffeeWeek);

        System.out.println("\nModifying prices by percentage");

        modifyPricesByPercentage("Colombian", 0.10f, 9.00f);

        System.out.println("\nCOFFEES table after modifying prices by percentage:");

        viewTable();

        System.out.println("\nPerforming batch updates; adding new coffees");
        batchUpdate();
        viewTable();

//      System.out.println("\nDropping Coffee and Suplliers table:");
//      
//      .dropTable();
//      mySuppliersTable.dropTable();

    }
}
