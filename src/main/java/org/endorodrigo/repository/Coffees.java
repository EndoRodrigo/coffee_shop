package org.endorodrigo.repository;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

public class Coffees {
    static DatabaseConnection db = DatabaseConnection.getInstance();
    static Connection conn = db.getConnection();
    public static void createTable() throws SQLException {
        String query =
                "create table COFFEES " + "(COF_NAME varchar(32) NOT NULL, " +
                        "SUP_ID int NOT NULL, " + "PRICE numeric(10,2) NOT NULL, " +
                        "SALES integer NOT NULL, " + "TOTAL integer NOT NULL, " +
                        "PRIMARY KEY (COF_NAME), " +
                        "FOREIGN KEY (SUP_ID) REFERENCES SUPPLIERS (SUP_ID))";
        try(Statement sttm = conn.createStatement() ){
            sttm.executeUpdate(query);
        }catch (SQLException e){
            System.out.println(e.getMessage());
        }
    }

    public static void populateTable(){
        try(Statement sttm = conn.createStatement()) {
            sttm.executeUpdate("insert into COFFEES " +
                    "values('Colombian', 00101, 7.99, 0, 0)");
            sttm.executeUpdate("insert into COFFEES " +
                    "values('French_Roast', 00049, 8.99, 0, 0)");
            sttm.executeUpdate("insert into COFFEES " +
                    "values('Espresso', 00150, 9.99, 0, 0)");
            sttm.executeUpdate("insert into COFFEES " +
                    "values('Colombian_Decaf', 00101, 8.99, 0, 0)");
            sttm.executeUpdate("insert into COFFEES " +
                    "values('French_Roast_Decaf', 00049, 9.99, 0, 0)");
            System.out.println("Informacion registrada con exito");
        }catch (SQLException e){
            System.out.println(e.getMessage());
        }
    }

    public static void updateCoffeeSales(HashMap<String,Integer> salesForWeek) throws SQLException {
        String querySales = "update coffees set sales = ? where cof_name = ?";
        String queryTotal = "update coffees set total = total + ? where cof_name = ?";

        try(PreparedStatement updateSales = conn.prepareStatement(querySales);
        PreparedStatement updateTotal = conn.prepareStatement(queryTotal)) {
            conn.setAutoCommit(false);
            for (Map.Entry<String, Integer> e: salesForWeek.entrySet()){
                updateSales.setInt(1, e.getValue());
                updateSales.setString(2,e.getKey());
                updateSales.executeUpdate();

                updateTotal.setInt(1, e.getValue());
                updateTotal.setString(2,e.getKey());
                updateTotal.executeUpdate();
                conn.commit();
                System.out.println("Datos actualizados con exito");
            }
        }catch (SQLException e){
            System.out.println(e.getMessage());
            if (conn != null) {
                try{
                    System.out.println("Transaction is being rolled back");
                    conn.rollback();
                }catch (SQLException ex){
                    System.out.println(e.getMessage());
                }
            }
        }
    }

    public static void modifyPrices(float porcentage) throws SQLException {
        try (Statement sttm = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE)){
            ResultSet rs = sttm.executeQuery("select * from coffees");
            while (rs.next()){
                float f = rs.getFloat("price");
                rs.updateFloat("price", f * porcentage);
                rs.updateRow();
            }
            System.out.println("Columna actualizada..");
        }catch (SQLException e){
            System.out.println(e.getMessage());
        }
    }

    public static void modifyPricesByPercetage(String coffeeName, float priceModifier, float maximumPrice) throws SQLException {
        conn.setAutoCommit(false);
        String priceQuery = "SELECT COF_NAME, PRICE FROM COFFEES " +
                "WHERE COF_NAME = ?";
        String updateQuery = "UPDATE COFFEES SET PRICE = ? " +
                "WHERE COF_NAME = ?";
        try(PreparedStatement getPrice = conn.prepareStatement(priceQuery, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            PreparedStatement updatePrice = conn.prepareStatement(updateQuery)) {
            Savepoint save1 = conn.setSavepoint();
            getPrice.setString(1, coffeeName);
        }catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public static void main(String[] args) throws SQLException {
        //createTable();
        //populateTable();

        /*Actualizar el campos sales de las tablas coffee
        HashMap<String,Integer> salesCoffeeWeek = new HashMap<String,Integer>();
        salesCoffeeWeek.put("Colombian", 175);
        salesCoffeeWeek.put("French_Roast", 150);
        salesCoffeeWeek.put("Espresso", 60);
        salesCoffeeWeek.put("Colombian_Decaf", 155);
        salesCoffeeWeek.put("French_Roast_Decaf", 90);
        updateCoffeeSales(salesCoffeeWeek);*/

        /* Actualizacion de los precios de los caffess*/
        modifyPrices(1.25f);

    }
}
