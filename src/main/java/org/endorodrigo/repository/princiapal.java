package org.endorodrigo.repository;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

public class princiapal {

    static DatabaseConnection dbConnection = DatabaseConnection.getInstance();
    static Connection conn = dbConnection.getConnection();

    public static void createTable(){
        String query = "create table SUPPLIERS " + "(SUP_ID integer NOT NULL, " +
                "SUP_NAME varchar(40) NOT NULL, " + "STREET varchar(40) NOT NULL, " +
                "CITY varchar(20) NOT NULL, " + "STATE char(2) NOT NULL, " +
                "ZIP char(5), " + "PRIMARY KEY (SUP_ID))";
        try(Statement sttm = conn.createStatement()) {
            sttm.execute(query);
            System.out.println("Creacion de la tabla completado");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void populateTable(){
        try(Statement sttm = conn.createStatement()) {
            sttm.executeUpdate("insert into SUPPLIERS " +
                    "values(49, 'Superior Coffee', '1 Party Place', " +
                    "'Mendocino', 'CA', '95460')");
            sttm.executeUpdate("insert into SUPPLIERS " +
                    "values(101, 'Acme, Inc.', '99 Market Street', " +
                    "'Groundsville', 'CA', '95199')");
            sttm.executeUpdate("insert into SUPPLIERS " +
                    "values(150, 'The High Ground', '100 Coffee Lane', " +
                    "'Meadows', 'CA', '93966')");
            System.out.println("Informacion registrada");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void viewSuppliers(){
        String query = "select sup_name, sup_id from suppliers";
        try(Statement sttm = conn.createStatement()) {
            ResultSet rs = sttm.executeQuery(query);
            System.out.println("Suppliers and their ID Numbers:");
            while (rs.next()){
                String name = rs.getString("sup_name");
                int id = rs.getInt("sup_id");
                System.out.println("| Ciudad: "+name+"| ID: "+id);
            }
        } catch (SQLException e) {
            System.out.println(e.getStackTrace().toString());
        }
    }

    public static void viewTable() throws SQLException{
        String query = "select * from suppliers";
        try(Statement sttm = conn.createStatement()) {
            ResultSet rs = sttm.executeQuery(query);
            while (rs.next()) {
                int supplierID = rs.getInt("SUP_ID");
                String supplierName = rs.getString("SUP_NAME");
                String street = rs.getString("STREET");
                String city = rs.getString("CITY");
                String state = rs.getString("STATE");
                String zip = rs.getString("ZIP");
                System.out.println(supplierName + "(" + supplierID + "): " + street +
                        ", " + city + ", " + state + ", " + zip);
            }
        }catch (SQLException e){
            System.out.println(e.getStackTrace().toString());
        }
    }


        public static void main(String[] args) throws SQLException {
        //createTable();
        //populateTable();
        //viewSuppliers();
        viewTable();
    }
}