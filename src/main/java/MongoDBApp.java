import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.*;
import org.bson.Document;
import org.bson.conversions.Bson;

import javax.print.Doc;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Scanner;
import java.util.Set;

public class MongoDBApp {

    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        Scanner choice = new Scanner(System.in);
        Scanner value = new Scanner(System.in);
        while (true){
            System.out.println("Hello! Please select one of the following collections: ");
            System.out.println("1. Nari Dynamic");
            System.out.println("2. Nari Static");
            System.out.println("3. Anfr");
            System.out.println("4. Exit");

            String input = sc.nextLine();

            switch (input){
                case "1":
                    System.out.println("Please select one of the following queries: ");
                    System.out.println("1. Retrieve the ships' coordinates and true heading regarding their rate of turn");
                    System.out.println("2. Retrieve the ships' coordinates and true heading with speed over ground bigger than value");
                    System.out.println("3. Retrieve the ships' coordinates and true heading regarding their rate of turn and navigational status");
                    String input1 = choice.nextLine();
                    switch (input1){
                        case "1":
                            System.out.println("Please enter the rate of turn: ");
                            String rateInput = value.nextLine();
                            double doubleRateInput = Double.parseDouble(rateInput);
                            rateOfTurnQuery(doubleRateInput);
                            break;
                        case "2":
                            System.out.println("Please enter the speed over ground: ");
                            String speedInput = value.nextLine();
                            double doubleSpeedInput = Double.parseDouble(speedInput);
                            speedOverGroundQuery(doubleSpeedInput);
                            break;
                        case "3":
                            System.out.println("Please enter the rate of turn: ");
                            String rateCInput = value.nextLine();
                            System.out.println("Please enter the navigational status: ");
                            String navInput = value.nextLine();
                            double doublerRateCInput = Double.parseDouble(rateCInput);
                            int intNavInput = Integer.parseInt(navInput);
                            compoundQuery(intNavInput, doublerRateCInput);
                            break;
                        default:
                            System.out.println("Invalid input");
                    }
                    break;
                case "2":
                    System.out.println("Please select one of the following queries: ");
                    System.out.println("1. Give the ship's name and retrieve its eta");
                    System.out.println("2. Find ships according to destination and port");
                    System.out.println("3. Find ships within a draught range");
                    String input2 = choice.nextLine();
                    switch (input2){
                        case "1":
                            System.out.println("Please enter the ship's name (i.e. AEROUANT BREIZH): ");
                            String shipInput = value.nextLine();
                            etaQuery(shipInput);
                            break;
                        case "2":
                            System.out.println("Please enter the destination (i.e. BREST): ");
                            String destInput = value.nextLine();
                            System.out.println("Please enter the port (i.e. 1): ");
                            String portInput = value.nextLine();
                            int port = Integer.parseInt(portInput);
                            destinationQuery(destInput, port);
                            break;
                        case "3":
                            System.out.println("Please enter the min draught (i.e. 10): ");
                            String minDraughtInput = value.nextLine();
                            double minDraught = Double.parseDouble(minDraughtInput);
                            System.out.println("Please enter the max draught (i.e. 12): ");
                            String maxDraughtInput = value.nextLine();
                            double maxDraught = Double.parseDouble(maxDraughtInput);
                            draughtQuery(minDraught, maxDraught);
                            break;
                        default:
                            System.out.println("Invalid input");
                    }
                    break;
                case "3":
                    System.out.println("Please select one of the following queries: ");
                    System.out.println("1. Find ships according to length");
                    System.out.println("2. Find the longest ship of certain type");
                    String input3 = choice.nextLine();
                    switch (input3){
                        case "1":
                            System.out.println("Please enter the length (i.e. 20): ");
                            String lengthInput = value.nextLine();
                            double length = Double.parseDouble(lengthInput);
                            lengthQuery(length);
                            break;
                        case "2":
                            System.out.println("Please enter the ship type (i.e. FISHING): ");
                            String typeInput = value.nextLine();
                            longestQuery(typeInput);
                            break;
                        default:
                            System.out.println("Invalid input");
                    }
                    break;
                case "4":
                    System.out.println("Looking forward to meet again!");
                    System.exit(0);
                    break;
                default:
                    System.out.println("Please try again. Type 1, 2, 3 or 4");
            }
        }

    }

    private static void longestQuery(String typeInput) {
        MongoClient mongoClient = new MongoClient();
        MongoDatabase database = mongoClient.getDatabase("mongoProject");
        MongoCollection<Document> collection = database.getCollection("anfr");

        Block<Document> printBlock = document -> System.out.println(document.toJson());

        Bson having = Aggregates.match(new Document("shiptype", typeInput));
        Bson sort = Aggregates.sort(Sorts.descending("length"));
        Bson project = Aggregates.project(Projections.fields(Projections.excludeId(), Projections.include("ship_name"), Projections.include("length")));
        Bson limit = Aggregates.limit(1);

        collection.aggregate(Arrays.asList(
                having, sort, project, limit
        )).forEach(printBlock);
    }

    private static void lengthQuery(double length) {
        MongoClient mongoClient = new MongoClient();
        MongoDatabase database = mongoClient.getDatabase("mongoProject");
        MongoCollection<Document> collection = database.getCollection("anfr");

        Block<Document> printBlock = document -> System.out.println(document.toJson());

        Bson project = Aggregates.project(Projections.fields(Projections.excludeId(), Projections.include("ship_name"), Projections.include("length")));

        collection.aggregate(Arrays.asList(
                Aggregates.match(Filters.eq("length",new Document("$gt", length))), project
        )).forEach(printBlock);
    }

    private static void rateOfTurnQuery(double doubleRateInput) {

        MongoClient mongoClient = new MongoClient();
        MongoDatabase database = mongoClient.getDatabase("mongoProject");
        MongoCollection<Document> collection = database.getCollection("nariDynamic");

        Block<Document> printBlock = document -> System.out.println(document.toJson());

        Bson project = Aggregates.project(Projections.fields(Projections.excludeId(), Projections.include("location"), Projections.include("trueheading")));

        collection.aggregate(Arrays.asList(
                Aggregates.match(Filters.eq("rateofturn",doubleRateInput)), project
        )).forEach(printBlock);
    }

    private static void speedOverGroundQuery(double doubleSpeedInput) {

        MongoClient mongoClient = new MongoClient();
        MongoDatabase database = mongoClient.getDatabase("mongoProject");
        MongoCollection<Document> collection = database.getCollection("nariDynamic");

        Block<Document> printBlock = document -> System.out.println(document.toJson());

        Bson having =  Aggregates.match(new Document("speedoverground", new Document("$gte", doubleSpeedInput)));
        Bson project = Aggregates.project(Projections.fields(Projections.excludeId(), Projections.include("location"),
                Projections.include("trueheading"), Projections.include("speedoverground")));

        collection.aggregate(Arrays.asList(
            having, project
        )).forEach(printBlock);

    }

    private static void compoundQuery(int intNavInput, double doubleTurnInput) {

        MongoClient mongoClient = new MongoClient();
        MongoDatabase database = mongoClient.getDatabase("mongoProject");
        MongoCollection<Document> collection = database.getCollection("nariDynamic");

        Block<Document> printBlock = document -> System.out.println(document.toJson());

        Bson having = Aggregates.match(new Document("navigationalstatus", intNavInput));
        Bson having2 = Aggregates.match(new Document("rateofturn", new Document("$gte", doubleTurnInput)));
        Bson project = Aggregates.project(Projections.fields(Projections.excludeId(), Projections.include("location"),
                Projections.include("rateofturn"), Projections.include("navigationalstatus")));

        collection.aggregate(Arrays.asList(
                having, having2, project
        )).forEach(printBlock);
    }

    private static void etaQuery(String shipInput) {
        MongoClient mongoClient = new MongoClient();
        MongoDatabase database = mongoClient.getDatabase("mongoProject");
        MongoCollection<Document> collection = database.getCollection("nariStatic");

        Block<Document> printBlock = document -> System.out.println(document.toJson());

        Bson having = Aggregates.match(new Document("shipname", shipInput));
        Bson project = Aggregates.project(Projections.fields(Projections.excludeId(), Projections.include("shipname")
                , Projections.include("eta")));
        Bson limit = Aggregates.limit(1);

        collection.aggregate(Arrays.asList(
                having, project, limit
        )).forEach(printBlock);

    }


    private static void destinationQuery(String destInput, int port ) {
        MongoClient mongoClient = new MongoClient();
        MongoDatabase database = mongoClient.getDatabase("mongoProject");
        MongoCollection<Document> collection = database.getCollection("nariStatic");

        Block<Document> printBlock = document -> System.out.println(document.toJson());

        Bson having = Aggregates.match(new Document("destination", destInput));
        Bson having2 = Aggregates.match(new Document("toport", port));
        Bson group = Aggregates.group("$" + "shipname", Accumulators.first("shipname", "$" + "shipname"));
        Bson project = Aggregates.project(Projections.fields(Projections.excludeId(), Projections.include("shipname")));

        collection.aggregate(Arrays.asList(
                having, having2,group, project
        )).forEach(printBlock);
    }

    private static void draughtQuery(double minDraught, double maxDraught) {
        MongoClient mongoClient = new MongoClient();
        MongoDatabase database = mongoClient.getDatabase("mongoProject");
        MongoCollection<Document> collection = database.getCollection("nariStatic");

        Block<Document> printBlock = document -> System.out.println(document.toJson());

        Bson having = Aggregates.match(new Document("draught", new Document("$gt", maxDraught)));
        Bson having2 = Aggregates.match(new Document("draught", new Document("$lt", minDraught)));
        Bson group = Aggregates.group("$" + "shipname", Accumulators.first("shipname", "$" + "shipname"));
        Bson project = Aggregates.project(Projections.fields(Projections.excludeId(), Projections.include("shipname")));

        collection.aggregate(Arrays.asList(
                having, having2, group, project
        )).forEach(printBlock);
    }

}
