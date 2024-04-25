import gurobipy as grb
import sqlite3
import csv
import time

# Task 1 - Reading data and writing to the database

starttime = time.time()
# ----------------- Creating Database ------------------

conn = sqlite3.connect("../Outputs/Charbrew.db")
sql_cursor = conn.cursor()

# Declaring Input Paths
Plant_path = "../Inputs/plant_data.csv"
dealership_path = "../Inputs/dealership_data.csv"
tarrifs_path = "../Inputs/tariffs.csv"
distance_path = "../Inputs/distance.csv"
Manufacture_costs_path = "../Inputs/manufacture_costs.csv"
railtransportation_path = "../Inputs/rail_transportation_costs.csv"
roadtransportation_path = "../Inputs/road_transportation_costs.csv"
distanceroad_path = "../Inputs/distance_road.csv"
distancerail_path = "../Inputs/distance_rail.csv"
buildoutcost_path = "../Inputs/buildout_cost.csv"

# Declaring List for storing data

Plant = []
dealership = []
tarrifs = []
distance = []
Manufacture_costs = []
railtransportation_cost = []
roadtransportation_cost = []
distanceroad = []
distancerail = []
buildoutcost = []

# Reading the csv files

with open(Plant_path, "r") as Plant_file:
    plant_csv = csv.reader(Plant_file)
    next(plant_csv)
    for rows in plant_csv:
        Plant.append(rows)

with open(dealership_path, "r") as dealership_file:
    dealership_csv = csv.reader(dealership_file)
    next(dealership_csv)
    for rows in dealership_csv:
        dealership.append(rows)

with open(tarrifs_path, "r") as tarrifs_file:
    tarrifs_csv = csv.reader(tarrifs_file)
    next(tarrifs_csv)
    for rows in tarrifs_csv:
        tarrifs.append(rows)

with open(distance_path, "r") as distance_file:
    distance_csv = csv.reader(distance_file)
    next(distance_csv)
    for rows in distance_csv:
        distance.append(rows)

with open(Manufacture_costs_path, "r") as Manufacture_costs_file:
    Manufacture_costs_csv = csv.reader(Manufacture_costs_file)
    next(Manufacture_costs_csv)
    for rows in Manufacture_costs_csv:
        Manufacture_costs.append(rows)

with open(railtransportation_path, "r") as railtransportation_file:
    railtransportation_csv = csv.reader(railtransportation_file)
    next(railtransportation_csv)
    for rows in railtransportation_csv:
        railtransportation_cost.append(rows)

with open(roadtransportation_path, "r") as roadtransportation_file:
    roadtransportation_csv = csv.reader(roadtransportation_file)
    next(roadtransportation_csv)
    for rows in roadtransportation_csv:
        roadtransportation_cost.append(rows)

with open(distanceroad_path, "r") as distanceroad_file:
    distanceroad_csv = csv.reader(distanceroad_file)
    next(distanceroad_csv)
    for rows in distanceroad_csv:
        distanceroad.append(rows)

with open(distancerail_path, "r") as distancerail_file:
    distancerail_csv = csv.reader(distancerail_file)
    next(distancerail_csv)
    for rows in distancerail_csv:
        distancerail.append(rows)

with open(buildoutcost_path, "r") as buildoutcost_file:
    buildoutcost_csv = csv.reader(buildoutcost_file)
    next(buildoutcost_csv)
    for rows in buildoutcost_csv:
        buildoutcost.append(rows)

Plant_Schema = """ CREATE TABLE IF NOT EXISTS Plant (
                                Plant_Code string,
                                Country string,
                                Unicorn float,
                                Rainbow float,
                                Leprechaun float,
                                Penguin float,
                                StarDust float,
                                ShootingStar float,
                                Happiness float,
                                SnowFlake float,
                                Bunny float,
                                Hours_Available float
                                );
                                """

dealership_Schema = """ CREATE TABLE IF NOT EXISTS Dealership (
                                Dealership_ID string,
                                Country string,
                                Unicorn float,
                                Rainbow float,
                                Leprechaun float,
                                Penguin float,
                                StarDust float,
                                ShootingStar float,
                                Happiness float,
                                SnowFlake float,
                                Bunny float
                                );
                                """

Tarrif_Schema = """ CREATE TABLE IF NOT EXISTS Tarrif (
                                Made_In string,
                                Shipped_To string,
                                Model string,
                                Tarrif float
                                );
                                """

Distance_Schema = """ CREATE TABLE IF NOT EXISTS Distance (
                                Plant_Code string,
                                Dealer_Code string,
                                Miles float
                                );
                                """

Manufacture_cost_Schema = """ CREATE TABLE IF NOT EXISTS Manufacture_Cost (
                                Car_Model string,
                                Cost_US float,
                                Cost_MX float,
                                Cost_CA float
                                );
                                """

railtransportation_Cost_Schema = """ CREATE TABLE IF NOT EXISTS railtransportation_Cost (
                                Origin string,
                                To_MX float,
                                To_CA float,
                                To_US float
                                );
                                """

roadtransportation_Cost_Schema = """ CREATE TABLE IF NOT EXISTS roadtransportation_Cost (
                                Origin string,
                                To_MX float,
                                To_CA float,
                                To_US float
                                );
                                """

distanceroad_Schema = """ CREATE TABLE IF NOT EXISTS distanceroad (
                                dealership  string,
                                transfer_center string,
                                distance float
                                );
                                """

distancerail_Schema = """ CREATE TABLE IF NOT EXISTS distancerail (
                                Plant string,
                                Transfer_Center string,
                                Distance float
                                );
                                """

buildoutcost_Schema = """ CREATE TABLE IF NOT EXISTS buildoutcost (
                                transfer_center string,
                                buildout_cost float,
                                Country string
                                );
                                """

sql_cursor.execute(Plant_Schema)
sql_cursor.execute(dealership_Schema)
sql_cursor.execute(Tarrif_Schema)
sql_cursor.execute(Distance_Schema)
sql_cursor.execute(Manufacture_cost_Schema)
sql_cursor.execute(railtransportation_Cost_Schema)
sql_cursor.execute(roadtransportation_Cost_Schema)
sql_cursor.execute(distanceroad_Schema)
sql_cursor.execute(distancerail_Schema)
sql_cursor.execute(buildoutcost_Schema)

# Inserting the data into the Respective Table

# Deleting existing rows if exists
sql_delete = """ DELETE FROM Plant;"""
sql_cursor.execute(sql_delete)
sql_delete = """ DELETE FROM Dealership"""
sql_cursor.execute(sql_delete)
sql_delete = """ DELETE FROM Tarrif"""
sql_cursor.execute(sql_delete)
sql_delete = """ DELETE FROM Distance"""
sql_cursor.execute(sql_delete)
sql_delete = """ DELETE FROM Manufacture_Cost"""
sql_cursor.execute(sql_delete)
sql_delete = """ DELETE FROM railtransportation_Cost"""
sql_cursor.execute(sql_delete)
sql_delete = """ DELETE FROM roadtransportation_Cost"""
sql_cursor.execute(sql_delete)
sql_delete = """ DELETE FROM distanceroad"""
sql_cursor.execute(sql_delete)
sql_delete = """ DELETE FROM distancerail"""
sql_cursor.execute(sql_delete)
sql_delete = """ DELETE FROM buildoutcost"""
sql_cursor.execute(sql_delete)

# Inserting the Data into the sql Table
sql_cursor.executemany('INSERT INTO Plant VALUES(?,?,?,?,?,?,?,?,?,?,?,?);', Plant)
Plant_d = sql_cursor.execute("SELECT * FROM Plant").fetchall()
sql_cursor.executemany('INSERT INTO Dealership VALUES(?,?,?,?,?,?,?,?,?,?,?);', dealership)
Dealership_d = sql_cursor.execute("SELECT * FROM Dealership").fetchall()
sql_cursor.executemany('INSERT INTO Tarrif VALUES(?,?,?,?);', tarrifs)
Tarrif_d = sql_cursor.execute("SELECT * FROM Tarrif").fetchall()
sql_cursor.executemany('INSERT INTO Distance VALUES(?,?,?);', distance)
Distance_d = sql_cursor.execute("SELECT * FROM Distance").fetchall()
sql_cursor.executemany('INSERT INTO Manufacture_Cost VALUES(?,?,?,?);', Manufacture_costs)
ManufactureCost_d = sql_cursor.execute("SELECT * FROM Manufacture_Cost").fetchall()
sql_cursor.executemany('INSERT INTO railtransportation_Cost VALUES(?,?,?,?);', railtransportation_cost)
railtransportationCost_d = sql_cursor.execute("SELECT * FROM railtransportation_Cost").fetchall()
sql_cursor.executemany('INSERT INTO roadtransportation_Cost VALUES(?,?,?,?);', roadtransportation_cost)
roadtransportationCost_d = sql_cursor.execute("SELECT * FROM roadtransportation_Cost").fetchall()
sql_cursor.executemany('INSERT INTO distanceroad VALUES(?,?,?);', distanceroad)
distanceroad_d = sql_cursor.execute("SELECT * FROM distanceroad").fetchall()
sql_cursor.executemany('INSERT INTO distancerail VALUES(?,?,?);', distancerail)
distancerail_d = sql_cursor.execute("SELECT * FROM distancerail").fetchall()
sql_cursor.executemany('INSERT INTO buildoutcost VALUES(?,?,?);', buildoutcost)
buildoutcost_d = sql_cursor.execute("SELECT * FROM buildoutcost").fetchall()

# Commit the changes to Table & close
conn.commit()

# Creating Indices
Plant = []  # Plant Code
for rows in Plant_d:
    Plant.append(rows[0])

Dealership = []  # Dealership
for rows in Dealership_d:
    Dealership.append(rows[0])

Transfer_Center = []  # Transfer Center
for rows in buildoutcost_d:
    Transfer_Center.append(rows[0])

Car_Type = ["Unicorn", "Rainbow", "Leprechaun", "Penguin", "Stardust", "ShootingStar", "Happiness", "Snowflake",
            "Bunny"]

Country_Origin = ["US", "CA", "MX"]  # Country of Origin
Country_Shipped = Country_Origin  # Country Shipped To

# Creating Datasets
Data = {}

# Dataset 1 : Distance.csv
Distance = {}
keys = []
values = []
for rows in Distance_d:
    keys.append((rows[0], rows[1]))
    values.append((rows[2]))

for i in range(len(keys)):
    Distance[keys[i]] = values[i]

# Dataset 2 : Tarrif between Country to Country (Tarrif.csv)
Tarrif = {}
keys = []
values = []
for rows in Tarrif_d:
    keys.append((rows[0], rows[1], rows[2]))
    values.append((rows[3]))

for i in range(len(keys)):
    Tarrif[keys[i]] = values[i]

# Dataset 3 : Manufacture Cost (Manufacturecost.csv)
Manufacture_Cost = {}
keys = []
values = []
for rows in ManufactureCost_d:
    keys.append((rows[0], "US"))
    keys.append((rows[0], "MX"))
    keys.append((rows[0], "CA"))
    values.append((rows[1]))
    values.append((rows[2]))
    values.append((rows[3]))

for i in range(len(keys)):
    Manufacture_Cost[keys[i]] = values[i]

#  Dataset 4 : Plant Data (Plant.csv)
Supply = {}
keys = []
values = []
for rows in Plant_d:
    keys.append(rows[0])
    values.append((rows[11]))

for i in range(len(keys)):
    Supply[keys[i]] = values[i]

# Hours to Manufacture
Manufacture_Time = {}
keys = []
values = []
for rows in Plant_d:
    for cars in Car_Type:
        keys.append((rows[0], cars))
        values.append(rows[2])
        values.append(rows[3])
        values.append(rows[4])
        values.append(rows[5])
        values.append(rows[6])
        values.append(rows[7])
        values.append(rows[8])
        values.append(rows[9])
        values.append(rows[10])

for i in range(len(keys)):
    Manufacture_Time[keys[i]] = values[i]

#  Dataset 5 :  dealership_data.csv

# Demand in Dealership
Demand = {}
keys = []
values = []
for rows in Dealership_d:
    for cars in Car_Type:
        keys.append((rows[0], cars))
        values.append(rows[2])
        values.append(rows[3])
        values.append(rows[4])
        values.append(rows[5])
        values.append(rows[6])
        values.append(rows[7])
        values.append(rows[8])
        values.append(rows[9])
        values.append(rows[10])

for i in range(len(keys)):
    Demand[keys[i]] = values[i]

#  Dataset 6 :  buildout_cost.csv

Buildout_Cost = {}
keys = []
values = []
for rows in buildoutcost_d:
    keys.append((rows[0], rows[2]))
    values.append(rows[1])

for i in range(len(keys)):
    Buildout_Cost[keys[i]] = values[i]

#  Dataset 7 :  distance_rail.csv
Distance_Rail = {}
keys = []
values = []
for rows in distancerail_d:
    keys.append((rows[0], rows[1]))
    values.append((rows[2]))

for i in range(len(keys)):
    Distance_Rail[keys[i]] = values[i]

#  Dataset 8 :  distance_road.csv
Distance_Road = {}
keys = []
values = []
for rows in distanceroad_d:
    keys.append((rows[0], rows[1]))
    values.append((rows[2]))

for i in range(len(keys)):
    Distance_Road[keys[i]] = values[i]

#  Dataset 9 : Rail Transportation Cost
RailTransportation_Cost = {}
keys = []
values = []
for rows in railtransportationCost_d:
    keys.append((rows[0], "MX"))
    keys.append((rows[0], "CA"))
    keys.append((rows[0], "US"))
    values.append((rows[1]))
    values.append((rows[2]))
    values.append((rows[3]))

for i in range(len(keys)):
    RailTransportation_Cost[keys[i]] = values[i]

#  Dataset 10 : Road Transportation Cost
RoadTransportation_Cost = {}
keys = []
values = []
for rows in roadtransportationCost_d:
    keys.append((rows[0], "MX"))
    keys.append((rows[0], "CA"))
    keys.append((rows[0], "US"))
    values.append((rows[1]))
    values.append((rows[2]))
    values.append((rows[3]))

for i in range(len(keys)):
    RoadTransportation_Cost[keys[i]] = values[i]

# Creating the Main Dictionary Dataset
Data["Distance"] = Distance
Data["Tarrif"] = Tarrif
Data["Manufacture_Cost"] = Manufacture_Cost
Data["Supply"] = Supply
Data["Manufacture_Time"] = Manufacture_Time
Data["Demand"] = Demand
Data["Buildout_Cost"] = Buildout_Cost
Data["Distance_Rail"] = Distance_Rail
Data["Distance_Road"] = Distance_Road
Data["RailTransportation_Cost"] = RailTransportation_Cost
Data["RoadTransportation_Cost"] = RoadTransportation_Cost

# Creating the Model
Auto = grb.Model()  # Model Object use this going forward
Auto.modelSense = grb.GRB.MINIMIZE
Auto.update()

#  ---------------------------------- Decision Variable ----------------------------------
X = {}  # Units of Car Type "V" shipped from Plant "P" to Dealership "D"
Y = {}  # Units of Car Type "V" shipped from Plant "P" to Transfer Center "T"
Z = {}  # Units of Car Type "V" shipped from Transfer Center "T" to Dealership "D"
S = {}  # Number of Car in Transfer Center "T"

# Declaring Binary Decision Variable for Transfer Center
Binary_Decision = {}

for t in Transfer_Center:
    Binary_Decision[t] = Auto.addVar(vtype=grb.GRB.BINARY, name=f"U_{t}")

# U = Auto.addVar(vtype=grb.GRB.BINARY, name="U")  # Value of 1 indicates the Transfer Center is to built, 0 otherwise
Auto.update()

# Decision Variable for whether Transfer Center is open or not


# 1 :  Supply

for t in Transfer_Center:
    for v in Car_Type:
        S[t, v] = Auto.addVar(vtype=grb.GRB.CONTINUOUS, name='S{0}{1}'.format(t, v))

# ---------------------------------- Writing Objective Function ----------------------------------

# 1 :  Manufacture Cost
for p in Plant:
    for d in Dealership:
        for v in Car_Type:
            for co in Country_Origin:
                X[p, d, v] = Auto.addVar(obj=Data["Manufacture_Cost"][v, co], vtype=grb.GRB.CONTINUOUS,
                                         name='X{0}_{1}_{2}'.format(p, d, v))

for p in Plant:
    for t in Transfer_Center:
        for v in Car_Type:
            for co in Country_Origin:
                Y[p, t, v] = Auto.addVar(obj=Data["Manufacture_Cost"][v, co], vtype=grb.GRB.CONTINUOUS,
                                         name='Y{0}_{1}_{2}'.format(p, t, v))

Auto.update()

# 2 :  BuildOut Cost
Buildout = {}
for t, cs in Data["Buildout_Cost"].keys():
    Buildout[t] = Auto.addVar(obj=(Data["Buildout_Cost"][t, cs]), vtype=grb.GRB.BINARY,
                              name=f'Cost_{t}')
Auto.update()

# 3 :  Tarrif

for p in Plant:
    for d in Dealership:
        for v in Car_Type:
            for co in Country_Origin:
                for cs in Country_Shipped:
                    X[p, d, v] = Auto.addVar(obj=Data["Tarrif"][co, cs, v], vtype=grb.GRB.CONTINUOUS,
                                             name='X{0}_{1}_{2}'.format(p, d, v))

for p in Plant:
    for t in Transfer_Center:
        for v in Car_Type:
            for co in Country_Origin:
                for cs in Country_Shipped:
                    Y[p, t, v] = Auto.addVar(obj=Data["Tarrif"][co, cs, v], vtype=grb.GRB.CONTINUOUS,
                                             name='Y{0}_{1}_{2}'.format(p, t, v))

# 3 :  Transportation Cost (by road and rail)
for p in Plant:
    for d in Dealership:
        for v in Car_Type:
            for co in Country_Origin:
                for cs in Country_Shipped:
                    X[p, d, v] = Auto.addVar(obj=Data["RoadTransportation_Cost"][co, cs], vtype=grb.GRB.CONTINUOUS,
                                             name='X{0}_{1}_{2}'.format(p, d, v))

for p in Plant:
    for t in Transfer_Center:
        for v in Car_Type:
            for co in Country_Origin:
                for cs in Country_Shipped:
                    Y[p, t, v] = Auto.addVar(obj=Data["RailTransportation_Cost"][co, cs], vtype=grb.GRB.CONTINUOUS,
                                             name='X{0}_{1}_{2}'.format(p, t, v))

for t in Transfer_Center:
    for d in Dealership:
        for v in Car_Type:
            for co in Country_Origin:
                for cs in Country_Shipped:
                    Z[t, d, v] = Auto.addVar(obj=Data["RoadTransportation_Cost"][co, cs], vtype=grb.GRB.CONTINUOUS,
                                             name='X{0}_{1}_{2}'.format(t, d, v))

# Subject To Constraints
my_constrnts = {}  # Declaring Constraints as a Dictionary

# Constarint 1 : Manufacture Hours at each plant
for p in Plant:
    my_name = "Supply"
    my_constrnts[my_name] = Auto.addConstr(
        grb.quicksum((
                             X[p, d, v] + Y[p, t, v]) * Data["Manufacture_Time"][p, v] for t in Transfer_Center for d in
                     Dealership for v
                     in Car_Type) <=
        Data["Supply"][p], name=my_name)
Auto.update()

# Constarint 2 : Supply and Demand at Transfer Center and Dealership

for t in Transfer_Center:
    for v in Car_Type:
        my_name = "Supply_at_Tranfer_Center"
        my_constrnts[my_name] = Auto.addConstr(grb.quicksum(Y[p, t, v] for p in Plant) == S[t, v],
                                               name=my_name)

for t in Transfer_Center:
    my_name = "Supply_Lessthan or eqaul to Demand"
    my_constrnts[my_name] = Auto.addConstr(grb.quicksum(S[t, v] for v in Car_Type) <= (78000 * Binary_Decision[t]),
                                           name=my_name)  # BIG M =78000 (Sum of demand across dealer and Cartype)

for t in Transfer_Center:
    for v in Car_Type:
        my_name = "Dealer can draw no more vehicles than supply at T"
        my_constrnts[my_name] = Auto.addConstr(grb.quicksum(Z[t, d, v] for d in Dealership) <= S[t, v],
                                               name=my_name)

Auto.update()

# Constarint 3 : Non Negative Constraint
my_name = "Non-Negative"
my_constrnts[my_name] = Auto.addConstr(
    grb.quicksum(X[p, d, v] for p in Plant for v in Car_Type for d in Dealership) >= 0, name=my_name)

my_name = "Non-Negative"
my_constrnts[my_name] = Auto.addConstr(
    grb.quicksum(Y[p, t, v] for p in Plant for v in Car_Type for t in Transfer_Center) >= 0, name=my_name)

my_name = "Non-Negative"
my_constrnts[my_name] = Auto.addConstr(
    grb.quicksum(Z[t, d, v] for d in Dealership for v in Car_Type for t in Transfer_Center) >= 0, name=my_name)

Auto.update()
Auto.write("../Outputs/Auto.lp")
Auto.optimize()
Auto.write('../Outputs/Auto.sol')

# saving results in database

if Auto.Status == grb.GRB.OPTIMAL:
    conn = sqlite3.connect('../Outputs/Charbrew.db')
    cursor = conn.cursor()
    Variable_X = []
    Variable_Y = []
    Variable_Z = []
    for v in X:
        if X[v].x > 0:
            a = (v[0], v[1], v[2], X[v].x)
            Variable_X.append(a)

    for v in Y:
        if Y[v].x > 0:
            a = (v[0], v[1], v[2], Y[v].x)
            Variable_X.append(a)

    for v in Z:
        if Z[v].x > 0:
            a = (v[0], v[1], v[2], Z[v].x)
            Variable_X.append(a)

sql_cursor.execute(
    'CREATE TABLE IF NOT EXISTS Car_Plant_Dealer(Plant_ID text, Dealer_ID text, Type_of_Car text, No_of_Cars float)')
sql_cursor.execute(
    'CREATE TABLE IF NOT EXISTS Plant_Transfer_Center(Plant_ID text, Transfer_Center text, Type_of_Car text, No_of_Cars float)')
sql_cursor.execute(
    'CREATE TABLE IF NOT EXISTS TransferCenter_Dealer(Transfer_Center text, Dealer_ID text, Type_of_Car text, No_of_Cars float)')

# Deleting existing rows if exists
sql_delete = """ DELETE FROM Car_Plant_Dealer"""
sql_cursor.execute(sql_delete)

sql_delete = """ DELETE FROM Plant_Transfer_Center"""
sql_cursor.execute(sql_delete)

sql_delete = """ DELETE FROM TransferCenter_Dealer"""
sql_cursor.execute(sql_delete)


sql_cursor.executemany('INSERT INTO Car_Plant_Dealer VALUES(?,?,?,?)', Variable_X)
sql_cursor.executemany('INSERT INTO Plant_Transfer_Center VALUES(?,?,?,?)', Variable_Y)
sql_cursor.executemany('INSERT INTO TransferCenter_Dealer VALUES(?,?,?,?)', Variable_Z)

conn.commit()
conn.close()
